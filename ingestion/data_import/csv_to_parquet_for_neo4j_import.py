#!/usr/bin/env python3
"""Convert datatransformed CSV files to Parquet for neo4j-admin import.

Neo4j (your version) supports `neo4j-admin database import full --input-type=parquet`.
This script converts one or many CSV files to Parquet while preserving the exact
column names that neo4j-admin relies on (e.g. 'bank_id:ID(Bank){label:Bank}', ':START_ID(Account)',
'amount_paid:float', 'timestamp_date:datetime').

Why:
- CSV works, but Parquet can reduce disk footprint and improve read performance.
- We keep the Neo4j header semantics in the column names.

Requirements:
- Python 3
- `pyarrow` installed (this script will exit with instructions if missing)

Usage (from repo root):
  python3 ingestion/data_importer/csv_to_parquet_for_neo4j_import.py \
    --input-dir datatransformed \
    --out-dir datatransformed_parquet

Then import with neo4j-admin:
  neo4j-admin database import full finance \
    --input-type=parquet \
    --id-type=string \
    --nodes=Bank=datatransformed_parquet/banks.parquet \
    ...

Notes:
- Conversion is streaming/batched to handle large files.
- ID columns are kept as strings (do not coerce), to stay consistent with `--id-type=string`.
- Non-ID property columns with suffixes ':int', ':float', ':boolean' are coerced when safe.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable


def _require_pyarrow():
    try:
        import pyarrow as pa  # noqa: F401
        import pyarrow.csv as pacsv  # noqa: F401
        import pyarrow.parquet as pq  # noqa: F401

        return True
    except Exception:
        return False


def _iter_csv_files(input_dir: Path, patterns: list[str]) -> Iterable[Path]:
    seen: set[Path] = set()
    for pattern in patterns:
        for p in sorted(input_dir.glob(pattern)):
            if p.is_file() and p.suffix.lower() == ".csv" and p not in seen:
                seen.add(p)
                yield p


def _is_id_column(col_name: str) -> bool:
    # Neo4j header conventions for IDs, ex:
    # - bank_id:ID(Bank)
    # - :START_ID(Account)
    # - :END_ID(Transaction)
    return ":ID(" in col_name or col_name.startswith(":START_ID") or col_name.startswith(":END_ID")


def _infer_target_type(col_name: str):
    # Only use suffix-based coercion. Keep IDs as strings.
    if _is_id_column(col_name):
        return None

    lower = col_name.lower()
    if lower.endswith(":int"):
        return "int"
    if lower.endswith(":float"):
        return "float"
    if lower.endswith(":boolean"):
        return "bool"
    # Keep datetime as string by default (Neo4j can parse based on header).
    # If you want physical timestamp in Parquet, adjust here.
    return None


def _coerce_table(table, column_names: list[str]):
    import pyarrow as pa
    import pyarrow.compute as pc

    arrays = []
    for name in column_names:
        arr = table[name]
        target = _infer_target_type(name)
        if target is None:
            arrays.append(arr)
            continue

        # CSV reader yields string by default in our configuration. Convert safely:
        # - empty strings -> null
        # - invalid -> null
        as_str = pc.cast(arr, pa.string())
        cleaned = pc.if_else(pc.equal(as_str, ""), None, as_str)

        if target == "int":
            arrays.append(pc.cast(cleaned, pa.int64(), safe=False))
        elif target == "float":
            arrays.append(pc.cast(cleaned, pa.float64(), safe=False))
        elif target == "bool":
            # Accept true/false (lowercase) already produced by transform_f2.
            # Anything else becomes null.
            lowered = pc.utf8_lower(cleaned)
            is_true = pc.equal(lowered, "true")
            is_false = pc.equal(lowered, "false")
            arrays.append(pc.if_else(is_true, True, pc.if_else(is_false, False, None)))
        else:
            arrays.append(arr)

    return pa.Table.from_arrays(arrays, names=column_names)


def convert_one(csv_path: Path, out_path: Path, batch_size: int) -> None:
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    out_path.parent.mkdir(parents=True, exist_ok=True)

    read_options = pacsv.ReadOptions(block_size=batch_size)
    parse_options = pacsv.ParseOptions(newlines_in_values=False)

    # Read all columns as strings to preserve Neo4j headers and avoid accidental ID coercion.
    convert_options = pacsv.ConvertOptions(column_types=None, strings_can_be_null=True)

    reader = pacsv.open_csv(
        csv_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )

    schema = None
    writer = None

    try:
        for batch in reader:
            table = pa.Table.from_batches([batch])
            table = _coerce_table(table, table.column_names)

            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(out_path, schema=schema, compression="snappy")

            writer.write_table(table)

    finally:
        if writer is not None:
            writer.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Neo4j-import CSVs (datatransformed) to Parquet for neo4j-admin."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("datatransformed"),
        help="Directory containing CSV files (default: datatransformed)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("datatransformed_parquet"),
        help="Directory to write Parquet files (default: datatransformed_parquet)",
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        default=["*.csv"],
        help="One or more glob patterns to select CSVs (default: *.csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1 << 20,
        help="Read block size in bytes for streaming CSV parsing (default: 1048576)",
    )

    args = parser.parse_args()

    if not _require_pyarrow():
        raise SystemExit(
            "Missing dependency: pyarrow\n"
            "Install it with: python3 -m pip install --upgrade pyarrow\n"
        )

    input_dir: Path = args.input_dir
    out_dir: Path = args.out_dir

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input dir not found: {input_dir}")

    csv_files = list(_iter_csv_files(input_dir, args.patterns))
    if not csv_files:
        raise SystemExit(f"No CSV files found in {input_dir} for patterns: {args.patterns}")

    for csv_path in csv_files:
        out_path = out_dir / (csv_path.stem + ".parquet")
        print(f"Converting {csv_path} -> {out_path}")
        convert_one(csv_path=csv_path, out_path=out_path, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
