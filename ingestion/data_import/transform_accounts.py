#!/usr/bin/env python3
"""
Input (expected headers):
  Bank Name,Bank ID,Account Number,Entity ID,Entity Name

Outputs (CSV) are generated for Neo4j Data Importer using neo4j-admin import style
headers (e.g. ':ID', ':START_ID', ':END_ID').

By default, this script writes 3 node files and 2 relationship files matching
ingestion/cypher_load_csv/accounts.cypher:
    - banks.csv                (:Bank nodes; bank_id, bank_name)
    - entities.csv             (:Entity nodes; entity_id, entity_name)
    - accounts.csv             (:Account nodes; account_number)
    - entity_owns_account.csv  (Entity)-[:OWNS]->(Account)
    - account_part_of_bank.csv (Account)-[:PART_OF]->(Bank)

Usage:
  python ingestion/transform_li_small_accounts_for_neo4j_importer.py \
    --input dataset/LI-Small_accounts.csv \
    --out-dir datatransformed/li-small-neo4j-importer

Notes:
- No external dependencies (streams through the input file).
- Dedupe is done in-memory for Bank ID and Entity ID.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Iterable


EXPECTED_HEADERS = ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"]
OUTPUT_FORMATS = {"csv", "parquet"}


def _require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime dependency check
        raise SystemExit(
            "Missing dependency: pyarrow\nInstall it with: python3 -m pip install --upgrade pyarrow"
        ) from exc
    return pa, pq


class _ParquetDictWriter:
    """Small helper to stream rows into a Parquet file using pyarrow."""

    def __init__(self, path: Path, fieldnames: list[str], chunk_size: int = 5000) -> None:
        pa, pq = _require_pyarrow()
        self._pa = pa
        self._pq = pq
        self._path = path
        self._fieldnames = fieldnames
        self._chunk_size = max(chunk_size, 1)
        self._buffer: list[dict[str, str]] = []
        self._writer: Any | None = None
        self._schema = self._pa.schema(
            [self._pa.field(name, self._pa.string()) for name in self._fieldnames]
        )
        self._closed = False
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def writerow(self, row: dict[str, str]) -> None:
        normalized = {name: (row.get(name) or "") for name in self._fieldnames}
        self._buffer.append(normalized)
        if len(self._buffer) >= self._chunk_size:
            self._flush()

    def close(self) -> None:
        if self._closed:
            return
        if self._buffer:
            self._flush()
        elif self._writer is None:
            empty_arrays = [self._pa.array([], type=self._pa.string()) for _ in self._fieldnames]
            empty_table = self._pa.Table.from_arrays(empty_arrays, names=self._fieldnames)
            self._writer = self._pq.ParquetWriter(
                self._path, schema=self._schema, compression="snappy"
            )
            self._writer.write_table(empty_table)
        if self._writer is not None:
            self._writer.close()
        self._closed = True

    def _flush(self) -> None:
        if not self._buffer:
            return
        arrays = [
            self._pa.array([row[name] for row in self._buffer], type=self._pa.string())
            for name in self._fieldnames
        ]
        table = self._pa.Table.from_arrays(arrays, names=self._fieldnames)
        if self._writer is None:
            self._writer = self._pq.ParquetWriter(
                self._path, schema=self._schema, compression="snappy"
            )
        self._writer.write_table(table)
        self._buffer.clear()


def _open_csv_writer(path: Path, fieldnames: list[str]) -> tuple[csv.DictWriter, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open("w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    return writer, f


def _open_parquet_writer(path: Path, fieldnames: list[str]) -> tuple[_ParquetDictWriter, object]:
    writer = _ParquetDictWriter(path, fieldnames)
    return writer, writer


def _open_writer(path: Path, fieldnames: list[str], output_format: str):
    if output_format == "csv":
        return _open_csv_writer(path, fieldnames)
    if output_format == "parquet":
        return _open_parquet_writer(path, fieldnames)
    raise ValueError(f"Unsupported output format: {output_format}")


def _path_for_format(path: Path, output_format: str) -> Path:
    suffix = ".csv" if output_format == "csv" else ".parquet"
    return path.with_suffix(suffix)


def _admin_header_for_format(header: str, output_format: str) -> str:
    if output_format != "parquet":
        return header
    if ":ID(" in header:
        prefix = header.split(":ID(", 1)[0]
        return f"{prefix}:ID"
    if header.startswith(":START_ID("):
        return ":START_ID"
    if header.startswith(":END_ID("):
        return ":END_ID"
    return header


def _validate_headers(actual: Iterable[str]) -> None:
    actual_list = list(actual)
    if actual_list != EXPECTED_HEADERS:
        raise ValueError(
            "Unexpected CSV headers.\n"
            f"Expected: {EXPECTED_HEADERS}\n"
            f"Actual:   {actual_list}"  # keep exact to help debugging
        )


def transform(input_csv: Path, out_dir: Path, output_format: str = "csv") -> None:
    if output_format not in OUTPUT_FORMATS:
        raise ValueError(f"Unsupported output format: {output_format}")

    banks_path = _path_for_format(out_dir / "banks.csv", output_format)
    entities_path = _path_for_format(out_dir / "entities.csv", output_format)
    accounts_path = _path_for_format(out_dir / "accounts.csv", output_format)
    account_part_of_bank_path = _path_for_format(
        out_dir / "account_part_of_bank.csv", output_format
    )
    entity_owns_account_path = _path_for_format(
        out_dir / "entity_owns_account.csv", output_format
    )

    bank_id_field = _admin_header_for_format("bank_id:ID(Bank){label:Bank}", output_format)
    entity_id_field = _admin_header_for_format("entity_id:ID(Entity){label:Entity}", output_format)
    account_id_field = _admin_header_for_format("account_number:ID(Account){label:Account}", output_format)

    entity_account_start_field = _admin_header_for_format(":START_ID(Entity)", output_format)
    entity_account_end_field = _admin_header_for_format(":END_ID(Account)", output_format)
    account_bank_start_field = _admin_header_for_format(":START_ID(Account)", output_format)
    account_bank_end_field = _admin_header_for_format(":END_ID(Bank)", output_format)

    bank_fieldnames = [bank_id_field, "bank_name"]
    entity_fieldnames = [entity_id_field, "entity_name"]
    account_fieldnames = [account_id_field]

    entity_account_rel_fieldnames = [entity_account_start_field, entity_account_end_field]
    account_bank_rel_fieldnames = [account_bank_start_field, account_bank_end_field]

    bank_writer, bank_resource = _open_writer(banks_path, bank_fieldnames, output_format)
    entity_writer, entity_resource = _open_writer(
        entities_path, entity_fieldnames, output_format
    )
    account_writer, account_resource = _open_writer(
        accounts_path, account_fieldnames, output_format
    )
    account_bank_writer, account_bank_resource = _open_writer(
        account_part_of_bank_path, account_bank_rel_fieldnames, output_format
    )
    entity_account_writer, entity_account_resource = _open_writer(
        entity_owns_account_path, entity_account_rel_fieldnames, output_format
    )

    seen_bank_ids: set[str] = set()
    seen_entity_ids: set[str] = set()
    seen_account_numbers: set[str] = set()

    try:
        with input_csv.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError("Input CSV has no header row")
            _validate_headers(reader.fieldnames)

            for row in reader:
                bank_name = (row.get("Bank Name") or "").strip()
                bank_id = (row.get("Bank ID") or "").strip()
                account_number = (row.get("Account Number") or "").strip()
                entity_id = (row.get("Entity ID") or "").strip()
                entity_name = (row.get("Entity Name") or "").strip()

                if not bank_id or not account_number or not entity_id:
                    # Skip incomplete rows rather than generating invalid IDs
                    continue

                if bank_id not in seen_bank_ids:
                    seen_bank_ids.add(bank_id)
                    bank_writer.writerow({bank_id_field: bank_id, "bank_name": bank_name})

                if entity_id not in seen_entity_ids:
                    seen_entity_ids.add(entity_id)
                    entity_writer.writerow(
                        {
                            entity_id_field: entity_id,
                            "entity_name": entity_name,
                        }
                    )

                if account_number not in seen_account_numbers:
                    seen_account_numbers.add(account_number)
                    account_writer.writerow({account_id_field: account_number})

                entity_account_writer.writerow(
                    {
                        entity_account_start_field: entity_id,
                        entity_account_end_field: account_number,
                    }
                )

                account_bank_writer.writerow(
                    {
                        account_bank_start_field: account_number,
                        account_bank_end_field: bank_id,
                    }
                )

    finally:
        bank_resource.close()
        entity_resource.close()
        account_resource.close()
        account_bank_resource.close()
        entity_account_resource.close()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Transform LI-Small_accounts.csv into Neo4j Data Importer files."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=repo_root / "dataset" / "LI-Small_accounts.csv",
        help="Path to LI-Small_accounts.csv",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=repo_root / "datatransformed" / "li-small-neo4j-importer",
        help="Output directory for generated CSVs",
    )

    parser.add_argument(
        "--output-format",
        choices=sorted(OUTPUT_FORMATS),
        default="csv",
        help="Output format for generated files (csv or parquet). Default: csv",
    )

    args = parser.parse_args()

    input_csv = args.input
    out_dir = args.out_dir
    output_format = args.output_format

    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")

    transform(input_csv=input_csv, out_dir=out_dir, output_format=output_format)


if __name__ == "__main__":
    main()
