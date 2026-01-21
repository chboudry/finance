#!/usr/bin/env python3
"""Transform *_Trans.csv into Neo4j admin-import friendly CSVs (schema F2).

Matches the schema in ingestion/cypher_load_csv/transactions.cypher:
  - (:Transaction { transaction_id, timestamp, timestamp_date, from_bank, from_account,
					to_bank, to_aAccount, amount_received, receiving_currency,
					amount_paid, payment_currency, payment_format, is_laundering })
	- (a:Account)-[:FROM]->(t:Transaction)
	- (t:Transaction)-[:TO]->(a:Account)

Important:
- No ':LABEL' columns are emitted.
- No ':TYPE' columns are emitted (relationship types are provided via neo4j-admin CLI).

Usage (from repo root):
  python3 ingestion/data_importer/transform_f2.py \
	--input dataset/LI-Small_Trans.csv \
	--out-dir datatransformed

Optional:
	--split-by-date true|false

When --split-by-date is true (default), output files are split by day extracted from `Timestamp`:
	- YYYY_MM_DD_transactions.csv
	- YYYY_MM_DD_transactions_from.csv
	- YYYY_MM_DD_transactions_to.csv

When --split-by-date is false, output files are not split:
	- transactions.csv
	- transactions_from.csv
	- transactions_to.csv
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


EXPECTED_HEADERS = [
	"Timestamp",
	"From Bank",
	"FromAccount",
	"To Bank",
	"ToAccount",
	"Amount Received",
	"Receiving Currency",
	"Amount Paid",
	"Payment Currency",
	"Payment Format",
	"Is Laundering",
]

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
	"""Stream dict rows into a Parquet file using pyarrow."""

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


def _prepare_fieldnames(
	base_fieldnames: list[str], output_format: str
) -> tuple[list[str], dict[str, str]]:
	fieldnames: list[str] = []
	mapping: dict[str, str] = {}
	for name in base_fieldnames:
		adjusted = _admin_header_for_format(name, output_format)
		fieldnames.append(adjusted)
		mapping[name] = adjusted
	return fieldnames, mapping


def _validate_headers(actual: Iterable[str]) -> None:
	actual_list = list(actual)
	if actual_list != EXPECTED_HEADERS:
		raise ValueError(
			"Unexpected CSV headers.\n"
			f"Expected: {EXPECTED_HEADERS}\n"
			f"Actual:   {actual_list}"
		)


def _parse_timestamp_date(timestamp_value: str) -> str:
	"""Convert 'YYYY/MM/DD HH:MM' to ISO-8601 'YYYY-MM-DDTHH:MM:SS'.

	Returns empty string if parsing fails.
	"""

	value = (timestamp_value or "").strip()
	if not value:
		return ""
	try:
		dt = datetime.strptime(value, "%Y/%m/%d %H:%M")
	except ValueError:
		return ""
	return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _to_int_string(value: str) -> str:
	v = (value or "").strip()
	if not v:
		return ""
	try:
		return str(int(v))
	except ValueError:
		return ""


def _to_float_string(value: str) -> str:
	v = (value or "").strip()
	if not v:
		return ""
	try:
		return str(float(v))
	except ValueError:
		return ""


def _to_bool_string_is_laundering(value: str) -> str:
	return "true" if (value or "").strip() == "1" else "false"


def _day_key_from_timestamp(timestamp_value: str) -> str:
	"""Extract day key from 'YYYY/MM/DD HH:MM' as 'YYYY_MM_DD'.

	Returns 'unknown' if it cannot be parsed.
	"""

	value = (timestamp_value or "").strip()
	if len(value) < 10:
		return "unknown"
	date_part = value[:10]
	try:
		# Validate the date portion.
		datetime.strptime(date_part, "%Y/%m/%d")
	except ValueError:
		return "unknown"
	return date_part.replace("/", "_")


def _parse_bool(value: str) -> bool:
	v = (value or "").strip().lower()
	if v in {"true", "1", "yes", "y", "t"}:
		return True
	if v in {"false", "0", "no", "n", "f"}:
		return False
	raise argparse.ArgumentTypeError("Expected a boolean value: true/false")


def transform(input_csv: Path, out_dir: Path, split_by_date: bool, output_format: str = "csv") -> None:
	if output_format not in OUTPUT_FORMATS:
		raise ValueError(f"Unsupported output format: {output_format}")

	transaction_base_fieldnames = [
		"transaction_id:ID(Transaction){label:Transaction}",
		"timestamp",
		"timestamp_date:datetime",
		"from_bank:int",
		"from_account",
		"to_bank:int",
		"to_aAccount",
		"amount_received:float",
		"receiving_currency",
		"amount_paid:float",
		"payment_currency",
		"payment_format",
		"is_laundering:boolean",
	]
	transaction_fieldnames, transaction_columns = _prepare_fieldnames(
		transaction_base_fieldnames, output_format
	)
	transaction_id_column = transaction_columns[
		"transaction_id:ID(Transaction){label:Transaction}"
	]

	from_rel_base_fieldnames = [":START_ID(Account)", ":END_ID(Transaction)"]
	from_rel_fieldnames, from_columns = _prepare_fieldnames(
		from_rel_base_fieldnames, output_format
	)
	from_start_column = from_columns[":START_ID(Account)"]
	from_end_column = from_columns[":END_ID(Transaction)"]

	to_rel_base_fieldnames = [":START_ID(Transaction)", ":END_ID(Account)"]
	to_rel_fieldnames, to_columns = _prepare_fieldnames(
		to_rel_base_fieldnames, output_format
	)
	to_start_column = to_columns[":START_ID(Transaction)"]
	to_end_column = to_columns[":END_ID(Account)"]

	tx_writers: dict[str, Any] = {}
	tx_resources: dict[str, object] = {}

	from_writers: dict[str, Any] = {}
	from_resources: dict[str, object] = {}

	to_writers: dict[str, Any] = {}
	to_resources: dict[str, object] = {}

	if not split_by_date:
		tx_path = _path_for_format(out_dir / "transactions.csv", output_format)
		tx_writer, tx_res = _open_writer(tx_path, transaction_fieldnames, output_format)
		from_path = _path_for_format(out_dir / "transactions_from.csv", output_format)
		from_writer, from_res = _open_writer(from_path, from_rel_fieldnames, output_format)
		to_path = _path_for_format(out_dir / "transactions_to.csv", output_format)
		to_writer, to_res = _open_writer(to_path, to_rel_fieldnames, output_format)

		tx_writers["all"] = tx_writer
		tx_resources["all"] = tx_res
		from_writers["all"] = from_writer
		from_resources["all"] = from_res
		to_writers["all"] = to_writer
		to_resources["all"] = to_res

	try:
		with input_csv.open("r", newline="", encoding="utf-8") as f:
			reader = csv.DictReader(f)
			if reader.fieldnames is None:
				raise ValueError("Input CSV has no header row")
			_validate_headers(reader.fieldnames)

			# Match transactions.cypher semantics: txId uses the CSV line number.
			# With HEADERS, the first data row is line 2.
			for tx_id, row in enumerate(reader, start=2):
				timestamp = (row.get("Timestamp") or "").strip()
				day_key = _day_key_from_timestamp(timestamp) if split_by_date else "all"
				from_bank = _to_int_string(row.get("From Bank") or "")
				from_account = (row.get("FromAccount") or "").strip()
				to_bank = _to_int_string(row.get("To Bank") or "")
				to_account = (row.get("ToAccount") or "").strip()
				amount_received = _to_float_string(row.get("Amount Received") or "")
				receiving_currency = (row.get("Receiving Currency") or "").strip()
				amount_paid = _to_float_string(row.get("Amount Paid") or "")
				payment_currency = (row.get("Payment Currency") or "").strip()
				payment_format = (row.get("Payment Format") or "").strip()
				is_laundering = _to_bool_string_is_laundering(row.get("Is Laundering") or "")

				tx_id_str = str(tx_id)

				if day_key not in tx_writers:
					transactions_path = _path_for_format(
						out_dir / f"{day_key}_transactions.csv", output_format
					)
					tx_writer, tx_res = _open_writer(transactions_path, transaction_fieldnames, output_format)
					tx_writers[day_key] = tx_writer
					tx_resources[day_key] = tx_res

				tx_writers[day_key].writerow(
					{
						transaction_id_column: tx_id_str,
						transaction_columns["timestamp"]: timestamp,
						transaction_columns["timestamp_date:datetime"]: _parse_timestamp_date(timestamp),
						transaction_columns["from_bank:int"]: from_bank,
						transaction_columns["from_account"]: from_account,
						transaction_columns["to_bank:int"]: to_bank,
						# Keep the property name exactly as in transactions.cypher.
						transaction_columns["to_aAccount"]: to_account,
						transaction_columns["amount_received:float"]: amount_received,
						transaction_columns["receiving_currency"]: receiving_currency,
						transaction_columns["amount_paid:float"]: amount_paid,
						transaction_columns["payment_currency"]: payment_currency,
						transaction_columns["payment_format"]: payment_format,
						transaction_columns["is_laundering:boolean"]: is_laundering,
					}
				)

				if from_account:
					if day_key not in from_writers:
						from_path = _path_for_format(
							out_dir / f"{day_key}_transactions_from.csv", output_format
						)
						from_writer, from_res = _open_writer(from_path, from_rel_fieldnames, output_format)
						from_writers[day_key] = from_writer
						from_resources[day_key] = from_res
					from_writers[day_key].writerow(
						{from_start_column: from_account, from_end_column: tx_id_str}
					)
				if to_account:
					if day_key not in to_writers:
						# Keep the filename exactly as requested: DATE_transactions_to.csv
						to_path = _path_for_format(
							out_dir / f"{day_key}_transactions_to.csv", output_format
						)
						to_writer, to_res = _open_writer(to_path, to_rel_fieldnames, output_format)
						to_writers[day_key] = to_writer
						to_resources[day_key] = to_res
					to_writers[day_key].writerow(
						{to_start_column: tx_id_str, to_end_column: to_account}
					)

	finally:
		for resource in tx_resources.values():
			resource.close()
		for resource in from_resources.values():
			resource.close()
		for resource in to_resources.values():
			resource.close()


def main() -> None:
	repo_root = Path(__file__).resolve().parents[2]

	parser = argparse.ArgumentParser(
		description="Transform *_Trans.csv into Neo4j admin-import CSVs (transactions)."
	)
	parser.add_argument(
		"--input",
		type=Path,
		default=repo_root / "dataset" / "LI-Small_Trans.csv",
		help="Path to *_Trans.csv (expects FromAccount/ToAccount headers)",
	)
	parser.add_argument(
		"--out-dir",
		type=Path,
		default=repo_root / "datatransformed",
		help="Output directory for generated CSVs",
	)
	parser.add_argument(
		"--split-by-date",
		type=_parse_bool,
		default=False,
		help="Whether to split output files by day (true/false). Default: false",
	)
	parser.add_argument(
		"--output-format",
		choices=sorted(OUTPUT_FORMATS),
		default="csv",
		help="Output format for generated files (csv or parquet). Default: csv",
	)

	args = parser.parse_args()

	if not args.input.exists():
		raise SystemExit(f"Input file not found: {args.input}")

	transform(
		input_csv=args.input,
		out_dir=args.out_dir,
		split_by_date=args.split_by_date,
		output_format=args.output_format,
	)


if __name__ == "__main__":
	main()

