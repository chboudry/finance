#!/usr/bin/env python3
"""Transform *_Trans.csv into Neo4j admin-import friendly CSVs (schema F2).

Matches the schema in ingestion/cypher_load_csv/transactions.cypher:
  - (:Transaction { transaction_id, timestamp, timestamp_date, from_bank, from_account,
					to_bank, to_aAccount, amount_received, receiving_currency,
					amount_paid, payment_currency, payment_format, is_laundering })
  - (t:Transaction)-[:FROM]->(a:Account)
  - (t:Transaction)-[:TO]->(a:Account)

Important:
- No ':LABEL' columns are emitted.
- No ':TYPE' columns are emitted (relationship types are provided via neo4j-admin CLI).

Usage (from repo root):
  python3 ingestion/data_importer/transform_f2.py \
	--input dataset/LI-Small_Trans.csv \
	--out-dir datatransformed
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable


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


def _open_csv_writer(path: Path, fieldnames: list[str]) -> tuple[csv.DictWriter, object]:
	path.parent.mkdir(parents=True, exist_ok=True)
	f = path.open("w", newline="", encoding="utf-8")
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()
	return writer, f


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


def transform(input_csv: Path, out_dir: Path) -> None:
	transactions_path = out_dir / "transactions.csv"
	transaction_from_path = out_dir / "transaction_from.csv"
	transaction_to_path = out_dir / "transaction_to.csv"

	transaction_fieldnames = [
		"transaction_id:ID(Transaction)",
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

	from_rel_fieldnames = [":START_ID(Transaction)", ":END_ID(Account)"]
	to_rel_fieldnames = [":START_ID(Transaction)", ":END_ID(Account)"]

	tx_writer, tx_f = _open_csv_writer(transactions_path, transaction_fieldnames)
	from_writer, from_f = _open_csv_writer(transaction_from_path, from_rel_fieldnames)
	to_writer, to_f = _open_csv_writer(transaction_to_path, to_rel_fieldnames)

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

				tx_writer.writerow(
					{
						"transaction_id:ID(Transaction)": tx_id_str,
						"timestamp": timestamp,
						"timestamp_date:datetime": _parse_timestamp_date(timestamp),
						"from_bank:int": from_bank,
						"from_account": from_account,
						"to_bank:int": to_bank,
						# Keep the property name exactly as in transactions.cypher.
						"to_aAccount": to_account,
						"amount_received:float": amount_received,
						"receiving_currency": receiving_currency,
						"amount_paid:float": amount_paid,
						"payment_currency": payment_currency,
						"payment_format": payment_format,
						"is_laundering:boolean": is_laundering,
					}
				)

				if from_account:
					from_writer.writerow(
						{":START_ID(Transaction)": tx_id_str, ":END_ID(Account)": from_account}
					)
				if to_account:
					to_writer.writerow(
						{":START_ID(Transaction)": tx_id_str, ":END_ID(Account)": to_account}
					)

	finally:
		tx_f.close()
		from_f.close()
		to_f.close()


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

	args = parser.parse_args()

	if not args.input.exists():
		raise SystemExit(f"Input file not found: {args.input}")

	transform(input_csv=args.input, out_dir=args.out_dir)


if __name__ == "__main__":
	main()

