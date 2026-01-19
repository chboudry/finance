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
from typing import Iterable


EXPECTED_HEADERS = ["Bank Name", "Bank ID", "Account Number", "Entity ID", "Entity Name"]


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
            f"Actual:   {actual_list}"  # keep exact to help debugging
        )


def transform(input_csv: Path, out_dir: Path) -> None:
    banks_path = out_dir / "banks.csv"
    entities_path = out_dir / "entities.csv"
    accounts_path = out_dir / "accounts.csv"
    account_part_of_bank_path = out_dir / "account_part_of_bank.csv"
    entity_owns_account_path = out_dir / "entity_owns_account.csv"

    bank_fieldnames = ["bank_id:ID(Bank){label:Bank}", "bank_name"]
    entity_fieldnames = ["entity_id:ID(Entity){label:Entity}", "entity_name"]
    account_fieldnames = ["account_number:ID(Account){label:Account}"]

    entity_account_rel_fieldnames = [":START_ID(Entity)", ":END_ID(Account)"]
    account_bank_rel_fieldnames = [":START_ID(Account)", ":END_ID(Bank)"]

    bank_writer, bank_f = _open_csv_writer(banks_path, bank_fieldnames)
    entity_writer, entity_f = _open_csv_writer(entities_path, entity_fieldnames)
    account_writer, account_f = _open_csv_writer(accounts_path, account_fieldnames)
    account_bank_writer, account_bank_f = _open_csv_writer(
        account_part_of_bank_path, account_bank_rel_fieldnames
    )
    entity_account_writer, entity_account_f = _open_csv_writer(
        entity_owns_account_path, entity_account_rel_fieldnames
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
                    bank_writer.writerow(
                        {"bank_id:ID(Bank){label:Bank}": bank_id, "bank_name": bank_name}
                    )

                if entity_id not in seen_entity_ids:
                    seen_entity_ids.add(entity_id)
                    entity_writer.writerow(
                        {
                            "entity_id:ID(Entity){label:Entity}": entity_id,
                            "entity_name": entity_name,
                        }
                    )

                if account_number not in seen_account_numbers:
                    seen_account_numbers.add(account_number)
                    account_writer.writerow(
                        {"account_number:ID(Account){label:Account}": account_number}
                    )

                entity_account_writer.writerow(
                    {
                        ":START_ID(Entity)": entity_id,
                        ":END_ID(Account)": account_number,
                    }
                )

                account_bank_writer.writerow(
                    {
                        ":START_ID(Account)": account_number,
                        ":END_ID(Bank)": bank_id,
                    }
                )

    finally:
        bank_f.close()
        entity_f.close()
        account_f.close()
        account_bank_f.close()
        entity_account_f.close()


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Transform LI-Small_accounts.csv into Neo4j Data Importer CSVs."
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

    args = parser.parse_args()

    input_csv = args.input
    out_dir = args.out_dir

    if not input_csv.exists():
        raise SystemExit(f"Input file not found: {input_csv}")

    transform(input_csv=input_csv, out_dir=out_dir)


if __name__ == "__main__":
    main()
