#!/usr/bin/env python3
"""Python equivalent of script.sh.

Behavior:
- For each file matching '*_Trans.csv' in the current working directory:
  - Print: "Updating <basename>   [size: <human_size>]"
  - Update the header line (first line) in-place:
      first occurrence of ',Account,'  -> ',FromAccount,'
      second occurrence of ',Account,' -> ',ToAccount,'

This mirrors the original sed command:
  sed -i '' '1s/,Account,/,FromAccount,/; 1s/,Account,/,ToAccount,/'
"""

from __future__ import annotations

import os
from pathlib import Path


def _human_size(num_bytes: int) -> str:
    units = ["B", "K", "M", "G", "T", "P"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{num_bytes}B"


def _update_header_in_place(path: Path) -> None:
    tmp_path = path.with_name(path.name + ".tmp")

    with path.open("r", encoding="utf-8", newline="") as src, tmp_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        first_line = src.readline()
        if first_line:
            first_line = first_line.replace(",Account,", ",FromAccount,", 1)
            first_line = first_line.replace(",Account,", ",ToAccount,", 1)
            dst.write(first_line)
        for line in src:
            dst.write(line)

    os.replace(tmp_path, path)


def main() -> None:
    cwd = Path.cwd()

    for path in sorted(cwd.glob("*_Trans.csv")):
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            # Skip if the file disappears during iteration.
            continue

        print(f"Updating {path.name}   [size: {_human_size(size)}]")
        _update_header_in_place(path)


if __name__ == "__main__":
    main()
