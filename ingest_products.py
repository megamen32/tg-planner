"""CLI utility for converting product JSONL data to Parquet."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the ingest utility."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to the input JSONL file containing ProductRaw dictionaries.",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Path where the resulting Parquet file will be written.",
    )
    parser.add_argument(
        "--compression",
        choices=("snappy", "gzip", "brotli"),
        default="snappy",
        help="Compression codec to use when writing the Parquet file (default: snappy).",
    )
    return parser.parse_args(argv)


def read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file into a list of dictionaries while dropping raw sources."""
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive branch
                print(
                    f"Failed to parse JSON on line {line_number}: {exc}",
                    file=sys.stderr,
                )
                raise SystemExit(1) from exc
            record.pop("sources", None)
            records.append(record)
    return records


def main(argv: Iterable[str] | None = None) -> int:
    """Entry point for the JSONL to Parquet conversion utility."""
    args = parse_args(argv)

    input_path = Path(args.input)
    if not input_path.is_file():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    records = read_jsonl(input_path)
    dataframe = pd.DataFrame.from_records(records)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, compression=args.compression)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
