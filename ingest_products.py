"""CLI utility for converting product JSONL data to Parquet."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

LITE_COLUMNS = [
    "id",
    "name",
    "brand",
    "supplier",
    "description",
    "price",
    "sale_price",
    "rating",
    "feedbacks",
    "category_id",
    "category_parent_id",
]


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
        "--mode",
        choices=("raw", "lite"),
        default="raw",
        help="Schema mode to use when writing Parquet: raw (default) or lite.",
    )
    parser.add_argument(
        "--compression",
        choices=("snappy", "gzip", "brotli"),
        default="snappy",
        help="Compression codec to use when writing the Parquet file (default: snappy).",
    )
    parser.add_argument(
        "--partition-by",
        nargs="+",
        help="Column name(s) to partition by when writing the Parquet dataset.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the existing Parquet dataset, deduplicating rows by id.",
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

    partition_cols = list(args.partition_by) if args.partition_by else None

    records = read_jsonl(input_path)
    dataframe = pd.DataFrame.from_records(records)

    if "id" not in dataframe.columns:
        if len(dataframe) > 0:
            print("Column 'id' is required in the input data.", file=sys.stderr)
            return 1
        dataframe["id"] = pd.Series(dtype="object")

    if args.mode == "lite":
        missing_for_lite = [column for column in LITE_COLUMNS if column not in dataframe.columns]
        for column in missing_for_lite:
            dataframe[column] = pd.NA
        dataframe = dataframe[LITE_COLUMNS]
        if partition_cols:
            extra_partitions = [col for col in partition_cols if col not in LITE_COLUMNS]
            if extra_partitions:
                missing = ", ".join(extra_partitions)
                print(
                    f"Partition columns not available in lite mode: {missing}",
                    file=sys.stderr,
                )
                return 1

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = [dataframe]
    if args.append and output_path.exists():
        existing_df = pd.read_parquet(output_path)
        if args.mode == "lite":
            missing_columns = [column for column in LITE_COLUMNS if column not in existing_df.columns]
            for column in missing_columns:
                existing_df[column] = pd.NA
            existing_df = existing_df[LITE_COLUMNS]
        frames.insert(0, existing_df)

    if frames:
        combined_df = pd.concat(frames, ignore_index=True)
    else:
        combined_df = pd.DataFrame()

    if "id" not in combined_df.columns:
        if len(combined_df) > 0:
            print("Column 'id' is required to deduplicate appended data.", file=sys.stderr)
            return 1
        combined_df["id"] = pd.Series(dtype="object")

    combined_df = combined_df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)

    if args.mode == "lite":
        combined_df = combined_df[LITE_COLUMNS]

    if partition_cols:
        for column in partition_cols:
            if column not in combined_df.columns:
                combined_df[column] = pd.NA

    if output_path.exists():
        if output_path.is_dir():
            shutil.rmtree(output_path)
        else:
            output_path.unlink()

    combined_df.to_parquet(
        output_path,
        compression=args.compression,
        partition_cols=partition_cols,
    )

    partitions_display = partition_cols if partition_cols is not None else []
    print(
        f"[INFO] Wrote {len(combined_df)} rows to {output_path} "
        f"(mode={args.mode}, append={args.append}, partitions={partitions_display})"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
