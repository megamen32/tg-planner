from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

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
    "shard"
]

STATE_FILE_NAME = "state.parquet"
STATE_COLUMN_NAME = "id"
ID_STR_COLUMN = "__id_str__"
N_SHARDS = 10

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
        help="Path where the resulting Parquet dataset directory will be written.",
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
    """Read a JSONL file into a list of dictionaries, filtering bad rows and dropping raw sources."""
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"[WARN] Failed to parse JSON on line {line_number}: {exc}", file=sys.stderr)
                continue

            record.pop("sources", None)

            # обязательное поле id
            if not record.get("id"):
                continue
            # цена должна быть > 0
            price = record.get("price")
            if price is None or price == 0:
                continue
            # название и описание не пустые
            if not record.get("name") or len(str(record["name"])) < 3:
                continue
            if not record.get("description") or len(str(record["description"])) < 10:
                continue

            records.append(record)
    return records


def load_state_ids(state_path: Path) -> set[str]:
    """Load the set of already ingested product ids from the state file."""
    if not state_path.is_file():
        return set()

    table = pq.read_table(state_path)
    if STATE_COLUMN_NAME not in table.column_names:
        return set()

    ids_column = table.column(STATE_COLUMN_NAME)
    return {str(value) for value in ids_column.to_pylist() if value is not None}


def write_state_ids(state_path: Path, ids: set[str]) -> None:
    """Persist the updated set of product ids into the state file."""
    state_path.parent.mkdir(parents=True, exist_ok=True)
    sorted_ids = sorted(str(value) for value in ids)
    ids_array = pa.array(sorted_ids, type=pa.string())
    table = pa.table({STATE_COLUMN_NAME: ids_array})
    pq.write_table(table, state_path)


def main(argv: Iterable[str] | None = None) -> int:
    """Entry point for the JSONL to Parquet conversion utility."""
    args = parse_args(argv)

    input_path = Path(args.input)
    if not input_path.is_file():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    partition_cols = list(args.partition_by) if args.partition_by else []

    records = read_jsonl(input_path)
    dataframe = pd.DataFrame.from_records(records)
    dataframe['shard'] = np.arange(len(dataframe)) % N_SHARDS

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
    if partition_cols:
        for column in partition_cols:
            if column not in dataframe.columns:
                dataframe[column] = pd.NA

    original_count = len(dataframe)

    id_strings = dataframe["id"].astype("string")
    if id_strings.isna().any():
        print("Column 'id' contains null values; cannot deduplicate.", file=sys.stderr)
        return 1
    dataframe[ID_STR_COLUMN] = id_strings

    deduped_df = dataframe.drop_duplicates(subset=[ID_STR_COLUMN], keep="last").copy()

    output_path = Path(args.output)
    state_path = output_path / STATE_FILE_NAME

    if args.append:
        if output_path.exists() and not output_path.is_dir():
            print(
                f"Output path must be a directory when using --append: {output_path}",
                file=sys.stderr,
            )
            return 1
        existing_ids = load_state_ids(state_path)
    else:
        if output_path.exists():
            if output_path.is_dir():
                shutil.rmtree(output_path)
            else:
                output_path.unlink()
        existing_ids = set()

    output_path.mkdir(parents=True, exist_ok=True)

    mask_new = ~deduped_df[ID_STR_COLUMN].isin(existing_ids)
    rows_to_write = deduped_df.loc[mask_new].copy()
    new_row_id_strings = [str(value) for value in rows_to_write[ID_STR_COLUMN].tolist()]
    appended_count = len(rows_to_write)
    duplicates_count = original_count - appended_count
    rows_to_write = rows_to_write.drop(columns=ID_STR_COLUMN, errors="ignore")

    if appended_count > 0:
        table = pa.Table.from_pandas(rows_to_write, preserve_index=False)
        partitioning = None
        if partition_cols:
            partition_fields = [table.schema.field(name) for name in partition_cols]
            partitioning = ds.partitioning(pa.schema(partition_fields), flavor="hive")
        write_options = ds.ParquetFileFormat().make_write_options(compression=args.compression)
        basename_template = f"part-{int(time.time() * 1000)}-{{i}}.parquet"
        ds.write_dataset(
            data=table,
            base_dir=str(output_path),
            format="parquet",
            partitioning=partitioning,
            file_options=write_options,
            basename_template=basename_template,
            existing_data_behavior="overwrite_or_ignore",
        )

    updated_state_ids = existing_ids.union(set(new_row_id_strings))
    if appended_count > 0 or not state_path.exists():
        write_state_ids(state_path, updated_state_ids)

    dataset_display = output_path.as_posix()
    if not dataset_display.endswith("/"):
        dataset_display = f"{dataset_display}/"

    print(
        f"[INFO] Appended {appended_count} new rows (skipped {duplicates_count} duplicates). "
        f"Dataset path: {dataset_display}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
