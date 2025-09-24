"""Batch utilities for fetching Wildberries product payloads."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

from wb_parser import fetch_product_raw

_TOKEN_SPLIT_PATTERN = re.compile(r"[,\s]+")


def _tokenize_nm_id_string(text: str) -> list[str]:
    """Split ``text`` into nm_id tokens while ignoring comments and blanks."""

    stripped = text.strip()
    if not stripped:
        return []
    if "#" in stripped:
        stripped = stripped.split("#", 1)[0].strip()
    if not stripped:
        return []
    return [part for part in _TOKEN_SPLIT_PATTERN.split(stripped) if part]


def _coerce_nm_id(value: str) -> object:
    """Convert ``value`` to ``int`` when possible, otherwise return the original."""

    try:
        return int(value)
    except ValueError:
        return value


def parse_nm_id_tokens(tokens: Sequence[str]) -> list[object]:
    """Normalize nm_id entries provided directly via CLI tokens."""

    nm_ids: list[object] = []
    for token in tokens:
        for part in _tokenize_nm_id_string(token):
            nm_ids.append(_coerce_nm_id(part))
    return nm_ids


def load_nm_ids_from_file(path: Path, *, encoding: str = "utf-8") -> list[object]:
    """Read nm_id values from ``path`` supporting whitespace/comma separated lists."""

    nm_ids: list[object] = []
    with path.open("r", encoding=encoding) as handle:
        for line in handle:
            for token in _tokenize_nm_id_string(line):
                nm_ids.append(_coerce_nm_id(token))
    return nm_ids


def collect_nm_ids(
    positional: Sequence[str],
    file_path: Path | None,
    *,
    encoding: str = "utf-8",
) -> list[object]:
    """Collect nm_id values either from ``positional`` tokens or ``file_path``."""

    nm_ids: list[object] = []
    if file_path is not None:
        nm_ids.extend(load_nm_ids_from_file(file_path, encoding=encoding))
    nm_ids.extend(parse_nm_id_tokens(positional))
    return nm_ids


def write_products_to_jsonl(
    nm_ids: Iterable[object],
    output_path: Path,
    *,
    encoding: str = "utf-8",
) -> tuple[int, int]:
    """Fetch ``nm_ids`` and persist product payloads to ``output_path`` in JSONL.

    Returns a tuple ``(success_count, error_count)`` describing the execution
    outcome.
    """

    success_count = 0
    error_count = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=encoding) as stream:
        for nm_id in nm_ids:
            try:
                product = fetch_product_raw(nm_id)
            except Exception as exc:  # pragma: no cover - defensive logging
                error_count += 1
                print(
                    f"[ERROR] Failed to fetch nm_id {nm_id}: {exc}",
                    file=sys.stderr,
                )
                continue

            record: dict[str, Any] = product.to_dict()
            stream.write(json.dumps(record, ensure_ascii=False))
            stream.write("\n")
            success_count += 1

    return success_count, error_count


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for the batch utility."""

    parser = argparse.ArgumentParser(
        description=(
            "Fetch Wildberries product cards via wb_parser.fetch_product_raw and "
            "store results in JSONL format."
        )
    )
    parser.add_argument(
        "nm_ids",
        nargs="*",
        help="nm_id values to process (whitespace or comma separated).",
    )
    parser.add_argument(
        "-f",
        "--file",
        type=Path,
        help=(
            "Path to a file containing nm_id values. Lines may contain multiple "
            "entries separated by commas or whitespace; comments starting with # are ignored."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=Path,
        help="Destination path for the resulting JSONL file.",
    )
    parser.add_argument(
        "--input-encoding",
        default="utf-8",
        help="Encoding used for reading nm_id files (default: utf-8).",
    )
    parser.add_argument(
        "--output-encoding",
        default="utf-8",
        help="Encoding used for the JSONL output (default: utf-8).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the CLI interface."""

    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        nm_ids = collect_nm_ids(
            args.nm_ids,
            args.file,
            encoding=args.input_encoding,
        )
    except FileNotFoundError as exc:
        parser.error(f"Input file not found: {exc.filename}")
    except OSError as exc:
        parser.error(f"Failed to read input file {args.file}: {exc}")

    if not nm_ids:
        parser.error("Provide at least one nm_id via positional arguments or --file.")

    output_path: Path = args.output

    try:
        success_count, error_count = write_products_to_jsonl(
            nm_ids,
            output_path,
            encoding=args.output_encoding,
        )
    except OSError as exc:
        parser.error(f"Failed to write output file {output_path}: {exc}")

    if error_count:
        print(
            f"[WARN] Completed with {error_count} failures; "
            f"{success_count} records written to {output_path}",
            file=sys.stderr,
        )
        return 1

    print(
        f"[INFO] Successfully wrote {success_count} records to {output_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI execution
    raise SystemExit(main())
