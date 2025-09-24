from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Sequence
import aiofiles
from etl.parser.wb_parser import fetch_product_raw  # важно: пакетный импорт


async def fetch_one(nm_id: int, semaphore: asyncio.Semaphore) -> dict[str, Any] | None:
    """Асинхронная обёртка для fetch_product_raw."""
    async with semaphore:
        loop = asyncio.get_running_loop()
        try:
            product = await loop.run_in_executor(None, fetch_product_raw, nm_id)
            return product.to_dict()
        except Exception as exc:
            print(f"[ERROR] Failed to fetch nm_id {nm_id}: {exc}", file=sys.stderr)
            return None


async def write_products_to_jsonl(
    nm_ids: list[int],
    output_path: Path,
    *,
    encoding: str = "utf-8",
    max_concurrent: int = 10,
) -> tuple[int, int]:
    """Асинхронно обработать nm_ids и сохранить результат в JSONL."""
    semaphore = asyncio.Semaphore(max_concurrent)
    success_count = 0
    error_count = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(output_path, "w", encoding=encoding) as stream:
        tasks = [fetch_one(nm_id, semaphore) for nm_id in nm_ids]

        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result:
                await stream.write(json.dumps(result, ensure_ascii=False) + "\n")
                success_count += 1
            else:
                error_count += 1

    return success_count, error_count


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Wildberries product cards asynchronously and store results in JSONL."
    )
    parser.add_argument("-f", "--file", type=Path, required=True, help="Path to a file with nm_id values.")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Destination JSONL file.")
    parser.add_argument("--input-encoding", default="utf-8", help="Encoding of input file (default: utf-8).")
    parser.add_argument("--output-encoding", default="utf-8", help="Encoding of output file (default: utf-8).")
    parser.add_argument("--max-concurrent", type=int, default=10, help="Maximum concurrent requests.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        nm_ids = [int(line.strip()) for line in args.file.read_text(encoding=args.input_encoding).splitlines() if line.strip()]
    except FileNotFoundError as exc:
        parser.error(f"Input file not found: {exc.filename}")
    except OSError as exc:
        parser.error(f"Failed to read input file {args.file}: {exc}")

    if not nm_ids:
        parser.error("No nm_id provided.")

    output_path: Path = args.output

    success_count, error_count = asyncio.run(
        write_products_to_jsonl(
            nm_ids,
            output_path,
            encoding=args.output_encoding,
            max_concurrent=args.max_concurrent,
        )
    )

    if error_count:
        print(f"[WARN] Completed with {error_count} failures; {success_count} records written to {output_path}", file=sys.stderr)
        return 1

    print(f"[INFO] Successfully wrote {success_count} records to {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
