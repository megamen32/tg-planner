from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import requests

CATALOG_URL_TEMPLATE = "https://catalog.wb.ru/catalog/{shard}/catalog"
CATALOG_BASE_PARAMS = {
    "sort": "popular",
    "appType": 1,
    "curr": "rub",
    "dest": -1257786,
    "regions": "80",
    "resultset": "catalog",
    "limit": 100,
    "spp": 30,
}
CATALOG_FALLBACK_REGIONS = "80,64,83,4,38,33,70,82,86,75,69,68,30,48,22,1,66,31,40"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WbDiscovery/1.0)",
    "Accept": "application/json",
}
DEFAULT_TIMEOUT = 10
RATE_LIMIT_SLEEP = 1.0


@dataclass(slots=True)
class Category:
    """Representation of a Wildberries catalog category."""

    id: int
    name: str
    shard: str


class DiscoveryError(RuntimeError):
    """Internal exception used to signal fatal CLI errors."""


def _positive_int(value: str) -> int:
    """Parse ``value`` as a strictly positive integer for CLI arguments."""

    try:
        number = int(value)
    except ValueError as exc:  # pragma: no cover - argparse formatting
        raise argparse.ArgumentTypeError(f"Expected integer, got {value!r}") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("Value must be a positive integer.")
    return number


def build_argument_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("categories.json"),
        help="Path to the categories JSON file (default: categories.json).",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--cat-id",
        type=_positive_int,
        help="Category identifier to download products for.",
    )
    group.add_argument(
        "--name",
        help="Category name to search for (case-insensitive exact match).",
    )
    parser.add_argument(
        "--pages",
        type=_positive_int,
        default=1,
        help="Number of catalog pages to traverse (default: 1).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Destination file where the collected nm_id values will be stored as JSON.",
    )
    parser.add_argument(
        "--timeout",
        type=_positive_int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds for catalog requests (default: {DEFAULT_TIMEOUT}).",
    )
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="Write output as JSON Lines (one nm_id per line) instead of a JSON array.",
    )
    return parser


def load_categories(path: Path) -> list[Category]:
    """Load categories from ``path`` returning valid entries only."""

    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise DiscoveryError(f"Categories file not found: {exc.filename}") from exc
    except json.JSONDecodeError as exc:
        raise DiscoveryError(f"Failed to parse categories JSON: {exc}") from exc

    if not isinstance(data, list):
        raise DiscoveryError("Categories JSON must contain a list of objects.")

    categories: list[Category] = []
    for entry in data:
        if not isinstance(entry, Mapping):
            print(f"[WARN] Skipping malformed category entry: {entry!r}", file=sys.stderr)
            continue
        cat_id = entry.get("id")
        shard = entry.get("shard")
        name = entry.get("name")
        if not isinstance(cat_id, int):
            print(f"[WARN] Category entry missing numeric 'id': {entry!r}", file=sys.stderr)
            continue
        if not isinstance(shard, str) or not shard:
            print(f"[WARN] Category entry missing 'shard': {entry!r}", file=sys.stderr)
            continue
        if not isinstance(name, str) or not name:
            print(f"[WARN] Category entry missing 'name': {entry!r}", file=sys.stderr)
            continue
        categories.append(Category(id=cat_id, name=name, shard=shard))

    if not categories:
        raise DiscoveryError("No valid categories loaded from the input file.")

    return categories


def find_category(
    categories: Sequence[Category],
    *,
    cat_id: int | None = None,
    name: str | None = None,
) -> Category:
    """Select a category either by ``cat_id`` or ``name``."""

    if cat_id is not None:
        for category in categories:
            if category.id == cat_id:
                return category
        raise DiscoveryError(f"Category with id {cat_id} not found in the input file.")

    if name is None:
        raise DiscoveryError("Either cat_id or name must be provided.")

    target = name.casefold()
    matches = [category for category in categories if category.name.casefold() == target]
    if not matches:
        raise DiscoveryError(f"Category named {name!r} not found in the input file.")
    if len(matches) > 1:
        print(
            f"[WARN] Multiple categories matched the name {name!r}; using id {matches[0].id}",
            file=sys.stderr,
        )
    return matches[0]


def _extract_ids(products: Iterable[Mapping[str, object]]) -> set[int]:
    """Extract integer nm_id values from ``products`` list."""

    ids: set[int] = set()
    for product in products:
        nm_id = product.get("id")
        if isinstance(nm_id, int):
            ids.add(nm_id)
            continue
        if isinstance(nm_id, str) and nm_id.isdigit():
            try:
                ids.add(int(nm_id))
            except ValueError:
                print(
                    f"[WARN] Unable to convert product id to integer: {nm_id!r}",
                    file=sys.stderr,
                )
        else:
            print(
                f"[WARN] Skipping product with unexpected id type: {nm_id!r}",
                file=sys.stderr,
            )
    return ids


def fetch_catalog_page(
    session: requests.Session,
    category: Category,
    page: int,
    *,
    timeout: int,
) -> tuple[set[int], bool]:
    """Fetch a single catalog page returning discovered nm_id values."""

    url = CATALOG_URL_TEMPLATE.format(shard=category.shard)
    region_attempts = [CATALOG_BASE_PARAMS["regions"]]
    if CATALOG_FALLBACK_REGIONS and CATALOG_FALLBACK_REGIONS != region_attempts[0]:
        region_attempts.append(CATALOG_FALLBACK_REGIONS)

    last_success = False
    for index, regions in enumerate(region_attempts):
        params = dict(CATALOG_BASE_PARAMS)
        params["cat"] = category.id
        params["page"] = page
        params["regions"] = regions

        print(
            "[INFO] Requesting catalog page: "
            f"cat_id={category.id} shard={category.shard} page={page} regions={regions}",
            file=sys.stderr,
        )

        try:
            response = session.get(
                url,
                params=params,
                headers=DEFAULT_HEADERS,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            print(
                f"[ERROR] Request failed for page {page} (regions {regions}): {exc}",
                file=sys.stderr,
            )
            return set(), False

        if response.status_code == 429:
            print(
                f"[WARN] Rate limited on page {page}; sleeping for {RATE_LIMIT_SLEEP:.1f}s",
                file=sys.stderr,
            )
            time.sleep(RATE_LIMIT_SLEEP)
            return set(), False

        if response.status_code >= 400:
            print(
                f"[WARN] HTTP {response.status_code} for page {page}: {response.url}",
                file=sys.stderr,
            )
            return set(), False

        content_type = response.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            print(
                f"[WARN] Unexpected content type for page {page}: {content_type or 'unknown'}",
                file=sys.stderr,
            )
            return set(), False

        try:
            payload = response.json()
        except ValueError as exc:
            print(f"[WARN] Failed to decode JSON on page {page}: {exc}", file=sys.stderr)
            return set(), False

        data_section = payload.get("data")
        if not isinstance(data_section, Mapping):
            print(f"[WARN] Missing 'data' section on page {page}", file=sys.stderr)
            last_success = True
            if index == 0 and len(region_attempts) > 1:
                print(
                    f"[INFO] Page {page} returned no products for regions {regions}; retrying with fallback regions.",
                    file=sys.stderr,
                )
                continue
            print(
                f"[INFO] Page {page} returned no products for regions {regions}.",
                file=sys.stderr,
            )
            break

        products = data_section.get("products")
        if not isinstance(products, list):
            print(f"[WARN] Unexpected 'products' structure on page {page}", file=sys.stderr)
            last_success = True
            if index == 0 and len(region_attempts) > 1:
                print(
                    f"[INFO] Page {page} returned no products for regions {regions}; retrying with fallback regions.",
                    file=sys.stderr,
                )
                continue
            print(
                f"[INFO] Page {page} returned no products for regions {regions}.",
                file=sys.stderr,
            )
            break

        ids = _extract_ids(product for product in products if isinstance(product, Mapping))
        if ids:
            return ids, True

        last_success = True
        if index == 0 and len(region_attempts) > 1:
            print(
                f"[INFO] Page {page} returned no products for regions {regions}; retrying with fallback regions.",
                file=sys.stderr,
            )
            continue

        print(
            f"[INFO] Page {page} returned no products for regions {regions}.",
            file=sys.stderr,
        )
        break

    return set(), last_success


def write_output(ids: Iterable[int], path: Path, *, jsonl: bool) -> None:
    """Persist collected nm_id values to ``path`` in the desired format."""

    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_ids = sorted(ids)
    if jsonl:
        with path.open("w", encoding="utf-8") as handle:
            for nm_id in sorted_ids:
                handle.write(json.dumps(nm_id, ensure_ascii=False))
                handle.write("\n")
    else:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(sorted_ids, handle, ensure_ascii=False, indent=2)
            handle.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the wb_discovery CLI."""

    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        categories = load_categories(args.input)
        category = find_category(
            categories,
            cat_id=args.cat_id,
            name=args.name,
        )
    except DiscoveryError as exc:
        parser.error(str(exc))

    session = requests.Session()
    discovered: set[int] = set()

    for page in range(1, args.pages + 1):
        ids, success = fetch_catalog_page(
            session,
            category,
            page,
            timeout=args.timeout,
        )
        if ids:
            discovered.update(ids)
            print(f"[INFO] Page {page}: collected {len(ids)} ids", file=sys.stderr)
        elif success:
            print(f"[INFO] Page {page}: no products found.", file=sys.stderr)
        else:
            print(
                f"[WARN] Page {page}: request failed or was rate limited; continuing.",
                file=sys.stderr,
            )

    if not discovered:
        print("[WARN] No nm_id values collected.", file=sys.stderr)

    try:
        write_output(discovered, args.output, jsonl=args.jsonl)
    except OSError as exc:
        parser.error(f"Failed to write output file {args.output}: {exc}")

    print(
        f"[INFO] Stored {len(discovered)} unique nm_id values in {args.output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI execution
    raise SystemExit(main())
