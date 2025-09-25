"""Async utilities for collecting normalized Wildberries product payloads."""
from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from etl.schemas import ProductRaw

from etl.extract.wb_client_async import (
    extract_nm,
    get_card_api,
    get_content_v2,
    get_info_card_json,
)


async def fetch_product_raw(nm_id: object) -> ProductRaw:
    """Асинхронно собирает и нормализует карточку товара WB."""

    nm_int = extract_nm(nm_id)
    card_data = _ensure_mapping(await get_card_api(nm_id))
    product = _extract_primary_product(card_data)

    product_id = _extract_int(product, "id") if product else None
    if product_id is None:
        product_id = nm_int
        if product is None and card_data:
            print(f"[WARN] Card API returned no product details for nm_id {nm_int}")

    name = _normalize_string(product.get("name")) if product else None
    brand = _normalize_string(product.get("brand")) if product else None
    supplier = _normalize_string(product.get("supplier")) if product else None

    info_card = _ensure_mapping(await get_info_card_json(nm_id))
    description = _extract_description_from_basket(info_card)

    content_data: dict[str, Any] = {}
    if not description:
        content_data = _ensure_mapping(await get_content_v2(nm_id))
        description = _extract_description_from_content(content_data)

    if not description and nm_int is not None:
        print(f"[WARN] Description missing for nm_id {nm_int}")

    price = sale_price = rating = feedbacks = None
    category_id = category_parent_id = root_id = kind_id = None
    colors: list[str] = []
    sizes: list[str] = []
    if product:
        price = _extract_int(product, "priceU")
        sale_price = _extract_int(product, "salePriceU")
        rating = _extract_float(product, "reviewRating") or _extract_float(product, "rating")
        feedbacks = _extract_int(product, "feedbacks")
        category_id = _extract_int(product, "subjectId")
        category_parent_id = _extract_int(product, "subjectParentId")
        root_id = _extract_int(product, "root")
        kind_id = _extract_int(product, "kindId")

        if price is None or sale_price is None:
            derived_price, derived_sale = _extract_prices_from_sizes(product.get("sizes"))
            price = price or derived_price
            sale_price = sale_price or derived_sale

    sources = {
        "card_api": card_data,
        "info_card_json": info_card,
        "content_v2": content_data,
    }

    return ProductRaw(
        id=product_id,
        name=name,
        brand=brand,
        supplier=supplier,
        description=description,
        sources=sources,
        price=price,
        sale_price=sale_price,
        rating=rating,
        feedbacks=feedbacks,
        category_id=category_id,
        category_parent_id=category_parent_id,
        root=root_id,
        kind_id=kind_id
    )


async def collect_product_records(
    nm_ids: Iterable[object],
    *,
    concurrency: int = 10,
) -> list[dict[str, Any]]:
    """
    Асинхронно получает несколько карточек товаров параллельно.

    Args:
        nm_ids: список id или ссылок.
        concurrency: макс. число одновременных запросов.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _worker(nm_id: object) -> dict[str, Any]:
        async with sem:
            product = await fetch_product_raw(nm_id)
            return product.to_dict()

    tasks = [asyncio.create_task(_worker(nm)) for nm in nm_ids]
    return await asyncio.gather(*tasks)


def _ensure_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _extract_primary_product(card_data: Mapping[str, Any]) -> Mapping[str, Any] | None:
    data = card_data.get("data")
    if isinstance(data, Mapping):
        products = data.get("products")
        if isinstance(products, Sequence) and not isinstance(products, (str, bytes, bytearray)):
            for entry in products:
                if isinstance(entry, Mapping):
                    return entry
    return None


def _extract_int(data: Mapping[str, Any], key: str) -> int | None:
    value = data.get(key)
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_float(data: Mapping[str, Any], key: str) -> float | None:
    value = data.get(key)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _extract_prices_from_sizes(sizes: Any) -> tuple[int | None, int | None]:
    if not isinstance(sizes, Sequence) or isinstance(sizes, (str, bytes, bytearray)):
        return (None, None)
    for entry in sizes:
        if not isinstance(entry, Mapping):
            continue
        price_info = entry.get("price")
        if not isinstance(price_info, Mapping):
            continue
        basic = price_info.get("basic")
        sale = price_info.get("product") or price_info.get("total")
        basic_int = None
        sale_int = None
        try:
            if isinstance(basic, str) and basic.strip():
                basic_int = int(float(basic))
            elif isinstance(basic, (int, float)):
                basic_int = int(basic)
        except ValueError:
            basic_int = None
        try:
            if isinstance(sale, str) and sale.strip():
                sale_int = int(float(sale))
            elif isinstance(sale, (int, float)):
                sale_int = int(sale)
        except ValueError:
            sale_int = None
        if basic_int is not None or sale_int is not None:
            return (basic_int, sale_int)
    return (None, None)


def _extract_description_from_basket(data: Mapping[str, Any]) -> str | None:
    description = data.get("description")
    if isinstance(description, str) and description.strip():
        return description
    return _find_first_string(data, ("description",))


def _extract_description_from_content(data: Mapping[str, Any]) -> str | None:
    if not data:
        return None
    direct = data.get("description")
    if isinstance(direct, str) and direct.strip():
        return direct
    nested = data.get("data")
    if isinstance(nested, Mapping):
        for key in ("products", "cards", "list"):
            entries = nested.get(key)
            if isinstance(entries, Sequence) and not isinstance(entries, (str, bytes, bytearray)):
                for entry in entries:
                    if isinstance(entry, Mapping):
                        candidate = _find_first_string(entry, ("description", "descriptionText", "description_html", "text"))
                        if candidate:
                            return candidate
        candidate = _find_first_string(nested, ("description", "descriptionText", "description_html", "text"))
        if candidate:
            return candidate
    return _find_first_string(data, ("description", "descriptionText", "description_html", "text"))


def _find_first_string(data: Any, keys: tuple[str, ...]) -> str | None:
    if isinstance(data, Mapping):
        for key in keys:
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value
        for value in data.values():
            result = _find_first_string(value, keys)
            if result:
                return result
    elif isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        for item in data:
            result = _find_first_string(item, keys)
            if result:
                return result
    return None