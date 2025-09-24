"""Utilities for collecting normalized Wildberries product payloads."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from wb_client import (
    extract_nm,
    get_card_api,
    get_content_v2,
    get_info_card_json,
)


@dataclass(slots=True)
class ProductRaw:
    """Normalized representation of a Wildberries product."""

    id: int | None
    name: str | None
    brand: str | None
    supplier: str | None
    description: str | None
    sources: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """Return the product payload as plain Python data structures."""

        return {
            "id": self.id,
            "name": self.name,
            "brand": self.brand,
            "supplier": self.supplier,
            "description": self.description,
            "sources": self.sources,
        }


def fetch_product_raw(nm_id: object) -> ProductRaw:
    """Collect and normalize Wildberries product data for ``nm_id``."""

    nm_int = extract_nm(nm_id)
    card_data = _ensure_mapping(get_card_api(nm_id))
    product = _extract_primary_product(card_data)

    product_id = _extract_int(product, "id") if product else None
    if product_id is None:
        product_id = nm_int
        if product is None and card_data:
            print(f"[WARN] Card API returned no product details for nm_id {nm_int}")

    name = _normalize_string(product.get("name")) if product else None
    brand = _normalize_string(product.get("brand")) if product else None
    supplier = _normalize_string(product.get("supplier")) if product else None

    info_card = _ensure_mapping(get_info_card_json(nm_id))
    description = _extract_description_from_basket(info_card)

    content_data: dict[str, Any] = {}
    if not description:
        content_data = _ensure_mapping(get_content_v2(nm_id))
        description = _extract_description_from_content(content_data)

    if not description and nm_int is not None:
        print(f"[WARN] Description missing for nm_id {nm_int}")

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
    )


def collect_product_records(nm_ids: Iterable[object]) -> list[dict[str, Any]]:
    """Fetch multiple products and return JSON/parquet-friendly records."""

    records: list[dict[str, Any]] = []
    for nm_id in nm_ids:
        product = fetch_product_raw(nm_id)
        records.append(product.to_dict())
    return records


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


def _normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


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


__all__ = ["ProductRaw", "fetch_product_raw", "collect_product_records"]
