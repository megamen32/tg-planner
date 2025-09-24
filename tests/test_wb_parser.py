from typing import Any

import pytest

import wb_parser
from wb_parser import (
    ProductRaw,
    _ensure_mapping,
    _extract_description_from_basket,
    _extract_description_from_content,
    _extract_int,
    _extract_primary_product,
    _find_first_string,
    _normalize_string,
    collect_product_records,
    fetch_product_raw,
)


def test_product_raw_to_dict() -> None:
    product = ProductRaw(
        id=1,
        name="Name",
        brand="Brand",
        supplier="Supplier",
        description="Description",
        sources={"card_api": {"id": 1}},
        price=100,
        sale_price=90,
        rating=4.5,
        feedbacks=10,
        category_id=200,
        category_parent_id=20,
        root=500,
        kind_id=600,
        colors=["Red"],
        sizes=["M"],
        image_urls=["https://example.com/img.jpg"],
        text_index="Name\nBrand\nSupplier\nDescription",
    )

    result = product.to_dict()

    assert result == {
        "id": 1,
        "name": "Name",
        "brand": "Brand",
        "supplier": "Supplier",
        "description": "Description",
        "sources": {"card_api": {"id": 1}},
        "price": 100,
        "sale_price": 90,
        "rating": 4.5,
        "feedbacks": 10,
        "category_id": 200,
        "category_parent_id": 20,
        "root": 500,
        "kind_id": 600,
        "colors": ["Red"],
        "sizes": ["M"],
        "image_urls": ["https://example.com/img.jpg"],
        "text_index": "Name\nBrand\nSupplier\nDescription",
    }


def test_ensure_mapping() -> None:
    original = {"key": "value"}
    result = _ensure_mapping(original)

    assert result == original
    assert result is not original
    assert _ensure_mapping(None) == {}


def test_extract_primary_product_success() -> None:
    data = {"data": {"products": [{"id": 1}, {"id": 2}]}}
    product = _extract_primary_product(data)
    assert product == {"id": 1}


def test_extract_primary_product_missing() -> None:
    data: dict[str, Any] = {"data": {"items": []}}
    assert _extract_primary_product(data) is None


def test_extract_int() -> None:
    mapping = {"value": "10"}
    assert _extract_int(mapping, "value") == 10
    assert _extract_int(mapping, "missing") is None
    assert _extract_int({"value": "bad"}, "value") is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("  spaced  ", "spaced"),
        ("", None),
        (None, None),
        (123, "123"),
    ],
)
def test_normalize_string(value: Any, expected: str | None) -> None:
    assert _normalize_string(value) == expected


def test_find_first_string_mapping() -> None:
    data = {"a": {"b": "value"}}
    assert _find_first_string(data, ("b",)) == "value"


def test_find_first_string_sequence() -> None:
    data = [1, {"nested": [{"target": "found"}]}]
    assert _find_first_string(data, ("target",)) == "found"


def test_extract_description_from_basket() -> None:
    data = {"info": {"description": "desc"}}
    assert _extract_description_from_basket(data) == "desc"


def test_extract_description_from_content_variants() -> None:
    direct = {"description": "Direct"}
    assert _extract_description_from_content(direct) == "Direct"

    nested = {"data": {"products": [{"descriptionText": "Nested"}]}}
    assert _extract_description_from_content(nested) == "Nested"

    fallback = {"data": {"details": {"text": "Fallback"}}}
    assert _extract_description_from_content(fallback) == "Fallback"


def test_fetch_product_raw_prefers_info_card(monkeypatch: pytest.MonkeyPatch) -> None:
    card_payload = {
        "data": {
            "products": [
                {
                    "id": 100,
                    "name": "Item",
                    "brand": "Brand",
                    "supplier": "Supplier",
                    "priceU": 1500,
                    "salePriceU": 990,
                    "reviewRating": 4.7,
                    "feedbacks": 42,
                    "subjectId": 321,
                    "subjectParentId": 123,
                    "root": 777,
                    "kindId": 555,
                    "colors": [{"name": "Красный"}, {"name": "Синий"}],
                    "sizes": [
                        {"name": "M"},
                        {"origName": "L"},
                    ],
                    "pics": 2,
                }
            ]
        }
    }
    info_payload = {"description": "From info", "media": {"photo_count": 3}}

    monkeypatch.setattr(wb_parser, "get_card_api", lambda nm: card_payload)
    monkeypatch.setattr(wb_parser, "get_info_card_json", lambda nm: info_payload)
    monkeypatch.setattr(wb_parser, "get_content_v2", lambda nm: (_ for _ in ()).throw(AssertionError("should not call")))

    product = fetch_product_raw("http://example.com/item/100")

    assert product.id == 100
    assert product.name == "Item"
    assert product.brand == "Brand"
    assert product.supplier == "Supplier"
    assert product.description == "From info"
    assert product.price == 1500
    assert product.sale_price == 990
    assert product.rating == 4.7
    assert product.feedbacks == 42
    assert product.category_id == 321
    assert product.category_parent_id == 123
    assert product.root == 777
    assert product.kind_id == 555
    assert product.colors == ["Красный", "Синий"]
    assert product.sizes == ["M", "L"]
    assert product.image_urls == [
        "https://images.wbstatic.net/big/new/0/0/100-1.jpg",
        "https://images.wbstatic.net/big/new/0/0/100-2.jpg",
        "https://images.wbstatic.net/big/new/0/0/100-3.jpg",
    ]
    assert product.text_index == "Item\nBrand\nSupplier\nFrom info"
    assert product.sources["card_api"] == card_payload
    assert product.sources["info_card_json"] == info_payload
    assert product.sources["content_v2"] == {}


def test_fetch_product_raw_uses_content(monkeypatch: pytest.MonkeyPatch) -> None:
    card_payload = {"data": {"products": [{"id": 101, "name": "Item2", "brand": "Brand2", "supplier": "Supplier2"}]}}
    info_payload: dict[str, Any] = {}
    content_payload = {"data": {"products": [{"description_html": "<p>HTML</p>"}]}}

    monkeypatch.setattr(wb_parser, "get_card_api", lambda nm: card_payload)
    monkeypatch.setattr(wb_parser, "get_info_card_json", lambda nm: info_payload)
    monkeypatch.setattr(wb_parser, "get_content_v2", lambda nm: content_payload)

    product = fetch_product_raw(101)

    assert product.id == 101
    assert product.description == "<p>HTML</p>"
    assert product.sources["content_v2"] == content_payload
    assert product.text_index == "Item2\nBrand2\nSupplier2\n<p>HTML</p>"


def test_fetch_product_raw_handles_missing_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wb_parser, "get_card_api", lambda nm: {"unexpected": []})
    monkeypatch.setattr(wb_parser, "get_info_card_json", lambda nm: {})
    monkeypatch.setattr(wb_parser, "get_content_v2", lambda nm: {})

    product = fetch_product_raw("999")

    assert product.id == 999
    assert product.name is None
    assert product.description is None
    assert product.price is None
    assert product.colors == []
    assert product.text_index == ""


def test_collect_product_records(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch(nm: object) -> ProductRaw:
        return ProductRaw(
            id=int(wb_parser.extract_nm(nm) or 0),
            name="Name",
            brand="Brand",
            supplier="Supplier",
            description="Desc",
            sources={},
        )

    monkeypatch.setattr(wb_parser, "fetch_product_raw", fake_fetch)

    records = collect_product_records([1, "2"])

    assert records == [
        {
            "id": 1,
            "name": "Name",
            "brand": "Brand",
            "supplier": "Supplier",
            "description": "Desc",
            "sources": {},
            "price": None,
            "sale_price": None,
            "rating": None,
            "feedbacks": None,
            "category_id": None,
            "category_parent_id": None,
            "root": None,
            "kind_id": None,
            "colors": [],
            "sizes": [],
            "image_urls": [],
            "text_index": "",
        },
        {
            "id": 2,
            "name": "Name",
            "brand": "Brand",
            "supplier": "Supplier",
            "description": "Desc",
            "sources": {},
            "price": None,
            "sale_price": None,
            "rating": None,
            "feedbacks": None,
            "category_id": None,
            "category_parent_id": None,
            "root": None,
            "kind_id": None,
            "colors": [],
            "sizes": [],
            "image_urls": [],
            "text_index": "",
        },
    ]
