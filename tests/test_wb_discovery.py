import json
from pathlib import Path
from typing import Any

import pytest

import wb_discovery
from wb_discovery import (
    CATALOG_BASE_PARAMS,
    RATE_LIMIT_SLEEP,
    Category,
    DiscoveryError,
    _extract_ids,
    fetch_catalog_page,
    find_category,
    load_categories,
    write_output,
)


class DummyResponse:
    def __init__(
        self,
        json_data: Any,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        url: str = "http://example",
    ) -> None:
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "application/json"}
        self.url = url

    def json(self) -> Any:
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class DummySession:
    def __init__(self, response: DummyResponse) -> None:
        self._response = response
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
    ) -> DummyResponse:
        self.calls.append({
            "url": url,
            "params": params,
            "headers": headers,
            "timeout": timeout,
        })
        return self._response


class FailingJsonResponse(DummyResponse):
    def json(self) -> Any:  # pragma: no cover - handled in parent test expectation
        raise ValueError("bad json")


def test_load_categories_filters_invalid_entries(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    categories_path = tmp_path / "categories.json"
    categories_path.write_text(
        json.dumps(
            [
                {"id": 10, "name": "Boots", "shard": "boots"},
                {"name": "No id", "shard": "shoes"},
                {"id": 11, "shard": "missing name"},
                {"id": 12, "name": "", "shard": "bad"},
                "malformed",
                {"id": 13, "name": "Sneakers", "shard": "sneakers"},
            ]
        ),
        encoding="utf-8",
    )

    categories = load_categories(categories_path)
    captured = capsys.readouterr()

    assert categories == [
        Category(id=10, name="Boots", shard="boots"),
        Category(id=13, name="Sneakers", shard="sneakers"),
    ]
    assert "Skipping malformed category entry" in captured.err
    assert "missing numeric" in captured.err
    assert "missing 'name'" in captured.err


def test_load_categories_errors(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    with pytest.raises(DiscoveryError, match="Categories file not found"):
        load_categories(missing)

    bad_json = tmp_path / "bad.json"
    bad_json.write_text("not json", encoding="utf-8")
    with pytest.raises(DiscoveryError, match="Failed to parse categories JSON"):
        load_categories(bad_json)

    wrong_type = tmp_path / "wrong.json"
    wrong_type.write_text(json.dumps({"id": 1}), encoding="utf-8")
    with pytest.raises(DiscoveryError, match="must contain a list"):
        load_categories(wrong_type)

    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps([]), encoding="utf-8")
    with pytest.raises(DiscoveryError, match="No valid categories"):
        load_categories(empty)


def test_find_category_supports_id_and_name(capsys: pytest.CaptureFixture[str]) -> None:
    categories = [
        Category(id=1, name="Shoes", shard="shoes"),
        Category(id=2, name="Clothes", shard="clothes"),
        Category(id=3, name="Shoes", shard="shoe-alt"),
    ]

    assert find_category(categories, cat_id=2) is categories[1]
    assert find_category(categories, name="sHOeS") is categories[0]

    find_category(categories, name="Shoes")
    captured = capsys.readouterr()
    assert "Multiple categories matched" in captured.err

    with pytest.raises(DiscoveryError, match="not found"):
        find_category(categories, cat_id=99)

    with pytest.raises(DiscoveryError, match="must be provided"):
        find_category(categories)


def test_extract_ids_handles_various_types(capsys: pytest.CaptureFixture[str]) -> None:
    products: list[dict[str, Any]] = [
        {"id": 100},
        {"id": "101"},
        {"id": "bad"},
        {"id": 9.5},
        {"id": None},
    ]

    result = _extract_ids(products)
    captured = capsys.readouterr()

    assert result == {100, 101}
    assert "unexpected id type" in captured.err


def test_fetch_catalog_page_success() -> None:
    category = Category(id=321, name="Test", shard="test-shard")
    payload = {"data": {"products": [{"id": 1}, {"id": "2"}]}}
    response = DummyResponse(payload)
    session = DummySession(response)

    ids, success = fetch_catalog_page(session, category, 4, timeout=7)

    assert ids == {1, 2}
    assert success is True
    assert session.calls[0]["url"].endswith("/catalog/test-shard/catalog")
    assert session.calls[0]["params"]["cat"] == category.id
    assert session.calls[0]["params"]["page"] == 4
    base_params = {**CATALOG_BASE_PARAMS, "cat": category.id, "page": 4}
    assert session.calls[0]["params"] == base_params
    assert session.calls[0]["timeout"] == 7


def test_fetch_catalog_page_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    category = Category(id=1, name="Test", shard="test")
    response = DummyResponse({}, status_code=429)
    session = DummySession(response)

    slept: list[float] = []
    monkeypatch.setattr(wb_discovery.time, "sleep", lambda value: slept.append(value))

    ids, success = fetch_catalog_page(session, category, 1, timeout=5)

    assert ids == set()
    assert success is False
    assert slept == [RATE_LIMIT_SLEEP]


def test_fetch_catalog_page_handles_errors() -> None:
    category = Category(id=1, name="Test", shard="test")

    html_response = DummyResponse({}, headers={"Content-Type": "text/html"})
    assert fetch_catalog_page(DummySession(html_response), category, 1, timeout=1) == (set(), False)

    bad_json_response = FailingJsonResponse({}, headers={"Content-Type": "application/json"})
    assert fetch_catalog_page(DummySession(bad_json_response), category, 1, timeout=1) == (set(), False)

    missing_data = DummyResponse({"payload": {}}, headers={"Content-Type": "application/json"})
    assert fetch_catalog_page(DummySession(missing_data), category, 1, timeout=1) == (set(), True)

    missing_products = DummyResponse({"data": {}}, headers={"Content-Type": "application/json"})
    assert fetch_catalog_page(DummySession(missing_products), category, 1, timeout=1) == (set(), True)


def test_write_output_json_and_jsonl(tmp_path: Path) -> None:
    ids = {5, 2, 9}
    json_path = tmp_path / "out" / "data.json"
    jsonl_path = tmp_path / "out" / "data.jsonl"

    write_output(ids, json_path, jsonl=False)
    write_output(ids, jsonl_path, jsonl=True)

    assert json.loads(json_path.read_text(encoding="utf-8")) == [2, 5, 9]
    lines = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines()]
    assert lines == [2, 5, 9]


def test_module_exports() -> None:
    assert wb_discovery.DEFAULT_TIMEOUT > 0
    assert wb_discovery.DEFAULT_HEADERS["User-Agent"].startswith("Mozilla/5.0")
