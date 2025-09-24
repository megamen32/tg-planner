import math
import re
from typing import Any

import pytest

from requests import HTTPError

import wb_client


class DummyResponse:
    def __init__(self, json_data: Any, status_code: int = 200, url: str = "http://example") -> None:
        self._json_data = json_data
        self.status_code = status_code
        self.url = url

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._json_data


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (258368289, 258368289),
        ("258368289", 258368289),
        ("https://example.com/catalog/258368289/detail", 258368289),
        ("no digits", None),
        (0, None),
        (-1, None),
    ],
)
def test_extract_nm(value: object, expected: int | None) -> None:
    assert wb_client.extract_nm(value) == expected


def test_get_card_api_calls_request(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_request(url: str, *, params: dict[str, Any] | None = None, timeout: int) -> dict[str, Any]:
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return {"status": "ok"}

    monkeypatch.setattr(wb_client, "_request_with_retries", fake_request)
    result = wb_client.get_card_api("258368289")

    assert result == {"status": "ok"}
    assert captured["url"] == wb_client.CARD_API_URL
    assert captured["params"] == {**wb_client.CARD_API_PARAMS, "nm": 258368289}
    assert captured["timeout"] == wb_client.DEFAULT_TIMEOUT


def test_get_card_api_invalid_nm(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def fake_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return {"unexpected": True}

    monkeypatch.setattr(wb_client, "_request_with_retries", fake_request)
    result = wb_client.get_card_api("bad value")

    assert result == {}
    assert not called


def test_get_info_card_json_attempts_hosts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_single(url: str, *, params: dict[str, Any] | None = None, timeout: int) -> dict[str, Any] | None:
        calls.append(url)
        if len(calls) == 2:
            return {"payload": True}
        return None

    monkeypatch.setattr(wb_client, "_single_request", fake_single)
    result = wb_client.get_info_card_json(123456789)

    assert result == {"payload": True}
    assert calls[0].startswith("http://basket-")

    expected_hosts = wb_client._guess_basket_hosts(123456789 // 100000)
    called_hosts: list[int] = []
    for url in calls:
        match = re.search(r"basket-(\d+)", url)
        if match:
            called_hosts.append(int(match.group(1)))

    assert called_hosts == expected_hosts[: len(called_hosts)]


def test_get_info_card_json_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wb_client, "_single_request", lambda *args, **kwargs: None)
    result = wb_client.get_info_card_json("bad")
    assert result == {}


def test_get_content_v2_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any] | None]] = []

    def fake_single(url: str, *, params: dict[str, Any] | None = None, timeout: int) -> dict[str, Any] | None:
        calls.append((url, params))
        if len(calls) == 3:
            return {"content": True}
        return None

    monkeypatch.setattr(wb_client, "_single_request", fake_single)
    result = wb_client.get_content_v2(321)

    assert result == {"content": True}
    assert all(params == {"nm": 321} for _, params in calls)


def test_get_content_v2_invalid_nm(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def fake_single(*args: Any, **kwargs: Any) -> dict[str, Any] | None:
        nonlocal called
        called = True
        return None

    monkeypatch.setattr(wb_client, "_single_request", fake_single)
    result = wb_client.get_content_v2("oops")

    assert result == {}
    assert not called


def test_single_request_success(monkeypatch: pytest.MonkeyPatch) -> None:
    response = DummyResponse({"ok": True})

    def fake_get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, timeout: int) -> DummyResponse:
        return response

    monkeypatch.setattr(wb_client.requests, "get", fake_get)
    result = wb_client._single_request("http://example.com")
    assert result == {"ok": True}


def test_single_request_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    response = DummyResponse({"ok": False}, status_code=500)

    def fake_get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, timeout: int) -> DummyResponse:
        return response

    monkeypatch.setattr(wb_client.requests, "get", fake_get)
    result = wb_client._single_request("http://example.com")
    assert result is None


def test_single_request_bad_json(monkeypatch: pytest.MonkeyPatch) -> None:
    class BadJsonResponse(DummyResponse):
        def json(self) -> Any:
            raise ValueError("bad json")

    response = BadJsonResponse({"ignored": True})

    def fake_get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, timeout: int) -> DummyResponse:
        return response

    monkeypatch.setattr(wb_client.requests, "get", fake_get)
    result = wb_client._single_request("http://example.com")
    assert result is None


def test_request_with_retries_success(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts: list[int] = []

    def fake_single(url: str, *, params: dict[str, Any] | None = None, timeout: int) -> dict[str, Any] | None:
        attempts.append(1)
        if len(attempts) == 3:
            return {"ok": True}
        return None

    monkeypatch.setattr(wb_client, "_single_request", fake_single)
    result = wb_client._request_with_retries("http://example.com")

    assert result == {"ok": True}
    assert len(attempts) == 3


def test_request_with_retries_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wb_client, "_single_request", lambda *args, **kwargs: None)
    result = wb_client._request_with_retries("http://example.com")
    assert result == {}


def test_guess_basket_hosts_negative() -> None:
    assert wb_client._guess_basket_hosts(-1) == [9, 1, 2]


def test_guess_basket_hosts_positive() -> None:
    hosts = wb_client._guess_basket_hosts(320)
    assert hosts == [3, 2, 1, 4]


@pytest.mark.parametrize(
    ("vol", "expected"),
    [
        (0, 1),
        (144, 2),
        (320, 3),
        (720, 5),
        (1116, 8),
        (5000, 27),
        (6126, 31),
        (7000, wb_client.MAX_BASKET_HOST),
    ],
)
def test_guess_basket_hosts_primary_mapping(vol: int, expected: int) -> None:
    hosts = wb_client._guess_basket_hosts(vol)
    assert hosts
    assert hosts[0] == expected


def test_sleep_with_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[float] = []

    monkeypatch.setattr(wb_client.time, "sleep", lambda value: recorded.append(value))
    monkeypatch.setattr(wb_client.random, "uniform", lambda a, b: 0.0)

    wb_client._sleep_with_backoff(3)

    expected = wb_client.BACKOFF_FACTOR * (2 ** (3 - 1))
    assert len(recorded) == 1
    assert math.isclose(recorded[0], expected)
