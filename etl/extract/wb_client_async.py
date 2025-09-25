"""Asynchronous Wildberries API client with retry/backoff helpers."""
from __future__ import annotations

from bisect import bisect_left
import random
import re
import asyncio
from typing import Any

import httpx # type: ignore

DEFAULT_TIMEOUT = 10.0
MAX_RETRIES = 4
BACKOFF_FACTOR = 0.5
BACKOFF_JITTER = 0.5

CARD_API_URL = "https://card.wb.ru/cards/v2/detail"
CARD_API_PARAMS = {
    "appType": 1,
    "curr": "rub",
    "dest": -1257786,
    "spp": 0,
}

BASKET_URL_TEMPLATE = (
    "http://basket-{host:02d}.wbbasket.ru/vol{vol}/part{part}/{nm}/info/ru/card.json"
)
MAX_BASKET_HOST = 32

_BASKET_VOL_THRESHOLDS: tuple[int, ...] = (
    143, 287, 431, 719, 1007, 1061, 1115, 1169,
    1313, 1601, 1655, 1919, 2045, 2189, 2405, 2621,
    2837, 3053, 3269, 3485, 3701, 3917, 4133, 4349,
    4565, 4877, 5189, 5501, 5813, 6125, 6437,
)

CONTENT_V2_URLS: tuple[str, ...] = (
    "https://content.wb.ru/content/v2/cards/details",
    "https://content.wb.ru/content/v1/cards/detail",
)

_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WildberriesClientAsync/1.0)"
}

NM_PATTERN = re.compile(r"(?:(?:^|[^\d])(\d+)(?:[^\d]|$))")

__all__ = ["get_card_api", "get_info_card_json", "get_content_v2", "extract_nm"]


def extract_nm(url_or_id: object) -> int | None:
    """Return integer nm_id extracted from a string or integer."""
    if isinstance(url_or_id, int):
        return url_or_id if url_or_id > 0 else None
    if isinstance(url_or_id, str):
        candidate = url_or_id.strip()
        if candidate.isdigit():
            value = int(candidate)
            return value if value > 0 else None
        match = NM_PATTERN.search(candidate)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
    return None


# ---------------- Core async request helpers ----------------

async def _single_request(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any] | None:
    resp = None
    try:
        resp = await client.get(url, params=params, headers=_DEFAULT_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
        printable_url = getattr(resp, "url", url) if resp is not None else url
        print(f"[WARN] Request to {printable_url} failed: {exc}")
        return None


async def _request_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        data = await _single_request(client, url, params=params, timeout=timeout)
        if data is not None:
            return data
        if attempt < MAX_RETRIES:
            await _sleep_with_backoff(attempt)
    return {}


async def _sleep_with_backoff(attempt: int) -> None:
    delay = BACKOFF_FACTOR * (2 ** (attempt - 1))
    jitter = random.uniform(0, BACKOFF_JITTER)
    await asyncio.sleep(delay + jitter)


def _guess_basket_hosts(vol: int) -> list[int]:
    if vol < 0:
        return [9, 1, 2]

    index = bisect_left(_BASKET_VOL_THRESHOLDS, vol)
    primary_host = min(index + 1, MAX_BASKET_HOST)

    candidates = [primary_host]
    seen = {primary_host}

    base = max(1, (vol + 159) // 160)
    for candidate in (base, base - 1, base + 1, base - 2, base + 2):
        if 1 <= candidate <= MAX_BASKET_HOST and candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)
    return candidates


# ---------------- Public async functions ----------------

async def get_card_api(nm_id: object, *, timeout: float = DEFAULT_TIMEOUT) -> dict[str, Any]:
    nm = extract_nm(nm_id)
    if nm is None:
        print(f"[WARN] Unable to extract nm_id from {nm_id!r}")
        return {}

    params = dict(CARD_API_PARAMS)
    params["nm"] = nm

    async with httpx.AsyncClient() as client:
        return await _request_with_retries(client, CARD_API_URL, params=params, timeout=timeout)


async def get_info_card_json(nm_id: object, *, timeout: float = DEFAULT_TIMEOUT) -> dict[str, Any]:
    nm = extract_nm(nm_id)
    if nm is None:
        print(f"[WARN] Unable to extract nm_id from {nm_id!r}")
        return {}

    vol = nm // 100000
    part = nm // 1000
    hosts = _guess_basket_hosts(vol)

    async with httpx.AsyncClient() as client:
        attempt = 0
        for host in hosts:
            if not (1 <= host <= MAX_BASKET_HOST):
                continue
            attempt += 1
            url = BASKET_URL_TEMPLATE.format(host=host, vol=vol, part=part, nm=nm)
            data = await _single_request(client, url, timeout=timeout)
            if data is not None:
                return data
            if attempt >= MAX_RETRIES:
                break
            await _sleep_with_backoff(attempt)
    return {}


async def get_content_v2(nm_id: object, *, timeout: float = DEFAULT_TIMEOUT) -> dict[str, Any]:
    nm = extract_nm(nm_id)
    if nm is None:
        print(f"[WARN] Unable to extract nm_id from {nm_id!r}")
        return {}

    params = {"nm": nm}
    async with httpx.AsyncClient() as client:
        for attempt in range(1, MAX_RETRIES + 1):
            for url in CONTENT_V2_URLS:
                data = await _single_request(client, url, params=params, timeout=timeout)
                if data is not None:
                    return data
            if attempt < MAX_RETRIES:
                await _sleep_with_backoff(attempt)
    return {}
