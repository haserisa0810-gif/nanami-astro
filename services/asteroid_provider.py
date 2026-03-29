from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import requests

from config import FREEASTRO_BASE

DEFAULT_TIMEOUT = 12
DEFAULT_NATAL_PATH = "/api/v1/western/natal"


class AsteroidProviderError(RuntimeError):
    pass


def _get_timeout() -> int:
    raw = os.getenv("FREEASTRO_API_TIMEOUT", str(DEFAULT_TIMEOUT)).strip()
    try:
        return max(1, int(raw))
    except Exception:
        return DEFAULT_TIMEOUT


def _normalize_candidate_url(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        return None

    parsed = urlparse(raw)
    path = (parsed.path or "").rstrip("/")

    # ルートURLだけ入っていたら natal endpoint に補完する
    if path in {"", "/"}:
        return raw.rstrip("/") + DEFAULT_NATAL_PATH

    # docs / base API が指定されていても natal endpoint に寄せる
    if path in {"/docs", "/api", "/api/v1"}:
        return f"{parsed.scheme}://{parsed.netloc}{DEFAULT_NATAL_PATH}"

    return raw


def _default_api_url() -> str:
    return FREEASTRO_BASE.rstrip("/") + DEFAULT_NATAL_PATH


def _get_api_url() -> str:
    raw = os.getenv("FREEASTRO_API_URL", "")
    normalized = _normalize_candidate_url(raw)
    return normalized or _default_api_url()


def _get_api_key() -> str | None:
    raw = os.getenv("FREEASTRO_API_KEY", "").strip()
    return raw or None


def provider_config() -> dict[str, Any]:
    raw_url = os.getenv("FREEASTRO_API_URL", "").strip()
    return {
        "name": "freeastro",
        "api_url": _get_api_url(),
        "raw_api_url": raw_url or None,
        "has_api_key": _get_api_key() is not None,
        "timeout": _get_timeout(),
        "base": FREEASTRO_BASE,
    }


def is_configured() -> bool:
    return _get_api_key() is not None


def fetch_asteroids(payload: dict[str, Any], names: list[str]) -> dict[str, Any]:
    api_url = _get_api_url()
    api_key = _get_api_key()
    if not api_key:
        raise AsteroidProviderError("FREEASTRO_API_KEY が未設定です")

    request_payload = {
        "year": payload.get("year"),
        "month": payload.get("month"),
        "day": payload.get("day"),
        "hour": payload.get("hour", 12),
        "minute": payload.get("minute", 0),
        "lat": payload.get("lat"),
        "lng": payload.get("lng"),
        "city": payload.get("city"),
        "zodiac_type": payload.get("zodiac_type", "tropical"),
        "house_system": payload.get("house_system", "P"),
        # API側が未対応でも害が少ないように補助パラメータを複数送る
        "include_asteroids": True,
        "asteroids": names,
        "bodies": names,
    }

    try:
        response = requests.post(
            api_url,
            json=request_payload,
            headers={"x-api-key": api_key, "Content-Type": "application/json"},
            timeout=_get_timeout(),
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise AsteroidProviderError(f"FreeAstro asteroid API 呼び出し失敗: {e}") from e

    if not isinstance(data, dict):
        raise AsteroidProviderError("FreeAstro asteroid API の応答形式が不正です")

    planets = data.get("planets")
    if not isinstance(planets, list):
        raise AsteroidProviderError("FreeAstro asteroid API の応答に planets がありません")

    wanted = {n.lower(): n for n in names}
    normalized: list[dict[str, Any]] = []
    for item in planets:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        canonical = wanted.get(name.lower())
        if canonical is None:
            continue
        try:
            lon = float(item.get("lon"))
        except Exception:
            continue
        normalized.append(
            {
                "name": canonical,
                "lon": lon,
                "retrograde": bool(item.get("retrograde", False)),
            }
        )

    return {
        "provider": "freeastro",
        "planets": normalized,
        "raw_meta": data.get("meta", {}),
        "api_url": api_url,
    }
