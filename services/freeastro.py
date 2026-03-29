# src/services/freeastro.py
from __future__ import annotations

import logging
import os
from typing import Any, Dict

import requests

from config import FREEASTRO_BASE

logger = logging.getLogger(__name__)


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"{name} が未設定です（export {name}=...）")
    return v


def call_freeastro_natal(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    FreeAstro natal calculate を叩いてJSONを返す（sync）。
    例外時も r 未定義にならないようにして、原因がログに残るようにする。
    """
    api_key = require_env("FREEASTRO_API_KEY")
    url = f"{FREEASTRO_BASE}/api/v1/natal/calculate"

    r: requests.Response | None = None
    try:
        logger.warning("FREEASTRO_CALLED")

        r = requests.post(
            url,
            json=payload,
            headers={"x-api-key": api_key, "Content-Type": "application/json"},
            timeout=20,
        )

        # HTTPエラーならここで例外
        r.raise_for_status()

        # JSONとして返す
        return r.json()

    except Exception as e:
        status = getattr(r, "status_code", None)
        body = ""
        try:
            if r is not None and isinstance(r.text, str):
                body = r.text[:800]
        except Exception:
            body = ""

        raise RuntimeError(f"FreeAstroAPI error status={status} body={body} err={e}")
