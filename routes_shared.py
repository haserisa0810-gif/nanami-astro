from __future__ import annotations

import json
import os
import traceback
from typing import Any

from fastapi.templating import Jinja2Templates


templates = Jinja2Templates(directory="templates")


DEFAULT_INDEX_CONTEXT: dict[str, Any] = {
    "ai_text": "",
    "reader_text": "",
    "line_text": "",
    "unknowns": [],
    "inputs_json": {},
    "payload_json": {},
    "raw_json": {},
    "handoff_json": "",
    "handoff_yaml": "",
    "handoff_json_full": "",
    "handoff_yaml_full": "",
    "handoff_json_delta": "",
    "handoff_yaml_delta": "",
    "bias_guard": {},
    "intake_light_url": "https://pay.nanami-astro.com/menu/light",
    "intake_standard_url": "https://pay.nanami-astro.com/menu/standard",
    "intake_premium_url": "https://pay.nanami-astro.com/menu/premium",
    "base_light_url": "https://nanami-astro.stores.jp/items/69cfa794bd9ebe15115df30b",
    "base_standard_url": "https://nanami-astro.stores.jp/items/69cf9de50f4efd197fd2a6ae",
    "base_premium_url": "https://nanami-astro.stores.jp/items/69cf9d245106c217fa3850fc",
    "daily_card_url": "/daily-card",
    "line_add_friend_url": "https://line.me/R/ti/p/@281jnwon",
}


def startup_platform_safe() -> None:
    mode = (os.getenv("BOOTSTRAP_ON_STARTUP") or "safe").strip().lower()
    if mode in {"", "0", "false", "off", "skip", "disabled"}:
        print("startup bootstrap skipped")
        return
    try:
        from bootstrap_platform import init_db, seed_defaults
        from db import db_session

        init_db()
        with db_session() as db:
            seed_defaults(db)
        print("startup bootstrap completed")
    except Exception:
        print("startup bootstrap failed")
        traceback.print_exc()
        if mode == "strict":
            raise


def build_full_astrologer_summary(*args, **kwargs):
    from services.astrologer_summary import build_full_astrologer_summary

    return build_full_astrologer_summary(*args, **kwargs)


def parse_jsonish_response(raw: str, fallback: dict[str, Any]) -> dict[str, Any]:
    cleaned = (raw or "").strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    candidates: list[str] = []
    if cleaned:
        candidates.append(cleaned)
        start_obj = cleaned.find("{")
        end_obj = cleaned.rfind("}")
        if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
            candidates.append(cleaned[start_obj : end_obj + 1])
        start_arr = cleaned.find("[")
        end_arr = cleaned.rfind("]")
        if start_arr != -1 and end_arr != -1 and end_arr > start_arr:
            candidates.append(cleaned[start_arr : end_arr + 1])

    for cand in candidates:
        try:
            parsed = json.loads(cand)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {**fallback, "items": parsed}
        except Exception:
            pass

    merged = dict(fallback)
    merged.setdefault("raw_text", raw or "")
    merged["summary"] = cleaned or merged.get("summary") or "生成結果を取得できませんでした。"
    return merged
