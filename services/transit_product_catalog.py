from __future__ import annotations

from calendar import monthrange
from datetime import date
from pathlib import Path
from typing import Any

DEFAULT_TRANSIT_VARIANT = "three_month_general"
PROMPTS_DIR = (Path(__file__).resolve().parents[1] / "prompts").resolve()

TRANSIT_PRODUCT_VARIANTS: list[dict[str, Any]] = [
    {
        "key": "three_month_general",
        "label": "3ヶ月運勢",
        "period_months": 3,
        "focus": "総合",
        "description": "全体の流れを広く読む標準版です。",
        "template_name": "transit_three_month_general",
        "prompt_file": "transit_three_month_general.txt",
        "available": True,
    },
    {
        "key": "three_month_work",
        "label": "3ヶ月運勢（仕事）",
        "period_months": 3,
        "focus": "仕事・キャリア",
        "description": "仕事、役割、評価、動き方に寄せて読む版です。",
        "template_name": "transit_three_month_work",
        "prompt_file": "transit_three_month_work.txt",
        "available": True,
    },
    {
        "key": "three_month_love",
        "label": "3ヶ月運勢（恋愛）",
        "period_months": 3,
        "focus": "恋愛・対人",
        "description": "恋愛、関係の進み方、気持ちの揺れを読む版です。",
        "template_name": "transit_three_month_love",
        "prompt_file": "transit_three_month_love.txt",
        "available": True,
    },
    {
        "key": "six_month_general",
        "label": "6ヶ月運勢",
        "period_months": 6,
        "focus": "総合",
        "description": "半年の流れを前半・後半で分けて読む版です。",
        "template_name": "transit_six_month_general",
        "prompt_file": "transit_six_month_general.txt",
        "available": True,
    },
    {
        "key": "one_year_general",
        "label": "1年運勢",
        "period_months": 12,
        "focus": "総合",
        "description": "将来追加する長期版の予約枠です。",
        "template_name": "transit_one_year_general",
        "prompt_file": "transit_one_year_general.txt",
        "available": False,
    },
]

TRANSIT_PRODUCT_VARIANT_MAP = {item["key"]: item for item in TRANSIT_PRODUCT_VARIANTS}


def get_transit_product_variant(key: str | None) -> dict[str, Any] | None:
    raw = (key or "").strip()
    if not raw:
        raw = DEFAULT_TRANSIT_VARIANT
    return TRANSIT_PRODUCT_VARIANT_MAP.get(raw) or TRANSIT_PRODUCT_VARIANT_MAP.get(DEFAULT_TRANSIT_VARIANT)


def list_transit_product_variants(*, include_unavailable: bool = True) -> list[dict[str, Any]]:
    items = [dict(item) for item in TRANSIT_PRODUCT_VARIANTS if include_unavailable or item.get("available")]
    return items


def _add_months(value: date, months: int) -> date:
    months = max(int(months or 0), 0)
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    day = min(value.day, monthrange(year, month)[1])
    return date(year, month, day)


def resolve_period_range(
    months: int,
    *,
    start_date: date | None = None,
) -> tuple[date, date]:
    start = start_date or date.today()
    return start, _add_months(start, months)


def resolve_variant_period_range(
    variant_key: str | None,
    *,
    start_date: date | None = None,
) -> dict[str, Any]:
    variant = get_transit_product_variant(variant_key) or TRANSIT_PRODUCT_VARIANT_MAP[DEFAULT_TRANSIT_VARIANT]
    start = start_date or date.today()
    end = _add_months(start, int(variant.get("period_months") or 3))
    return {
        "variant": dict(variant),
        "period_start": start,
        "period_end": end,
    }


def describe_transit_variant(variant_key: str | None) -> str:
    variant = get_transit_product_variant(variant_key)
    if not variant:
        return TRANSIT_PRODUCT_VARIANT_MAP[DEFAULT_TRANSIT_VARIANT]["label"]
    return str(variant.get("label") or DEFAULT_TRANSIT_VARIANT)


def read_transit_prompt(variant_key: str | None) -> str:
    variant = get_transit_product_variant(variant_key) or TRANSIT_PRODUCT_VARIANT_MAP[DEFAULT_TRANSIT_VARIANT]
    prompt_file = str(variant.get("prompt_file") or "").strip()
    if not prompt_file:
        raise FileNotFoundError("transit prompt file is not configured")
    path = (PROMPTS_DIR / prompt_file).resolve()
    if PROMPTS_DIR not in path.parents and path != PROMPTS_DIR:
        raise ValueError("invalid transit prompt path")
    if not path.exists():
        raise FileNotFoundError(f"Prompt template not found: {path}")
    return path.read_text(encoding="utf-8")
