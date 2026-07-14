from __future__ import annotations

"""四柱推命の大運深化と立春基準の流年計算。"""

from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from services.shichu_interactions import (
    detect_cross_interactions,
    detect_special_kanshi_relations,
)
from services.shichusuimei_calc import (
    KANSHI_60,
    Pillars4,
    _lichun_utc_for_year,
    ten_god,
    twelve_fortune,
)


def _birthday_in_year(birth_date: date, year: int) -> date:
    try:
        return birth_date.replace(year=year)
    except ValueError:
        return date(year, 2, 28)


def _age_in_completed_months(birth_date: date, target_date: date) -> float:
    """誕生日基準の満年齢に、誕生日後の経過月を加えた年齢。"""
    years = target_date.year - birth_date.year
    birthday = _birthday_in_year(birth_date, target_date.year)
    if target_date < birthday:
        years -= 1
        last_birthday = _birthday_in_year(birth_date, target_date.year - 1)
    else:
        last_birthday = birthday
    months = (target_date.year - last_birthday.year) * 12 + target_date.month - last_birthday.month
    if target_date.day < last_birthday.day:
        months -= 1
    return round(years + max(0, months) / 12.0, 6)


def _natal_inputs(p4: Pillars4) -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[tuple[str, str]]]:
    pillars = [("year", p4.year), ("month", p4.month), ("day", p4.day), ("hour", p4.hour)]
    stems = [(name, kanshi[0]) for name, kanshi in pillars]
    branches = [(name, kanshi[1]) for name, kanshi in pillars]
    return pillars, stems, branches


def build_timing(
    p4: Pillars4,
    daiun: dict,
    *,
    birth_local: datetime,
    target_date: date,
    tz_name: str,
    hour_known: bool,
    n_ryunen: int = 3,
) -> dict[str, Any]:
    """大運の深化情報と、基準日から始まる流年を生成する。"""
    tz = ZoneInfo(tz_name)
    if birth_local.tzinfo is None:
        birth_local = birth_local.replace(tzinfo=tz)
    else:
        birth_local = birth_local.astimezone(tz)

    natal_pillars, natal_stems, natal_branches = _natal_inputs(p4)
    day_stem = p4.day[0]
    target_age = _age_in_completed_months(birth_local.date(), target_date)
    notes: list[str] = []
    daiun_detail: list[dict[str, Any]] = []
    current_daiun: dict[str, Any] | None = None

    for raw_item in daiun.get("items", []) if isinstance(daiun.get("items"), list) else []:
        kanshi = str(raw_item.get("kanshi") or "")
        if len(kanshi) < 2:
            continue
        age_from = raw_item.get("age_from")
        age_to = raw_item.get("age_to")
        is_current = (
            isinstance(age_from, (int, float))
            and isinstance(age_to, (int, float))
            and age_from <= target_age < age_to
        )
        transit_stems = [("daiun", kanshi[0])]
        transit_branches = [("daiun", kanshi[1])]
        detail = {
            "index": raw_item.get("index"),
            "kanshi": kanshi,
            "age_from": age_from,
            "age_to": age_to,
            "approx_start_year": raw_item.get("approx_start_year"),
            "stem_ten_god": ten_god(day_stem, kanshi[0]),
            "branch_twelve_fortune": twelve_fortune(day_stem, kanshi[1]),
            "interactions_natal": detect_cross_interactions(
                natal_stems,
                natal_branches,
                transit_stems,
                transit_branches,
                scope="daiun_natal",
                hour_known=hour_known,
            ),
            "special": detect_special_kanshi_relations(
                natal_pillars,
                kanshi,
                "daiun",
                scope="daiun_natal",
                hour_known=hour_known,
            ),
            "is_current": is_current,
        }
        daiun_detail.append(detail)
        if is_current and current_daiun is None:
            current_daiun = detail

    if daiun_detail:
        first_age = daiun_detail[0].get("age_from")
        if isinstance(first_age, (int, float)) and target_age < first_age:
            current_daiun = None
            notes.append("起運前")

    target_local = datetime.combine(target_date, time.min, tzinfo=tz)
    lichun_local = _lichun_utc_for_year(target_date.year, tz_name).astimezone(tz)
    base_year = target_date.year if target_local >= lichun_local else target_date.year - 1
    ryunen_items: list[dict[str, Any]] = []

    for offset in range(max(0, int(n_ryunen))):
        year = base_year + offset
        kanshi = KANSHI_60[(year - 4) % 60]
        ryunen_stems = [("ryunen", kanshi[0])]
        ryunen_branches = [("ryunen", kanshi[1])]
        interactions_daiun: list[dict] = []
        special_daiun: list[dict] = []
        if current_daiun:
            daiun_kanshi = str(current_daiun["kanshi"])
            interactions_daiun = detect_cross_interactions(
                [("daiun", daiun_kanshi[0])],
                [("daiun", daiun_kanshi[1])],
                ryunen_stems,
                ryunen_branches,
                scope="ryunen_daiun",
                hour_known=hour_known,
            )
            special_daiun = detect_special_kanshi_relations(
                [("daiun", daiun_kanshi)],
                kanshi,
                "ryunen",
                scope="ryunen_daiun",
                hour_known=hour_known,
            )
        ryunen_items.append({
            "year": year,
            "kanshi": kanshi,
            "applies_from": "lichun",
            "is_current": offset == 0,
            "stem_ten_god": ten_god(day_stem, kanshi[0]),
            "branch_twelve_fortune": twelve_fortune(day_stem, kanshi[1]),
            "interactions_natal": detect_cross_interactions(
                natal_stems,
                natal_branches,
                ryunen_stems,
                ryunen_branches,
                scope="ryunen_natal",
                hour_known=hour_known,
            ),
            "interactions_daiun": interactions_daiun,
            "special_natal": detect_special_kanshi_relations(
                natal_pillars,
                kanshi,
                "ryunen",
                scope="ryunen_natal",
                hour_known=hour_known,
            ),
            "special_daiun": special_daiun,
        })

    return {
        "engine": "shichu_timing_v1",
        "target_date": target_date.isoformat(),
        "current_daiun": current_daiun,
        "daiun_detail": daiun_detail,
        "ryunen": {"items": ryunen_items},
        "notes": notes,
    }
