from __future__ import annotations

"""四柱推命の大運深化と立春基準の流年計算。"""

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from services.shichu_interactions import (
    detect_cross_interactions,
    detect_three_punishments,
    detect_special_kanshi_relations,
)
from services.shichusuimei_calc import (
    KANSHI_60,
    HIDDEN_STEMS,
    STEM_ELEMENT,
    BRANCH_ELEMENT,
    Pillars4,
    _lichun_utc_for_year,
    ten_god,
    twelve_fortune,
    kubo_from_day_kanshi,
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


def _daiun_context(p4: Pillars4, daiun: dict, ryunen: str, *, hour_known: bool) -> dict:
    _, natal_stems, natal_branches = _natal_inputs(p4)
    dk = daiun["kanshi"]
    stems, branches = [("ryunen", ryunen[0])], [("ryunen", ryunen[1])]
    return {
        **{k: daiun.get(k) for k in ("kanshi", "age_from", "age_to", "stem_ten_god", "branch_twelve_fortune")},
        "interactions_daiun": detect_cross_interactions(
            [("daiun", dk[0])], [("daiun", dk[1])], stems, branches,
            scope="ryunen_daiun", hour_known=hour_known,
        ),
        "special_daiun": detect_special_kanshi_relations(
            [("daiun", dk)], ryunen, "ryunen", scope="ryunen_daiun", hour_known=hour_known,
        ),
        "interactions_combined": [item for item in detect_cross_interactions(
            [*natal_stems, ("daiun", dk[0])], [*natal_branches, ("daiun", dk[1])],
            stems, branches, scope="ryunen_natal_daiun", hour_known=hour_known,
        ) if "daiun" in item["pillars"] and len(item["pillars"]) >= 3],
        "three_punishments": [
            {**item, "scope": "ryunen_combined"}
            for item in detect_three_punishments([*natal_branches, ("daiun", dk[1]), *branches], hour_known=hour_known)
            if "ryunen" in item["pillars"]
        ],
    }


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
        # 各年の同じ月日を参照する。未来の流年へ現在の大運を使い回さない。
        reference_date = _birthday_in_year(target_date, target_date.year + offset)
        starts_at = _lichun_utc_for_year(year, tz_name).astimezone(tz)
        ends_at = _lichun_utc_for_year(year + 1, tz_name).astimezone(tz)
        # 立春日が年によってずれても参照時点を対象流年内に保つ。
        reference_at = max(starts_at, min(
            datetime.combine(reference_date, time.min, tzinfo=tz),
            ends_at - timedelta(microseconds=1),
        ))
        reference_date = reference_at.date()
        reference_age = _age_in_completed_months(birth_local.date(), reference_date)
        year_daiun = next((item for item in daiun_detail
                           if isinstance(item.get("age_from"), (int, float))
                           and isinstance(item.get("age_to"), (int, float))
                           and item["age_from"] <= reference_age < item["age_to"]), None)
        start_age = _age_in_completed_months(birth_local.date(), starts_at.date())
        end_age = _age_in_completed_months(birth_local.date(), ends_at.date())
        overlaps = [item for item in daiun_detail
                    if isinstance(item.get("age_from"), (int, float))
                    and isinstance(item.get("age_to"), (int, float))
                    and item["age_from"] <= end_age and item["age_to"] > start_age]
        kanshi = KANSHI_60[(year - 4) % 60]
        ryunen_stems = [("ryunen", kanshi[0])]
        ryunen_branches = [("ryunen", kanshi[1])]
        daiun_context = _daiun_context(p4, year_daiun, kanshi, hour_known=hour_known) if year_daiun else {}
        # 三刑の完成は二支だけの刑と区別する。流年を含まない組は除外する。
        all_branches = [*natal_branches, *ryunen_branches]
        if year_daiun:
            all_branches.append(("daiun", year_daiun["kanshi"][1]))
        punishments = [
            {**item, "scope": "ryunen_combined"}
            for item in detect_three_punishments(all_branches, hour_known=hour_known)
            if "ryunen" in item["pillars"]
        ]
        ryunen_items.append({
            "year": year,
            "kanshi": kanshi,
            "applies_from": "lichun",
            "is_current": offset == 0,
            "reference_date": reference_date.isoformat(),
            "reference_at": reference_at.isoformat(),
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "current_daiun": year_daiun,
            "daiun_overlaps": [_daiun_context(p4, item, kanshi, hour_known=hour_known) for item in overlaps],
            "daiun_changes_in_year": len(overlaps) > 1 or bool(overlaps and start_age < overlaps[0]["age_from"]),
            "stem": kanshi[0],
            "branch": kanshi[1],
            "stem_element": STEM_ELEMENT[kanshi[0]],
            "branch_element": BRANCH_ELEMENT[kanshi[1]],
            "hidden_stems": [{"stem": stem, "ten_god": ten_god(day_stem, stem)}
                             for stem in HIDDEN_STEMS[kanshi[1]]],
            "kubo": kanshi[1] in kubo_from_day_kanshi(p4.day)["empty_branches"],
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
            "interactions_daiun": daiun_context.get("interactions_daiun", []),
            "special_natal": detect_special_kanshi_relations(
                natal_pillars,
                kanshi,
                "ryunen",
                scope="ryunen_natal",
                hour_known=hour_known,
            ),
            "special_daiun": daiun_context.get("special_daiun", []),
            "interactions_combined": daiun_context.get("interactions_combined", []),
            "three_punishments": punishments,
        })

    return {
        "engine": "shichu_timing_v2",
        "target_date": target_date.isoformat(),
        "timezone": tz_name,
        "hour_known": hour_known,
        "year_boundary_rule": "lichun(315deg)",
        "reference_time": "00:00",
        "daiun_calc_mode": "approx",
        "current_daiun": current_daiun,
        "daiun_detail": daiun_detail,
        "ryunen": {"items": ryunen_items},
        "notes": notes,
    }


def annual_fortune_from_timing(timing: dict[str, Any]) -> dict[str, Any]:
    """Products FULL の年運の主要フィールドも同じ計算結果から公開する。"""
    current = next((item for item in timing["ryunen"]["items"] if item["is_current"]), None)
    if not current:
        return {}
    relations = [*current["interactions_natal"], *current["interactions_daiun"]]
    return {
        **current,
        "year": date.fromisoformat(timing["target_date"]).year,
        "effective_year": current["year"],
        "ten_god_to_day_master": current["stem_ten_god"],
        "twelve_fortune_to_day_master": current["branch_twelve_fortune"],
        "relations": {
            "branch_relations": [item for item in relations if item["category"] == "branch"],
            "stem_relations": [item for item in relations if item["category"] == "stem"],
            "triads_with_natal": [item for item in current["interactions_natal"] if item["type"] in {"三合", "方合"}],
            "triads_with_natal_and_daiun": [*[
                item for item in current["interactions_natal"] if item["type"] in {"三合", "方合"}
            ], *current["interactions_combined"]],
            "three_punishments_with_natal": [item for item in current["three_punishments"] if "daiun" not in item["pillars"]],
            "three_punishments_with_natal_and_daiun": current["three_punishments"],
        },
        "debug": {
            "calculated_for": timing["target_date"],
            "lichun_local": current["starts_at"],
            "year_boundary_rule": timing["year_boundary_rule"],
        },
    }
