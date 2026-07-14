from datetime import date, datetime
from zoneinfo import ZoneInfo

from services.shichu_formatter import extract_shichu_data, render_shichu_summary_html
from services.shichu_interactions import (
    detect_cross_interactions,
    detect_special_kanshi_relations,
)
from services.shichu_timing import build_timing
from services.shichusuimei_calc import Pillars4, calc_shichusuimei_from_payload


def _by_rule(items, rule_id):
    return [item for item in items if item["rule_id"] == rule_id]


def _fake_daiun(age_from=5.0):
    return {
        "items": [
            {"index": 1, "kanshi": "甲子", "age_from": age_from, "age_to": age_from + 10, "approx_start_year": 2005},
            {"index": 2, "kanshi": "乙丑", "age_from": age_from + 10, "age_to": age_from + 20, "approx_start_year": 2015},
            {"index": 3, "kanshi": "丙寅", "age_from": age_from + 20, "age_to": age_from + 30, "approx_start_year": 2025},
        ]
    }


def _fake_pillars():
    return Pillars4(year="甲子", month="乙酉", day="庚辰", hour="丁亥")


def test_cross_detection_only_emits_pairs_spanning_both_groups():
    items = detect_cross_interactions(
        [],
        [("year", "子"), ("month", "午"), ("day", "酉")],
        [],
        [("ryunen", "卯")],
        scope="ryunen_natal",
    )

    clashes = _by_rule(items, "branch_clash_v1")
    assert len(clashes) == 1
    assert clashes[0]["pillars"] == ["day", "ryunen"]
    assert clashes[0]["members"] == ["酉", "卯"]
    assert not any(item["members"] == ["子", "午"] for item in items)


def test_cross_trine_is_completed_by_ryunen_and_suppresses_half_trines():
    completed = detect_cross_interactions(
        [],
        [("year", "申"), ("day", "辰")],
        [],
        [("ryunen", "子")],
        scope="ryunen_natal",
    )

    trines = _by_rule(completed, "branch_trine_v1")
    assert len(trines) == 1
    assert trines[0]["detail"]["completed_by"] == "ryunen"
    assert trines[0]["pillars"] == ["year", "day", "ryunen"]
    assert _by_rule(completed, "branch_half_trine_v1") == []

    already_complete = detect_cross_interactions(
        [],
        [("year", "申"), ("month", "子"), ("day", "辰")],
        [],
        [("ryunen", "子")],
        scope="ryunen_natal",
    )
    assert _by_rule(already_complete, "branch_trine_v1") == []


def test_fukugin_and_hangin_rules():
    fukugin = detect_special_kanshi_relations(
        [("year", "庚午")], "庚午", "daiun", scope="daiun_natal"
    )
    assert _by_rule(fukugin, "fukugin_v1")[0]["members"] == ["庚午", "庚午"]

    hangin = detect_special_kanshi_relations(
        [("year", "甲子")], "庚午", "daiun", scope="daiun_natal"
    )
    assert len(_by_rule(hangin, "hangin_v1")) == 1

    no_stem_clash = detect_special_kanshi_relations(
        [("year", "戊子")], "己午", "daiun", scope="daiun_natal"
    )
    no_branch_clash = detect_special_kanshi_relations(
        [("year", "甲子")], "庚子", "daiun", scope="daiun_natal"
    )
    assert _by_rule(no_stem_clash, "hangin_v1") == []
    assert _by_rule(no_branch_clash, "hangin_v1") == []


def test_build_timing_selects_one_current_daiun_and_handles_pre_start():
    birth = datetime(2000, 6, 15, 12, tzinfo=ZoneInfo("Asia/Tokyo"))
    timing = build_timing(
        _fake_pillars(),
        _fake_daiun(),
        birth_local=birth,
        target_date=date(2026, 7, 14),
        tz_name="Asia/Tokyo",
        hour_known=True,
    )
    current = [item for item in timing["daiun_detail"] if item["is_current"]]
    assert len(current) == 1
    assert timing["current_daiun"] is current[0]
    assert current[0]["kanshi"] == "丙寅"

    pre_start = build_timing(
        _fake_pillars(),
        _fake_daiun(),
        birth_local=birth,
        target_date=date(2003, 7, 14),
        tz_name="Asia/Tokyo",
        hour_known=True,
    )
    assert pre_start["current_daiun"] is None
    assert "起運前" in pre_start["notes"]


def test_ryunen_base_year_uses_lichun_boundary():
    birth = datetime(2000, 6, 15, 12, tzinfo=ZoneInfo("Asia/Tokyo"))
    before = build_timing(
        _fake_pillars(), {}, birth_local=birth, target_date=date(2026, 2, 3),
        tz_name="Asia/Tokyo", hour_known=True,
    )
    after = build_timing(
        _fake_pillars(), {}, birth_local=birth, target_date=date(2026, 2, 5),
        tz_name="Asia/Tokyo", hour_known=True,
    )

    assert (before["ryunen"]["items"][0]["year"], before["ryunen"]["items"][0]["kanshi"]) == (2025, "乙巳")
    assert (after["ryunen"]["items"][0]["year"], after["ryunen"]["items"][0]["kanshi"]) == (2026, "丙午")


def test_2026_ryunen_has_correct_kanshi_and_ten_god():
    timing = build_timing(
        _fake_pillars(), {},
        birth_local=datetime(2000, 6, 15, 12, tzinfo=ZoneInfo("Asia/Tokyo")),
        target_date=date(2026, 7, 14), tz_name="Asia/Tokyo", hour_known=True,
    )
    current = timing["ryunen"]["items"][0]
    assert current["kanshi"] == "丙午"
    assert current["stem_ten_god"] == "偏官"


def test_timing_golden_birth_1990_05_15_tokyo():
    result = calc_shichusuimei_from_payload(
        {"year": 1990, "month": 5, "day": 15, "hour": 8, "minute": 30, "city": "東京", "gender": "female"},
        target_date=date(2026, 7, 14),
    )

    assert result["timing"]["current_daiun"]["kanshi"] == "丁丑"
    assert result["timing"]["current_daiun"]["stem_ten_god"] == "正官"
    current_ryunen = result["timing"]["ryunen"]["items"][0]
    assert (current_ryunen["year"], current_ryunen["kanshi"], current_ryunen["stem_ten_god"]) == (2026, "丙午", "偏官")


def test_default_target_date_preserves_existing_daiun_item_shape():
    result = calc_shichusuimei_from_payload(
        {"year": 1990, "month": 5, "day": 15, "hour": 8, "minute": 30, "gender": "female"}
    )

    assert result["engine_version"] == "0.5.0"
    assert result["normalized_data"]["timing"] == result["timing"]
    assert set(result["features"]["daiun"]["items"][0]) == {
        "index", "kanshi", "age_from", "age_to", "approx_start_year"
    }


def test_formatter_displays_current_daiun_and_ryunen_only_when_timing_exists():
    raw = {
        "system": "shichusuimei",
        "normalized_data": {
            "timing": {
                "current_daiun": {"kanshi": "丁丑", "stem_ten_god": "正官", "age_from": 33.08},
                "ryunen": {"items": [{"kanshi": "丙午", "stem_ten_god": "偏官", "is_current": True}]},
            }
        },
    }
    rendered = render_shichu_summary_html(extract_shichu_data(raw))
    assert "現大運：丁丑（正官・33.08歳頃〜）" in rendered
    assert "今年：丙午（偏官）" in rendered

    old_rendered = render_shichu_summary_html(extract_shichu_data({"system": "shichusuimei"}))
    assert "現大運：" not in old_rendered
    assert "今年：" not in old_rendered
