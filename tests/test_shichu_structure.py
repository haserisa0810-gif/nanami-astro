from datetime import date

import pytest

from services.shichu_formatter import extract_shichu_data, render_shichu_summary_html
from services.shichu_structure import (
    climate_candidates,
    element_power,
    fu_yi_candidates,
    month_command,
    pattern_candidates,
    strength_v2,
    yojin_candidates,
)
from services.shichusuimei_calc import Pillars4, calc_shichusuimei_from_payload


def _neutral_strength():
    return {"score": 50, "special_pattern_suspicion": False}


def test_month_command_boundaries_and_overflow():
    assert month_command("卯", 9.5)["commanding_stem"] == "甲"
    assert month_command("卯", 9.5)["phase"] == "余気"
    assert month_command("卯", 10.5)["commanding_stem"] == "乙"
    assert month_command("卯", 10.5)["phase"] == "本気"
    assert month_command("寅", 8.0)["commanding_stem"] == "丙"
    assert month_command("寅", 8.0)["phase"] == "中気"
    assert month_command("寅", 35.0)["commanding_stem"] == "甲"
    assert month_command("寅", 35.0)["phase"] == "本気"


def test_element_power_matches_hand_calculation():
    p4 = Pillars4(year="甲子", month="丙卯", day="戊辰", hour="庚酉")
    command = month_command("卯", 5.0)

    # 天干: 木火土金に各1.0
    # 年支子: 水1.0*0.8=0.8
    # 月支卯: 木1.0*2.0=2.0
    # 日支辰: 土1.0*1.2=1.2, 木0.5*1.2=0.6, 水0.3*1.2=0.36
    # 時支酉: 金1.0*1.0=1.0、司令甲: 木+0.5
    power = element_power(p4, command)

    assert power["raw"] == {"木": 4.1, "火": 1.0, "土": 2.2, "金": 2.0, "水": 1.16}
    assert sum(power["percent"].values()) == pytest.approx(100.0, abs=0.1)


def test_strength_v2_separates_supporters_and_excludes_day_stem_itself():
    p4 = Pillars4(year="甲子", month="丙卯", day="戊辰", hour="庚酉")
    power = element_power(p4, month_command("卯", 5.0))
    strength = strength_v2("戊", power)

    # 土2.2から日干自身1.0を引き、印の火1.0を足す = supporters 2.2
    assert strength["supporters"] == {"elements": ["土", "火"], "power": 2.2}
    assert strength["drainers"]["elements"] == ["金", "水", "木"]
    assert strength["drainers"]["power"] == pytest.approx(7.26)
    assert strength["score"] == 23
    assert strength["band"] == "弱め域"


def test_strength_v2_flags_an_element_at_55_percent_or_more():
    power = {
        "raw": {"木": 6.0, "火": 1.0, "土": 1.0, "金": 1.0, "水": 1.0},
        "percent": {"木": 60.0, "火": 10.0, "土": 10.0, "金": 10.0, "水": 10.0},
    }
    strength = strength_v2("甲", power)

    assert strength["special_pattern_suspicion"] is True
    assert "木が五行力量の55%以上" in strength["suspicion_reasons"]


def test_jianlu_month_blade_and_yin_stem_policy():
    jianlu = pattern_candidates(
        Pillars4("丙子", "戊寅", "甲辰", "庚申"), month_command("寅", 20), _neutral_strength()
    )
    blade = pattern_candidates(
        Pillars4("甲子", "戊午", "丙辰", "庚申"), month_command("午", 20), _neutral_strength()
    )
    yin = pattern_candidates(
        Pillars4("丙子", "戊卯", "乙辰", "庚申"), month_command("卯", 20), _neutral_strength()
    )

    assert [(item["name"], item["confidence"]) for item in jianlu["candidates"]] == [("建禄格", "high")]
    assert [(item["name"], item["confidence"]) for item in blade["candidates"]] == [("月刃格", "high")]
    assert [(item["name"], item["confidence"]) for item in yin["candidates"]] == [("建禄格", "high")]


def test_transparency_fallback_and_peer_hidden_stem_exclusion():
    main_visible = pattern_candidates(
        Pillars4("丙子", "己丑", "甲辰", "庚申"), month_command("丑", 20), _neutral_strength()
    )
    no_visible = pattern_candidates(
        Pillars4("丙子", "丁丑", "甲辰", "庚申"), month_command("丑", 20), _neutral_strength()
    )
    peer_visible = pattern_candidates(
        Pillars4("甲子", "丁亥", "甲辰", "庚申"), month_command("亥", 20), _neutral_strength()
    )

    assert [(item["name"], item["confidence"]) for item in main_visible["candidates"]] == [("正財格", "high")]
    assert [(item["name"], item["confidence"]) for item in no_visible["candidates"]] == [("正財格", "medium")]
    assert all(item["name"] not in ("比肩格", "劫財格") for item in peer_visible["candidates"])
    assert peer_visible["candidates"][0]["name"] == "偏印格"


def test_pattern_counter_evidence_contains_month_branch_clash():
    result = pattern_candidates(
        Pillars4("丁未", "己丑", "甲辰", "庚申"), month_command("丑", 20), _neutral_strength()
    )
    assert "月支に冲があり格の成立が揺らぐ" in result["candidates"][0]["counter_evidence"]


def test_fu_yi_strong_weak_hold_and_withheld_branches():
    strong = fu_yi_candidates("甲", {"score": 71, "special_pattern_suspicion": False})
    weak = fu_yi_candidates("甲", {"score": 30, "special_pattern_suspicion": False})
    hold = fu_yi_candidates("甲", {"score": 50, "special_pattern_suspicion": False})
    withheld = fu_yi_candidates("甲", {"score": 50, "special_pattern_suspicion": True})

    assert [item["element"] for item in strong["candidates"]] == ["火", "土", "金"]
    assert [item["element"] for item in weak["candidates"]] == ["水", "木"]
    assert hold["status"] == "hold" and hold["candidates"] == []
    assert withheld["status"] == "withheld" and withheld["candidates"] == []


def test_climate_and_agreement_rules():
    assert climate_candidates("子")["candidates"][0] == {
        "element": "火", "confidence": "medium", "rationale": ["寒月のため暖が必要"], "counter_evidence": []
    }
    assert climate_candidates("午")["candidates"][0]["element"] == "水"
    assert climate_candidates("卯")["candidates"] == []

    matched = yojin_candidates("甲", "子", {"score": 71, "special_pattern_suspicion": False})
    conflicted = yojin_candidates("甲", "子", {"score": 30, "special_pattern_suspicion": False})
    assert matched["agreement"] == {"matched_elements": ["火"], "conflicted": False}
    assert conflicted["agreement"] == {"matched_elements": [], "conflicted": True}


def test_structure_golden_case_and_backward_compatibility():
    result = calc_shichusuimei_from_payload(
        {"year": 1990, "month": 5, "day": 15, "hour": 8, "minute": 30, "city": "東京", "gender": "female"},
        target_date=date(2026, 7, 14),
    )

    command = result["normalized_data"]["hidden_stem_command"]
    assert (command["month_branch"], command["commanding_stem"], command["phase"]) == ("巳", "庚", "中気")
    assert [(item["name"], item["confidence"]) for item in result["structure_report"]["pattern_candidates"]["candidates"]] == [
        ("偏官格", "medium")
    ]
    assert result["structure_report"]["strength_index"]["score"] == 65
    assert result["features"]["main_hidden_stems"] == {"year": "丁", "month": "丙", "day": "戊", "hour": "戊"}
    assert result["engine_version"] == "0.5.0"


def test_formatter_displays_structure_candidates_and_hold_state():
    raw = {
        "system": "shichusuimei",
        "normalized_data": {"hidden_stem_command": {"commanding_stem": "丙", "phase": "中気"}},
        "structure_report": {
            "pattern_candidates": {"candidates": [{"name": "正官格", "confidence": "high"}]},
            "yojin_candidates": {
                "fu_yi": {"status": "proposed", "candidates": [{"element": "火"}, {"element": "土"}]},
                "climate": {"candidates": [{"element": "火"}]},
            },
        },
    }
    rendered = render_shichu_summary_html(extract_shichu_data(raw))
    assert "月令：丙司令（中気）" in rendered
    assert "格局候補：正官格（高）" in rendered
    assert "推奨五行：火・土（扶抑）／火（調候）" in rendered

    raw["structure_report"]["yojin_candidates"]["fu_yi"] = {"status": "hold", "candidates": []}
    held = render_shichu_summary_html(extract_shichu_data(raw))
    assert "推奨五行：保留（境界域）" in held
