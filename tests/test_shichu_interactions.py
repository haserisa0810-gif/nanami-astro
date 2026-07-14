from services.shichu_formatter import extract_shichu_data, render_shichu_summary_html
from services.shichu_interactions import detect_interactions, detect_shinsatsu
from services.shichusuimei_calc import calc_shichusuimei_from_payload


def _by_rule(items, rule_id):
    return [item for item in items if item["rule_id"] == rule_id]


def test_branch_clash_is_sorted_by_pillar_order():
    items = detect_interactions([], [("day", "子"), ("month", "午")])

    clashes = _by_rule(items, "branch_clash_v1")
    assert len(clashes) == 1
    assert clashes[0]["pillars"] == ["month", "day"]
    assert clashes[0]["members"] == ["午", "子"]


def test_branch_harmony_is_detected():
    items = detect_interactions([], [("year", "子"), ("month", "丑")])

    harmonies = _by_rule(items, "branch_harmony_v1")
    assert len(harmonies) == 1
    assert harmonies[0]["type"] == "支合"
    assert harmonies[0]["detail"]["transformation"] == "not_evaluated"


def test_trine_and_half_trine_are_distinguished():
    full = detect_interactions([], [("year", "申"), ("month", "子"), ("day", "辰")])
    assert len(_by_rule(full, "branch_trine_v1")) == 1
    assert _by_rule(full, "branch_half_trine_v1") == []

    half = detect_interactions([], [("year", "申"), ("day", "子")])
    assert len(_by_rule(half, "branch_half_trine_v1")) == 1

    no_peak = detect_interactions([], [("year", "申"), ("day", "辰")])
    assert _by_rule(no_peak, "branch_half_trine_v1") == []


def test_self_punishment_is_detected_per_pillar_pair():
    items = detect_interactions([], [("month", "午"), ("hour", "午")])

    punishments = _by_rule(items, "branch_self_punishment_v1")
    assert len(punishments) == 1
    assert punishments[0]["pillars"] == ["month", "hour"]


def test_harm_destruction_and_stem_combination():
    harm = detect_interactions([], [("year", "子"), ("day", "未")])
    destruction = detect_interactions([], [("year", "子"), ("day", "酉")])
    stem_combination = detect_interactions([("year", "甲"), ("day", "己")], [])

    assert len(_by_rule(harm, "branch_harm_v1")) == 1
    assert len(_by_rule(destruction, "branch_destruction_v1")) == 1
    combinations = _by_rule(stem_combination, "stem_combination_v1")
    assert len(combinations) == 1
    assert combinations[0]["detail"] == {"element": "土", "transformation": "not_evaluated"}


def test_shinsatsu_tenotsu_kaigou_and_yin_stem_policy():
    tenotsu = detect_shinsatsu(
        [("day", "甲")],
        [("year", "丑"), ("month", "未"), ("day", "辰")],
    )
    item = _by_rule(tenotsu, "tenotsu_kijin_v1")[0]
    assert item["found_in"] == ["year", "month"]
    assert item["members"] == ["丑", "未"]

    kaigou = detect_shinsatsu([("day", "庚")], [("day", "辰")])
    assert _by_rule(kaigou, "kaigou_v1")[0]["members"] == ["庚辰"]

    yin_stem = detect_shinsatsu(
        [("day", "乙")],
        [("year", "辰"), ("month", "卯"), ("day", "寅"), ("hour", "酉")],
    )
    assert _by_rule(yin_stem, "youjin_v1") == []


def test_only_hour_related_results_are_provisional_when_hour_is_unknown():
    items = detect_interactions(
        [],
        [("year", "子"), ("month", "丑"), ("day", "辰"), ("hour", "午")],
        hour_known=False,
    )

    assert items
    for item in items:
        assert item["provisional"] is ("hour" in item["pillars"])

    shinsatsu = detect_shinsatsu(
        [("day", "甲")],
        [("year", "丑"), ("month", "辰"), ("day", "子"), ("hour", "未")],
        hour_known=False,
    )
    tenotsu = _by_rule(shinsatsu, "tenotsu_kijin_v1")[0]
    assert tenotsu["found_in"] == ["year", "hour"]
    assert tenotsu["provisional"] is True


def test_calc_keeps_existing_keys_and_adds_interactions():
    result = calc_shichusuimei_from_payload(
        {"year": 1990, "month": 5, "day": 15, "hour": 8, "minute": 30, "city": "東京", "gender": "female"}
    )

    for key in ("pillars", "summary", "features", "normalized_data"):
        assert key in result
    assert "pillars" in result["normalized_data"]
    assert result["engine_version"] == "0.3.0"
    assert result["normalized_data"]["interactions"] == result["interactions"]
    assert result["normalized_data"]["shinsatsu"] == result["shinsatsu"]
    assert result["time_certainty"]["hour_known"] is True


def test_calc_marks_missing_hour_as_unknown_without_changing_fallback_pillar_shape():
    result = calc_shichusuimei_from_payload(
        {"year": 1990, "month": 5, "day": 15, "gender": "female"}
    )

    assert result["time_certainty"]["hour_known"] is False
    assert isinstance(result["pillars"]["hour"], str)
    assert len(result["pillars"]["hour"]) == 2
    for item in result["interactions"]["items"]:
        assert item["provisional"] is ("hour" in item["pillars"])
    for item in result["shinsatsu"]["items"]:
        assert item["provisional"] is ("hour" in item["found_in"])


def test_golden_birth_1990_05_15_tokyo():
    result = calc_shichusuimei_from_payload(
        {"year": 1990, "month": 5, "day": 15, "hour": 8, "minute": 30, "city": "東京", "gender": "female"}
    )

    assert result["summary"] == {
        "year_kanshi": "庚午",
        "month_kanshi": "辛巳",
        "day_kanshi": "庚辰",
        "hour_kanshi": "庚辰",
    }
    assert [(item["rule_id"], item["pillars"]) for item in result["interactions"]["items"]] == [
        ("branch_self_punishment_v1", ["day", "hour"]),
    ]
    assert [(item["name"], item["found_in"]) for item in result["shinsatsu"]["items"]] == [
        ("華蓋", ["day", "hour"]),
        ("魁罡", ["day"]),
    ]


def test_formatter_displays_interactions_shinsatsu_and_provisional_note():
    raw = {
        "system": "shichusuimei",
        "normalized_data": {
            "interactions": {"items": [{"type": "冲", "members": ["子", "午"], "provisional": True}]},
            "shinsatsu": {"items": [{"name": "天乙貴人", "found_in": ["month"], "provisional": False}]},
        },
    }

    data = extract_shichu_data(raw)
    rendered = render_shichu_summary_html(data)
    assert "命式内の作用：子午 冲 ※時刻未確定" in rendered
    assert "神殺：天乙貴人（月支）" in rendered
