"""Final request regression checks; providers are replaced before any API call."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from services import ai_report
from services.prompt_builder import build_role_prompt, get_age_system_prompt


@pytest.mark.parametrize("zero", [0, "0", 0.0])
def test_explicit_zero_is_preserved_in_integrated_distribution(zero):
    prompt = build_role_prompt({"western": zero, "shichu": 100, "vedic": zero})
    assert "西洋:0 / 四柱:100 / インド:0" in prompt
    assert "【西洋占星術】\n・使用しない" in prompt
    assert "【四柱推命】\n・主軸として使用" in prompt
    assert "【インド占星術】\n・使用しない" in prompt


def test_missing_values_keep_defaults_and_positive_integrated_roles():
    expected = build_role_prompt({"western": 45, "shichu": 45, "vedic": 10})
    assert build_role_prompt(None) == expected
    assert build_role_prompt({}) == expected
    assert build_role_prompt({"western": None}) == expected
    assert "【西洋占星術】\n・主軸として使用" in expected
    assert "西洋:45 / 四柱:45 / インド:10" in expected


@pytest.mark.parametrize("bad", [-1, "bad", "", True, 1.5, [], float("inf")])
def test_invalid_distribution_is_not_silently_replaced(bad):
    with pytest.raises(ValueError):
        build_role_prompt({"western": bad})


def test_all_zero_distribution_is_not_replaced_with_defaults():
    with pytest.raises(ValueError):
        build_role_prompt({"western": 0, "shichu": 0, "vedic": 0})


@pytest.mark.parametrize("age", ["child", "teen", "adult"])
def test_shichu_controls_override_stale_recommendation(age):
    bundle = ai_report._build_prompt_control_bundle(
        {"astrology_system": "shichusuimei", "age_mode": age},
        {"role_distribution": {"western": 100, "shichu": 0, "vedic": 100},
         "vedic_trigger": {"level": "strong"}},
    )
    assert bundle["role_distribution"] == {"western": 0, "shichu": 100, "vedic": 0}
    assert bundle["vedic_level"] == "off"
    assert "四柱推命のみを主軸" in bundle["role_prompt"]
    assert "西洋を主軸" not in bundle["age_system_prompt"]
    assert "四柱推命" in bundle["age_system_prompt"]


def test_integrated_distribution_stays_unchanged():
    distribution = {"western": 30, "shichu": 60, "vedic": 10}
    bundle = ai_report._build_prompt_control_bundle(
        {"astrology_system": "integrated3", "age_mode": "adult"},
        {"role_distribution": distribution, "vedic_trigger": {"level": "light"}},
    )
    assert bundle["role_distribution"] == distribution
    assert bundle["role_prompt"] == build_role_prompt(distribution, vedic_level="light")
    assert bundle["age_system_prompt"] == get_age_system_prompt("adult")


@pytest.fixture
def capture_generation(monkeypatch):
    calls = []
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-placeholder")
    monkeypatch.setenv("GEMINI_API_KEY", "test-placeholder")
    monkeypatch.setattr(ai_report, "Anthropic", lambda **kw: object())
    monkeypatch.setattr(ai_report, "genai", SimpleNamespace(Client=lambda **kw: object()))
    monkeypatch.setattr(ai_report, "types", object())

    def generate(**kwargs):
        calls.append(kwargs)
        return "6. 成長と転換期\n7. 実践アクション\n10. 実践アクション\n鑑定本文"

    monkeypatch.setattr(ai_report, "_generate_once_claude", generate)
    monkeypatch.setattr(ai_report, "_generate_once", generate)
    return calls


def fictional_input():
    return {
        "shichusuimei": {
            "system": "shichusuimei", "day_master": "甲",
            "summary": {"day_kanshi": "甲子"},
            "normalized_data": {"pillars": {"day": {"kanshi": "甲子"}}},
        },
        "western": {"planets": [{"name": "FORBIDDEN_WESTERN_PAYLOAD", "lon": 15}]},
        "vedic": {"ascendant": {"sign": "FORBIDDEN_VEDIC_PAYLOAD"}},
    }


@pytest.mark.parametrize("provider", ["claude", "gemini"])
@pytest.mark.parametrize("age", ["child", "teen", "adult"])
@pytest.mark.parametrize("style", ["general", "structured", "love", "work", "timing", "deep"])
@pytest.mark.parametrize("report_type", [
    "single_web", "single_line", "single_web_reader", "single_line_reader", "compat_web", "compat_line",
])
def test_final_shichu_request_contains_no_other_system_usage(capture_generation, provider, age, style, report_type):
    raw = fictional_input()
    before = deepcopy(raw)
    text = ai_report.generate_report(raw, report_type=report_type, meta={
        "astrology_system": "shichusuimei", "ai_provider": provider,
        "age_mode": age, "reading_style": style,
        "auto_recommendation": {
            "selected_systems": ["western", "vedic"],
            "role_distribution": {"western": 100, "shichu": 0, "vedic": 100},
            "vedic_trigger": {"level": "high", "score": 10, "reasons": ["FORBIDDEN_REASON"]},
        },
    })
    assert "鑑定本文" in text
    assert raw == before  # 送信データを絞っても保存用の原データは変更しない。
    assert len(capture_generation) == 1
    call = capture_generation[0]
    prompt = call["prompt"]
    combined = prompt + "\n" + call.get("system_prompt", "")
    assert "四柱推命のみを主軸" in combined
    assert "甲子" in prompt
    for incompatible in (
        "西洋を主軸", "同一ハウスの天体集中", "アスペクト、ハウス集中",
        "インド占星術の内容を必ず本文に含める", "土星3室合水星",
        "太陽が牡羊座にあり", "ハウス重なりによる影響領域",
        "【最終強制指示】", "FORBIDDEN_",
    ):
        assert incompatible not in combined
    assert "{vedic_usage_rules}" not in combined
    assert "{reader_evidence_example}" not in combined
    assert "{line_translation_example}" not in combined
    assert "{compat_influence_heading}" not in combined


@pytest.mark.parametrize("provider", ["claude", "gemini"])
def test_free_theme_still_uses_four_pillars_template(capture_generation, provider):
    ai_report.generate_report(fictional_input(), meta={
        "astrology_system": "shichusuimei", "ai_provider": provider, "theme": "free_reading",
    })
    assert "四柱推命データの参照先" in capture_generation[0]["prompt"]


@pytest.mark.parametrize("provider", ["claude", "gemini"])
def test_direct_legacy_chart_without_system_marker_is_preserved(capture_generation, provider):
    raw = fictional_input()["shichusuimei"]
    raw.pop("system")
    ai_report.generate_report(raw, meta={
        "astrology_system": "shichusuimei", "ai_provider": provider,
    })
    assert "甲子" in capture_generation[0]["prompt"]


def test_gemini_continuation_keeps_shichu_scope_and_data(monkeypatch, capture_generation):
    calls = []
    outputs = iter(["1. 基本構造", "10. 実践アクション"])

    def generate(**kwargs):
        calls.append(kwargs["prompt"])
        return next(outputs)

    monkeypatch.setattr(ai_report, "_generate_once", generate)
    text = ai_report.generate_report(fictional_input(), meta={
        "astrology_system": "shichusuimei", "ai_provider": "gemini",
    })
    assert "10. 実践アクション" in text
    assert len(calls) == 2
    assert "甲子" in calls[1] and "四柱推命のみを主軸" in calls[1]
    assert "西洋を主軸" not in calls[1]


def test_existing_non_shichu_reader_keeps_vedic_rules_and_example(capture_generation):
    ai_report.generate_report({}, report_type="single_web_reader", meta={
        "astrology_system": "integrated3", "ai_provider": "claude",
    })
    prompt = capture_generation[0]["prompt"]
    assert "インド占星術の内容を必ず本文に含める" in prompt
    assert "土星3室合水星" in prompt
