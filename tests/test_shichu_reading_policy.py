"""Reading protocol checks; these are not empirical LLM-quality scores."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from services import ai_report
from services.astrologer_summary import _shichu_themes
from services.bias_guard import GuardResult, build_fix_instructions, validate_generated_text
from services.shichu_reading_context import build_reading_context, project_for_reading
from services.shichu_structure import build_structure_analysis
from services.shichusuimei_calc import Pillars4
from services.style_prompts import get_style_prompt


def chart(band="弱め域", *, hour_known=True, special=False, proposal="proposed"):
    return {
        "system": "shichusuimei", "day_master": "甲",
        "normalized_data": {"pillars": {"day": {"kanshi": "甲子"}}, "timing": {"hour_known": hour_known}},
        "features": {"strength": {"label": "OLD_STRENGTH_SENTINEL", "score": 53}},
        "structure_report": {
            "day_master": "甲", "strength_index": {"label": "OLD_STRENGTH_SENTINEL", "score": 53},
            "strength_v2": {"band": band, "score": 30, "special_pattern_suspicion": special},
            "pattern_candidates": {"candidates": [{"name": "七殺格", "confidence": "high", "counter_evidence": ["反証"]}]},
            "yojin_candidates": {"fu_yi": {"status": proposal, "candidates": []}},
        },
    }


@pytest.mark.parametrize("band,hour,special,expected", [
    ("弱め域", True, False, "candidate"), ("強め域", True, False, "candidate"),
    ("中庸域", True, False, "ambiguous"), ("弱め域", False, False, "ambiguous"),
    ("強め域", True, True, "ambiguous"),
])
def test_existing_strength_state_is_preserved_without_claiming_confirmation(band, hour, special, expected):
    data = chart(band, hour_known=hour, special=special)
    context = build_reading_context(data)
    assert context["strength"]["status"] == expected
    assert context["pattern_candidates"]["status"] == "candidate"
    assert context["strength"]["source"] == "structure_report.strength_v2"


@pytest.mark.parametrize("status", ["hold", "withheld"])
def test_held_element_recommendations_remain_ambiguous(status):
    context = build_reading_context(chart(proposal=status))
    assert context["element_recommendation"]["selection_status"] == status
    assert context["element_recommendation"]["status"] == "ambiguous"


def test_projection_hides_legacy_labels_without_mutating_saved_data():
    data = chart()
    before = deepcopy(data)
    projected = project_for_reading(data)
    assert data == before
    assert "strength" not in projected["features"]
    assert "strength_index" not in projected["structure_report"]
    assert projected["structure_report"]["strength_v2"] == data["structure_report"]["strength_v2"]
    assert projected["structure_report"]["pattern_candidates"] == data["structure_report"]["pattern_candidates"]
    assert projected["reading_context"]["strength"]["legacy_values_omitted"]


def test_legacy_only_data_is_retained_as_reference_and_missing_data_is_not_balanced():
    data = chart()
    data["structure_report"].pop("strength_v2")
    projected = project_for_reading(data)
    assert projected["features"]["strength"] == data["features"]["strength"]
    assert projected["reading_context"]["strength"]["role"] == "legacy_reference"
    assert build_reading_context({})["strength"]["status"] == "ambiguous"
    assert build_reading_context({})["strength"]["source"] is None


def test_astrologer_summary_uses_weighted_candidate_instead_of_old_verdict():
    theme = next(t for t in _shichu_themes(chart()) if t["id"] == "shichu_balance")
    assert "力量方式の候補" in theme["summary"]
    assert "OLD_STRENGTH_SENTINEL" not in str(theme)


@pytest.mark.parametrize("provider", ["claude", "gemini"])
@pytest.mark.parametrize("system", ["shichusuimei", "integrated_w_shichu", "integrated3"])
@pytest.mark.parametrize("trace", [False, True])
def test_final_prompt_uses_projection_policy_and_opt_in_trace(monkeypatch, provider, system, trace):
    calls = []
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-placeholder")
    monkeypatch.setenv("GEMINI_API_KEY", "test-placeholder")
    monkeypatch.setattr(ai_report, "Anthropic", lambda **kw: object())
    monkeypatch.setattr(ai_report, "genai", SimpleNamespace(Client=lambda **kw: object()))
    monkeypatch.setattr(ai_report, "types", object())
    def fake(**kw):
        calls.append(kw)
        return "6. 成長と転換期\n7. 実践アクション"
    monkeypatch.setattr(ai_report, "_generate_once_claude", fake)
    monkeypatch.setattr(ai_report, "_generate_once", fake)
    data = chart("中庸域", proposal="hold")
    raw = data if system == "shichusuimei" else {"shichusuimei": data}
    before = deepcopy(raw)
    ai_report.generate_report(raw, meta={"ai_provider": provider, "astrology_system": system, "include_reading_trace": trace})
    assert len(calls) == 1 and raw == before
    prompt = calls[0]["prompt"]
    assert "OLD_STRENGTH_SENTINEL" not in prompt
    assert "ambiguous" in prompt and "候補" in prompt
    assert "主要構造を最大2つ" in prompt and "反証" in prompt
    assert "根拠のある厳しい読みを励ましで消さない" in prompt
    assert ("【検証用構造記録" in prompt) is trace


@pytest.mark.parametrize("provider", ["claude", "gemini"])
@pytest.mark.parametrize("system", ["integrated_w_shichu", "integrated3"])
@pytest.mark.parametrize("style", ["love", "work", "relationship", "timing"])
def test_combined_specialized_prompts_do_not_reintroduce_old_story(monkeypatch, provider, system, style):
    calls = []
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-placeholder")
    monkeypatch.setenv("GEMINI_API_KEY", "test-placeholder")
    monkeypatch.setattr(ai_report, "Anthropic", lambda **kw: object())
    monkeypatch.setattr(ai_report, "genai", SimpleNamespace(Client=lambda **kw: object()))
    monkeypatch.setattr(ai_report, "types", object())
    def fake(**kw):
        calls.append(kw)
        return "6. 成長と転換期\n7. 実践アクション"
    monkeypatch.setattr(ai_report, "_generate_once_claude", fake)
    monkeypatch.setattr(ai_report, "_generate_once", fake)
    ai_report.generate_report({"shichusuimei": chart()}, meta={
        "ai_provider": provider, "astrology_system": system, "reading_style": style,
    })
    prompt = calls[0]["prompt"]
    for old in ("その無自覚な代償", "関係性の中での自己消失", "ストレスが身体に出やすい部位", "甲』の柔らかさ", "{focused_reading_prompt}"):
        assert old not in prompt
    assert "主要構造を最大2つ" in prompt and "反証" in prompt


@pytest.mark.parametrize("plan", ["light", "standard", "premium"])
def test_external_chapter_builder_carries_policy_without_importing_db_app(plan):
    # Load the real pure prompt function without its unrelated SVG/model imports.
    import ast
    import json
    from pathlib import Path
    from typing import Any
    path = Path(ai_report.__file__).with_name("external_report_template_renderer.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "build_chapter_json_prompt")
    namespace = {"Any": Any, "json": json, "TEMPLATE_DIR": path.parents[1] / "templates",
                 "chapter_specs": lambda *a: [{"num": "01", "title": "基本性格・本質"}]}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    prompt = namespace["build_chapter_json_prompt"](
        SimpleNamespace(customer_name="架空", consultation_text="仕事"),
        plan=plan, handoff_yaml="fictional chart", report_options={},
    )
    assert "主要構造を最大2つ" in prompt
    assert "候補の high を成立や本人の経験の確認に変えない" in prompt
    assert "まず人物像を一つ作ります" not in prompt


def test_shortened_chapters_do_not_trigger_false_continuation():
    assert not ai_report._is_incomplete_web("## 6. 実践アクション\n具体策", astrology_system="shichusuimei")
    assert ai_report._is_incomplete_web("本文で実践アクションに触れただけ", astrology_system="shichusuimei")


@pytest.mark.parametrize("style", ["structured", "love", "work", "deep"])
def test_styles_keep_supported_conflict_without_mandatory_suffering(style):
    text = get_style_prompt(style, "shichusuimei")
    assert text
    for old_requirement in ("最初はうまくいくのに壊れやすい", "傷ついていても切れない", "説明より体感重視", "ズレやすい前提"):
        assert old_requirement not in text


def test_regeneration_does_not_invent_accidents_or_strengthen_certainty():
    instructions = build_fix_instructions(GuardResult(False, ["曖昧語が多い"]), "single_web")
    assert "事故導線（発火条件→行動→結果）を具体化する" not in instructions
    assert "候補・保留を維持" in instructions
    assert "根拠のある厳しい傾向は残し" in instructions
    assert "断定や深刻さで解消しない" in instructions


def test_stable_reader_memo_passes_without_eight_risk_bullets():
    text = "\n".join([
        "### 1) 主要構造と根拠", "月令と支えが一致する候補。",
        "### 2) 現れ方と条件", "役割と裁量が一致する環境を検討。",
        "### 3) 現状確認の質問", "どの場面で力を使えますか。",
        "### 4) 伝え方", "条件に沿って伝える。",
        "### 5) 活かし方と注意点", "確認した範囲では問題ない。未確認のリスクを補わない。",
    ])
    assert validate_generated_text(text=text, report_type="single_web_reader").ok


@pytest.mark.parametrize("pillars", [
    ("甲寅", "乙卯", "甲寅", "甲子"), ("庚申", "辛酉", "甲午", "庚申"),
    ("甲子", "己丑", "丙午", "辛卯"), ("丙子", "戊寅", "甲辰", "庚申"),
    ("丁未", "己丑", "甲辰", "庚申"), ("丙寅", "丙寅", "甲子", "戊辰"),
    ("丙寅", "辛酉", "丙申", "戊戌"), ("甲子", "丙卯", "戊辰", "庚酉"),
])
def test_diverse_computed_materials_survive_reading_projection(pillars):
    computed = build_structure_analysis(Pillars4(*pillars), 20)
    original = {"structure_report": computed, "normalized_data": {"timing": {"hour_known": True}}}
    projected = project_for_reading(original)
    assert projected["structure_report"] == original["structure_report"]
    assert projected["reading_context"]["strength"]["status"] in {"candidate", "ambiguous"}
    assert projected["structure_report"]["pattern_candidates"] == computed["pattern_candidates"]
