import json
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from jinja2 import Environment, FileSystemLoader

from services.ai_report import _build_structure_summary
from services.astrologer_summary import build_full_astrologer_summary, render_astrologer_memo
from services.handoff_log import build_handoff
from services.shichu_annual_reading import build_annual_themes
from services.shichu_formatter import extract_shichu_data, render_shichu_summary_html
from services.shichu_timing import annual_fortune_from_timing, build_timing
from services.shichusuimei_calc import Pillars4, calc_shichusuimei_from_payload


@pytest.fixture
def chart():
    return calc_shichusuimei_from_payload(
        dict(year=1990, month=5, day=15, hour=8, minute=30, gender="female"),
        target_date=date(2026, 10, 15),
    )


def timing(pillars=None, daiun=None, target=date(2026, 10, 15), hour_known=True):
    return build_timing(
        pillars or Pillars4(year="甲子", month="乙酉", day="庚辰", hour="丁亥"),
        daiun or {}, birth_local=datetime(2000, 6, 15, 12, tzinfo=ZoneInfo("Asia/Tokyo")),
        target_date=target, tz_name="Asia/Tokyo", hour_known=hour_known,
    )


def test_annual_fortune_matches_full_fields_and_respects_lichun():
    before = annual_fortune_from_timing(timing(target=date(2026, 1, 15)))
    after_timing = timing(target=date(2026, 10, 15))
    after = annual_fortune_from_timing(after_timing)
    assert (before["year"], before["effective_year"], before["kanshi"]) == (2026, 2025, "乙巳")
    assert (after["effective_year"], after["kanshi"], after["ten_god_to_day_master"]) == (2026, "丙午", "偏官")
    assert after["hidden_stems"] == [{"stem": "丁", "ten_god": "正官"}, {"stem": "己", "ten_god": "印綬"}]
    assert after["debug"]["year_boundary_rule"] == "lichun(315deg)"
    assert datetime.fromisoformat(after["starts_at"]) < datetime(2026, 10, 15, tzinfo=ZoneInfo("Asia/Tokyo")) < datetime.fromisoformat(after["ends_at"])
    assert after["ends_at"] == after_timing["ryunen"]["items"][1]["starts_at"]
    assert annual_fortune_from_timing({"ryunen": {"items": []}}) == {}


def test_future_year_uses_its_own_daiun_and_keeps_switch_contexts():
    result = timing(daiun={"items": [
        {"kanshi": "乙丑", "age_from": 15, "age_to": 25},
        {"kanshi": "丙寅", "age_from": 25, "age_to": 35},
    ]}, target=date(2024, 10, 15))
    this_year, next_year = result["ryunen"]["items"][:2]
    assert result["current_daiun"]["kanshi"] == "乙丑"
    assert this_year["current_daiun"]["kanshi"] == "乙丑"
    assert next_year["current_daiun"]["kanshi"] == "丙寅"
    assert next_year["daiun_changes_in_year"] is True
    assert [d["kanshi"] for d in next_year["daiun_overlaps"]] == ["乙丑", "丙寅"]
    assert all("interactions_daiun" in d for d in next_year["daiun_overlaps"])
    # 乙巳の流年と新大運・丙寅の刑。旧大運・乙丑を流用すると消えてしまう。
    assert any(r["type"] == "刑" and r["members"] == ["寅", "巳"] for r in next_year["interactions_daiun"])


@pytest.mark.parametrize("year", [2026, 2027, 2028, 2029])
def test_future_reference_near_lichun_stays_inside_its_annual_period(year):
    for item in timing(target=date(year, 2, 4))["ryunen"]["items"]:
        reference = datetime.fromisoformat(item["reference_at"])
        assert datetime.fromisoformat(item["starts_at"]) <= reference < datetime.fromisoformat(item["ends_at"])


def test_natal_daiun_annual_trine_is_detected_with_provisional_hour():
    result = timing(
        Pillars4(year="甲午", month="乙卯", day="庚戌", hour="甲申"),
        {"items": [{"kanshi": "壬辰", "age_from": 20, "age_to": 70}]},
        target=date(2032, 10, 15), hour_known=False,
    )
    current = result["ryunen"]["items"][0]
    trine = next(r for r in current["interactions_combined"] if r["type"] == "三合")
    assert set(trine["members"]) == {"申", "子", "辰"}
    assert set(trine["pillars"]) == {"hour", "daiun", "ryunen"}
    assert trine["provisional"] is True
    assert trine["detail"]["transformation"] == "not_evaluated"
    assert not any(r["type"] == "三合" for r in current["interactions_natal"])


def test_completed_three_punishment_requires_annual_participation():
    result = timing(
        Pillars4(year="甲寅", month="乙酉", day="庚辰", hour="丁亥"),
        {"items": [{"kanshi": "乙巳", "age_from": 20, "age_to": 70}]},
        target=date(2028, 10, 15),
    )
    current = result["ryunen"]["items"][0]
    assert current["three_punishments"]
    assert all(r["detail"]["complete"] and "ryunen" in r["pillars"] for r in current["three_punishments"])
    assert all(set(r["pillars"]) == {"year", "daiun", "ryunen"} for r in current["three_punishments"])
    annual = annual_fortune_from_timing(result)
    assert annual["relations"]["three_punishments_with_natal"] == []
    assert annual["relations"]["three_punishments_with_natal_and_daiun"]
    assert not timing(target=date(2026, 10, 15))["ryunen"]["items"][0]["three_punishments"]


def test_reader_uses_current_daiun_and_exposes_deep_evidence(chart):
    summary = build_full_astrologer_summary(chart)
    themes = summary["shichu"]["themes"]
    daiun_theme = next(t for t in themes if t["id"] == "shichu_timing")
    assert any("丁丑" in r for r in daiun_theme["reasons"])
    depth = next(t for t in themes if t["id"] == "shichu_structure_depth")
    assert any("五行力量" in e for e in depth["evidence"])
    assert any("格局候補" in e for e in depth["evidence"])
    assert any("保留" in e for e in depth["evidence"])
    annuals = [t for t in themes if t["id"].startswith("shichu_annual_")]
    assert len(annuals) == 10
    assert annuals[-1]["id"] == "shichu_annual_2035"
    assert all(t["questions"] and t["evidence"] for t in annuals)
    assert "2026年の年運" in render_astrologer_memo(summary)
    assert "現状確認の質問" in render_astrologer_memo(summary)


def test_unknown_hour_and_missing_daiun_do_not_become_certainties():
    raw = {"system": "shichusuimei", "timing": timing(hour_known=False)}
    theme = build_annual_themes(raw)[0]
    assert any("出生時刻不明" in r for r in theme["reasons"])
    assert any("重ね読みは保留" in e for e in theme["evidence"])
    assert build_annual_themes({"system": "shichusuimei"}) == []


@pytest.mark.parametrize("birth_time, known", [(None, False), ("", False), ("12:00", True)])
def test_form_noon_placeholder_is_distinguished_from_known_noon(birth_time, known):
    # 入力フォームは時刻不明も hour=12 に正規化するため、原文を優先する。
    result = calc_shichusuimei_from_payload(
        dict(year=1990, month=5, day=15, hour=12, minute=0, birth_time=birth_time, gender="female"),
        target_date=date(2026, 10, 15),
    )
    assert result["time_certainty"]["hour_known"] is known
    assert result["timing"]["hour_known"] is known
    assert any("仮置き" in text for text in result["unknowns"]) is (not known)
    assert any("出生時刻不明" in text for text in build_annual_themes(result)[0]["reasons"]) is (not known)


@pytest.mark.parametrize("integrated", [False, True])
def test_structure_summary_and_full_handoff_preserve_annual_data(chart, integrated):
    raw = {"shichusuimei": chart} if integrated else chart
    ss = json.loads(_build_structure_summary(raw))
    assert ss["shichusuimei"]["normalized_data"]["annual_fortune"]["kanshi"] == "丙午"
    assert ss["shichusuimei"]["structure_report"]["yojin_candidates"]
    log = build_handoff(inputs_view={}, payload_view={}, unknowns=[], structure_summary=ss, reports={}, mode="full")
    assert len(log["shichu"]["timing"]["ryunen"]["items"]) == 10
    assert log["shichu"]["timing"]["ryunen"]["items"][-1]["year"] == 2035
    assert log["shichu"]["annual_fortune"]["effective_year"] == 2026
    # 保存済み構造要約から占い師画面を開いた場合も年運が復元できる。
    assert any(t["id"] == "shichu_annual_2026" for t in build_full_astrologer_summary({}, ss)["shichu"]["themes"])


def test_reader_template_and_export_include_annual_evidence(chart):
    env = Environment(loader=FileSystemLoader("templates"), autoescape=True)
    summary = build_full_astrologer_summary(chart)
    rendered = env.get_template("astrologer_result.html").render(summary=summary, reader_text="", result=chart, structure_summary={})
    assert "2026年の年運" in rendered and "2027年の年運" in rendered
    assert "現状確認の質問" in rendered and "計算根拠と適用期間" in rendered
    export = render_shichu_summary_html(extract_shichu_data(chart))
    assert "2026年の年運" in export and "丙午" in export and "立春基準" in export
    assert "2028年の年運" in export
    assert "2035年の年運" in rendered and "2035年の年運" in export


def render_result(raw, structure=None):
    env = Environment(loader=FileSystemLoader("templates"), autoescape=True)
    return env.get_template("result.html").render(
        raw_json=raw, structure_summary_json=structure or {}, inputs_json={}, payload_json={},
        reader_text="", ai_text="AI本文の確認用", transit_data=None, include_reader=False,
        handoff_json="", handoff_yaml="", handoff_json_full="", handoff_yaml_full="",
        handoff_json_delta="", handoff_yaml_delta="", unknowns=[], from_order_code=None,
    )


@pytest.mark.parametrize("integrated", [False, True])
def test_ten_annual_rows_are_in_the_default_result_panel_without_javascript(chart, integrated):
    from bs4 import BeautifulSoup
    raw = {"shichusuimei": chart} if integrated else chart
    page = BeautifulSoup(render_result(raw), "html.parser")
    panel = page.select_one("#pg_result.active #shichuAnnualOverview")
    assert panel is not None
    assert "四柱推命の年運（10年分）" in panel.get_text()
    cards = panel.select(".annual-year-card")
    assert [card["id"] for card in cards] == [f"shichu-year-{y}" for y in range(2026, 2036)]
    assert "2035年" in cards[-1].select_one("summary").get_text()
    assert "命式・大運との作用" in cards[-1].get_text()
    # 本文の表示処理・チャート用JSが失敗しても年運は既にHTML内にある。
    assert not panel.find("script")
    assert "丁丑" in cards[0].get_text()


def test_saved_shorter_data_is_not_filled_with_invented_years(chart):
    import copy
    from bs4 import BeautifulSoup
    old = copy.deepcopy(chart)
    old["normalized_data"]["timing"]["ryunen"]["items"] = old["normalized_data"]["timing"]["ryunen"]["items"][:3]
    page = BeautifulSoup(render_result({}, {"shichusuimei": old}), "html.parser")
    panel = page.select_one("#shichuAnnualOverview")
    assert len(panel.select(".annual-year-card")) == 3
    assert "再解析すると10年分" in panel.get_text()
    assert "2035年" not in panel.get_text()


def test_result_explains_how_to_enable_four_pillars_when_not_selected():
    from bs4 import BeautifulSoup
    page = BeautifulSoup(render_result({"system": "western"}), "html.parser")
    panel = page.select_one("#pg_result.active #shichuAnnualOverview")
    assert "この解析には四柱推命が含まれていません" in panel.get_text()
    assert "手動カスタム" in panel.get_text()
    assert not panel.select(".annual-year-card")


@pytest.mark.parametrize("system", ["shichusuimei", "integrated_w_shichu", "integrated3"])
def test_ai_reader_prompt_receives_full_annual_context(monkeypatch, chart, system):
    from services import ai_report
    captured = []
    monkeypatch.setenv("GEMINI_API_KEY", "test-placeholder")
    monkeypatch.setattr(ai_report, "genai", SimpleNamespace(Client=lambda **kw: object()))
    monkeypatch.setattr(ai_report, "types", object())
    monkeypatch.setattr(ai_report, "_generate_once", lambda **kw: captured.append(kw["prompt"]) or "6. 成長と転換期\n7. 実践アクション")
    raw = chart if system == "shichusuimei" else {"shichusuimei": chart}
    result = ai_report.generate_report(raw, report_type="single_web_reader", meta={"astrology_system": system, "ai_provider": "gemini"})
    assert result and len(captured) == 1
    prompt = captured[0]
    assert "年運" in prompt and "立春切替" in prompt
    assert "daiun_changes_in_year" in prompt and "2027" in prompt
    assert "丁丑" in prompt and "現状確認の質問" in prompt
    assert "shichu_annual_2035" in prompt and "含まれる全年度" in prompt
    assert "{shichu_annual_reading}" not in prompt


def test_external_report_uses_annual_rules_when_year_chapter_enabled():
    from services.external_report_template_renderer import build_chapter_json_prompt
    order = SimpleNamespace(customer_name="テスト", consultation_text="仕事")
    prompt = build_chapter_json_prompt(order, plan="premium", handoff_yaml="{}", report_options={})
    assert "daiun_changes_in_year" in prompt and "立春切替" in prompt
    disabled = build_chapter_json_prompt(order, plan="light", handoff_yaml="{}", report_options={})
    assert "【四柱推命の詳細鑑定・年運】" not in disabled
