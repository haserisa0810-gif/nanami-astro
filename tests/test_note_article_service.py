from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from services.note_article_service import _parse_json_response, extract_monthly_transit_context, generate_note_article
from services.type_catalog import get_type_definitions_for_prompt, get_type_subtype_combinations_for_prompt


def _snapshot_loader(*, target_date):
    day = target_date.day
    return {
        "transit_date": target_date.strftime("%Y-%m-%d"),
        "today_planets": [],
        "aspects": [
            {
                "planet_a": "Venus",
                "planet_b": "Saturn",
                "aspect": "trine",
                "orb": abs(day - 10) / 5,
            },
            {
                "planet_a": "Mercury",
                "planet_b": "Mars",
                "aspect": "square",
                "orb": abs(day - 20) / 4,
            },
            {
                "planet_a": "Uranus",
                "planet_b": "Pluto",
                "aspect": "trine",
                "orb": abs(day - 25) / 6,
            },
        ],
    }


def test_extract_monthly_transit_context_classifies_aspects():
    result = extract_monthly_transit_context("2026-06", snapshot_loader=_snapshot_loader)

    assert result["snapshot_count"] == 30
    assert result["tight_aspects"][0]["orb"] == 0
    assert result["love_aspects"]
    assert result["work_aspects"]
    assert result["change_aspects"]
    assert result["inner_review_aspects"]
    assert result["caution_dates"][0]["date"] == "2026-06-20"
    assert result["movement_dates"][0]["date"] == "2026-06-10"
    assert len(result["theme_candidates"]) >= 3


def test_generate_note_article_uses_claude_only_after_transit_extraction():
    calls = []

    class FakeMessages:
        def create(self, **kwargs):
            calls.append(kwargs)
            payload = {
                "title": "6月の星読み",
                "article_body": "焦って進むより、整えてから動くと後で楽ですよ．",
                "zodiac_fortunes": "",
                "sns_copy": "6月の星読みを公開しました｡",
            }
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text=__import__("json").dumps(payload, ensure_ascii=False))],
                stop_reason="max_tokens" if len(calls) == 1 else "end_turn",
            )

    class FakeClient:
        def __init__(self, **kwargs):
            self.messages = FakeMessages()

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        result = generate_note_article(
            target_month="2026-06",
            article_type="monthly_reading",
            model_key="haiku",
            snapshot_loader=_snapshot_loader,
            client_factory=FakeClient,
        )

    assert calls
    assert "主要アスペクト" in calls[0]["messages"][0]["content"]
    assert result["article_body"].endswith("ですよ。")
    assert result["sns_copy"].endswith("。")


def test_type_monthly_fortunes_prompt_includes_type_catalog_and_boundaries():
    calls = []

    class FakeMessages:
        def create(self, **kwargs):
            calls.append(kwargs)
            payload = {
                "title": "6月のタイプ別運勢",
                "article_body": "## 突破集中型（燃焼タイプ） × 短距離全力\n\n根拠：\n- 金星 × 土星 トライン（orb 0.00°）\n\n今月の読み：\n区切ると動きやすい流れです。",
                "zodiac_fortunes": "",
                "sns_copy": "タイプ別運勢を公開しました。",
            }
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text=__import__("json").dumps(payload, ensure_ascii=False))]
            )

    class FakeClient:
        def __init__(self, **kwargs):
            self.messages = FakeMessages()

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        result = generate_note_article(
            target_month="2026-06",
            article_type="type_monthly_fortunes",
            custom_theme="仕事",
            model_key="haiku",
            snapshot_loader=_snapshot_loader,
            client_factory=FakeClient,
        )

    prompt = calls[0]["messages"][0]["content"]
    assert "タイプ別運勢の追加条件" in prompt
    assert "無料の /type 診断本文ではなく" in prompt
    assert "今回出力する親タイプ × サブタイプ" in prompt
    assert "backstage_leader" in prompt
    assert "裏方リーダー型" in prompt
    assert "各組み合わせは「根拠2個まで」と「今月の読み2〜3文」まで" in prompt
    assert "仕事" in prompt
    assert len(calls) == 10
    assert all(call["max_tokens"] == 1800 for call in calls)
    assert any("breakthrough_burnout" in call["messages"][0]["content"] for call in calls)
    assert any("sprint_fullpower" in call["messages"][0]["content"] for call in calls)
    assert any("突破集中型（燃焼タイプ） × 短距離全力" in call["messages"][0]["content"] for call in calls)
    assert result["article_type_label"] == "タイプ別運勢"
    assert result["article_body"].startswith("# 2026年6月 タイプ別運勢素材")
    assert result["warnings"] == ["出力が途中で切れた可能性があります。Claudeのmax_tokens上限に到達しました。"]


def test_type_catalog_contains_public_type_ids():
    rows = get_type_definitions_for_prompt()
    ids = {row["type_id"] for row in rows}

    assert len(rows) == 10
    assert {"backstage_leader", "ideal_first", "solo_fighter"} <= ids
    assert all(row["type_name"] and row["summary_for_reference_only"] for row in rows)


def test_type_subtype_combinations_contains_all_public_pairs():
    rows = get_type_subtype_combinations_for_prompt()
    names = {row["display_name"] for row in rows}

    assert len(rows) == 30
    assert "突破集中型（燃焼タイプ） × 短距離全力" in names
    assert "裏方リーダー型 × 分析特化" in names
    assert all(row["type_id"] and row["subtype_id"] for row in rows)


def test_parse_json_response_accepts_markdown_fenced_json():
    raw = """```json
{
  "title": "6月のタイプ別運勢",
  "article_body": "## 裏方リーダー型の今月のテーマ\n整えやすい流れです。",
  "zodiac_fortunes": "",
  "sns_copy": "公開しました。"
}
```"""

    result = _parse_json_response(raw)

    assert result["title"] == "6月のタイプ別運勢"
    assert "裏方リーダー型" in result["article_body"]
    assert result["sns_copy"] == "公開しました。"


def test_parse_json_response_extracts_json_with_surrounding_text():
    raw = """承知しました。以下です。

{
  "title": "タイプ別運勢",
  "body": "本文キーが崩れても拾います。",
  "sns": "SNS文です。"
}

以上です。"""

    result = _parse_json_response(raw)

    assert result["article_body"] == "本文キーが崩れても拾います。"
    assert result["sns_copy"] == "SNS文です。"


def test_parse_json_response_falls_back_to_raw_text_when_not_json():
    raw = "## 裏方リーダー型の今月のテーマ\n今月は整えやすい流れです。"

    result = _parse_json_response(raw)

    assert result["title"] == ""
    assert result["article_body"].startswith("## 裏方リーダー型")
    assert result["zodiac_fortunes"] == ""
