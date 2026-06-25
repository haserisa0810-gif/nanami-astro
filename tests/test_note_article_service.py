from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from services.note_article_service import extract_monthly_transit_context, generate_note_article


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
        ],
    }


def test_extract_monthly_transit_context_classifies_aspects():
    result = extract_monthly_transit_context("2026-06", snapshot_loader=_snapshot_loader)

    assert result["snapshot_count"] == 30
    assert result["tight_aspects"][0]["orb"] == 0
    assert result["love_aspects"]
    assert result["work_aspects"]
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
                content=[SimpleNamespace(type="text", text=__import__("json").dumps(payload, ensure_ascii=False))]
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
