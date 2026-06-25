from __future__ import annotations

import routes_note_articles


def test_note_page_context_defaults_to_monthly_reading():
    context = routes_note_articles._context(
        request=object(),
        staff={"role": "admin"},
        target_month="2026-06",
        article_type="monthly_reading",
        custom_theme="",
        model_key="haiku",
    )

    assert context["article_type"] == "monthly_reading"
    assert "zodiac_fortunes" in context["article_types"]
    assert set(context["claude_models"]) == {"haiku", "sonnet"}
