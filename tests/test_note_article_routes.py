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
    assert context["article_types"]["type_monthly_fortunes"] == "タイプ別運勢"
    assert set(context["claude_models"]) == {"haiku", "sonnet"}


def test_unexpected_generation_error_logs_traceback_without_exposing_it(monkeypatch, caplog):
    def fail(**kwargs):
        raise TypeError("SDK incompatible keyword")

    monkeypatch.setattr(routes_note_articles, "generate_note_article", fail)
    monkeypatch.setattr(routes_note_articles.templates, "TemplateResponse", lambda **kwargs: kwargs["context"])
    response = routes_note_articles.note_articles_generate(
        request=object(), target_month="2026-10", article_type="zodiac_fortunes",
        custom_theme="", model_key="haiku", staff={"role": "admin"},
    )
    assert response["result"] is None
    assert "生成に失敗" in response["error"]
    assert "SDK incompatible" not in response["error"]
    record = next(r for r in caplog.records if "note article generation failed" in r.message)
    assert record.exc_info[0] is TypeError
    assert "2026-10" in record.message and "haiku" in record.message
