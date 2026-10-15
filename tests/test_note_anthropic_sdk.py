"""Use the installed SDK, mocking only HTTP, to catch SDK breaking changes.

Install the application's requirements before running this test. A replacement
client with **kwargs would miss removed parameters such as temperature.
"""
import json

import anthropic
import httpx
import pytest

from services.note_article_service import generate_note_article


@pytest.mark.parametrize("model_key", ["haiku", "sonnet"])
@pytest.mark.parametrize("article_type,max_tokens", [("monthly_reading", 5000), ("zodiac_fortunes", 14000)])
def test_note_generation_through_real_sdk(monkeypatch, model_key, article_type, max_tokens):
    requests = []

    def respond(request):
        body = json.loads(request.content)
        requests.append(body)
        return httpx.Response(200, json={
            "id": "msg_offline_test", "type": "message", "role": "assistant",
            "model": body["model"],
            "content": [{"type": "text", "text": json.dumps({
                "title": "SDK検証", "article_body": "本文です。",
                "zodiac_fortunes": "牡羊座の本文です。" if article_type == "zodiac_fortunes" else "",
                "sns_copy": "公開のお知らせです。",
            }, ensure_ascii=False)}],
            "stop_reason": "end_turn", "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        })

    def snapshot_loader(*, target_date):
        return {"transit_date": target_date.date().isoformat(), "today_planets": [], "aspects": []}

    monkeypatch.setenv("ANTHROPIC_API_KEY", "offline-test-key")
    monkeypatch.setattr("services.note_article_service.extract_note_vedic_context", lambda month: None)
    with anthropic.Anthropic(
        api_key="offline-test-key", max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    ) as client:
        result = generate_note_article(
            target_month="2026-10", article_type=article_type, model_key=model_key,
            snapshot_loader=snapshot_loader, client_factory=lambda **kwargs: client,
        )

    assert len(requests) == 1
    assert requests[0]["max_tokens"] == max_tokens
    assert requests[0]["temperature"] == 0.65
    assert result["title"] == "SDK検証"
    assert result["warnings"] == []
    assert "writing_instruction:" in result["yaml_preview"]
