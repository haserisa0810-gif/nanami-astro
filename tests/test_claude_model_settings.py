from types import SimpleNamespace

import pytest

from services import ai_report


class _FakeStream:
    def __init__(self, calls, kwargs, stop_reason="end_turn"):
        self._calls = calls
        self._kwargs = kwargs
        self._stop_reason = stop_reason

    def __enter__(self):
        self._calls.append(self._kwargs)
        return self

    def __exit__(self, *exc):
        return False

    def get_final_message(self):
        return SimpleNamespace(
            stop_reason=self._stop_reason,
            content=[
                SimpleNamespace(type="thinking", thinking=""),
                SimpleNamespace(type="text", text="鑑定本文"),
            ],
        )


def _fake_client(calls, stop_reason="end_turn"):
    return SimpleNamespace(messages=SimpleNamespace(
        stream=lambda **kwargs: _FakeStream(calls, kwargs, stop_reason),
    ))


def test_claude_request_uses_effort_without_temperature():
    calls = []
    text = ai_report._generate_once_claude(
        client=_fake_client(calls), model_name=ai_report.CLAUDE_SONNET_MODEL, prompt="p", max_tokens=32000,
    )
    assert text == "鑑定本文"  # thinking ブロックは本文に混ざらない
    assert "temperature" not in calls[0]
    assert calls[0]["output_config"] == {"effort": "medium"}
    assert calls[0]["model"] == "claude-sonnet-5-5"


def test_claude_refusal_is_not_returned_as_reading():
    with pytest.raises(RuntimeError):
        ai_report._generate_once_claude(
            client=_fake_client([], stop_reason="refusal"), model_name="claude-sonnet-5-5", prompt="p", max_tokens=100,
        )


@pytest.mark.parametrize("requested,expected", [
    ("sonnet", "claude-sonnet-5-5"),
    ("haiku", "claude-haiku-5-5"),
    ("pro", "claude-sonnet-5-5"),
    ("claude-haiku-4-5-20251001", None),  # 旧世代は選べない
])
def test_model_aliases(requested, expected):
    assert ai_report._normalize_requested_claude_model(requested) == expected


def test_default_model_is_sonnet():
    model, _ = ai_report._resolve_claude_model_name({}, "western")
    assert model == "claude-sonnet-5-5"


def test_free_reading_uses_haiku_with_free_prompt():
    meta = {"ai_provider": "claude", "ai_model": "haiku", "allow_manual_ai_model": True, "is_free_reading": True}
    assert ai_report._should_use_claude(meta, "western") is True
    model, _ = ai_report._resolve_claude_model_name(meta, "western")
    assert model == "claude-haiku-5-5"
    files = ai_report._select_prompt_files("western", "single_web", "web", "free_reading", use_claude=True)
    assert files["single_web"] == "free_reading_web.txt"
    assert ai_report.CLAUDE_FREE_READING_EFFORT == "low"
