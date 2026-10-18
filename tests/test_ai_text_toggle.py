import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import routes_analyze
from auth import get_current_staff
from services import analyze_engine


@pytest.fixture
def client_and_calls(monkeypatch):
    calls = []

    def fake_generate_report(*args, **kwargs):
        calls.append(kwargs.get("report_type"))
        return "AIの鑑定本文"

    monkeypatch.setattr(analyze_engine, "generate_report", fake_generate_report)
    monkeypatch.setattr(routes_analyze, "_recommend_helpers", lambda: lambda **kwargs: {})
    app = FastAPI()
    app.include_router(routes_analyze.router)
    app.dependency_overrides[get_current_staff] = lambda: {"role": "admin"}
    return TestClient(app), calls


BASE = {"birth_date": "1990-01-15", "prefecture": "Tokyo", "include_reader": "true"}


def test_yaml_only_does_not_call_ai(client_and_calls):
    client, calls = client_and_calls
    response = client.post("/analyze", data={**BASE, "yaml_only": "true"})
    assert response.status_code == 200
    assert calls == []
    assert "YAMLログのみ作成モード" in response.text


def test_generate_ai_off_does_not_call_ai(client_and_calls):
    client, calls = client_and_calls
    # チェックボックスを外すと generate_ai は送られない。生成オプション欄を使っている画面では hidden で false が来る想定も含めて確認。
    response = client.post("/analyze", data={**BASE, "generate_ai": "false"})
    assert response.status_code == 200
    assert calls == []
    assert analyze_engine.AI_TEXT_OFF_MESSAGE in response.text


def test_generate_ai_on_calls_ai(client_and_calls):
    client, calls = client_and_calls
    response = client.post("/analyze", data={**BASE, "generate_ai": "true"})
    assert response.status_code == 200
    assert "single_web" in calls and "single_web_reader" in calls


def test_compatibility_respects_toggle(client_and_calls):
    client, calls = client_and_calls
    response = client.post("/analyze", data={
        **BASE, "yaml_only": "true", "analysis_type": "compatibility",
        "birth_date_b": "1992-03-03", "prefecture_b": "Osaka",
    })
    assert response.status_code == 200
    assert calls == []


def test_checkbox_with_hidden_false_uses_checked_value(client_and_calls):
    # 画面は hidden(false) → checkbox(true) の順に送る。チェックありなら true が優先される。
    client, calls = client_and_calls
    response = client.post("/analyze", data={**BASE, "generate_ai": ["false", "true"]})
    assert response.status_code == 200
    assert "single_web" in calls


def test_unchecked_checkbox_sends_only_hidden_false(client_and_calls):
    client, calls = client_and_calls
    response = client.post("/analyze", data={**BASE, "generate_ai": ["false"]})
    assert response.status_code == 200
    assert calls == []
