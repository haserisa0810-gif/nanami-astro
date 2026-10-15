import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import routes_analyze
from services.analyze_form_errors import AnalyzeFormRoute


@pytest.fixture
def client(monkeypatch):
    # Exercise the real form parsing, input validation, calculations and templates.
    # Recommendation is unrelated to error rendering; no external AI calls occur.
    monkeypatch.setattr(routes_analyze, "_recommend_helpers", lambda: lambda **kwargs: {})
    app = FastAPI()
    app.include_router(routes_analyze.router)
    return TestClient(app)


def assert_readable_html(response, status, message):
    assert response.status_code == status
    assert response.headers["content-type"] == "text/html; charset=utf-8"
    assert '<meta charset="utf-8">' in response.text
    assert message in response.content.decode("utf-8")
    assert 'href="/"' in response.text
    assert "入力画面に戻る" in response.text
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("user_agent", [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) Mobile/15E148 Safari Line/26.15.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/153.0.0.0 Safari/537.36",
])
def test_missing_location_returns_utf8_html(client, user_agent):
    response = client.post("/analyze", data={"birth_date": "1990-01-15", "yaml_only": "true"},
                           headers={"user-agent": user_agent, "accept": "text/html"})
    assert_readable_html(response, 400, "出生地は都道府県を選ぶか、地図で地点を選択してください。")


@pytest.mark.parametrize("data,message", [
    ({}, "生年月日を入力してください。"),
    ({"birth_date": "1990-01-15", "lat": "invalid-private-input"}, "緯度は数値で入力してください。"),
    ({"birth_date": "1990-01-15", "astrology_system": "invalid-private-input"}, "占星術タイプを確認"),
])
def test_request_validation_is_japanese_without_echoing_inputs(client, data, message):
    response = client.post("/analyze", data=data)
    assert_readable_html(response, 422, message)
    assert "invalid-private-input" not in response.text


def test_birth_time_error_returns_html(client):
    response = client.post("/analyze", data={
        "birth_date": "1990-01-15", "birth_time": "12:30:00", "prefecture": "Tokyo", "yaml_only": "true",
    })
    assert_readable_html(response, 400, "出生時刻の形式が不正です（HH:MM）")


def test_valid_analysis_still_renders_results(client):
    response = client.post("/analyze", data={
        "birth_date": "1990-01-15", "prefecture": "Tokyo", "yaml_only": "true",
    })
    assert response.status_code == 200
    assert "解析結果" in response.text
    assert 'id="pg_result"' in response.text


def test_recommendation_api_still_returns_json(client):
    response = client.post("/analyze/recommend-options", data={})
    assert response.status_code == 422
    assert response.headers["content-type"] == "application/json"
    assert "detail" in response.json()


@pytest.mark.parametrize("exception,status,public_message", [
    (HTTPException(502, "internal-private-detail"), 502, "鑑定処理に失敗しました。"),
    (RuntimeError("internal-private-detail"), 500, "鑑定処理に失敗しました。"),
    (HTTPException(400, '<script>alert("private")</script>'), 400, "&lt;script&gt;"),
])
def test_errors_do_not_expose_internal_details_or_execute_html(exception, status, public_message):
    app = FastAPI()
    app.router.route_class = AnalyzeFormRoute

    @app.post("/analyze")
    def fail():
        raise exception

    response = TestClient(app).post("/analyze")
    assert_readable_html(response, status, public_message)
    assert "internal-private-detail" not in response.text
    assert '<script>alert("private")</script>' not in response.text
