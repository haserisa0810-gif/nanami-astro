import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import routes_analyze
from auth import get_current_staff
from services.style_prompts import resolve_reading_style


@pytest.mark.parametrize("selected,suggested,expected", [
    ("general", "structured", "structured"),
    ("general", "general", "general"),
    ("general", "", "general"),
    ("", "structured", "structured"),
    (None, "structured", "structured"),
    # 旧画面・過去データの structured も判定に従う
    ("structured", "general", "general"),
    ("structured", "structured", "structured"),
    # 特化スタイルは判定で変えない
    ("love", "structured", "love"),
    ("timing", "structured", "timing"),
])
def test_resolve_reading_style(selected, suggested, expected):
    assert resolve_reading_style(selected, suggested) == expected


@pytest.mark.parametrize("analysis_mode", ["auto", "manual"])
def test_general_reading_uses_chart_detection_in_both_modes(monkeypatch, analysis_mode):
    monkeypatch.setattr(
        routes_analyze,
        "_recommend_helpers",
        lambda: lambda **kwargs: {"suggested_reading_style": "structured", "suggested_astrology_system": "western"},
    )
    captured = {}
    original_helpers = routes_analyze._analyze_helpers

    def capturing_helpers():
        build_payload_a, build_base_meta, format_reports, build_handoff_logs, run_compatibility, run_single = original_helpers()

        def run_single_capture(*args, **kwargs):
            captured["style"] = kwargs["base_meta"]["style"]
            return run_single(*args, **kwargs)

        return build_payload_a, build_base_meta, format_reports, build_handoff_logs, run_compatibility, run_single_capture

    monkeypatch.setattr(routes_analyze, "_analyze_helpers", capturing_helpers)
    app = FastAPI()
    app.include_router(routes_analyze.router)
    app.dependency_overrides[get_current_staff] = lambda: {"role": "admin"}

    response = TestClient(app).post("/analyze", data={
        "birth_date": "1990-01-15", "prefecture": "Tokyo", "yaml_only": "true",
        "analysis_mode": analysis_mode, "reading_style": "general",
    })

    assert response.status_code == 200
    assert captured["style"] == "structured"
