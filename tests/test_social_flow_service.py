from __future__ import annotations

from services.social_flow_service import build_social_flow_context, generate_social_flow


def _snapshot_loader(*, target_date):
    return {
        "transit_date": target_date.strftime("%Y-%m-%d"),
        "today_planets": [
            {"name": "Sun", "sign": "Cancer", "degree": 10.0},
            {"name": "Saturn", "sign": "Aries", "degree": 10.5},
            {"name": "Uranus", "sign": "Gemini", "degree": 1.0},
        ],
        "aspects": [
            {
                "planet_a": "Sun",
                "planet_b": "Saturn",
                "aspect": "square",
                "orb": 0.5,
            },
            {
                "planet_a": "Mercury",
                "planet_b": "Uranus",
                "aspect": "sextile",
                "orb": 0.8,
            },
        ],
        "aspect_count": 2,
    }


def _event_loader(year, month):
    if (year, month) != (2026, 8):
        return []
    return [
        {
            "id": "perseids-2026",
            "type": "meteor_shower",
            "subtype": "perseids",
            "name_ja": "ペルセウス座流星群",
            "local_datetime": "2026-08-13T01:47:23+09:00",
            "local_date": "2026-08-13",
            "astrology_role": "editorial_sky_note",
        },
        {
            "id": "solar-eclipse-20260812T1745Z",
            "type": "solar_eclipse",
            "subtype": "total",
            "phase": "new_moon",
            "local_datetime": "2026-08-13T02:45:59+09:00",
            "local_date": "2026-08-13",
            "position": {"sign": "Leo", "degree": 20, "minute": 2},
            "visibility": {"reference_location": "Tokyo", "visible": False},
        },
        {
            "id": "lunar-eclipse-20260828T0412Z",
            "type": "lunar_eclipse",
            "subtype": "partial",
            "phase": "full_moon",
            "local_datetime": "2026-08-28T13:12:57+09:00",
            "local_date": "2026-08-28",
            "position": {"sign": "Pisces", "degree": 4, "minute": 51},
            "visibility": {"reference_location": "Tokyo", "visible": False},
        },
    ]


def _eclipse_summary_loader(year):
    return {
        "year": year,
        "eclipse_count": 4,
        "eclipse_season_count": 2,
        "wording_note": "食の回数と食シーズンの回数を混同しないこと。",
    }


class _TextBlock:
    type = "text"
    text = """
{
  "note_draft": {
    "title_options": ["社会の流れをやわらかく読む", "今月、意識されやすいテーマ"],
    "opening": "マンデン占星術では、個人ではなく社会全体の空気を見ていきます。",
    "social_theme": "制度や判断の見直しが目立ちやすい時期です。",
    "notable_fields": ["AI・技術", "経済", "制度"],
    "news_viewpoints": ["何が問われているか", "見直しの流れがどこに出るか"],
    "ai_yaml_lead": "必要な部分はYAMLにして、次の下書きへ渡せます。",
    "soft_service_lead": "個人の体感は出生図によって変わります。"
  },
  "sns": {
    "threads": "今月は、社会の空気として見直しが意識されやすい流れです。",
    "x": "社会の流れとして、制度や判断の見直しが目立ちやすい時期。",
    "instagram": ["1枚目: 今月の社会の流れ", "2枚目: 意識されやすいテーマ"]
  },
  "yaml": {
    "base_date": "2026-07-01",
    "target_period": "2026年7月",
    "theme": "全体",
    "major_social_themes": ["見直し", "制度", "技術"],
    "notable_fields": ["AI・技術", "経済"],
    "sns_short_text": "見直しが意識されやすい時期。",
    "note_heading_ideas": ["社会テーマ", "ニュースを見る視点"]
  },
  "future_verification": {
    "previous_forecast_theme": "",
    "actual_news": [],
    "reflection_notes": ""
  }
}
"""


class _FakeResponse:
    stop_reason = "end_turn"
    content = [_TextBlock()]


class _FakeMessages:
    def __init__(self):
        self.last_kwargs = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        return _FakeResponse()


class _FakeClient:
    messages = _FakeMessages()

    def __init__(self, api_key):
        self.api_key = api_key


def test_build_social_flow_context_month():
    context = build_social_flow_context(
        base_date="2026-08-01",
        period="month",
        snapshot_loader=_snapshot_loader,
        event_loader=_event_loader,
        eclipse_summary_loader=_eclipse_summary_loader,
    )

    assert context["period_label"] == "2026年8月"
    assert context["target_range"] == {"start_date": "2026-08-01", "end_date": "2026-08-31"}
    assert context["snapshot_count"] == 31
    assert context["major_aspects"]
    assert [event["type"] for event in context["astronomical_events"]] == [
        "meteor_shower",
        "solar_eclipse",
        "lunar_eclipse",
    ]
    assert context["eclipse_year_summary"]["eclipse_count"] == 4
    assert context["eclipse_year_summary"]["eclipse_season_count"] == 2


def test_generate_social_flow_with_fake_client(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

    result = generate_social_flow(
        base_date="2026-08-01",
        period="month",
        theme="overall",
        output_type="all",
        model_key="sonnet",
        snapshot_loader=_snapshot_loader,
        event_loader=_event_loader,
        eclipse_summary_loader=_eclipse_summary_loader,
        client_factory=_FakeClient,
    )

    assert result["model_label"] == "Claude Sonnet 5 相当（標準）"
    assert result["generated"]["note_draft"]["social_theme"]
    assert "future_verification" in result["yaml_preview"]
    assert "社会の流れ" in result["yaml_preview"]
    assert "astronomical_events" in result["yaml_preview"]
    assert "eclipse_year_summary" in result["yaml_preview"]
    assert "caution_dates" in result["yaml_preview"]
    prompt = _FakeClient.messages.last_kwargs["messages"][0]["content"]
    assert "皆既日食" in prompt
    assert "部分月食" in prompt
    assert "食4回、食シーズン2回" in prompt
