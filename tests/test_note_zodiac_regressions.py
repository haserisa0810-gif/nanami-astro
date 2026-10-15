from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
import yaml

from fixture_zodiac_2026_10 import TRANSITS
from services.note_article_service import (
    _build_user_prompt, _zodiac_for_prompt, build_note_article_yaml,
    extract_monthly_transit_context, generate_note_article,
)
from services.note_writing_contract import ZODIAC_READING_GUIDE
from services.planetary_motion import calc_monthly_station_events
from services.zodiac_context import build_zodiac_context


def test_june_mercury_retrograde_and_station_at_month_end():
    context = extract_monthly_transit_context("2026-06")
    ruler = context["zodiac_context"]["Gemini"]["ruler"]
    assert ruler["month_end"]["retrograde"] is True
    assert [(e["planet"], e["date"], e["turn"]) for e in ruler["stations"]] == [
        ("Mercury", "2026-06-30", "retrograde")
    ]
    assert context["snapshot_count"] == 30
    assert context["daily_snapshots"][0]["date"] == "2026-06-01"
    assert context["daily_snapshots"][-1]["date"] == "2026-06-30"


def test_april_mars_station_at_month_start():
    context = extract_monthly_transit_context("2027-04")
    ruler = context["zodiac_context"]["Aries"]["ruler"]
    assert ruler["month_start"]["retrograde"] is True
    assert ruler["month_end"]["retrograde"] is False
    assert [(e["planet"], e["date"], e["turn"]) for e in ruler["stations"]] == [
        ("Mars", "2027-04-01", "direct")
    ]


@pytest.mark.parametrize("month", ["2026-02", "2026-05", "2026-07", "2026-10"])
def test_slow_planet_flags_survive_month_endpoints_and_yaml(month):
    context = extract_monthly_transit_context(month)
    loaded = yaml.safe_load(build_note_article_yaml({"transit_context": context}))
    aries = loaded["astrology_data"]["western_astrology"]["transits"]["zodiac_context"]["Aries"]
    for which, snap in [("start", context["daily_snapshots"][0]), ("end", context["daily_snapshots"][-1])]:
        by_name = {p["name"]: p for p in snap["planet_positions"]}
        for name in ["Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"]:
            assert isinstance(by_name[name]["retrograde"], bool)
            assert aries[f"placements_month_{which}"][name]["retrograde"] is by_name[name]["retrograde"]


def test_legacy_unknown_is_null_in_export_and_no_stations_are_invented():
    source = deepcopy(TRANSITS)
    source["zodiac_context"] = build_zodiac_context(source)
    loaded = yaml.safe_load(build_note_article_yaml({"transit_context": source}))
    c = loaded["astrology_data"]["western_astrology"]["transits"]["zodiac_context"]
    assert "retrograde" in c["Taurus"]["ruler"]["month_end"]
    assert c["Taurus"]["ruler"]["month_end"]["retrograde"] is None
    assert c["Aries"]["placements_month_start"]["Pluto"]["retrograde"] is None
    assert c["Aries"]["placements_month_start"]["Sun"]["retrograde"] is False
    assert not c["Aries"].get("stations_by_house")


def test_custom_snapshot_source_does_not_silently_mix_in_live_stations():
    def loader(*, target_date):
        return {"transit_date": target_date.date().isoformat(), "today_planets": [], "aspects": []}

    with patch("services.note_article_service.calc_monthly_station_events", side_effect=AssertionError("unexpected calculation")):
        context = extract_monthly_transit_context("2026-10", snapshot_loader=loader)
    assert context["station_events"] == []
    assert context["motion_data"]["stations"] == "unavailable"


@pytest.mark.parametrize("year,month", [(2026, 6), (2026, 12), (2028, 2)])
def test_station_search_jst_month_boundaries_and_zero_speed(year, month):
    start = datetime(year, month, 1, tzinfo=ZoneInfo("Asia/Tokyo"))
    end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=start.tzinfo)
    roots = {"Mercury": start, "Venus": end - timedelta(seconds=30), "Mars": end}

    def loader(dt, planet):
        return {"speed": (dt - roots[planet]).total_seconds() / 1e5, "sign": "Can"}

    events = calc_monthly_station_events(year, month, position_loader=loader)
    assert [e["planet"] for e in events] == ["Mercury", "Venus"]
    assert events[0]["date"] == start.date().isoformat()
    assert events[1]["date"] == (end - timedelta(days=1)).date().isoformat()
    for event in events:
        actual = datetime.fromisoformat(event["local_datetime"])
        assert abs((actual - roots[event["planet"]]).total_seconds()) <= 1


def test_zodiac_api_receives_all_signs_and_same_editorial_brief_as_export():
    calls = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.messages = self

        def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(content=[SimpleNamespace(type="text", text='{"title":"test","article_body":"test","zodiac_fortunes":"test","sns_copy":"test"}')], stop_reason="end_turn")

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}), patch(
        "services.note_article_service.extract_note_vedic_context", return_value=None
    ):
        result = generate_note_article(target_month="2026-10", article_type="zodiac_fortunes", client_factory=FakeClient)
    prompt = calls[0]["messages"][0]["content"]
    assert _zodiac_for_prompt(result["transit_context"]) in prompt
    assert "各80〜140字" not in prompt
    assert "内惑星（月・水星・金星）の動きは前振り" not in prompt
    assert "各500〜750字" in prompt
    assert calls[0]["max_tokens"] == 14000
    loaded = yaml.safe_load(result["yaml_preview"])
    assert loaded["writing_instruction"]["zodiac_reading_guide"] == ZODIAC_READING_GUIDE
    zodiac = loaded["astrology_data"]["western_astrology"]["transits"]["zodiac_context"]
    assert len(zodiac) == 12
    for sign in zodiac:
        assert f"  {sign}:" in prompt
    sent = yaml.safe_load(prompt.split("12星座別の計算済み根拠と執筆基準:\n", 1)[1].split("# 出力形式", 1)[0])
    for sign, houses in [("Taurus", [7, 7]), ("Scorpio", [1, 1])]:
        aspect = next(a for a in sent["zodiac_context"][sign]["aspects_by_house"] if a["label"].startswith("太陽 × 金星"))
        assert aspect["houses"] == houses


def test_non_zodiac_prompt_is_not_forced_to_write_twelve_signs():
    context = extract_monthly_transit_context("2026-10")
    prompt = _build_user_prompt(context=context, article_type="monthly_reading", custom_theme="")
    assert "zodiac_reading_guide:" not in prompt
    assert "zodiac_fortunes は空文字" in prompt
