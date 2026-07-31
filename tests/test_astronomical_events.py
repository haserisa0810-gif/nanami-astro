from __future__ import annotations

from datetime import date

from services.astronomical_events import eclipse_year_summary, events_for_month, events_near_date


def test_august_2026_events_are_calculated() -> None:
    events = events_for_month(2026, 8)
    by_type = {event["type"]: event for event in events}

    assert by_type["solar_eclipse"]["subtype"] == "total"
    assert by_type["solar_eclipse"]["position"]["sign"] == "Leo"
    assert by_type["lunar_eclipse"]["subtype"] == "partial"
    assert by_type["lunar_eclipse"]["position"]["sign"] == "Pisces"
    assert by_type["meteor_shower"]["subtype"] == "perseids"
    assert eclipse_year_summary(2026)["eclipse_count"] == 4
    assert eclipse_year_summary(2026)["eclipse_season_count"] == 2


def test_daily_window_includes_jst_eclipse_date() -> None:
    events = events_near_date(date(2026, 8, 13))
    assert any(event["type"] == "solar_eclipse" for event in events)
    assert any(event["type"] == "meteor_shower" for event in events)


def test_next_year_needs_no_new_fixture() -> None:
    assert events_for_month(2027, 8)
