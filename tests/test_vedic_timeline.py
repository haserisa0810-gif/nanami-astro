from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

import pytest

from services.ai_report import _build_vedic_focus_summary
from services.vedic_calc import (
    VIMSHOTTARI_INDEX,
    VIMSHOTTARI_SEQUENCE,
    VIMSHOTTARI_YEARS,
    _lord_natal_context,
    build_vedic_gochara,
    calc_vedic_from_payload,
)


AS_OF = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _payload(*, birth_time: str | None = "08:30") -> dict:
    return {
        "year": 1990,
        "month": 5,
        "day": 15,
        "hour": 8 if birth_time else 12,
        "minute": 30 if birth_time else 0,
        "birth_time": birth_time,
        "lat": 35.6895,
        "lng": 139.6917,
        "tz_offset_hours": 9,
    }


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def test_timeline_is_continuous_ordered_and_has_exact_period_ratios():
    result = calc_vedic_from_payload(
        _payload(),
        as_of=AS_OF,
        forecast_years=50,
    )
    timeline = result["dasha_timeline"]
    periods = timeline["periods"]

    for current, following in zip(periods, periods[1:]):
        assert current["end_utc"] == following["start_utc"]
        assert (
            VIMSHOTTARI_INDEX[following["lord"]]
            == (VIMSHOTTARI_INDEX[current["lord"]] + 1)
            % len(VIMSHOTTARI_SEQUENCE)
        )

    window_start = _dt(timeline["as_of_utc"])
    window_end = _dt(timeline["forecast_until_utc"])
    complete_mahas = [
        period
        for period in periods
        if _dt(period["start_utc"]) >= window_start
        and _dt(period["end_utc"]) <= window_end
    ]
    assert complete_mahas
    for maha in complete_mahas:
        start = _dt(maha["start_utc"])
        end = _dt(maha["end_utc"])
        duration = end - start
        assert abs(
            duration.total_seconds()
            - VIMSHOTTARI_YEARS[maha["lord"]] * 365.2425 * 86400
        ) <= 1

        # Only periods at the forecast edges are filtered. A fully-contained
        # maha includes all nine contiguous antaras and both outer boundaries.
        antara = maha["antara"]
        assert len(antara) == 9
        assert antara[0]["start_utc"] == maha["start_utc"]
        assert antara[-1]["end_utc"] == maha["end_utc"]
        for current, following in zip(antara, antara[1:]):
            assert current["end_utc"] == following["start_utc"]
        for subperiod in antara:
            actual = _dt(subperiod["end_utc"]) - _dt(subperiod["start_utc"])
            expected = duration * (
                VIMSHOTTARI_YEARS[subperiod["lord"]] / 120.0
            )
            assert abs((actual - expected).total_seconds()) <= 1


def test_current_period_is_unique_and_matches_existing_dasha():
    result = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    current_mahas = [
        period
        for period in result["dasha_timeline"]["periods"]
        if period["is_current"]
    ]

    assert len(current_mahas) == 1
    assert current_mahas[0]["lord"] == result["dasha"]["maha"]["lord"]
    current_antaras = [
        period for period in current_mahas[0]["antara"] if period["is_current"]
    ]
    assert len(current_antaras) == 1
    assert current_antaras[0]["lord"] == result["dasha"]["antara"]["lord"]


def test_end_boundary_belongs_to_the_following_maha():
    first = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    boundary = _dt(first["dasha_timeline"]["periods"][0]["end_utc"])
    result = calc_vedic_from_payload(_payload(), as_of=boundary)
    current = next(
        period
        for period in result["dasha_timeline"]["periods"]
        if period["is_current"]
    )

    assert current["start_utc"] == boundary.isoformat()
    assert current["lord"] == "Jupiter"


def test_forecast_window_changes_period_count_and_only_returns_overlaps():
    one_year = calc_vedic_from_payload(
        _payload(), as_of=AS_OF, forecast_years=1
    )["dasha_timeline"]
    ten_years = calc_vedic_from_payload(
        _payload(), as_of=AS_OF, forecast_years=10
    )["dasha_timeline"]

    assert len(one_year["periods"]) < len(ten_years["periods"])
    for timeline in (one_year, ten_years):
        start = _dt(timeline["as_of_utc"])
        end = _dt(timeline["forecast_until_utc"])
        assert all(
            _dt(period["end_utc"]) > start
            and _dt(period["start_utc"]) < end
            for period in timeline["periods"]
        )


def test_lord_natal_respects_birth_time_and_node_ownership_rules():
    known = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    unknown = calc_vedic_from_payload(
        _payload(birth_time=None),
        as_of=AS_OF,
    )
    known_jupiter = known["dasha_timeline"]["periods"][1]["lord_natal"]
    unknown_current = unknown["dasha_timeline"]["periods"][0]["lord_natal"]

    assert known_jupiter["house_no"] == 1
    assert known_jupiter["ruled_houses"] == [7, 10]
    assert unknown_current["house_no"] is None
    assert unknown_current["ruled_houses"] is None
    assert _lord_natal_context(
        "Rahu",
        known["planets_map"],
        known["house_lords"],
    )["ruled_houses"] is None


def test_gochara_adds_whole_sign_houses_without_breaking_default_signature():
    with patch("services.vedic_calc._sidereal_state", return_value=(120.0, 1.0)):
        legacy = build_vedic_gochara(transit_date=date(2026, 7, 14))
        enriched = build_vedic_gochara(
            transit_date=date(2026, 7, 14),
            natal_moon_rashi_no=2,
            natal_asc_rashi_no=3,
        )

    assert "house_from_moon" not in legacy["planets"]["Sun"]
    assert "house_from_asc" not in legacy["planets"]["Sun"]
    assert enriched["planets"]["Sun"]["rashi_no"] == 5
    assert enriched["planets"]["Sun"]["house_from_moon"] == 4
    assert enriched["planets"]["Sun"]["house_from_asc"] == 3

    unknown = calc_vedic_from_payload(
        _payload(birth_time=None),
        as_of=AS_OF,
    )
    assert all(
        "house_from_moon" in planet
        and "house_from_asc" not in planet
        for planet in unknown["gochara"]["planets"].values()
    )


def test_gochara_at_is_separate_from_dasha_clock_and_rejects_naive_datetime():
    baseline = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    shifted = calc_vedic_from_payload(
        _payload(),
        as_of=AS_OF,
        gochara_at=datetime(2036, 7, 14, tzinfo=timezone.utc),
    )

    assert shifted["dasha"] == baseline["dasha"]
    assert shifted["dasha_timeline"] == baseline["dasha_timeline"]
    assert shifted["gochara"]["dt_utc"] != baseline["gochara"]["dt_utc"]
    assert shifted["calculation_context"]["gochara_at_utc"].startswith("2036")
    with pytest.raises(ValueError, match="timezone-aware"):
        calc_vedic_from_payload(
            _payload(),
            as_of=AS_OF,
            gochara_at=datetime(2036, 7, 14),
        )


def test_timeline_golden_output_and_reliability_section_are_reproducible():
    first = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    second = calc_vedic_from_payload(_payload(), as_of=AS_OF)

    assert first["dasha_timeline"] == second["dasha_timeline"]
    assert first["dasha_timeline"]["periods"][0]["lord"] == "Rahu"
    current_antara = next(
        period
        for period in first["dasha_timeline"]["periods"][0]["antara"]
        if period["is_current"]
    )
    assert current_antara["lord"] == "Venus"
    assert first["dasha_timeline"]["periods"][0]["end_utc"] == (
        "2031-03-15T23:13:43.931254+00:00"
    )
    assert first["sections"]["dasha_timeline"] == first["sections"]["dasha"]
    assert first["calculation_context"]["forecast_until_utc"] == (
        "2036-07-13T10:12:00+00:00"
    )


def test_ai_summary_mentions_next_period_and_unknown_time_is_provisional():
    known = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    unknown = calc_vedic_from_payload(
        _payload(birth_time=None),
        as_of=AS_OF,
    )

    known_summary = _build_vedic_focus_summary({"vedic": known}, {})
    unknown_summary = _build_vedic_focus_summary({"vedic": unknown}, {})
    assert "現在はRahu期（Venusアンタラ）" in known_summary
    assert "次の切替は2027年10月頃にSun期へ" in known_summary
    assert "出生時刻不明のため暫定" in unknown_summary


def test_existing_dasha_shape_and_gochara_keys_remain_available():
    result = calc_vedic_from_payload(_payload(), as_of=AS_OF)

    assert set(result["dasha"]) == {"maha", "antara", "active_themes"}
    assert set(result["dasha"]["maha"]) == {"lord", "start", "end"}
    assert set(result["dasha"]["antara"]) == {"lord", "start", "end"}
    row = result["gochara"]["planets"]["Sun"]
    assert {
        "sidereal_lon_deg",
        "rashi_no",
        "rashi_name",
        "deg_in_sign",
        "nakshatra_name",
        "nakshatra_pada",
        "is_retrograde",
    } <= set(row)
    assert "dasha_timeline" in result["sections"]
