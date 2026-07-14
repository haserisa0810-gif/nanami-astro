from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from services.ai_report import _build_vedic_focus_summary
from services.astrologer_summary import build_full_astrologer_summary
from services.vedic_calc import (
    ASC_DEPENDENT_YOGA_SLUGS,
    _moon_day_stability,
    calc_vedic_from_payload,
)


AS_OF = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _payload(
    year: int = 1990,
    month: int = 5,
    day: int = 15,
    *,
    birth_time: str | None = "08:30",
) -> dict:
    return {
        "year": year,
        "month": month,
        "day": day,
        "hour": 8 if birth_time else 12,
        "minute": 30 if birth_time else 0,
        "birth_time": birth_time,
        "lat": 35.6895,
        "lng": 139.6917,
        "tz_offset_hours": 9,
    }


def test_known_time_keeps_existing_values_and_adds_reliability_metadata():
    result = calc_vedic_from_payload(_payload(), as_of=AS_OF)

    assert result["ascendant"]["rashi_name"] == "Gemini"
    assert result["ascendant"]["nakshatra_name"] == "Punarvasu"
    assert result["moon_nakshatra"]["nakshatra_name"] == "Uttara Ashadha"
    assert result["houses"][0]["rashi_name"] == "Gemini"
    assert result["varga"]["D9"]["ascendant"]["rashi_name"] == "Taurus"
    assert result["dasha"]["maha"]["lord"] == "Rahu"
    assert result["input_reliability"] == {
        "birth_time_status": "known",
        "overall": "high",
        "reasons": [],
    }
    assert all(
        section["available"] is True and section["reliability"] == "high"
        for section in result["sections"].values()
    )
    assert result["assumptions"]["node_mode"] == "mean"

    legacy_payload = _payload()
    legacy_payload.pop("birth_time")
    assert calc_vedic_from_payload(
        legacy_payload,
        as_of=AS_OF,
    )["input_reliability"]["birth_time_status"] == "known"


def test_missing_birth_time_removes_asc_sections_and_asc_yogas():
    result = calc_vedic_from_payload(_payload(birth_time=None), as_of=AS_OF)

    for key in ("ascendant", "houses", "houses_meta", "house_lords", "house_lords_placement"):
        assert key not in result
    assert "ascendant" not in result["varga"]["D9"]
    assert "ascendant" not in result["nakshatra_summary"]
    assert all("house_no" not in planet for planet in result["planets"])
    yoga_slugs = {item["slug"] for item in result["yogas"]}
    assert yoga_slugs
    assert yoga_slugs <= {
        "gajakesari_yoga",
        "chandra_mangala_yoga",
        "kemadruma_yoga",
    }
    assert not (yoga_slugs & ASC_DEPENDENT_YOGA_SLUGS)
    assert result["sections"]["ascendant"] == {
        "available": False,
        "reliability": "unavailable",
        "reasons": ["birth_time_unknown"],
    }
    assert result["sections"]["yogas"]["reliability"] == "provisional"
    assert result["sections"]["d9"]["reliability"] == "low"

    legacy_unknown = _payload(birth_time=None)
    legacy_unknown.pop("birth_time")
    legacy_unknown["hour"] = None
    assert calc_vedic_from_payload(
        legacy_unknown,
        as_of=AS_OF,
    )["input_reliability"]["birth_time_status"] == "unknown"


def test_constant_moon_marks_all_day_stable():
    # 実際の月は約13度/日進みパダ幅3°20′を必ず越えるため、
    # 「3層すべてstable」は定位置をモックして境界検出単体として検証する。
    with patch("services.vedic_calc._moon_lon_at", return_value=100.0):
        stability = _moon_day_stability(_payload(birth_time=None))

    assert stability == {
        "moon_rashi_stable_all_day": True,
        "moon_nakshatra_stable_all_day": True,
        "moon_pada_stable_all_day": True,
        "boundaries": [],
    }


def test_real_rashi_boundary_has_time_and_chandra_candidates():
    # 2024-01-02 JST: 月は Leo→Virgo、境界は約 12:59 UTC。
    result = calc_vedic_from_payload(
        _payload(2024, 1, 2, birth_time=None),
        as_of=AS_OF,
    )
    boundaries = [
        item for item in result["moon_day_stability"]["boundaries"]
        if item["kind"] == "rashi"
    ]

    assert result["moon_day_stability"]["moon_rashi_stable_all_day"] is False
    assert boundaries
    assert boundaries[0]["at_utc"].startswith("2024-01-02T12:59")
    assert boundaries[0]["from"] == "Leo"
    assert boundaries[0]["to"] == "Virgo"
    assert result["sections"]["chandra_lagna"]["reliability"] == "provisional"
    assert result["chandra_lagna"]["candidates"] == ["Leo", "Virgo"]


def test_pisces_to_aries_wrap_is_detected_without_360_error():
    # 2024-12-11 JST: 月は Pisces→Aries、境界は約 06:18 UTC。
    result = calc_vedic_from_payload(
        _payload(2024, 12, 11, birth_time=None),
        as_of=AS_OF,
    )
    rashi_boundaries = [
        item for item in result["moon_day_stability"]["boundaries"]
        if item["kind"] == "rashi"
    ]

    assert any(
        item["from"] == "Pisces"
        and item["to"] == "Aries"
        and item["at_utc"].startswith("2024-12-11T06:18")
        for item in rashi_boundaries
    )


def test_nakshatra_change_makes_dasha_low_with_two_scenarios():
    # 2024-01-01 JST: Magha→Purva Phalguni、境界は約 03:06 UTC。
    result = calc_vedic_from_payload(
        _payload(2024, 1, 1, birth_time=None),
        as_of=AS_OF,
    )

    assert result["moon_day_stability"]["moon_nakshatra_stable_all_day"] is False
    assert result["sections"]["dasha"]["reliability"] == "low"
    assert result["dasha"]["reliability"] == "low"
    assert len(result["dasha"]["scenarios"]) == 2
    assert {
        scenario["moon_nakshatra"]
        for scenario in result["dasha"]["scenarios"]
    } == {"Magha", "Purva Phalguni"}


def test_as_of_is_reproducible_advances_and_rejects_naive_datetime():
    payload = _payload()
    first = calc_vedic_from_payload(payload, as_of=AS_OF)
    second = calc_vedic_from_payload(payload, as_of=AS_OF)
    ten_years_later = calc_vedic_from_payload(
        payload,
        as_of=datetime(2036, 7, 14, tzinfo=timezone.utc),
    )

    assert first == second
    assert first["dasha"]["maha"]["lord"] != ten_years_later["dasha"]["maha"]["lord"]
    with pytest.raises(ValueError, match="timezone-aware"):
        calc_vedic_from_payload(payload, as_of=datetime(2026, 7, 14))


def test_calculation_context_reports_request_and_default_sources():
    requested = calc_vedic_from_payload(_payload(), as_of=AS_OF)
    defaulted = calc_vedic_from_payload(_payload())

    assert requested["calculation_context"]["as_of_utc"] == AS_OF.isoformat()
    assert requested["calculation_context"]["as_of_source"] == "request"
    assert defaulted["calculation_context"]["as_of_source"] == "default_now"
    assert defaulted["calculation_context"]["as_of_utc"].endswith("+00:00")


def test_consumers_accept_unknown_time_vedic_output():
    vedic = calc_vedic_from_payload(
        _payload(2024, 1, 2, birth_time=None),
        as_of=AS_OF,
    )

    focus = _build_vedic_focus_summary({"vedic": vedic}, {})
    summary = build_full_astrologer_summary({"vedic": vedic})

    assert isinstance(focus, str)
    assert "暫定" in focus
    assert isinstance(summary, dict)


def test_vedic_prompt_guard_respects_section_availability_and_reliability():
    guard = Path("prompts/guard_vedic.txt").read_text(encoding="utf-8")

    assert "available: false の項目には言及しない" in guard
    assert "reliability: provisional または low の項目は確定事項として断定しない" in guard
