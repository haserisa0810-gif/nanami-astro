from __future__ import annotations

"""Automatically calculated eclipses and recurring sky events.

No year-specific file or runtime network access is needed. Eclipses come from
Swiss Ephemeris and the Perseids peak is derived from solar longitude 140°.
"""

import math
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import swisseph as swe

from services.western_calc import configure_ephemeris, sign_of


JST = ZoneInfo("Asia/Tokyo")
TOKYO_GEO = (139.6917, 35.6895, 0.0)
FULL_SIGN_NAMES = {
    "Ari": "Aries",
    "Tau": "Taurus",
    "Gem": "Gemini",
    "Can": "Cancer",
    "Leo": "Leo",
    "Vir": "Virgo",
    "Lib": "Libra",
    "Sco": "Scorpio",
    "Sag": "Sagittarius",
    "Cap": "Capricorn",
    "Aqu": "Aquarius",
    "Pis": "Pisces",
}


def _jd_to_utc(jd_ut: float) -> datetime:
    year, month, day, decimal_hour = swe.revjul(jd_ut, swe.GREG_CAL)
    return datetime(year, month, day, tzinfo=timezone.utc) + timedelta(hours=decimal_hour)


def _position(jd_ut: float, body: int, flags: int) -> dict[str, Any]:
    longitude = float(swe.calc_ut(jd_ut, body, flags)[0][0]) % 360
    sign, degree = sign_of(longitude)
    whole_degree = int(degree)
    minute = int(round((degree - whole_degree) * 60))
    if minute == 60:
        whole_degree += 1
        minute = 0
    return {
        "sign": FULL_SIGN_NAMES.get(sign, sign),
        "degree": whole_degree,
        "minute": minute,
        "longitude": round(longitude, 6),
    }


def _eclipse_type(kind: str, result_flags: int) -> str:
    if kind == "solar" and result_flags & swe.ECL_ANNULAR_TOTAL:
        return "hybrid"
    for flag, label in (
        (swe.ECL_TOTAL, "total"),
        (swe.ECL_ANNULAR, "annular"),
        (swe.ECL_PARTIAL, "partial"),
        (swe.ECL_PENUMBRAL, "penumbral"),
    ):
        if result_flags & flag:
            return label
    return "unknown"


def _visible_from_tokyo(kind: str, maximum_jd: float, flags: int) -> bool | None:
    """Representative-location check; this is not a nationwide claim."""
    try:
        start = maximum_jd - 1.5
        if kind == "solar":
            _, local_times, _ = swe.sol_eclipse_when_loc(start, TOKYO_GEO, flags)
        else:
            _, local_times, _ = swe.lun_eclipse_when_loc(start, TOKYO_GEO, flags)
        return abs(float(local_times[0]) - maximum_jd) < 1.0
    except Exception:
        return None


def _raw_eclipses(start_year: int, end_year: int) -> list[dict[str, Any]]:
    flags = configure_ephemeris()
    start_jd = swe.julday(start_year, 1, 1, 0.0, swe.GREG_CAL) - 0.01
    end_jd = swe.julday(end_year + 1, 1, 1, 0.0, swe.GREG_CAL)
    events: list[dict[str, Any]] = []

    for kind in ("solar", "lunar"):
        cursor = start_jd
        while cursor < end_jd:
            if kind == "solar":
                result_flags, times = swe.sol_eclipse_when_glob(cursor, flags=flags)
                body, phase = swe.SUN, "new_moon"
            else:
                result_flags, times = swe.lun_eclipse_when(cursor, flags=flags)
                body, phase = swe.MOON, "full_moon"
            maximum_jd = float(times[0])
            if maximum_jd >= end_jd:
                break
            if maximum_jd <= cursor:
                cursor += 1.0
                continue

            utc_dt = _jd_to_utc(maximum_jd)
            local_dt = utc_dt.astimezone(JST)
            position = _position(maximum_jd, body, flags)
            event: dict[str, Any] = {
                "id": f"{kind}-eclipse-{utc_dt:%Y%m%dT%H%MZ}",
                "type": f"{kind}_eclipse",
                "subtype": _eclipse_type(kind, int(result_flags)),
                "phase": phase,
                "maximum_utc": utc_dt.isoformat().replace("+00:00", "Z"),
                "local_datetime": local_dt.isoformat(),
                "local_date": local_dt.date().isoformat(),
                "timezone": "Asia/Tokyo",
                "position": position,
                "importance": "critical",
                "visibility": {
                    "reference_location": "Tokyo",
                    "visible": _visible_from_tokyo(kind, maximum_jd, flags),
                    "scope_note": "東京の代表地点での計算。日本全国の可視性を断定しないこと。",
                },
                "source": "Swiss Ephemeris eclipse search",
            }
            if kind == "lunar":
                event["axis"] = {"moon": position, "sun": _position(maximum_jd, swe.SUN, flags)}
            events.append(event)
            cursor = maximum_jd + 10.0
    return sorted(events, key=lambda item: item["maximum_utc"])


def _with_eclipse_seasons(events: list[dict[str, Any]], year: int) -> list[dict[str, Any]]:
    groups: list[list[dict[str, Any]]] = []
    for event in events:
        current = datetime.fromisoformat(str(event["maximum_utc"]).replace("Z", "+00:00"))
        if not groups:
            groups.append([event])
            continue
        previous = datetime.fromisoformat(str(groups[-1][-1]["maximum_utc"]).replace("Z", "+00:00"))
        if (current - previous).days <= 40:
            groups[-1].append(event)
        else:
            groups.append([event])

    relevant = [
        group for group in groups
        if any(datetime.fromisoformat(str(item["local_datetime"])).year == year for item in group)
    ]
    output: list[dict[str, Any]] = []
    for sequence, group in enumerate(relevant, start=1):
        for event in group:
            if datetime.fromisoformat(str(event["local_datetime"])).year != year:
                continue
            copied = dict(event)
            copied["eclipse_season"] = {
                "id": f"{year}-eclipse-season-{sequence}",
                "sequence": sequence,
                "count_in_year": len(relevant),
            }
            output.append(copied)
    return output


def _solar_longitude_jd(year: int, target: float) -> float:
    flags = configure_ephemeris()
    low = swe.julday(year, 8, 8, 0.0, swe.GREG_CAL)
    high = swe.julday(year, 8, 17, 0.0, swe.GREG_CAL)
    for _ in range(48):
        middle = (low + high) / 2
        longitude = float(swe.calc_ut(middle, swe.SUN, flags)[0][0]) % 360
        if longitude < target:
            low = middle
        else:
            high = middle
    return (low + high) / 2


def _perseids_event(year: int) -> dict[str, Any]:
    flags = configure_ephemeris()
    peak_jd = _solar_longitude_jd(year, 140.0)
    utc_dt = _jd_to_utc(peak_jd)
    local_dt = utc_dt.astimezone(JST)
    sun_lon = float(swe.calc_ut(peak_jd, swe.SUN, flags)[0][0]) % 360
    moon_lon = float(swe.calc_ut(peak_jd, swe.MOON, flags)[0][0]) % 360
    elongation = abs((moon_lon - sun_lon + 180) % 360 - 180)
    illuminated_fraction = (1 - math.cos(math.radians(elongation))) / 2
    return {
        "id": f"perseids-{year}",
        "type": "meteor_shower",
        "subtype": "perseids",
        "name_ja": "ペルセウス座流星群",
        "peak_utc": utc_dt.isoformat().replace("+00:00", "Z"),
        "local_datetime": local_dt.isoformat(),
        "local_date": local_dt.date().isoformat(),
        "timezone": "Asia/Tokyo",
        "active_period": {"start": f"{year}-07-17", "end": f"{year}-08-24"},
        "moon_illuminated_fraction": round(illuminated_fraction, 3),
        "observing_note": "極大が昼でも前後の夜に観察機会がある。天候・街明かりを別途考慮すること。",
        "astrology_role": "editorial_sky_note",
        "importance": "high",
        "source": "Solar longitude 140.0 degrees; recurring Perseids model",
    }


@lru_cache(maxsize=16)
def events_for_year(year: int) -> tuple[dict[str, Any], ...]:
    year = int(year)
    eclipses = _with_eclipse_seasons(_raw_eclipses(year - 1, year + 1), year)
    return tuple(sorted(eclipses + [_perseids_event(year)], key=lambda item: str(item["local_datetime"])))


def events_for_month(year: int, month: int) -> list[dict[str, Any]]:
    prefix = f"{int(year):04d}-{int(month):02d}-"
    return [dict(event) for event in events_for_year(int(year)) if str(event["local_date"]).startswith(prefix)]


def events_near_date(target: date, *, eclipse_days: int = 3, meteor_days: int = 1) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for event in events_for_year(target.year):
        event_date = date.fromisoformat(str(event["local_date"]))
        distance = (event_date - target).days
        window = meteor_days if event["type"] == "meteor_shower" else eclipse_days
        if abs(distance) <= window:
            copied = dict(event)
            copied["days_from_target"] = distance
            copied["relation"] = "today" if distance == 0 else ("upcoming" if distance > 0 else "recent")
            output.append(copied)
    return output


def eclipse_year_summary(year: int) -> dict[str, Any]:
    eclipses = [event for event in events_for_year(int(year)) if str(event["type"]).endswith("_eclipse")]
    season_ids = {event["eclipse_season"]["id"] for event in eclipses}
    return {
        "year": int(year),
        "eclipse_count": len(eclipses),
        "eclipse_season_count": len(season_ids),
        "wording_note": "食の回数と食シーズンの回数を混同しないこと。",
    }
