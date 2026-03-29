from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Tuple

import swisseph as swe

from pathlib import Path


def configure_ephemeris() -> int:
    ephe_dir = Path(__file__).resolve().parents[2] / "ephe"
    if ephe_dir.exists() and any(ephe_dir.glob("*.se1")):
        swe.set_ephe_path(str(ephe_dir))
        return swe.FLG_SWIEPH
    return swe.FLG_MOSEPH


def ephemeris_debug_info() -> dict:
    ephe_dir = Path(__file__).resolve().parents[2] / "ephe"
    files = sorted([p.name for p in ephe_dir.glob("*")]) if ephe_dir.exists() else []
    return {
        "ephe_dir": str(ephe_dir),
        "ephe_dir_exists": ephe_dir.exists(),
        "ephe_files": files,
        "has_se1": any(name.endswith(".se1") for name in files),
    }


RASHI_NAMES = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces",
]

NAKSHATRA_NAMES = [
    "Ashwini", "Bharani", "Krittika", "Rohini", "Mrigashirsha", "Ardra",
    "Punarvasu", "Pushya", "Ashlesha", "Magha", "Purva Phalguni", "Uttara Phalguni",
    "Hasta", "Chitra", "Swati", "Vishakha", "Anuradha", "Jyeshtha",
    "Mula", "Purva Ashadha", "Uttara Ashadha", "Shravana", "Dhanishta",
    "Shatabhisha", "Purva Bhadrapada", "Uttara Bhadrapada", "Revati",
]

NAKSHATRA_LORDS = [
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",
    "Ketu", "Venus", "Sun", "Moon", "Mars", "Rahu", "Jupiter", "Saturn", "Mercury",
]

NAKSHATRA_SPAN = 13 + 20 / 60
PADA_SPAN = NAKSHATRA_SPAN / 4

PLANET_IDS = {
    "Sun": swe.SUN,
    "Moon": swe.MOON,
    "Mercury": swe.MERCURY,
    "Venus": swe.VENUS,
    "Mars": swe.MARS,
    "Jupiter": swe.JUPITER,
    "Saturn": swe.SATURN,
    "Uranus": swe.URANUS,
    "Neptune": swe.NEPTUNE,
    "Pluto": swe.PLUTO,
    "Rahu": swe.MEAN_NODE,
}

TRADITIONAL_VEDIC_PLANETS = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "Rahu", "Ketu"]
SEVEN_CLASSICAL_PLANETS = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn"]

SIGN_LORDS = {
    "Aries": "Mars",
    "Taurus": "Venus",
    "Gemini": "Mercury",
    "Cancer": "Moon",
    "Leo": "Sun",
    "Virgo": "Mercury",
    "Libra": "Venus",
    "Scorpio": "Mars",
    "Sagittarius": "Jupiter",
    "Capricorn": "Saturn",
    "Aquarius": "Saturn",
    "Pisces": "Jupiter",
}

EXALTATION_SIGNS = {
    "Sun": "Aries",
    "Moon": "Taurus",
    "Mars": "Capricorn",
    "Mercury": "Virgo",
    "Jupiter": "Cancer",
    "Venus": "Pisces",
    "Saturn": "Libra",
    "Rahu": "Taurus",
    "Ketu": "Scorpio",
}

DEBILITATION_SIGNS = {
    "Sun": "Libra",
    "Moon": "Scorpio",
    "Mars": "Cancer",
    "Mercury": "Pisces",
    "Jupiter": "Capricorn",
    "Venus": "Virgo",
    "Saturn": "Aries",
    "Rahu": "Scorpio",
    "Ketu": "Taurus",
}

OWN_SIGNS = {
    "Sun": {"Leo"},
    "Moon": {"Cancer"},
    "Mercury": {"Gemini", "Virgo"},
    "Venus": {"Taurus", "Libra"},
    "Mars": {"Aries", "Scorpio"},
    "Jupiter": {"Sagittarius", "Pisces"},
    "Saturn": {"Capricorn", "Aquarius"},
}

PLANET_FRIENDS = {
    "Sun": {"Moon", "Mars", "Jupiter"},
    "Moon": {"Sun", "Mercury"},
    "Mars": {"Sun", "Moon", "Jupiter"},
    "Mercury": {"Sun", "Venus"},
    "Jupiter": {"Sun", "Moon", "Mars"},
    "Venus": {"Mercury", "Saturn"},
    "Saturn": {"Mercury", "Venus"},
}

PLANET_ENEMIES = {
    "Sun": {"Venus", "Saturn"},
    "Moon": set(),
    "Mars": {"Mercury"},
    "Mercury": {"Moon"},
    "Jupiter": {"Mercury", "Venus"},
    "Venus": {"Sun", "Moon"},
    "Saturn": {"Sun", "Moon", "Mars"},
}

COMBUST_ORBS = {
    "Moon": 12.0,
    "Mars": 17.0,
    "Mercury": 14.0,
    "Jupiter": 11.0,
    "Venus": 10.0,
    "Saturn": 15.0,
}

VIMSHOTTARI_SEQUENCE = [
    ("Ketu", 7), ("Venus", 20), ("Sun", 6), ("Moon", 10), ("Mars", 7),
    ("Rahu", 18), ("Jupiter", 16), ("Saturn", 19), ("Mercury", 17),
]
VIMSHOTTARI_YEARS = dict(VIMSHOTTARI_SEQUENCE)
VIMSHOTTARI_INDEX = {name: idx for idx, (name, _) in enumerate(VIMSHOTTARI_SEQUENCE)}

JST = timezone(timedelta(hours=9))


def _to_utc_from_payload(payload: Dict[str, Any]) -> datetime:
    y = int(payload["year"])
    m = int(payload["month"])
    d = int(payload["day"])
    hh = int(payload.get("hour", 12))
    mm = int(payload.get("minute", 0))
    local_dt = datetime(y, m, d, hh, mm, 0, tzinfo=JST)
    return local_dt.astimezone(timezone.utc)


def _julian_day_ut(dt_utc: datetime) -> float:
    if dt_utc.tzinfo is None:
        raise ValueError("dt_utc must be timezone-aware (UTC).")
    dt_utc = dt_utc.astimezone(timezone.utc)
    y, m, d = dt_utc.year, dt_utc.month, dt_utc.day
    hour = (
        dt_utc.hour
        + dt_utc.minute / 60
        + dt_utc.second / 3600
        + dt_utc.microsecond / 3_600_000_000
    )
    return swe.julday(y, m, d, hour, swe.GREG_CAL)


def _set_lahiri_sidereal() -> None:
    swe.set_sid_mode(swe.SIDM_LAHIRI, 0, 0)


def _sidereal_longitude(jd_ut: float, planet_id: int) -> float:
    _set_lahiri_sidereal()
    flags = configure_ephemeris() | swe.FLG_SIDEREAL
    xx, _ = swe.calc_ut(jd_ut, planet_id, flags)
    return xx[0] % 360.0


def _sidereal_state(jd_ut: float, planet_id: int) -> tuple[float, float]:
    _set_lahiri_sidereal()
    flags = configure_ephemeris() | swe.FLG_SIDEREAL | swe.FLG_SPEED
    xx, _ = swe.calc_ut(jd_ut, planet_id, flags)
    lon = xx[0] % 360.0
    speed_lon = float(xx[3]) if len(xx) > 3 else 0.0
    return lon, speed_lon


def _rashi_from_longitude(lon_deg: float) -> Tuple[int, str, float]:
    lon = lon_deg % 360.0
    idx0 = int(lon // 30.0)
    rashi_no = idx0 + 1
    name = RASHI_NAMES[idx0]
    deg_in_sign = lon - (idx0 * 30.0)
    return rashi_no, name, deg_in_sign


def _nakshatra_from_longitude(lon_deg: float) -> Tuple[int, str, int, float]:
    lon_deg = lon_deg % 360.0
    idx0 = int(lon_deg // NAKSHATRA_SPAN)
    nak_no = idx0 + 1
    name = NAKSHATRA_NAMES[idx0]
    start = idx0 * NAKSHATRA_SPAN
    offset = lon_deg - start
    pada = int(offset // PADA_SPAN) + 1
    progress = offset / NAKSHATRA_SPAN
    return nak_no, name, pada, progress


def _nakshatra_lord(nak_no: int) -> str:
    return NAKSHATRA_LORDS[int(nak_no) - 1]


def _ascendant_sidereal(jd_ut: float, lat: float, lon: float) -> float:
    _set_lahiri_sidereal()
    flags = configure_ephemeris() | swe.FLG_SIDEREAL
    if hasattr(swe, "houses_ex"):
        cusps, ascmc = swe.houses_ex(jd_ut, lat, lon, b"P", flags)
        asc = float(ascmc[0]) % 360.0
        return asc
    cusps, ascmc = swe.houses(jd_ut, lat, lon, b"P")
    asc = float(ascmc[0]) % 360.0
    return asc


def _whole_sign_house_no(asc_rashi_no: int, planet_rashi_no: int) -> int:
    a = asc_rashi_no - 1
    p = planet_rashi_no - 1
    return ((p - a) % 12) + 1


def _friend_enemy_status(planet: str, sign_name: str) -> str | None:
    lord = SIGN_LORDS.get(sign_name)
    if not lord or planet not in PLANET_FRIENDS:
        return None
    if lord == planet:
        return "own"
    if lord in PLANET_FRIENDS.get(planet, set()):
        return "friend"
    if lord in PLANET_ENEMIES.get(planet, set()):
        return "enemy"
    return "neutral"


def _is_combust(planet: str, lon: float, sun_lon: float) -> tuple[bool, float | None]:
    if planet == "Sun" or planet not in COMBUST_ORBS:
        return False, None
    dist = _angle_diff(lon, sun_lon)
    orb = COMBUST_ORBS[planet]
    return dist <= orb, dist


def _build_dignity(planet: str, sign_name: str, lon: float, speed_lon: float, sun_lon: float) -> dict[str, Any]:
    is_exalted = EXALTATION_SIGNS.get(planet) == sign_name
    is_debilitated = DEBILITATION_SIGNS.get(planet) == sign_name
    is_own_sign = sign_name in OWN_SIGNS.get(planet, set())
    friend_enemy_status = _friend_enemy_status(planet, sign_name)
    is_comb, combust_distance = _is_combust(planet, lon, sun_lon)
    is_retrograde = bool(speed_lon < 0) if planet not in {"Sun", "Moon", "Ketu"} else False

    score = 0.5
    if is_exalted:
        score += 0.25
    if is_own_sign:
        score += 0.20
    if friend_enemy_status == "friend":
        score += 0.10
    elif friend_enemy_status == "enemy":
        score -= 0.10
    if is_debilitated:
        score -= 0.25
    if is_comb:
        score -= 0.15
    if is_retrograde:
        score += 0.05
    score = max(0.0, min(1.0, round(score, 3)))

    sign_status = "neutral"
    if is_exalted:
        sign_status = "exalted"
    elif is_debilitated:
        sign_status = "debilitated"
    elif is_own_sign:
        sign_status = "own_sign"
    elif friend_enemy_status == "friend":
        sign_status = "friendly_sign"
    elif friend_enemy_status == "enemy":
        sign_status = "enemy_sign"

    return {
        "sign_status": sign_status,
        "is_exalted": is_exalted,
        "is_debilitated": is_debilitated,
        "is_own_sign": is_own_sign,
        "friend_enemy_status": friend_enemy_status,
        "is_combust": is_comb,
        "combust_distance_deg": round(combust_distance, 3) if combust_distance is not None else None,
        "is_retrograde": is_retrograde,
        "speed_lon_deg_per_day": round(speed_lon, 6),
        "strength_score": score,
    }


def _house_sign_name(asc_rashi_no: int, house_no: int) -> str:
    idx0 = (asc_rashi_no - 1 + house_no - 1) % 12
    return RASHI_NAMES[idx0]


def _build_house_lords(asc_rashi_no: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for house_no in range(1, 13):
        sign_name = _house_sign_name(asc_rashi_no, house_no)
        out[str(house_no)] = {
            "house_no": house_no,
            "rashi_name": sign_name,
            "lord": SIGN_LORDS[sign_name],
        }
    return out


def _build_house_lords_placement(house_lords: dict[str, Any], planets: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for house_key, item in house_lords.items():
        lord = item["lord"]
        lord_data = planets.get(lord) or {}
        out[house_key] = {
            "lord": lord,
            "placed_in_house": lord_data.get("house_no"),
            "placed_in_rashi": lord_data.get("rashi_name"),
            "strength_score": ((lord_data.get("dignity") or {}).get("strength_score")),
        }
    return out


def _house_distance(from_house: int, to_house: int) -> int:
    return ((to_house - from_house) % 12) + 1


def _build_planetary_aspects_vedic(planets: dict[str, Any]) -> list[dict[str, Any]]:
    aspect_map = {
        "Sun": [(7, "7th")],
        "Moon": [(7, "7th")],
        "Mercury": [(7, "7th")],
        "Venus": [(7, "7th")],
        "Mars": [(4, "special_4th"), (7, "7th"), (8, "special_8th")],
        "Jupiter": [(5, "special_5th"), (7, "7th"), (9, "special_9th")],
        "Saturn": [(3, "special_3rd"), (7, "7th"), (10, "special_10th")],
        "Rahu": [(5, "special_5th"), (7, "7th"), (9, "special_9th")],
        "Ketu": [(5, "special_5th"), (7, "7th"), (9, "special_9th")],
    }
    out: list[dict[str, Any]] = []
    for name, pdata in planets.items():
        from_house = pdata.get("house_no")
        if not isinstance(from_house, int):
            continue
        for dist, aspect_type in aspect_map.get(name, []):
            to_house = ((from_house + dist - 2) % 12) + 1
            out.append({
                "from": name,
                "from_house": from_house,
                "to_house": to_house,
                "distance": dist,
                "type": aspect_type,
            })
    return out


def _angle_diff(a: float, b: float) -> float:
    diff = abs((a % 360.0) - (b % 360.0)) % 360.0
    return min(diff, 360.0 - diff)


def _is_kendra(house_no: int | None) -> bool:
    return house_no in {1, 4, 7, 10}


def _is_trikona(house_no: int | None) -> bool:
    return house_no in {1, 5, 9}


def _is_dusthana(house_no: int | None) -> bool:
    return house_no in {6, 8, 12}


def _build_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None) -> list[dict[str, Any]]:
    yogas: list[dict[str, Any]] = []
    moon = planets.get("Moon") or {}
    jupiter = planets.get("Jupiter") or {}
    mars = planets.get("Mars") or {}

    moon_house = moon.get("house_no")
    jup_house = jupiter.get("house_no")
    mars_house = mars.get("house_no")

    if isinstance(moon_house, int) and isinstance(jup_house, int):
        rel = _house_distance(jup_house, moon_house)
        if rel in {1, 4, 7, 10}:
            yogas.append({
                "name": "Gajakesari Yoga",
                "strength": round(min(1.0, 0.45 + 0.3 * float((jupiter.get("dignity") or {}).get("strength_score") or 0.5)), 3),
                "evidence": ["Moon in kendra from Jupiter"],
                "tags": ["mental_support", "reputation_support"],
            })

    if isinstance(moon_house, int) and isinstance(mars_house, int) and moon_house == mars_house:
        yogas.append({
            "name": "Chandra-Mangala Yoga",
            "strength": round(min(1.0, 0.5 + 0.2 * float((mars.get("dignity") or {}).get("strength_score") or 0.5)), 3),
            "evidence": ["Moon conjunct Mars by house"],
            "tags": ["financial_drive", "emotional_heat"],
        })

    if isinstance(moon_house, int):
        has_neighbor = False
        for pname, pdata in planets.items():
            if pname in {"Rahu", "Ketu", "Uranus", "Neptune", "Pluto"}:
                continue
            h = pdata.get("house_no")
            if not isinstance(h, int) or pname == "Moon":
                continue
            if h in {((moon_house + 10) % 12) + 1, (moon_house % 12) + 1}:
                has_neighbor = True
                break
        if not has_neighbor:
            yogas.append({
                "name": "Kemadruma Yoga",
                "strength": 0.45,
                "evidence": ["No classical planet in 2nd/12th from Moon"],
                "tags": ["inner_isolation_tendency"],
            })

    if house_lords:
        kendra_lords = {house_lords[str(h)]["lord"] for h in (1, 4, 7, 10)}
        trikona_lords = {house_lords[str(h)]["lord"] for h in (1, 5, 9)}
        raja_hits: list[str] = []
        for p in sorted(kendra_lords & trikona_lords):
            pdata = planets.get(p) or {}
            if _is_kendra(pdata.get("house_no")) or _is_trikona(pdata.get("house_no")):
                raja_hits.append(p)
        if raja_hits:
            yogas.append({
                "name": "Raja Yoga",
                "strength": round(min(1.0, 0.5 + 0.1 * len(raja_hits)), 3),
                "evidence": [f"Kendra/trikona lord active: {', '.join(raja_hits)}"],
                "tags": ["status_support", "career_support"],
            })

        wealth_lords = {house_lords[str(h)]["lord"] for h in (2, 11, 5, 9)}
        dhan_hits = []
        for p in sorted(wealth_lords):
            pdata = planets.get(p) or {}
            if isinstance(pdata.get("house_no"), int) and pdata.get("house_no") in {2, 5, 9, 11}:
                dhan_hits.append(p)
        if len(dhan_hits) >= 2:
            yogas.append({
                "name": "Dhana Yoga",
                "strength": round(min(1.0, 0.45 + 0.1 * len(dhan_hits)), 3),
                "evidence": [f"Wealth lords active: {', '.join(dhan_hits)}"],
                "tags": ["wealth_forming_capacity"],
            })

        viparita_hits = []
        for h in (6, 8, 12):
            lord = house_lords[str(h)]["lord"]
            pdata = planets.get(lord) or {}
            if pdata.get("house_no") in {6, 8, 12}:
                viparita_hits.append(f"{h}L={lord}")
        if viparita_hits:
            yogas.append({
                "name": "Viparita Raja Yoga",
                "strength": round(min(1.0, 0.45 + 0.08 * len(viparita_hits)), 3),
                "evidence": viparita_hits,
                "tags": ["recovery_from_adversity"],
            })

    yogas.sort(key=lambda x: float(x.get("strength") or 0), reverse=True)
    return yogas


def _navamsa_sign_index(rashi_no: int, deg_in_sign: float) -> int:
    part = int((deg_in_sign % 30.0) / (30.0 / 9.0))
    sign_type = (rashi_no - 1) % 3
    if sign_type == 0:  # movable
        start = rashi_no
    elif sign_type == 1:  # fixed
        start = ((rashi_no + 8 - 1) % 12) + 1
    else:  # dual
        start = ((rashi_no + 4 - 1) % 12) + 1
    return ((start - 1 + part) % 12) + 1


def _build_d9(planets: dict[str, Any], asc_data: dict[str, Any] | None) -> dict[str, Any]:
    d9_planets: dict[str, Any] = {}
    for pname, pdata in planets.items():
        rashi_no = int(pdata["rashi_no"])
        deg_in_sign = float(pdata["deg_in_sign"])
        d9_rashi_no = _navamsa_sign_index(rashi_no, deg_in_sign)
        d9_sign_name = RASHI_NAMES[d9_rashi_no - 1]
        d9_deg_in_sign = round((deg_in_sign % (30.0 / 9.0)) * 9.0, 6)
        d9_planets[pname] = {
            "rashi_no": d9_rashi_no,
            "rashi_name": d9_sign_name,
            "deg_in_sign": d9_deg_in_sign,
            "strength_score": _build_dignity(pname, d9_sign_name, 0.0, 0.0, 999.0).get("strength_score") if pname in TRADITIONAL_VEDIC_PLANETS else None,
        }

    d9_asc = None
    if asc_data:
        d9_rashi_no = _navamsa_sign_index(int(asc_data["rashi_no"]), float(asc_data["deg_in_sign"]))
        d9_asc = {
            "rashi_no": d9_rashi_no,
            "rashi_name": RASHI_NAMES[d9_rashi_no - 1],
        }

    comparisons: dict[str, Any] = {}
    for pname in TRADITIONAL_VEDIC_PLANETS:
        if pname not in planets or pname not in d9_planets:
            continue
        d1_strength = float((planets[pname].get("dignity") or {}).get("strength_score") or 0.5)
        d9_strength = float(d9_planets[pname].get("strength_score") or 0.5)
        if d9_strength - d1_strength >= 0.15:
            summary = "improves_in_maturity"
        elif d1_strength - d9_strength >= 0.15:
            summary = "surface_strength_internal_weakness"
        else:
            summary = "stable_across_charts"
        comparisons[pname] = {
            "D1_strength": round(d1_strength, 3),
            "D9_strength": round(d9_strength, 3),
            "summary": summary,
        }

    return {"ascendant": d9_asc, "planets": d9_planets, "comparisons": comparisons}


def _dasha_chain_from_moon(moon_nak_no: int, remaining_fraction: float, birth_dt_utc: datetime, now_utc: datetime) -> dict[str, Any]:
    start_lord = _nakshatra_lord(moon_nak_no)
    first_years_total = VIMSHOTTARI_YEARS[start_lord]
    elapsed_first_years = first_years_total * (1.0 - remaining_fraction)
    cycle_days: list[tuple[str, datetime, datetime]] = []

    seq_idx = VIMSHOTTARI_INDEX[start_lord]
    cursor = birth_dt_utc - timedelta(days=elapsed_first_years * 365.2425)
    for offset in range(18):
        lord, years = VIMSHOTTARI_SEQUENCE[(seq_idx + offset) % len(VIMSHOTTARI_SEQUENCE)]
        start = cursor
        end = start + timedelta(days=years * 365.2425)
        cycle_days.append((lord, start, end))
        cursor = end
        if end > now_utc + timedelta(days=365.2425 * 40):
            break

    maha = None
    for lord, start, end in cycle_days:
        if start <= now_utc < end:
            maha = {"lord": lord, "start": start.isoformat(), "end": end.isoformat()}
            maha_start, maha_end, maha_lord = start, end, lord
            break
    if maha is None:
        lord, start, end = cycle_days[0]
        maha = {"lord": lord, "start": start.isoformat(), "end": end.isoformat()}
        maha_start, maha_end, maha_lord = start, end, lord

    antara = None
    maha_duration_days = (maha_end - maha_start).total_seconds() / 86400.0
    cursor = maha_start
    seq_idx = VIMSHOTTARI_INDEX[maha_lord]
    for offset in range(9):
        sub_lord, sub_years = VIMSHOTTARI_SEQUENCE[(seq_idx + offset) % 9]
        sub_days = maha_duration_days * (sub_years / 120.0)
        start = cursor
        end = start + timedelta(days=sub_days)
        if start <= now_utc < end:
            antara = {"lord": sub_lord, "start": start.isoformat(), "end": end.isoformat()}
            break
        cursor = end
    return {"maha": maha, "antara": antara}


def _active_dasha_themes(dasha: dict[str, Any], planets: dict[str, Any], house_lords_placement: dict[str, Any] | None) -> list[str]:
    themes: list[str] = []
    lords = []
    for key in ("maha", "antara"):
        item = dasha.get(key) or {}
        lord = item.get("lord")
        if lord:
            lords.append(lord)
    for lord in lords:
        pdata = planets.get(lord) or {}
        house_no = pdata.get("house_no")
        if house_no in {10, 11}:
            themes.append("career_responsibility")
        if house_no in {7, 8}:
            themes.append("relationship_restructuring")
        if house_no in {6, 12}:
            themes.append("inner_pressure")
        if house_no in {5, 9}:
            themes.append("learning_growth")
        if house_no in {2, 11}:
            themes.append("wealth_focus")

        if house_lords_placement:
            for h in (1, 4, 7, 10):
                info = house_lords_placement.get(str(h)) or {}
                if info.get("lord") == lord:
                    themes.append({1: "identity_reset", 4: "home_foundation", 7: "partnership_theme", 10: "career_execution"}[h])
    deduped = []
    seen = set()
    for t in themes:
        if t not in seen:
            deduped.append(t)
            seen.add(t)
    return deduped


def calc_vedic_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    dt_utc = _to_utc_from_payload(payload)
    jd = _julian_day_ut(dt_utc)

    lat = payload.get("lat")
    lng = payload.get("lng") or payload.get("lon")
    has_geo = (lat is not None) and (lng is not None)

    planets: Dict[str, Any] = {}
    raw_states: Dict[str, tuple[float, float]] = {}
    for name, pid in PLANET_IDS.items():
        lon, speed_lon = _sidereal_state(jd, pid)
        raw_states[name] = (lon, speed_lon)
        rashi_no, rashi_name, deg_in_sign = _rashi_from_longitude(lon)
        nak_no, nak_name, pada, progress = _nakshatra_from_longitude(lon)
        planets[name] = {
            "name": name,
            "sidereal_lon_deg": lon,
            "lon": lon,
            "rashi_no": rashi_no,
            "rashi_name": rashi_name,
            "sign": rashi_name,
            "deg_in_sign": deg_in_sign,
            "nakshatra_no": nak_no,
            "nakshatra_name": nak_name,
            "nakshatra_pada": pada,
            "nakshatra_progress": progress,
            "nakshatra_lord": _nakshatra_lord(nak_no),
            "speed_lon_deg_per_day": round(speed_lon, 6),
        }

    rahu_lon = planets["Rahu"]["sidereal_lon_deg"]
    ketu_lon = (rahu_lon + 180.0) % 360.0
    ketu_rashi_no, ketu_rashi_name, ketu_deg_in_sign = _rashi_from_longitude(ketu_lon)
    ketu_nak_no, ketu_nak_name, ketu_pada, ketu_progress = _nakshatra_from_longitude(ketu_lon)
    planets["Ketu"] = {
        "name": "Ketu",
        "sidereal_lon_deg": ketu_lon,
        "lon": ketu_lon,
        "rashi_no": ketu_rashi_no,
        "rashi_name": ketu_rashi_name,
        "sign": ketu_rashi_name,
        "deg_in_sign": ketu_deg_in_sign,
        "nakshatra_no": ketu_nak_no,
        "nakshatra_name": ketu_nak_name,
        "nakshatra_pada": ketu_pada,
        "nakshatra_progress": ketu_progress,
        "nakshatra_lord": _nakshatra_lord(ketu_nak_no),
        "speed_lon_deg_per_day": round(-raw_states["Rahu"][1], 6),
    }

    moon_lon = planets["Moon"]["sidereal_lon_deg"]
    nak_no, nak_name, pada, progress = _nakshatra_from_longitude(moon_lon)

    sun_lon = planets["Sun"]["sidereal_lon_deg"]
    for pname, pdata in planets.items():
        speed = pdata.get("speed_lon_deg_per_day") or 0.0
        pdata["dignity"] = _build_dignity(pname, str(pdata["rashi_name"]), float(pdata["sidereal_lon_deg"]), float(speed), sun_lon)

    asc_data: Dict[str, Any] | None = None
    houses_data: Dict[str, Any] | None = None
    house_lords: Dict[str, Any] | None = None
    house_lords_placement: Dict[str, Any] | None = None
    warnings: list[str] = []

    if has_geo:
        try:
            asc_lon = _ascendant_sidereal(jd, float(lat), float(lng))
            asc_rashi_no, asc_rashi_name, asc_deg_in_sign = _rashi_from_longitude(asc_lon)
            asc_nak_no, asc_nak_name, asc_pada, asc_progress = _nakshatra_from_longitude(asc_lon)
            asc_data = {
                "sidereal_lon_deg": asc_lon,
                "lon": asc_lon,
                "rashi_no": asc_rashi_no,
                "rashi_name": asc_rashi_name,
                "sign": asc_rashi_name,
                "deg_in_sign": asc_deg_in_sign,
                "nakshatra_no": asc_nak_no,
                "nakshatra_name": asc_nak_name,
                "nakshatra_pada": asc_pada,
                "nakshatra_progress": asc_progress,
                "nakshatra_lord": _nakshatra_lord(asc_nak_no),
            }

            house_signs = []
            for i in range(12):
                idx0 = (asc_rashi_no - 1 + i) % 12
                sign_name = RASHI_NAMES[idx0]
                house_signs.append(
                    {
                        "house": i + 1,
                        "house_no": i + 1,
                        "rashi_no": idx0 + 1,
                        "rashi_name": sign_name,
                        "sign": sign_name,
                        "lon": idx0 * 30.0,
                        "degree": 0.0,
                    }
                )

            for pname, pdata in planets.items():
                prashi = int(pdata["rashi_no"])
                pdata["house_no"] = _whole_sign_house_no(asc_rashi_no, prashi)

            house_lords = _build_house_lords(asc_rashi_no)
            house_lords_placement = _build_house_lords_placement(house_lords, planets)
            houses_data = {
                "system": "whole_sign",
                "asc_rashi_no": asc_rashi_no,
                "asc_rashi_name": asc_rashi_name,
                "houses": house_signs,
            }
        except Exception as e:
            warnings.append(f"ASC/ハウス計算に失敗しました: {e!r}")
    else:
        warnings.append("緯度経度が無いため、ASC（ラグナ）とハウスは未計算です。")

    planetary_aspects_vedic = _build_planetary_aspects_vedic(planets)
    yogas = _build_yogas(planets, house_lords)
    d9 = _build_d9(planets, asc_data)
    dasha = _dasha_chain_from_moon(nak_no, 1.0 - progress, dt_utc, datetime.now(timezone.utc))
    dasha["active_themes"] = _active_dasha_themes(dasha, planets, house_lords_placement)

    nakshatra_summary = {
        "moon": {
            "planet": "Moon",
            "nakshatra_name": planets["Moon"]["nakshatra_name"],
            "pada": planets["Moon"]["nakshatra_pada"],
            "lord": planets["Moon"]["nakshatra_lord"],
        },
        "ascendant": {
            "planet": "Ascendant",
            "nakshatra_name": asc_data.get("nakshatra_name") if asc_data else None,
            "pada": asc_data.get("nakshatra_pada") if asc_data else None,
            "lord": asc_data.get("nakshatra_lord") if asc_data else None,
        } if asc_data else None,
        "key_planets": {
            p: {
                "nakshatra_name": planets[p]["nakshatra_name"],
                "pada": planets[p]["nakshatra_pada"],
                "lord": planets[p]["nakshatra_lord"],
            }
            for p in ("Venus", "Mars", "Saturn", "Jupiter", "Mercury") if p in planets
        },
    }

    summary_flags = {
        "career": [],
        "relationship": [],
        "mental_pressure": [],
        "wealth": [],
        "spiritual": [],
    }
    for y in yogas:
        tags = y.get("tags") or []
        if "career_support" in tags or "status_support" in tags:
            summary_flags["career"].append(y["name"])
        if "wealth_forming_capacity" in tags:
            summary_flags["wealth"].append(y["name"])
        if "inner_isolation_tendency" in tags:
            summary_flags["mental_pressure"].append(y["name"])
    for theme in dasha.get("active_themes") or []:
        if theme in {"career_execution", "career_responsibility"}:
            summary_flags["career"].append(theme)
        elif theme in {"relationship_restructuring", "partnership_theme"}:
            summary_flags["relationship"].append(theme)
        elif theme == "inner_pressure":
            summary_flags["mental_pressure"].append(theme)
        elif theme == "wealth_focus":
            summary_flags["wealth"].append(theme)
        elif theme in {"learning_growth", "identity_reset"}:
            summary_flags["spiritual"].append(theme)

    out: Dict[str, Any] = {
        "system": "vedic",
        "ephemeris": ephemeris_debug_info(),
        "zodiac_type": "sidereal",
        "ayanamsha": "Lahiri",
        "dt_utc": dt_utc.isoformat(),
        "jd_ut": jd,
        "moon_nakshatra": {
            "moon_sidereal_lon_deg": moon_lon,
            "nakshatra_no": nak_no,
            "nakshatra_name": nak_name,
            "pada": pada,
            "lord": _nakshatra_lord(nak_no),
            "progress_in_nakshatra": progress,
            "remaining_in_nakshatra": 1.0 - progress,
        },
        "nakshatra_summary": nakshatra_summary,
        "ascendant": asc_data,
        "houses": houses_data["houses"] if isinstance(houses_data, dict) else None,
        "houses_meta": houses_data,
        "house_lords": house_lords,
        "house_lords_placement": house_lords_placement,
        "planets": list(planets.values()),
        "planets_map": planets,
        "planetary_aspects_vedic": planetary_aspects_vedic,
        "yogas": yogas,
        "varga": {"D9": d9},
        "dasha": dasha,
        "summary_flags": summary_flags,
    }

    if warnings:
        out["_warnings"] = warnings

    return out
