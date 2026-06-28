from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, Tuple

import swisseph as swe

from pathlib import Path


def configure_ephemeris() -> int:
    ephe_dir = Path(__file__).resolve().parents[1] / "ephe"
    if ephe_dir.exists() and any(ephe_dir.glob("*.se1")):
        swe.set_ephe_path(str(ephe_dir))
        return swe.FLG_SWIEPH
    return swe.FLG_MOSEPH


def ephemeris_debug_info() -> dict:
    ephe_dir = Path(__file__).resolve().parents[1] / "ephe"
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
GOCHARA_PLANETS = TRADITIONAL_VEDIC_PLANETS

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


def _coerce_transit_dt_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=JST)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, 12, 0, tzinfo=JST).astimezone(timezone.utc)

    text = str(value or "").strip()
    if not text:
        return datetime.now(timezone.utc)
    try:
        if len(text) == 10:
            parsed = datetime.strptime(text, "%Y-%m-%d")
            return parsed.replace(hour=12, minute=0, tzinfo=JST).astimezone(timezone.utc)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=JST)
        return parsed.astimezone(timezone.utc)
    except Exception as exc:
        raise ValueError(f"Invalid gochara date: {value!r}") from exc


def _gochara_planet_row(
    *,
    name: str,
    sidereal_lon_deg: float,
    speed_lon_deg_per_day: float,
) -> dict[str, Any]:
    rashi_no, rashi_name, deg_in_sign = _rashi_from_longitude(sidereal_lon_deg)
    nak_no, nak_name, pada, _progress = _nakshatra_from_longitude(sidereal_lon_deg)
    is_retrograde = False if name in {"Sun", "Moon"} else bool(speed_lon_deg_per_day < 0)
    return {
        "sidereal_lon_deg": round(sidereal_lon_deg % 360.0, 6),
        "rashi_no": rashi_no,
        "rashi_name": rashi_name,
        "deg_in_sign": round(deg_in_sign, 6),
        "nakshatra_name": nak_name,
        "nakshatra_pada": pada,
        "is_retrograde": is_retrograde,
    }


def build_vedic_gochara(
    *,
    transit_date: Any,
) -> dict[str, Any]:
    dt_utc = _coerce_transit_dt_utc(transit_date)
    jd = _julian_day_ut(dt_utc)

    planets: dict[str, Any] = {}
    rahu_speed = 0.0
    for name in GOCHARA_PLANETS:
        if name == "Ketu":
            continue
        pid = PLANET_IDS[name]
        lon, speed_lon = _sidereal_state(jd, pid)
        if name == "Rahu":
            rahu_speed = speed_lon
        planets[name] = _gochara_planet_row(
            name=name,
            sidereal_lon_deg=lon,
            speed_lon_deg_per_day=speed_lon,
        )

    rahu_lon = planets["Rahu"]["sidereal_lon_deg"]
    planets["Ketu"] = _gochara_planet_row(
        name="Ketu",
        sidereal_lon_deg=(float(rahu_lon) + 180.0) % 360.0,
        speed_lon_deg_per_day=rahu_speed,
    )

    return {
        "ayanamsha": "Lahiri",
        "zodiac_type": "sidereal",
        "date": dt_utc.astimezone(JST).date().isoformat(),
        "dt_utc": dt_utc.isoformat(),
        "planets": planets,
    }


def build_vedic_gochara_points(
    *,
    transit_dates: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ayanamsha": "Lahiri",
        "zodiac_type": "sidereal",
        "points": {
            key: build_vedic_gochara(
                transit_date=value,
            )
            for key, value in transit_dates.items()
        },
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


VEDIC_ASPECT_MAP = {
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


def _build_planetary_aspects_vedic(planets: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, pdata in planets.items():
        from_house = pdata.get("house_no")
        if not isinstance(from_house, int):
            continue
        for dist, aspect_type in VEDIC_ASPECT_MAP.get(name, []):
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


def _house_lord_name(house_lords: dict[str, Any] | None, house_no: int) -> str | None:
    if not house_lords:
        return None
    item = house_lords.get(str(house_no)) or {}
    lord = item.get("lord") if isinstance(item, dict) else None
    return str(lord) if lord else None


def _planet_house(planets: dict[str, Any], planet_name: str | None) -> int | None:
    if not planet_name:
        return None
    value = (planets.get(planet_name) or {}).get("house_no")
    return value if isinstance(value, int) else None


def _planet_strength(planets: dict[str, Any], planet_name: str | None, default: float = 0.5) -> float:
    if not planet_name:
        return default
    dignity = (planets.get(planet_name) or {}).get("dignity") or {}
    try:
        return float(dignity.get("strength_score"))
    except Exception:
        return default


def _confidence_from_strength(strength: float) -> str:
    if strength >= 0.7:
        return "high"
    if strength >= 0.45:
        return "medium"
    return "low"


def _yoga_item(
    *,
    name: str,
    slug: str,
    strength: float,
    evidence: list[str],
    interpretation_hint: str,
    tags: list[str] | None = None,
    confidence: str | None = None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "name": name,
        "slug": slug,
        "present": True,
        "strength": round(max(0.0, min(1.0, strength)), 3),
        "confidence": confidence or _confidence_from_strength(strength),
        "evidence": evidence,
        "interpretation_hint": interpretation_hint,
    }
    if tags:
        item["tags"] = tags
    if notes:
        item["notes"] = notes
    return item


def _aspected_house(from_planet: str, from_house: int, target_house: int) -> bool:
    for dist, _aspect_type in VEDIC_ASPECT_MAP.get(from_planet, []):
        if ((from_house + dist - 2) % 12) + 1 == target_house:
            return True
    return False


def _planet_relation(p1: str, p2: str, planets: dict[str, Any]) -> str | None:
    if p1 == p2:
        return f"{p1}が両方の支配星"
    h1 = _planet_house(planets, p1)
    h2 = _planet_house(planets, p2)
    if not isinstance(h1, int) or not isinstance(h2, int):
        return None
    if h1 == h2:
        return f"{p1}と{p2}が同じハウス"
    if _aspected_house(p1, h1, h2):
        return f"{p1}が{p2}の在住ハウスへアスペクト"
    if _aspected_house(p2, h2, h1):
        return f"{p2}が{p1}の在住ハウスへアスペクト"
    return None


def _sort_yoga_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda x: float(x.get("strength") or 0), reverse=True)


def detect_dhana_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not house_lords:
        return []
    out: list[dict[str, Any]] = []
    relation_houses = [1, 2, 5, 9, 11]
    relation_hits: list[str] = []
    for idx, h1 in enumerate(relation_houses):
        p1 = _house_lord_name(house_lords, h1)
        for h2 in relation_houses[idx + 1:]:
            p2 = _house_lord_name(house_lords, h2)
            if not p1 or not p2:
                continue
            rel = _planet_relation(p1, p2, planets)
            if rel:
                relation_hits.append(f"{h1}室支配星と{h2}室支配星の関係: {rel}")

    wealth_lords = {_house_lord_name(house_lords, h) for h in (2, 5, 9, 11)}
    wealth_lords.discard(None)
    placed_hits = [
        f"{p}が第{_planet_house(planets, p)}室"
        for p in sorted(str(x) for x in wealth_lords)
        if _planet_house(planets, p) in {2, 5, 9, 11}
    ]
    if relation_hits or len(placed_hits) >= 2:
        strength = min(1.0, 0.42 + 0.07 * min(5, len(relation_hits)) + 0.05 * len(placed_hits))
        out.append(_yoga_item(
            name="Dhana Yoga",
            slug="dhana_yoga",
            strength=strength,
            evidence=(relation_hits[:4] + placed_hits[:3]),
            interpretation_hint="才能・収益・人脈が結びつきやすい",
            tags=["wealth_forming_capacity"],
        ))

    lagna_lord = _house_lord_name(house_lords, 1)
    ninth_lord = _house_lord_name(house_lords, 9)
    lagna_strength = _planet_strength(planets, lagna_lord)
    ninth_strength = _planet_strength(planets, ninth_lord)
    ninth_house = _planet_house(planets, ninth_lord)
    # Lakshmi Yoga has lineage differences; this project uses a conservative D1 signal:
    # strong Lagna lord plus strong 9th lord in kendra/trikona.
    if lagna_strength >= 0.55 and ninth_strength >= 0.55 and (_is_kendra(ninth_house) or _is_trikona(ninth_house)):
        out.append(_yoga_item(
            name="Lakshmi Yoga",
            slug="lakshmi_yoga",
            strength=min(1.0, 0.45 + 0.2 * lagna_strength + 0.2 * ninth_strength),
            confidence="medium",
            evidence=[
                f"ラグナ支配星{lagna_lord}が一定以上の強さ",
                f"9室支配星{ninth_lord}がケンドラ/トリコーナに在住",
            ],
            interpretation_hint="品位・幸運・支援が豊かさへつながりやすい",
            tags=["wealth_forming_capacity", "fortune_support"],
            notes=["Lakshmi Yogaは流派差があるため、D1の主要条件に絞った中程度判定です。"],
        ))

    if relation_hits and not any(y["slug"] == "dhana_yoga" for y in out):
        out.append(_yoga_item(
            name="Wealth House Lord Link",
            slug="wealth_house_lord_link",
            strength=min(1.0, 0.36 + 0.06 * len(relation_hits)),
            confidence="medium",
            evidence=relation_hits[:5],
            interpretation_hint="収入・才能・幸運・自己決定が接続しやすい",
            tags=["wealth_forming_capacity"],
            notes=["伝統的な固有名ヨーガではなく、富に関わる支配星関係の機械判定です。"],
        ))
    return _sort_yoga_items(out)


def detect_raja_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not house_lords:
        return []
    out: list[dict[str, Any]] = []
    kendra_houses = [1, 4, 7, 10]
    trikona_houses = [1, 5, 9]
    raja_hits: list[str] = []
    for kh in kendra_houses:
        kp = _house_lord_name(house_lords, kh)
        for th in trikona_houses:
            tp = _house_lord_name(house_lords, th)
            if not kp or not tp:
                continue
            rel = _planet_relation(kp, tp, planets)
            if rel:
                raja_hits.append(f"{kh}室支配星と{th}室支配星の関係: {rel}")
    if raja_hits:
        out.append(_yoga_item(
            name="Raja Yoga",
            slug="raja_yoga",
            strength=min(1.0, 0.45 + 0.07 * min(5, len(raja_hits))),
            evidence=raja_hits[:5],
            interpretation_hint="役割・評価・実行力がまとまりやすい",
            tags=["status_support", "career_support"],
        ))

    ninth_lord = _house_lord_name(house_lords, 9)
    tenth_lord = _house_lord_name(house_lords, 10)
    dharma_karma_rel = _planet_relation(str(ninth_lord or ""), str(tenth_lord or ""), planets) if ninth_lord and tenth_lord else None
    if dharma_karma_rel:
        out.append(_yoga_item(
            name="Dharma-Karmadhipati Yoga",
            slug="dharma_karmadhipati_yoga",
            strength=min(1.0, 0.52 + 0.12 * (_planet_strength(planets, ninth_lord) + _planet_strength(planets, tenth_lord)) / 2),
            evidence=[f"9室支配星{ninth_lord}と10室支配星{tenth_lord}の関係: {dharma_karma_rel}"],
            interpretation_hint="信念・専門性・社会的役割が結びつきやすい",
            tags=["career_support", "purpose_work_link"],
        ))

    amala_hits = []
    moon_house = _planet_house(planets, "Moon")
    for benefic in ("Jupiter", "Venus", "Mercury"):
        h = _planet_house(planets, benefic)
        if h == 10:
            amala_hits.append(f"{benefic}がラグナから第10室")
        if isinstance(moon_house, int) and isinstance(h, int) and _house_distance(moon_house, h) == 10:
            amala_hits.append(f"{benefic}が月から第10室")
    if amala_hits:
        out.append(_yoga_item(
            name="Amala Yoga",
            slug="amala_yoga",
            strength=min(1.0, 0.48 + 0.08 * len(amala_hits)),
            confidence="medium",
            evidence=amala_hits[:4],
            interpretation_hint="仕事上の評判・清潔感・継続評価が育ちやすい",
            tags=["career_support", "reputation_support"],
            notes=["月から第10室を見る条件を含める流派と含めない流派があります。"],
        ))

    viparita_hits = []
    for h in (6, 8, 12):
        lord = _house_lord_name(house_lords, h)
        placed = _planet_house(planets, lord)
        if placed in {6, 8, 12}:
            viparita_hits.append(f"第{h}室支配星{lord}が第{placed}室に在住")
    if viparita_hits:
        out.append(_yoga_item(
            name="Viparita Raja Yoga",
            slug="viparita_raja_yoga",
            strength=min(1.0, 0.45 + 0.08 * len(viparita_hits)),
            confidence="medium",
            evidence=viparita_hits,
            interpretation_hint="困難や逆境が結果的に評価へ転じやすい",
            tags=["career_support", "recovery_from_adversity"],
            notes=["6/8/12室支配星がドゥシュタナに入る基本条件で判定しています。"],
        ))
    return _sort_yoga_items(out)


def detect_mind_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    del house_lords
    out: list[dict[str, Any]] = []
    moon = planets.get("Moon") or {}
    jupiter = planets.get("Jupiter") or {}
    mars = planets.get("Mars") or {}

    moon_house = moon.get("house_no")
    jup_house = jupiter.get("house_no")
    mars_house = mars.get("house_no")

    if isinstance(moon_house, int) and isinstance(jup_house, int):
        rel = _house_distance(moon_house, jup_house)
        if rel in {1, 4, 7, 10}:
            strength = min(1.0, 0.45 + 0.3 * _planet_strength(planets, "Jupiter"))
            out.append(_yoga_item(
                name="Gajakesari Yoga",
                slug="gajakesari_yoga",
                strength=strength,
                confidence="medium",
                evidence=["月から見たケンドラに木星"],
                interpretation_hint="精神的な支え、評判、助言者運",
                tags=["mental_support", "reputation_support"],
                notes=["木星の傷や月の状態で強弱が変わるため、中程度判定を基本にしています。"],
            ))

    if isinstance(moon_house, int) and isinstance(mars_house, int) and moon_house == mars_house:
        out.append(_yoga_item(
            name="Chandra-Mangala Yoga",
            slug="chandra_mangala_yoga",
            strength=min(1.0, 0.5 + 0.2 * _planet_strength(planets, "Mars")),
            evidence=["月と火星が同じハウス"],
            interpretation_hint="感情の熱量と行動力が収益・実行へ向かいやすい",
            tags=["financial_drive", "emotional_heat"],
        ))

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
            out.append(_yoga_item(
                name="Kemadruma Yoga",
                slug="kemadruma_yoga",
                strength=0.45,
                confidence="medium",
                evidence=["月の両隣に古典惑星なし"],
                interpretation_hint="内側の孤立感。静かな時間の確保が重要",
                tags=["inner_isolation_tendency"],
                notes=["ケンドラ在住惑星などをキャンセル条件に含める流派があります。ここでは基本形のみを判定します。"],
            ))
    return _sort_yoga_items(out)


def detect_challenge_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not house_lords:
        return []
    out: list[dict[str, Any]] = []
    daridra_hits = []
    for h in (2, 11):
        lord = _house_lord_name(house_lords, h)
        placed = _planet_house(planets, lord)
        if _is_dusthana(placed):
            daridra_hits.append(f"第{h}室支配星{lord}が第{placed}室")
    for h in (6, 8, 12):
        lord = _house_lord_name(house_lords, h)
        placed = _planet_house(planets, lord)
        if placed in {2, 11}:
            daridra_hits.append(f"第{h}室支配星{lord}が第{placed}室")
    if daridra_hits:
        out.append(_yoga_item(
            name="Daridra Yoga",
            slug="daridra_yoga",
            strength=min(1.0, 0.38 + 0.07 * len(daridra_hits)),
            confidence="medium",
            evidence=daridra_hits,
            interpretation_hint="収支・自己価値・人脈面で調整課題が出やすい",
            tags=["wealth_challenge"],
            notes=["不安を煽るためではなく、資源管理の課題として扱います。"],
        ))

    moon_house = _planet_house(planets, "Moon")
    jup_house = _planet_house(planets, "Jupiter")
    if isinstance(moon_house, int) and isinstance(jup_house, int) and _house_distance(moon_house, jup_house) in {6, 8, 12}:
        out.append(_yoga_item(
            name="Sakata Yoga",
            slug="sakata_yoga",
            strength=0.46,
            confidence="medium",
            evidence=["月から見て木星が6/8/12室"],
            interpretation_hint="気分・支援・見通しに波が出やすい",
            tags=["support_fluctuation"],
            notes=["Gajakesari Yogaと同じく月と木星の状態で強弱が変わります。"],
        ))

    malefics = {"Sun", "Mars", "Saturn", "Rahu", "Ketu"}
    kartari_hits = []
    for base_name, base_house in (("ラグナ", 1), ("月", moon_house)):
        if not isinstance(base_house, int):
            continue
        prev_house = ((base_house + 10) % 12) + 1
        next_house = (base_house % 12) + 1
        prev_malefics = [p for p in malefics if _planet_house(planets, p) == prev_house]
        next_malefics = [p for p in malefics if _planet_house(planets, p) == next_house]
        if prev_malefics and next_malefics:
            kartari_hits.append(f"{base_name}の両隣に凶星: {', '.join(sorted(prev_malefics))} / {', '.join(sorted(next_malefics))}")
    if kartari_hits:
        out.append(_yoga_item(
            name="Paap Kartari Yoga",
            slug="paap_kartari_yoga",
            strength=min(1.0, 0.42 + 0.08 * len(kartari_hits)),
            confidence="medium",
            evidence=kartari_hits,
            interpretation_hint="始動時に圧迫感が出やすく、環境設計が重要",
            tags=["pressure_pattern"],
            notes=["凶星の定義やキャンセル条件は流派差があります。"],
        ))
    return _sort_yoga_items(out)


def _vedic_yoga_summary(category: str, items: list[dict[str, Any]]) -> str:
    if items:
        names = "、".join(str(item.get("name")) for item in items[:3])
        return {
            "wealth": f"富・収益に関するヨーガは {names} を中心に確認できます。",
            "career": f"仕事・評価に関するヨーガは {names} を中心に確認できます。",
            "mind_support": f"心理的支えに関するヨーガは {names} を中心に確認できます。",
            "challenge": f"課題系ヨーガは {names} を確認します。本文ではリスクではなく調整テーマとして扱います。",
        }.get(category, f"検出ヨーガ: {names}")
    return {
        "wealth": "明確な富のヨーガは限定的です。収益判断は2室・11室・ダシャーなど他の根拠を優先します。",
        "career": "明確な仕事・権力系ヨーガは限定的です。キャリア判断は10室・ダシャーなど他の根拠を優先します。",
        "mind_support": "明確な月関連の支援ヨーガは限定的です。心理面は月・4室・ダシャーなど他の根拠を優先します。",
        "challenge": "主要な課題系ヨーガは強く出ていません。課題判断は個別配置の負荷を確認します。",
    }.get(category, "該当する主要ヨーガは限定的です。")


def analyze_vedic_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None) -> dict[str, Any]:
    wealth = detect_dhana_yogas(planets, house_lords)
    career = detect_raja_yogas(planets, house_lords)
    mind_support = detect_mind_yogas(planets, house_lords)
    challenge = detect_challenge_yogas(planets, house_lords)
    return {
        "wealth": wealth,
        "career": career,
        "mind_support": mind_support,
        "challenge": challenge,
        "summary": {
            "wealth": _vedic_yoga_summary("wealth", wealth),
            "career": _vedic_yoga_summary("career", career),
            "mind_support": _vedic_yoga_summary("mind_support", mind_support),
            "challenge": _vedic_yoga_summary("challenge", challenge),
        },
    }


def summarize_vedic_yogas_for_handoff(vedic_yogas: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"summary": vedic_yogas.get("summary") or {}}
    for category in ("wealth", "career", "mind_support", "challenge"):
        out[category] = [
            {
                "name": item.get("name"),
                "slug": item.get("slug"),
                "present": item.get("present", True),
                "strength": item.get("strength"),
                "confidence": item.get("confidence"),
                "evidence": item.get("evidence") or [],
                "interpretation_hint": item.get("interpretation_hint"),
                "notes": item.get("notes") or [],
            }
            for item in vedic_yogas.get(category, [])
            if isinstance(item, dict) and item.get("present", True)
        ]
    return out


def _build_yogas(planets: dict[str, Any], house_lords: dict[str, Any] | None) -> list[dict[str, Any]]:
    vedic_yogas = analyze_vedic_yogas(planets, house_lords)
    flat: list[dict[str, Any]] = []
    for category in ("wealth", "career", "mind_support", "challenge"):
        for item in vedic_yogas.get(category, []):
            compat = {
                "name": item.get("name"),
                "slug": item.get("slug"),
                "strength": item.get("strength"),
                "confidence": item.get("confidence"),
                "evidence": item.get("evidence") or [],
                "tags": item.get("tags") or [],
                "category": category,
                "interpretation_hint": item.get("interpretation_hint"),
            }
            if item.get("notes"):
                compat["notes"] = item.get("notes")
            flat.append(compat)
    return _sort_yoga_items(flat)


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
    vedic_yogas = analyze_vedic_yogas(planets, house_lords)
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

    gochara = build_vedic_gochara(
        transit_date=payload.get("gochara_date") or payload.get("target_date") or datetime.now(JST).date(),
    )

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
        "vedic_yogas": vedic_yogas,
        "vedic_yogas_handoff": summarize_vedic_yogas_for_handoff(vedic_yogas),
        "varga": {"D9": d9},
        "dasha": dasha,
        "gochara": gochara,
        "summary_flags": summary_flags,
    }

    if warnings:
        out["_warnings"] = warnings

    return out
