from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import swisseph as swe

from services.asteroid_provider import (
    AsteroidProviderError,
    fetch_asteroids,
    is_configured as asteroid_api_configured,
    provider_config as asteroid_provider_config,
)

SIGNS = [
    "Ari", "Tau", "Gem", "Can", "Leo", "Vir",
    "Lib", "Sco", "Sag", "Cap", "Aqu", "Pis",
]

PLANETS = [
    ("Sun", swe.SUN),
    ("Moon", swe.MOON),
    ("Mercury", swe.MERCURY),
    ("Venus", swe.VENUS),
    ("Mars", swe.MARS),
    ("Jupiter", swe.JUPITER),
    ("Saturn", swe.SATURN),
    ("Uranus", swe.URANUS),
    ("Neptune", swe.NEPTUNE),
    ("Pluto", swe.PLUTO),
]

ASTEROIDS = [
    ("Ceres", swe.CERES),
    ("Pallas", swe.PALLAS),
    ("Juno", swe.JUNO),
    ("Vesta", swe.VESTA),
]

DISPLAY_ORDER = [
    "Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn",
    "Uranus", "Neptune", "Pluto", "North Node", "South Node", "Lilith",
    "Chiron", "Ceres", "Pallas", "Juno", "Vesta", "ASC", "MC", "Vertex",
]
DISPLAY_INDEX = {name: idx for idx, name in enumerate(DISPLAY_ORDER)}

ASPECTS = {
    "conjunction": 0,
    "sextile": 60,
    "square": 90,
    "trine": 120,
    "opposition": 180,
}

ORB = {
    "conjunction": 8,
    "sextile": 4,
    "square": 6,
    "trine": 6,
    "opposition": 8,
}


def _ephe_candidates() -> list[Path]:
    candidates: list[Path] = []

    env_path = os.getenv("SWEPH_EPHE_PATH", "").strip()
    if env_path:
        candidates.append(Path(env_path))

    candidates.extend([
        Path(__file__).resolve().parents[2] / "ephe",
        Path("/app/ephe"),
    ])

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _resolve_ephe_dir() -> Path | None:
    for ephe_dir in _ephe_candidates():
        if ephe_dir.exists() and any(ephe_dir.glob("*.se1")):
            return ephe_dir
    return None


def configure_ephemeris() -> int:
    ephe_dir = _resolve_ephe_dir()
    if ephe_dir is not None:
        swe.set_ephe_path(str(ephe_dir))
        return swe.FLG_SWIEPH
    return swe.FLG_MOSEPH


def ephemeris_debug_info() -> dict[str, Any]:
    resolved = _resolve_ephe_dir()
    candidates = _ephe_candidates()
    target = resolved or candidates[0]
    files = sorted(p.name for p in target.glob("*")) if target.exists() else []
    return {
        "ephe_dir": str(target),
        "ephe_dir_exists": target.exists(),
        "ephe_files": files,
        "has_se1": any(name.endswith(".se1") for name in files),
        "resolved_ephe_dir": str(resolved) if resolved is not None else None,
        "searched_ephe_dirs": [str(p) for p in candidates],
        "env_sweph_ephe_path": os.getenv("SWEPH_EPHE_PATH", "").strip() or None,
    }


def norm360(x: float) -> float:
    return x % 360


def sign_of(lon: float) -> tuple[str, float]:
    lon = norm360(lon)
    i = int(lon / 30)
    return SIGNS[i], lon - i * 30


def house_of(lon: float, cusps: list[float]) -> int | None:
    lon = norm360(lon)
    if not cusps:
        return None
    for i in range(12):
        start = cusps[i]
        end = cusps[(i + 1) % 12]
        if start < end:
            if start <= lon < end:
                return i + 1
        else:
            if lon >= start or lon < end:
                return i + 1
    return None


def angle_diff(a: float, b: float) -> float:
    d = abs(norm360(a) - norm360(b))
    return min(d, 360 - d)


def calc_aspects(planets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(len(planets)):
        for j in range(i + 1, len(planets)):
            p1 = planets[i]
            p2 = planets[j]
            d = angle_diff(p1["lon"], p2["lon"])
            for name, ang in ASPECTS.items():
                orb = abs(d - ang)
                if orb <= ORB[name]:
                    out.append(
                        {
                            "planet1": p1["name"],
                            "planet2": p2["name"],
                            "type": name,
                            "orb": round(orb, 2),
                        }
                    )
    return sorted(out, key=lambda x: (x["orb"], x["planet1"], x["planet2"]))


def _normalize_house_system(value: Any) -> str:
    raw = str(value or "P").strip().upper()
    return {"P": "P", "PLACIDUS": "P", "K": "K", "KOCH": "K"}.get(raw, "P")


def _normalize_node_mode(value: Any) -> str:
    raw = str(value or "true").strip().lower()
    return "mean" if raw == "mean" else "true"


def _normalize_lilith_mode(value: Any) -> str:
    raw = str(value or "true").strip().lower()
    return "mean" if raw == "mean" else "true"


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "on", "yes"}


def _body_dict(name: str, lon: float, retrograde: bool, cusps: list[float]) -> dict[str, Any]:
    sign, deg = sign_of(lon)
    return {
        "name": name,
        "lon": norm360(lon),
        "sign": sign,
        "degree": deg,
        "retrograde": retrograde,
        "house": house_of(lon, cusps),
    }


def _append_angle(name: str, lon: float | None, fixed_house: int | None, planets: list[dict[str, Any]], cusps: list[float]) -> None:
    if lon is None:
        return
    sign, deg = sign_of(lon)
    planets.append(
        {
            "name": name,
            "lon": norm360(lon),
            "sign": sign,
            "degree": deg,
            "retrograde": False,
            "house": fixed_house if fixed_house is not None else house_of(lon, cusps),
        }
    )


def _sort_planets(planets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(planets, key=lambda p: (DISPLAY_INDEX.get(p["name"], 999), p["lon"], p["name"]))


def calc_western_from_payload(payload: dict[str, Any], house_system: str = "P") -> dict[str, Any]:
    year = int(payload["year"])
    month = int(payload["month"])
    day = int(payload["day"])
    hour = int(payload.get("hour", 12))
    minute = int(payload.get("minute", 0))
    lat = payload.get("lat")
    lng = payload.get("lng")
    tz = payload.get("tz_offset_hours", 9)

    selected_house_system = _normalize_house_system(payload.get("house_system", house_system))
    selected_node_mode = _normalize_node_mode(payload.get("node_mode", "true"))
    selected_lilith_mode = _normalize_lilith_mode(payload.get("lilith_mode", "true"))
    include_asteroids = _as_bool(payload.get("include_asteroids"), False)
    include_chiron = _as_bool(payload.get("include_chiron"), True)
    include_lilith = _as_bool(payload.get("include_lilith"), True)
    include_vertex = _as_bool(payload.get("include_vertex"), True)

    dt = datetime(year, month, day, hour, minute, tzinfo=timezone(timedelta(hours=tz)))
    utc = dt.astimezone(timezone.utc)
    ut = utc.hour + utc.minute / 60
    jd = swe.julday(utc.year, utc.month, utc.day, ut)

    engine_flag = configure_ephemeris()
    flags = engine_flag | swe.FLG_SPEED
    ephe_info = ephemeris_debug_info()
    core_mode = "swieph" if engine_flag == swe.FLG_SWIEPH else "moseph_fallback"

    cusps: list[float] = []
    asc: float | None = None
    mc: float | None = None
    vertex: float | None = None
    if lat is not None and lng is not None:
        cusps_, ascmc = swe.houses(jd, lat, lng, selected_house_system.encode())
        cusps = list(cusps_)[:12]
        asc = ascmc[0]
        mc = ascmc[1]
        try:
            vertex = ascmc[3]
        except Exception:
            vertex = None

    planets: list[dict[str, Any]] = []
    skipped_bodies: list[dict[str, Any]] = []
    calc_engine: dict[str, Any] = {
        "core": "swisseph",
        "core_mode": core_mode,
        "asteroid_engine": "disabled",
        "asteroid_api_status": "disabled",
        "asteroid_provider": asteroid_provider_config(),
    }

    def add_body(name: str, body_id: int, *, require_swieph: bool = False, custom_reason: str | None = None) -> bool:
        if require_swieph and engine_flag != swe.FLG_SWIEPH:
            skipped_bodies.append(
                {
                    "name": name,
                    "reason": custom_reason or "Swiss Ephemeris (*.se1) が無いため、この天体は計算できません",
                }
            )
            return False
        try:
            xx, _ = swe.calc_ut(jd, body_id, flags)
        except swe.Error:
            skipped_bodies.append(
                {
                    "name": name,
                    "reason": custom_reason or "Swiss Ephemeris (*.se1) が無いため、この天体は計算できません",
                }
            )
            return False

        planets.append(_body_dict(name, xx[0], xx[3] < 0, cusps))
        return True

    for name, body_id in PLANETS:
        add_body(name, body_id)

    node_body = swe.MEAN_NODE if selected_node_mode == "mean" else swe.TRUE_NODE
    add_body("North Node", node_body)
    node = next((x for x in planets if x["name"] == "North Node"), None)
    if node is not None:
        south_lon = norm360(node["lon"] + 180)
        planets.append(_body_dict("South Node", south_lon, bool(node["retrograde"]), cusps))

    if include_lilith:
        lilith_body = swe.OSCU_APOG if selected_lilith_mode == "true" else swe.MEAN_APOG
        add_body("Lilith", lilith_body)

    if include_chiron:
        add_body("Chiron", swe.CHIRON, require_swieph=True)

    if include_asteroids:
        asteroid_names = [name for name, _ in ASTEROIDS]
        if engine_flag == swe.FLG_SWIEPH:
            calc_engine["asteroid_engine"] = "local_swieph"
            local_ok = True
            for name, body_id in ASTEROIDS:
                ok = add_body(name, body_id, require_swieph=True)
                local_ok = local_ok and ok
            calc_engine["asteroid_api_status"] = "not_needed" if local_ok else "partial_local"
        else:
            calc_engine["asteroid_engine"] = "freeastro_api"
            if asteroid_api_configured():
                try:
                    result = fetch_asteroids(payload, asteroid_names)
                    returned = {item["name"]: item for item in result.get("planets", [])}
                    for asteroid_name in asteroid_names:
                        item = returned.get(asteroid_name)
                        if item is None:
                            skipped_bodies.append({
                                "name": asteroid_name,
                                "reason": "FreeAstro API 応答にこの小惑星が含まれていません",
                            })
                            continue
                        planets.append(_body_dict(asteroid_name, float(item["lon"]), bool(item.get("retrograde", False)), cusps))
                    calc_engine["asteroid_api_status"] = "success"
                except AsteroidProviderError as e:
                    calc_engine["asteroid_api_status"] = "failed"
                    for asteroid_name in asteroid_names:
                        skipped_bodies.append({
                            "name": asteroid_name,
                            "reason": f"FreeAstro API 取得失敗: {e}",
                        })
            else:
                calc_engine["asteroid_api_status"] = "not_configured"
                for asteroid_name in asteroid_names:
                    skipped_bodies.append({
                        "name": asteroid_name,
                        "reason": "Swiss Ephemeris (*.se1) が無く、FreeAstro API も未設定です",
                    })

    _append_angle("ASC", asc, 1, planets, cusps)
    _append_angle("MC", mc, 10, planets, cusps)
    if include_vertex:
        _append_angle("Vertex", vertex, None, planets, cusps)

    houses: list[dict[str, Any]] = []
    for i, c in enumerate(cusps, 1):
        sign, deg = sign_of(c)
        houses.append({"house": i, "lon": c, "sign": sign, "degree": deg})

    planets = _sort_planets(planets)
    aspects = calc_aspects(planets)

    return {
        "module": "western",
        "engine": "swisseph",
        "calc_engine": calc_engine,
        "ephemeris": ephe_info,
        "generated_at": datetime.utcnow().isoformat(),
        "subject": {
            "datetime": dt.isoformat(),
            "location": {"lat": lat, "lng": lng, "city": payload.get("city")},
        },
        "options": {
            "house_system": selected_house_system,
            "node_mode": selected_node_mode,
            "lilith_mode": selected_lilith_mode,
            "include_asteroids": include_asteroids,
            "include_chiron": include_chiron,
            "include_lilith": include_lilith,
            "include_vertex": include_vertex,
        },
        "planets": planets,
        "houses": houses,
        "aspects": aspects,
        "angles": {"asc": asc, "mc": mc, "vertex": vertex if include_vertex else None},
        "skipped_bodies": skipped_bodies,
    }
