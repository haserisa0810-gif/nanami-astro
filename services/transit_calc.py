"""transit_calc.py
今日（または指定日）の天体位置 vs 出生図のアスペクトを計算する。
シナストリーモード対応：A出生図×今日、B出生図×今日、今日×今日の3層を返す。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from itertools import combinations

from services.western_calc import calc_western_from_payload, angle_diff

# 外惑星は動きが遅いのでORBを絞る
TRANSIT_ORB_BY_PLANET: dict[str, float] = {
    "Sun":     2.0,
    "Moon":    1.0,
    "Mercury": 2.0,
    "Venus":   2.0,
    "Mars":    2.0,
    "Jupiter": 3.0,
    "Saturn":  3.0,
    "Uranus":  1.0,
    "Neptune": 1.0,
    "Pluto":   1.0,
}
DEFAULT_ORB = 2.0

TRANSIT_ASPECTS: dict[str, float] = {
    "conjunction": 0,
    "opposition":  180,
    "square":      90,
    "trine":       120,
    "sextile":     60,
}

# トランジットで使う天体（ASC/MC/Vertex/ノードは除外）
TRANSIT_BODIES = {
    "Sun", "Moon", "Mercury", "Venus", "Mars",
    "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto",
}

# 出生図側で受け取る天体
NATAL_BODIES = {
    "Sun", "Moon", "Mercury", "Venus", "Mars",
    "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto",
    "North Node", "ASC", "MC",
}


def _calc_today_planets(
    target_date: datetime | None = None,
    lat: float = 35.6895,
    lng: float = 139.6917,
) -> list[dict[str, Any]]:
    """今日（または指定日）の天体位置を計算して返す。"""
    now = target_date or datetime.now(timezone.utc)
    payload = {
        "year": now.year, "month": now.month, "day": now.day,
        "hour": now.hour, "minute": now.minute,
        "lat": lat, "lng": lng,
        "tz_offset_hours": 0,
        "include_asteroids": False,
        "include_chiron": False,
        "include_lilith": False,
        "include_vertex": False,
    }
    result = calc_western_from_payload(payload)
    return result.get("planets", [])


def _match_aspects(
    transit_planets: list[dict[str, Any]],
    natal_planets: list[dict[str, Any]],
    label_prefix: str = "",
) -> list[dict[str, Any]]:
    """トランジット天体 vs 出生天体のアスペクト一覧を返す。"""
    hits: list[dict[str, Any]] = []
    for t in transit_planets:
        if t["name"] not in TRANSIT_BODIES:
            continue
        orb_limit = TRANSIT_ORB_BY_PLANET.get(t["name"], DEFAULT_ORB)
        for n in natal_planets:
            if n["name"] not in NATAL_BODIES:
                continue
            d = angle_diff(t["lon"], n["lon"])
            for asp_name, ang in TRANSIT_ASPECTS.items():
                orb = abs(d - ang)
                if orb <= orb_limit:
                    hits.append({
                        "transit_planet": t["name"],
                        "natal_planet":   n["name"],
                        "aspect":         asp_name,
                        "orb":            round(orb, 2),
                        "transit_sign":   t["sign"],
                        "natal_sign":     n["sign"],
                        "label":          label_prefix,
                    })
    return sorted(hits, key=lambda x: x["orb"])




def _match_transit_to_transit_aspects(
    transit_planets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """その日の天体同士の主要アスペクト一覧を返す。"""
    hits: list[dict[str, Any]] = []
    for a, b in combinations(transit_planets, 2):
        if a["name"] not in TRANSIT_BODIES or b["name"] not in TRANSIT_BODIES:
            continue
        orb_limit = min(
            TRANSIT_ORB_BY_PLANET.get(a["name"], DEFAULT_ORB),
            TRANSIT_ORB_BY_PLANET.get(b["name"], DEFAULT_ORB),
        )
        d = angle_diff(a["lon"], b["lon"])
        for asp_name, ang in TRANSIT_ASPECTS.items():
            orb = abs(d - ang)
            if orb <= orb_limit:
                hits.append({
                    "planet_a": a["name"],
                    "planet_b": b["name"],
                    "aspect": asp_name,
                    "orb": round(orb, 2),
                    "sign_a": a["sign"],
                    "sign_b": b["sign"],
                })
    return sorted(hits, key=lambda x: x["orb"])


def calc_global_transit_snapshot(
    target_date: datetime | None = None,
    lat: float = 35.6895,
    lng: float = 139.6917,
) -> dict[str, Any]:
    """個人出生図を使わず、その日の全体トランジットだけを返す。"""
    today_planets = _calc_today_planets(target_date, lat, lng)
    aspects = _match_transit_to_transit_aspects(today_planets)
    transit_date = (target_date or datetime.now(timezone.utc)).strftime("%Y-%m-%d")

    return {
        "transit_date": transit_date,
        "today_planets": [
            {"name": p["name"], "sign": p["sign"], "degree": round(p["degree"], 2)}
            for p in today_planets if p["name"] in TRANSIT_BODIES
        ],
        "aspects": aspects,
        "aspect_count": len(aspects),
    }

def calc_transits_single(
    natal_planets: list[dict[str, Any]],
    target_date: datetime | None = None,
    lat: float = 35.6895,
    lng: float = 139.6917,
) -> dict[str, Any]:
    """1人分のトランジット計算。handoff_logのtransitセクションに入れる形式で返す。"""
    today_planets = _calc_today_planets(target_date, lat, lng)
    aspects = _match_aspects(today_planets, natal_planets, label_prefix="transit→natal")
    transit_date = (target_date or datetime.now(timezone.utc)).strftime("%Y-%m-%d")

    return {
        "transit_date": transit_date,
        "today_planets": [
            {"name": p["name"], "sign": p["sign"], "degree": round(p["degree"], 2)}
            for p in today_planets if p["name"] in TRANSIT_BODIES
        ],
        "aspects": aspects,
        "aspect_count": len(aspects),
    }


def calc_transits_synastry(
    natal_a: list[dict[str, Any]],
    natal_b: list[dict[str, Any]],
    target_date: datetime | None = None,
    lat: float = 35.6895,
    lng: float = 139.6917,
) -> dict[str, Any]:
    """2人分のトランジット計算（3層）。
    - layer_a: 今日の天体 → Aの出生図
    - layer_b: 今日の天体 → Bの出生図
    - layer_shared: AとBに同時にかかってるアスペクト（transit_planet一致）
    """
    today_planets = _calc_today_planets(target_date, lat, lng)
    transit_date = (target_date or datetime.now(timezone.utc)).strftime("%Y-%m-%d")

    aspects_a = _match_aspects(today_planets, natal_a, label_prefix="transit→A")
    aspects_b = _match_aspects(today_planets, natal_b, label_prefix="transit→B")

    # 同じトランジット天体が両方に同時にアスペクトしているものを抽出
    planets_hitting_a = {a["transit_planet"] for a in aspects_a}
    planets_hitting_b = {b["transit_planet"] for b in aspects_b}
    shared_planets = planets_hitting_a & planets_hitting_b

    layer_shared: list[dict[str, Any]] = []
    for planet in shared_planets:
        a_hits = [a for a in aspects_a if a["transit_planet"] == planet]
        b_hits = [b for b in aspects_b if b["transit_planet"] == planet]
        layer_shared.append({
            "transit_planet": planet,
            "hits_a": a_hits,
            "hits_b": b_hits,
        })
    layer_shared.sort(key=lambda x: x["transit_planet"])

    return {
        "transit_date": transit_date,
        "today_planets": [
            {"name": p["name"], "sign": p["sign"], "degree": round(p["degree"], 2)}
            for p in today_planets if p["name"] in TRANSIT_BODIES
        ],
        "layer_a": aspects_a,
        "layer_b": aspects_b,
        "layer_shared": layer_shared,
        "shared_planet_count": len(shared_planets),
    }


def calc_transits_long_term(
    natal_planets: list[dict[str, Any]],
    months_ahead: int = 6,
    lat: float = 35.6895,
    lng: float = 139.6917,
) -> list[dict[str, Any]]:
    """
    外惑星（Jupiter〜Pluto）の長期トランジットを計算する。
    今日から months_ahead ヶ月先まで週単位でスキャンし、
    アスペクトの「入り・ピーク・出」を検出して返す。
    """
    from datetime import timedelta, date
    from collections import defaultdict

    LONG_TERM_BODIES = {"Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"}
    LONG_TERM_ORB: dict[str, float] = {
        "Jupiter": 4.0, "Saturn": 4.0,
        "Uranus": 2.0,  "Neptune": 2.0, "Pluto": 2.0,
    }

    today = date.today()
    end_date = today + timedelta(days=months_ahead * 30)

    scan_dates: list[date] = []
    d = today
    while d <= end_date:
        scan_dates.append(d)
        d += timedelta(days=7)

    date_planets: list[tuple[date, list[dict[str, Any]]]] = []
    for sd in scan_dates:
        dt = datetime(sd.year, sd.month, sd.day, 12, 0, tzinfo=timezone.utc)
        planets = _calc_today_planets(dt, lat, lng)
        date_planets.append((sd, planets))

    timeline: dict[tuple[str, str, str], list[tuple[date, float]]] = defaultdict(list)

    for sd, t_planets in date_planets:
        for t in t_planets:
            if t["name"] not in LONG_TERM_BODIES:
                continue
            orb_limit = LONG_TERM_ORB.get(t["name"], 2.0)
            for n in natal_planets:
                if n["name"] not in NATAL_BODIES:
                    continue
                d_ang = angle_diff(t["lon"], n["lon"])
                for asp_name, ang in TRANSIT_ASPECTS.items():
                    orb = abs(d_ang - ang)
                    if orb <= orb_limit:
                        key = (t["name"], n["name"], asp_name)
                        timeline[key].append((sd, orb))

    results: list[dict[str, Any]] = []

    for (t_planet, n_planet, asp_name), entries in timeline.items():
        if not entries:
            continue
        entries.sort(key=lambda x: x[0])

        groups: list[list[tuple[date, float]]] = []
        current: list[tuple[date, float]] = [entries[0]]
        for prev, curr in zip(entries, entries[1:]):
            if (curr[0] - prev[0]).days <= 21:
                current.append(curr)
            else:
                groups.append(current)
                current = [curr]
        groups.append(current)

        for group in groups:
            start_d = group[0][0]
            end_d   = group[-1][0]
            peak_entry = min(group, key=lambda x: x[1])
            peak_d, peak_orb = peak_entry

            natal_sign = next((n.get("sign","") for n in natal_planets if n["name"]==n_planet), "")
            transit_sign = ""
            for sd2, tp2 in date_planets:
                if sd2 == peak_d:
                    transit_sign = next((t2.get("sign","") for t2 in tp2 if t2["name"]==t_planet), "")
                    break

            if start_d > today:
                status = "upcoming"
            elif end_d < today:
                status = "past"
            else:
                status = "active"

            results.append({
                "transit_planet": t_planet,
                "natal_planet":   n_planet,
                "aspect":         asp_name,
                "start_date":     start_d.isoformat(),
                "peak_date":      peak_d.isoformat(),
                "end_date":       end_d.isoformat(),
                "peak_orb":       round(peak_orb, 2),
                "transit_sign":   transit_sign,
                "natal_sign":     natal_sign,
                "status":         status,
            })

    results.sort(key=lambda x: x["peak_date"])
    return results
