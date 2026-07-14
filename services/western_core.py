"""西洋占星術の計算コア。

このファイルは nanami-astro が正本で、nanami-products へ
scripts/sync_western_core.py（products側）でコピー同期される。
編集は必ず nanami-astro 側で行い、商品固有のロジックを入れないこと。
"""

from __future__ import annotations

from typing import Any


WESTERN_CORE_VERSION = "2026.07.14-w0"
EXACT_THRESHOLD_DEG = 0.05

SIGNS = [
    "Ari", "Tau", "Gem", "Can", "Leo", "Vir",
    "Lib", "Sco", "Sag", "Cap", "Aqu", "Pis",
]

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
        elif lon >= start or lon < end:
            return i + 1
    return None


def angle_diff(a: float, b: float) -> float:
    d = abs(norm360(a) - norm360(b))
    return min(d, 360 - d)


def aspect_applying(
    lon1: float,
    speed1: float | None,
    lon2: float,
    speed2: float | None,
    exact_angle: float,
) -> bool | None:
    """指定アスペクトが接近中かを経度速度から判定する。"""
    if speed1 is None or speed2 is None:
        return None
    signed_sep = ((lon1 - lon2 + 180) % 360) - 180
    d_abs = abs(signed_sep)
    deviation = d_abs - exact_angle
    speed_delta = speed1 - speed2
    if signed_sep > 0:
        rate = speed_delta
    elif signed_sep < 0:
        rate = -speed_delta
    else:
        rate = abs(speed_delta)
    if deviation > 0:
        return rate < 0
    if deviation < 0:
        return rate > 0
    return False


def aspect_phase(deviation: float, applying: bool | None) -> str:
    """アスペクトの接近・正確・離反フェーズを返す。"""
    if applying is None:
        return "unknown"
    if abs(deviation) <= EXACT_THRESHOLD_DEG:
        return "exact"
    return "applying" if applying else "separating"


def calc_aspects(planets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(len(planets)):
        for j in range(i + 1, len(planets)):
            p1 = planets[i]
            p2 = planets[j]
            d = angle_diff(p1["lon"], p2["lon"])
            for name, ang in ASPECTS.items():
                deviation = d - ang
                orb = abs(deviation)
                if orb <= ORB[name]:
                    applying = aspect_applying(
                        p1["lon"], p1.get("speed"),
                        p2["lon"], p2.get("speed"),
                        ang,
                    )
                    phase = aspect_phase(deviation, applying)
                    if phase == "exact":
                        applying = None
                    item = {
                        "planet1": p1["name"],
                        "planet2": p2["name"],
                        "type": name,
                        "orb": round(orb, 2),
                        "exact_angle": ang,
                        "actual_angle": round(d, 2),
                        "signed_deviation": round(deviation, 2),
                        "orb_limit": ORB[name],
                        "applying": applying,
                        "phase": phase,
                    }
                    if p1["name"] == "South Node" or p2["name"] == "South Node":
                        item["axis_mirror"] = True
                    out.append(item)
    return sorted(out, key=lambda x: (x["orb"], x["planet1"], x["planet2"]))


def build_calculation_rules(
    house_system: str,
    node_mode: str,
    lilith_mode: str,
    notes: list[str],
) -> dict[str, Any]:
    return {
        "rule_set_id": "western_default_v1",
        "core_version": WESTERN_CORE_VERSION,
        "exact_threshold_deg": EXACT_THRESHOLD_DEG,
        "aspects": {
            name: {"angle": angle, "orb": ORB[name]}
            for name, angle in ASPECTS.items()
        },
        "house_system": house_system,
        "node_mode": node_mode,
        "lilith_mode": lilith_mode,
        "notes": notes,
    }
