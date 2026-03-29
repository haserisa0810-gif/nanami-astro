from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


# =========================
# Minimal Synastry Engine (MVP)
# =========================
# 目的:
# - FreeAstro の natal 結果（2名分）から、相互作用の「材料」を機械的に抽出する
# - 吉凶や断定はしない（AI側にもさせない）
# - 占星計算の厳密さより、入力データに基づく“作り足しゼロ”の安定を優先


SIGN_INDEX: Dict[str, int] = {
    # English
    "aries": 0,
    "taurus": 1,
    "gemini": 2,
    "cancer": 3,
    "leo": 4,
    "virgo": 5,
    "libra": 6,
    "scorpio": 7,
    "sagittarius": 8,
    "capricorn": 9,
    "aquarius": 10,
    "pisces": 11,
    # JP
    "牡羊座": 0,
    "牡牛座": 1,
    "双子座": 2,
    "蟹座": 3,
    "獅子座": 4,
    "乙女座": 5,
    "天秤座": 6,
    "蠍座": 7,
    "射手座": 8,
    "山羊座": 9,
    "水瓶座": 10,
    "魚座": 11,
}


KEY_PLANETS = {
    "sun",
    "moon",
    "mercury",
    "venus",
    "mars",
    "jupiter",
    "saturn",
    "uranus",
    "neptune",
    "pluto",
    "north_node",
}


ASPECTS: List[Tuple[str, int, float]] = [
    ("conjunction", 0, 6.0),
    ("sextile", 60, 4.5),
    ("square", 90, 5.0),
    ("trine", 120, 5.0),
    ("opposition", 180, 6.0),
]


def _norm_sign(sign: Any) -> Optional[str]:
    if not isinstance(sign, str):
        return None
    s = sign.strip()
    if not s:
        return None
    s2 = s.lower()
    return s2 if s2 in SIGN_INDEX else s


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _planet_longitude(p: Dict[str, Any]) -> Optional[float]:
    """sign + pos(0-30) から 0-360 の黄経を作る（natal結果に lon が無い想定の安全版）"""
    sign = _norm_sign(p.get("sign"))
    pos = _to_float(p.get("pos"))
    if sign is None or pos is None:
        return None
    idx = SIGN_INDEX.get(sign)
    if idx is None:
        return None
    # 0 <= pos < 30 を期待。逸脱でもそのまま入れる（補正しない）
    return idx * 30.0 + pos


def _extract_key_planets(astro: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for p in (astro.get("planets") or []):
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        if pid in KEY_PLANETS:
            out[str(pid)] = p
    return out


def _wrap_angle(d: float) -> float:
    d = abs(d) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _calc_synastry_aspects(
    planets_a: Dict[str, Dict[str, Any]],
    planets_b: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    aspects: List[Dict[str, Any]] = []
    for ida, pa in planets_a.items():
        la = _planet_longitude(pa)
        if la is None:
            continue
        for idb, pb in planets_b.items():
            lb = _planet_longitude(pb)
            if lb is None:
                continue
            d = _wrap_angle(la - lb)
            for name, deg, orb in ASPECTS:
                if abs(d - float(deg)) <= orb:
                    aspects.append(
                        {
                            "p1": ida,
                            "p2": idb,
                            "type": name,
                            "deg": deg,
                            "delta": round(d, 2),
                            "orb": round(abs(d - float(deg)), 2),
                        }
                    )
                    break
    # orbが小さい順にソート
    aspects.sort(key=lambda x: (x.get("orb", 99), x.get("p1", ""), x.get("p2", "")))
    return aspects


def _houses_longitudes(astro: Dict[str, Any]) -> List[Tuple[int, float]]:
    """house cusp の黄経（H1..H12）"""
    out: List[Tuple[int, float]] = []
    for h in (astro.get("houses") or []):
        if not isinstance(h, dict):
            continue
        hn = h.get("house")
        if hn is None:
            continue
        sign = _norm_sign(h.get("sign"))
        pos = _to_float(h.get("pos"))
        if sign is None or pos is None:
            continue
        idx = SIGN_INDEX.get(sign)
        if idx is None:
            continue
        out.append((int(hn), idx * 30.0 + pos))
    out.sort(key=lambda x: x[0])
    return out


def _house_of_longitude(houses: List[Tuple[int, float]], lon: float) -> Optional[int]:
    """cusp配列から、lonが属するハウス番号を返す（等間隔ではない前提の簡易区間判定）"""
    if len(houses) < 12:
        return None
    # 1..12 の昇順
    cusps = [c for _, c in houses]
    nums = [n for n, _ in houses]
    if nums != list(range(1, 13)):
        return None

    # 区間判定：cusp[i]..cusp[i+1]（最後は wrap）
    for i in range(12):
        start = cusps[i]
        end = cusps[(i + 1) % 12]
        house_num = i + 1

        if i < 11:
            if start <= lon < end:
                return house_num
        else:
            # H12: start..360 + 0..end
            if lon >= start or lon < end:
                return house_num
    return None


def _calc_house_overlays(
    houses_owner: List[Tuple[int, float]],
    planets_other: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    overlays: List[Dict[str, Any]] = []
    for pid, p in planets_other.items():
        lon = _planet_longitude(p)
        if lon is None:
            continue
        hn = _house_of_longitude(houses_owner, lon)
        if hn is None:
            continue
        overlays.append({"planet": pid, "in_house": hn})
    return overlays


def analyze_synastry(
    astro_a: Dict[str, Any],
    astro_b: Dict[str, Any],
    *,
    label_a: str = "A",
    label_b: str = "B",
) -> Dict[str, Any]:
    """相性用の“構造素材”を生成する（MVP）"""
    pa = _extract_key_planets(astro_a)
    pb = _extract_key_planets(astro_b)

    aspects = _calc_synastry_aspects(pa, pb)

    ha = _houses_longitudes(astro_a)
    hb = _houses_longitudes(astro_b)

    overlay_a = _calc_house_overlays(ha, pb) if ha else []  # Bの天体がAのどのハウスに入るか
    overlay_b = _calc_house_overlays(hb, pa) if hb else []

    return {
        "labels": {"a": label_a, "b": label_b},
        "key_planets": {
            "a": {k: {"sign": v.get("sign"), "pos": v.get("pos"), "house": v.get("house")} for k, v in pa.items()},
            "b": {k: {"sign": v.get("sign"), "pos": v.get("pos"), "house": v.get("house")} for k, v in pb.items()},
        },
        "synastry_aspects": aspects[:80],
        "house_overlays": {
            "b_planets_in_a_houses": overlay_a,
            "a_planets_in_b_houses": overlay_b,
        },
        "notes": {
            "engine": "synastry_engine_mvp",
            "limits": [
                "This is a minimal mechanical extraction. No mythic symbolism. No good/bad judgement.",
                "Aspects are approximate based on sign+pos degrees (if absolute longitude is not provided).",
            ],
        },
    }
