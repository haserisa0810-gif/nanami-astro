from __future__ import annotations

from itertools import combinations
from collections import defaultdict
from typing import Any, Dict, List, Optional

MAJOR_ASPECTS = {
    "CONJ": 0,
    "SEXT": 60,
    "SQR": 90,
    "TRI": 120,
    "OPP": 180,
}

MINOR_ASPECTS = {
    "SEMISEXT": 30,
    "SEMISQR": 45,
    "SESQUI": 135,
    "QUINCUNX": 150,
}

ORB_MAJOR = 8
ORB_MINOR = 2


def norm360(x: float) -> float:
    return x % 360.0


def angle_diff(a: float, b: float) -> float:
    diff = abs(a - b) % 360.0
    return min(diff, 360.0 - diff)


def compute_pair_angles(bodies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pairs = []
    for a, b in combinations(bodies, 2):
        diff = angle_diff(float(a["lon"]), float(b["lon"]))
        pairs.append({"a": a["name"], "b": b["name"], "diff": diff})
    return pairs


def detect_aspects(pair_angles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    found = []
    for pair in pair_angles:
        a, b, diff = pair["a"], pair["b"], pair["diff"]

        for name, angle in MAJOR_ASPECTS.items():
            err = abs(diff - angle)
            if err <= ORB_MAJOR:
                found.append(
                    {
                        "a": a,
                        "b": b,
                        "type": name,
                        "theory": angle,
                        "actual": diff,
                        "error": err,
                        "group": "major",
                    }
                )
                break

        for name, angle in MINOR_ASPECTS.items():
            err = abs(diff - angle)
            if err <= ORB_MINOR:
                found.append(
                    {
                        "a": a,
                        "b": b,
                        "type": name,
                        "theory": angle,
                        "actual": diff,
                        "error": err,
                        "group": "minor",
                    }
                )
                break

    found.sort(key=lambda x: x["error"])
    return found


def connection_counts(bodies: List[Dict[str, Any]], aspects: List[Dict[str, Any]]):
    counts = defaultdict(int)
    for asp in aspects:
        counts[asp["a"]] += 1
        counts[asp["b"]] += 1

    isolated = [b["name"] for b in bodies if counts[b["name"]] == 0]
    return dict(counts), isolated


def sign_density(bodies: List[Dict[str, Any]]):
    counts = defaultdict(int)
    for b in bodies:
        sign = b.get("sign") or "UNKNOWN"
        counts[sign] += 1

    values = list(counts.values())
    if not values:
        return {}, 0, 0

    values_sorted = sorted(values, reverse=True)
    max_val = values_sorted[0]
    second = values_sorted[1] if len(values_sorted) > 1 else max_val
    return dict(counts), max_val, second


def assign_houses(bodies: List[Dict[str, Any]], cusps: List[float]):
    results = []
    counts = [0] * 12

    for b in bodies:
        lon = float(b["lon"])
        for i in range(12):
            start = float(cusps[i])
            end = float(cusps[(i + 1) % 12])

            if start < end:
                cond = start <= lon < end
            else:
                cond = lon >= start or lon < end

            if cond:
                results.append({"body": b["name"], "house": i + 1})
                counts[i] += 1
                break

    return results, counts


def analyze_structure(bodies: List[Dict[str, Any]], house_cusps: Optional[List[float]] = None):
    pairs = compute_pair_angles(bodies)
    aspects = detect_aspects(pairs)
    conn, isolated = connection_counts(bodies, aspects)
    density, max_d, second_d = sign_density(bodies)

    result = {
        "pair_angles": pairs,
        "aspects": aspects,
        "connections": conn,
        "isolated": isolated,
        "density": {"by_sign": density, "max": max_d, "second": second_d},
    }

    if house_cusps and isinstance(house_cusps, list) and len(house_cusps) >= 12:
        houses, counts = assign_houses(bodies, house_cusps[:12])
        result["houses"] = houses
        result["house_counts"] = counts

    return result


# =========================
# Risk / shadow helpers (v1.6+)
# =========================


def _top_connected(connections: dict[str, int], *, top_n: int = 3) -> list[dict[str, Any]]:
    items = sorted(((k, int(v)) for k, v in (connections or {}).items()), key=lambda x: x[1], reverse=True)
    out: list[dict[str, Any]] = []
    for k, v in items[: max(0, top_n)]:
        out.append({"body": k, "count": v})
    return out


def derive_risk_flags(structure: dict[str, Any]) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []

    connections: dict[str, int] = structure.get("connections") or {}
    isolated: list[str] = structure.get("isolated") or []
    density = (structure.get("density") or {}).get("by_sign") or {}
    max_density = (structure.get("density") or {}).get("max") or 0

    house_counts = structure.get("house_counts") or []
    house_counts = [int(x) for x in house_counts] if isinstance(house_counts, list) else []

    if len(isolated) >= 2:
        flags.append(
            {
                "id": "social_isolation_risk",
                "severity": 3 if len(isolated) == 2 else 4,
                "confidence": "med",
                "evidence": {
                    "isolated_bodies": isolated,
                },
                "manifestation": "考えを抱え込みやすい / 相談が遅れやすい / 誤解が積み上がりやすい",
                "countermeasures": "『状況共有の最低ライン』を外部ルール化（週1の棚卸し・相談窓口の固定）",
            }
        )

    top = _top_connected(connections, top_n=1)
    if top and top[0]["count"] >= 6:
        flags.append(
            {
                "id": "obsession_burnout_cycle",
                "severity": 4,
                "confidence": "med",
                "evidence": {
                    "top_connected": top,
                },
                "manifestation": "一点集中→過集中→燃え尽き の往復 / 視野が狭くなる",
                "countermeasures": "作業を『90分単位で強制終了』し、復帰条件（休憩・食事・睡眠）を先に決める",
            }
        )

    if max_density >= 4:
        top_signs = sorted(density.items(), key=lambda x: int(x[1]), reverse=True)[:2]
        flags.append(
            {
                "id": "tunnel_vision_fixation",
                "severity": 3,
                "confidence": "med",
                "evidence": {"sign_density_top": top_signs},
                "manifestation": "同じ価値観で押し切りやすい / 切り替えが遅れる",
                "countermeasures": "反証チェック（『逆の仮説』を毎回1つ立てる）を手順として入れる",
            }
        )

    if len(house_counts) >= 12:
        def hc(h: int) -> int:
            return int(house_counts[h - 1])

        if hc(6) >= 3:
            flags.append(
                {
                    "id": "overwork_health_debt",
                    "severity": 4,
                    "confidence": "med",
                    "evidence": {"house_6_count": hc(6)},
                    "manifestation": "頑張り過ぎ→体調/気力の負債化→回復に時間がかかる",
                    "countermeasures": "『休む日』を先に予定へ固定。やる気ではなくカレンダーで守る",
                }
            )
        if hc(12) >= 2:
            flags.append(
                {
                    "id": "escape_avoidance",
                    "severity": 3,
                    "confidence": "low",
                    "evidence": {"house_12_count": hc(12)},
                    "manifestation": "現実処理の先送り / 気晴らしに逃げやすい",
                    "countermeasures": "『5分だけ着手』をトリガーにして回避を破る（開始だけが仕事）",
                }
            )
        if hc(2) >= 2 or hc(8) >= 2:
            flags.append(
                {
                    "id": "finance_volatility",
                    "severity": 3,
                    "confidence": "low",
                    "evidence": {"house_2_count": hc(2), "house_8_count": hc(8)},
                    "manifestation": "お金の出入りが感情に引っ張られやすい / 支出の波",
                    "countermeasures": "上限ルール（カード枠・月上限・サブ口座分離）を先に作る",
                }
            )

    flags.sort(key=lambda x: int(x.get("severity", 0)), reverse=True)
    return flags


# =========================
# Vedic structure helpers
# =========================


def _vedic_planets_map(vedic: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Accept both:
      - planets: {"Sun": {...}, "Moon": {...}}
      - planets: [{"name": "Sun", ...}, {"name": "Moon", ...}]
      - planets_map: {"Sun": {...}, ...}
    """
    planets_map = vedic.get("planets_map")
    if isinstance(planets_map, dict):
        return {str(k): v for k, v in planets_map.items() if isinstance(v, dict)}

    planets = vedic.get("planets") or {}

    if isinstance(planets, dict):
        return {str(k): v for k, v in planets.items() if isinstance(v, dict)}

    if isinstance(planets, list):
        out: dict[str, dict[str, Any]] = {}
        for p in planets:
            if not isinstance(p, dict):
                continue
            name = p.get("name")
            if name:
                out[str(name)] = p
        return out

    return {}


def _vedic_houses_list(vedic: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Accept both:
      - houses: [{"house_no": 1, ...}, ...]
      - houses: {"houses": [...]} 
      - houses_meta: {"houses": [...]} 
    """
    houses_meta = vedic.get("houses_meta")
    if isinstance(houses_meta, dict):
        hm = houses_meta.get("houses")
        if isinstance(hm, list):
            return hm

    houses = vedic.get("houses")

    if isinstance(houses, dict):
        inner = houses.get("houses")
        if isinstance(inner, list):
            return inner
        return []

    if isinstance(houses, list):
        return houses

    return []


def _safe_house_no(pdata: dict[str, Any]) -> int:
    try:
        return int(pdata.get("house_no") or 0)
    except Exception:
        return 0


def _vedic_house_counts(planets: dict[str, dict[str, Any]]) -> dict[int, int]:
    counts: dict[int, int] = {i: 0 for i in range(1, 13)}
    for pdata in planets.values():
        if not isinstance(pdata, dict):
            continue
        h = _safe_house_no(pdata)
        if 1 <= h <= 12:
            counts[h] += 1
    return counts


def analyze_vedic_structure(vedic: dict[str, Any]) -> dict[str, Any]:
    planets = _vedic_planets_map(vedic)
    houses = _vedic_houses_list(vedic)
    counts = _vedic_house_counts(planets)

    moon_house = _safe_house_no(planets.get("Moon", {}))
    rahu_house = _safe_house_no(planets.get("Rahu", {}))
    ketu_house = _safe_house_no(planets.get("Ketu", {}))

    sat = planets.get("Saturn", {})
    dignity = sat.get("dignity") if isinstance(sat.get("dignity"), dict) else {}

    saturn_strength = float(dignity.get("strength_score") or 0.0)
    saturn_sign_status = dignity.get("sign_status")
    saturn_weakened = bool(
        dignity.get("is_combust")
        or dignity.get("is_debilitated")
        or saturn_sign_status == "enemy_sign"
        or saturn_strength <= 0.35
    )

    out: dict[str, Any] = {
        "moon_12h": moon_house == 12,
        "sixth_house_emphasis": counts.get(6, 0) >= 2,
        "rahu_8h_ketu_2h": rahu_house == 8 and ketu_house == 2,
        "twelfth_house_emphasis": counts.get(12, 0) >= 2,
        "saturn_weakened": saturn_weakened,
        "house_counts": counts,
        "cluster_houses": [h for h, c in counts.items() if c >= 2],
        "house_lords": vedic.get("house_lords") or {},
        "house_lords_placement": vedic.get("house_lords_placement") or {},
        "yogas": vedic.get("yogas") or [],
        "dasha": vedic.get("dasha") or {},
        "summary_flags": vedic.get("summary_flags") or {},
    }

    if houses:
        out["houses"] = houses
    if planets:
        out["planets"] = planets

    return out


def derive_vedic_flags(vedic_structure: dict[str, Any]) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []
    counts = vedic_structure.get("house_counts") or {}
    yogas = vedic_structure.get("yogas") or []
    dasha = vedic_structure.get("dasha") or {}
    house_lords_placement = vedic_structure.get("house_lords_placement") or {}

    if vedic_structure.get("moon_12h"):
        flags.append({
            "id": "inner_withdrawal",
            "source": "vedic_structure",
            "severity": 4,
            "confidence": "med",
            "evidence": {"moon_house": 12},
            "manifestation": "感情処理を内側で抱え込みやすい / 一人で消化しようとしやすい",
            "countermeasures": "まず文章化して外に出し、信頼できる相手へ段階的に共有する",
        })

    if vedic_structure.get("sixth_house_emphasis"):
        flags.append({
            "id": "service_overload",
            "source": "vedic_structure",
            "severity": 4,
            "confidence": "med",
            "evidence": {"house_6_count": counts.get(6, 0)},
            "manifestation": "修正・対応・実務に力が流れ続け、疲弊しやすい",
            "countermeasures": "自分が背負う仕事と背負わなくていい仕事を先に分ける",
        })

    if vedic_structure.get("rahu_8h_ketu_2h"):
        flags.append({
            "id": "security_value_restructuring",
            "source": "vedic_structure",
            "severity": 3,
            "confidence": "low",
            "evidence": {"rahu_house": 8, "ketu_house": 2},
            "manifestation": "安心・所有・自己価値の揺れを通して価値観の組み替えが起こりやすい",
            "countermeasures": "お金・言葉・人間関係の基準を言語化しておく",
        })

    if vedic_structure.get("saturn_weakened"):
        flags.append({
            "id": "pressure_hardening",
            "source": "vedic_structure",
            "severity": 3,
            "confidence": "low",
            "evidence": {"saturn_weakened": True},
            "manifestation": "責任感が重圧として固まり、楽しむ前に構えやすい",
            "countermeasures": "負荷の高い役割は期限と出口を先に決める",
        })

    lord7 = house_lords_placement.get("7") or house_lords_placement.get(7) or {}
    try:
        if int(lord7.get("placed_in_house") or 0) == 6:
            flags.append({
                "id": "relationship_taskification",
                "source": "vedic_structure",
                "severity": 3,
                "confidence": "low",
                "evidence": {"7L_placed_in_house": 6, "7L": lord7.get("lord")},
                "manifestation": "対人関係やパートナーシップが、癒やしより調整・課題処理に寄りやすい",
                "countermeasures": "役割と感情を混ぜすぎず、関係ごとに期待値を言語化する",
            })
    except Exception:
        pass

    lord5 = house_lords_placement.get("5") or house_lords_placement.get(5) or {}
    try:
        if int(lord5.get("placed_in_house") or 0) == 12:
            flags.append({
                "id": "private_creativity_bias",
                "source": "vedic_structure",
                "severity": 2,
                "confidence": "low",
                "evidence": {"5L_placed_in_house": 12, "5L": lord5.get("lord")},
                "manifestation": "創造性や喜びが表現より内面処理・私的空間に向かいやすい",
                "countermeasures": "完成度より先に、外へ小さく出す習慣を作る",
            })
    except Exception:
        pass

    yoga_names = {str((y or {}).get("name") or "") for y in yogas if isinstance(y, dict)}

    if any("Viparita" in y for y in yoga_names):
        flags.append({
            "id": "recovery_from_adversity",
            "source": "vedic_structure",
            "severity": 2,
            "confidence": "low",
            "evidence": {"yogas": sorted(yoga_names)},
            "manifestation": "しんどい状況から立て直す力に変換しやすい",
            "countermeasures": "苦境時ほど行動ログを残し、回復パターンを再利用する",
        })

    if "Kemadruma Yoga" in yoga_names:
        flags.append({
            "id": "inner_isolation_tendency",
            "source": "vedic_structure",
            "severity": 3,
            "confidence": "low",
            "evidence": {"yogas": ["Kemadruma Yoga"]},
            "manifestation": "気持ちを自力で処理しようとして孤立感が強まりやすい",
            "countermeasures": "一人で抱える前提にせず、共有相手を固定しておく",
        })

    antara = (dasha.get("antara") or {}).get("lord")
    if antara == "Moon":
        flags.append({
            "id": "emotional_cycle_activation",
            "source": "vedic_structure",
            "severity": 2,
            "confidence": "low",
            "evidence": {"antara_lord": "Moon"},
            "manifestation": "感情の波や休息の必要性が表面化しやすい時期",
            "countermeasures": "睡眠・休息・一人時間を後回しにしない",
        })

    flags.sort(key=lambda x: int(x.get("severity", 0)), reverse=True)
    return flags


def merge_vedic_into_structure_graph(
    base: dict[str, Any] | None,
    vedic_structure: dict[str, Any],
    vedic_flags: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    out = dict(base or {})
    out["vedic_structure"] = vedic_structure
    out["vedic_flags"] = list(vedic_flags or derive_vedic_flags(vedic_structure))
    return out
