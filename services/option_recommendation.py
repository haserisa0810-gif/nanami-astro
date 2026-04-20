from __future__ import annotations

from typing import Any

from services.analyze_engine import build_payload_a
from services.analysis_engine import (
    detect_age_mode,
    detect_structured_mode,
    detect_vedic_trigger,
    decide_distribution,
)
from services.structure_engine import analyze_structure
from services.western_calc import calc_western_from_payload
from services.shichusuimei_calc import calc_shichusuimei_from_payload

PERSONAL_BODIES = {"Sun", "Moon", "Mercury", "Venus", "Mars", "ASC", "MC", "North Node", "South Node"}
ASTEROID_BODIES = {"Ceres", "Pallas", "Juno", "Vesta"}
PSYCHO_BODIES = {"Moon", "Venus", "Pluto", "Chiron", "Lilith", "Neptune"}
WORK_BODIES = {"Sun", "Mercury", "Mars", "Saturn", "Jupiter", "MC"}


def _as_planet_map(planets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(p.get("name")): p for p in (planets or []) if isinstance(p, dict) and p.get("name")}


def _aspect_matches(aspects: list[dict[str, Any]], target: str, counterparts: set[str], orb_max: float) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for asp in aspects or []:
        a = str(asp.get("planet1") or "")
        b = str(asp.get("planet2") or "")
        orb = float(asp.get("orb") or 999)
        if orb > orb_max:
            continue
        if a == target and b in counterparts:
            out.append(asp)
        elif b == target and a in counterparts:
            out.append(asp)
    return out


def _body_aspects(aspects: list[dict[str, Any]], bodies: set[str], orb_max: float, types: set[str] | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for asp in aspects or []:
        a = str(asp.get("planet1") or "")
        b = str(asp.get("planet2") or "")
        orb = float(asp.get("orb") or 999)
        t = str(asp.get("type") or "").lower()
        if orb > orb_max:
            continue
        if types and t not in types:
            continue
        if a in bodies and b in bodies:
            out.append(asp)
    return out


def _structured_score(structure: dict[str, Any], aspects: list[dict[str, Any]], planets: list[dict[str, Any]]) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    density = (structure.get("density") or {})
    max_density = int(density.get("max") or 0)
    second_density = int(density.get("second") or 0)
    house_counts = [int(x) for x in (structure.get("house_counts") or [])]
    connections = structure.get("connections") or {}
    max_conn = max([int(v) for v in connections.values()] or [0])
    tight_major = [a for a in aspects if float(a.get("orb") or 999) <= 1.5 and str(a.get("type") or "").lower() in {"conjunction", "opposition", "square", "trine", "sextile"}]
    personal_hard = _body_aspects(aspects, {"Sun", "Moon", "Mercury", "Venus", "Mars", "ASC", "MC", "Saturn", "Uranus", "Pluto"}, 2.2, {"opposition", "square", "quincunx"})
    anaretic = [p for p in planets if float(p.get("degree") or 0) >= 28.0]

    if max_conn >= 14:
        score += 3
        reasons.append(f"接続密度がかなり高い（最大{max_conn}）")
    elif max_conn >= 11:
        score += 2
        reasons.append(f"接続密度が高い（最大{max_conn}）")
    elif max_conn >= 9:
        score += 1
        reasons.append(f"接続密度がやや高い（最大{max_conn}）")

    if max(house_counts or [0]) >= 4:
        score += 1
        reasons.append(f"特定ハウス集中{max(house_counts)}")
    if max_density >= 4 or (max_density >= 3 and second_density >= 3):
        score += 1
        reasons.append(f"サイン分布に偏り（max={max_density}, second={second_density}）")
    if len(tight_major) >= 10:
        score += 2
        reasons.append(f"タイトな主要アスペクトが多い（{len(tight_major)}件）")
    elif len(tight_major) >= 7:
        score += 1
        reasons.append(f"タイトな主要アスペクトが多め（{len(tight_major)}件）")
    if len(personal_hard) >= 4:
        score += 2
        reasons.append(f"個人天体のハードアスペクトが多い（{len(personal_hard)}件）")
    elif len(personal_hard) >= 2:
        score += 1
        reasons.append(f"個人天体に矛盾が出やすい配置（{len(personal_hard)}件）")
    if anaretic:
        score += 1
        reasons.append("29度付近の天体あり")
    return score, reasons


def _element_of_sign(sign: str) -> str:
    mapping = {
        "Ari": "fire", "Leo": "fire", "Sag": "fire",
        "Tau": "earth", "Vir": "earth", "Cap": "earth",
        "Gem": "air", "Lib": "air", "Aqu": "air",
        "Can": "water", "Sco": "water", "Pis": "water",
    }
    return mapping.get(str(sign), "")


def _western_fit(planets: list[dict[str, Any]], aspects: list[dict[str, Any]], structure: dict[str, Any], theme_key: str, consultation_text: str) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    house_map = _as_planet_map(planets)
    personal = [house_map.get(name) for name in ["Sun", "Moon", "Mercury", "Venus", "ASC"] if house_map.get(name)]
    psych_houses = sum(1 for p in personal if int(p.get("house") or 0) in {1, 7, 8, 11, 12})
    if psych_houses >= 3:
        score += 3
        reasons.append("心理・関係性ハウスが強い")
    elif psych_houses >= 2:
        score += 2
        reasons.append("内面描写向きのハウス配置")

    psych_aspects = 0
    for body in ["Moon", "Venus", "Pluto", "Chiron", "Lilith"]:
        psych_aspects += len(_aspect_matches(aspects, body, PERSONAL_BODIES | {"Pluto", "Neptune", "Chiron", "Lilith"}, 3.0))
    if psych_aspects >= 8:
        score += 3
        reasons.append("感情・無意識に関わる接続が多い")
    elif psych_aspects >= 4:
        score += 2
        reasons.append("感情描写に厚みが出やすい")

    elements = [_element_of_sign(str(p.get("sign") or "")) for p in personal]
    water_air = sum(1 for e in elements if e in {"water", "air"})
    if water_air >= 3:
        score += 2
        reasons.append("水/風要素が強め")

    if theme_key in {"love", "relationship", "overall"}:
        score += 2
        reasons.append(f"テーマ{theme_key}は西洋と相性が良い")
    if any(k in consultation_text for k in ["気持ち", "恋愛", "人間関係", "自分", "本音", "わかって", "生きづら"]):
        score += 2
        reasons.append("相談文が心理描写寄り")

    max_conn = max([int(v) for v in (structure.get("connections") or {}).values()] or [0])
    if max_conn >= 11:
        score += 1
        reasons.append("構造の密度が高く西洋の組み合わせ読みに向く")
    return score, reasons


def _shichu_fit(payload: dict[str, Any], planets: list[dict[str, Any]], theme_key: str, birth_time: str | None, consultation_text: str, day_change_at_23: bool) -> tuple[int, list[str], dict[str, Any]]:
    score = 0
    reasons: list[str] = []
    shichu = calc_shichusuimei_from_payload(payload, day_change_at_23=day_change_at_23)
    normalized = shichu.get("normalized_data") if isinstance(shichu, dict) else {}
    five = (normalized or {}).get("five_elements") or {}
    visible = five.get("visible") or {}
    counts = [int(v or 0) for v in visible.values() if isinstance(v, (int, float))]
    if counts:
        spread = max(counts) - min(counts)
        if spread >= 3:
            score += 3
            reasons.append("五行バランスの偏りが大きい")
        elif spread >= 2:
            score += 2
            reasons.append("五行に偏りがある")
    if (shichu.get("day_master") or ""):
        score += 2
        reasons.append("日主が取れており四柱の解像度を使える")

    work_houses = sum(1 for p in planets if int(p.get("house") or 0) in {2, 6, 10} and str(p.get("name") or "") in WORK_BODIES)
    if work_houses >= 3:
        score += 3
        reasons.append("現実・仕事ハウスが強い")
    elif work_houses >= 2:
        score += 2
        reasons.append("仕事/社会適応の読みと相性が良い")

    earth = sum(1 for p in planets if _element_of_sign(str(p.get("sign") or "")) == "earth" and str(p.get("name") or "") in {"Sun", "Moon", "Mercury", "Venus", "Mars", "ASC"})
    if earth >= 3:
        score += 2
        reasons.append("地要素が強め")

    if theme_key in {"work", "timing"}:
        score += 3
        reasons.append(f"テーマ{theme_key}は四柱推命と相性が良い")
    if any(k in consultation_text for k in ["仕事", "適職", "現実", "お金", "運気", "流れ", "時期", "転機"]):
        score += 2
        reasons.append("相談文が現実/運気寄り")
    if not birth_time:
        score += 2
        reasons.append("出生時刻不明時は四柱推命が安定")
    return score, reasons, shichu




def _vedic_trigger(planets: list[dict[str, Any]], aspects: list[dict[str, Any]], structure: dict[str, Any], theme_key: str, consultation_text: str, observations_text: str, structured_score: int) -> tuple[int, str, list[str], dict[str, bool], dict[str, bool]]:
    score = 0
    reasons: list[str] = []
    consultation_text = str(consultation_text or "")
    observations_text = str(observations_text or "")
    text = consultation_text + "\n" + observations_text
    source_flags = {
        "has_consultation_text": bool(consultation_text.strip()),
        "has_observations_text": bool(observations_text.strip()),
        "matched_text_trigger": False,
        "matched_observation_trigger": False,
    }
    chart_flags = {
        'node': False,
        'deep_house': False,
        'pluto': False,
        'lilith_chiron': False,
    }

    strong_words = [
        "運命", "宿命", "意味", "カルマ", "魂", "人生のテーマ", "人生の流れ",
        "繰り返す", "同じパターン", "なぜいつも", "なぜ毎回", "転機", "タイミング",
    ]
    medium_words = [
        "なぜか", "流れ", "節目", "縁", "巡り合わせ", "抜け出せない", "反復", "周期",
        "しっくりこない", "説明できない", "違和感",
    ]
    negative_words = [
        "現実的", "仕事の進め方", "転職方法", "年収", "実務", "効率", "手順", "改善",
    ]

    for w in strong_words:
        if w in text:
            score += 5
            reasons.append(f"単語:{w}")
            if w in consultation_text:
                source_flags["matched_text_trigger"] = True
            if w in observations_text:
                source_flags["matched_observation_trigger"] = True
    for w in medium_words:
        if w in text:
            score += 3
            reasons.append(f"単語:{w}")
            if w in consultation_text:
                source_flags["matched_text_trigger"] = True
            if w in observations_text:
                source_flags["matched_observation_trigger"] = True
    for w in negative_words:
        if w in text:
            score -= 4
            reasons.append(f"抑制:{w}")

    if theme_key == 'timing':
        score += 4
        reasons.append('テーマtimingで時期読み需要')
    elif theme_key in {'overall', 'relationship', 'love'}:
        score += 1

    planet_map = _as_planet_map(planets)
    node_house = int((planet_map.get('North Node') or {}).get('house') or 0)
    south_house = int((planet_map.get('South Node') or {}).get('house') or 0)
    deep_houses = {8, 12}
    if node_house in deep_houses or south_house in deep_houses:
        score += 4
        reasons.append('ノードが深層ハウス')
        chart_flags['node'] = True
    elif _aspect_matches(aspects, 'North Node', PERSONAL_BODIES | {'Chiron', 'Lilith', 'Pluto', 'Neptune', 'MC', 'ASC'}, 2.2):
        score += 3
        reasons.append('ノード軸が主要ポイントと強接続')
        chart_flags['node'] = True

    deep_house_count = sum(1 for p in planets if int(p.get('house') or 0) in deep_houses and str(p.get('name') or '') in {'Moon','Pluto','Neptune','Lilith','Chiron','Vertex','North Node','South Node'})
    if deep_house_count >= 3:
        score += 3
        reasons.append('8/12ハウスに深層ポイント集中')
        chart_flags['deep_house'] = True

    scorpio_pluto = sum(1 for p in planets if (str(p.get('sign') or '') == 'Sco' or str(p.get('name') or '') == 'Pluto') and str(p.get('name') or '') in {'Moon','Venus','Mars','Pluto','ASC'})
    if scorpio_pluto >= 2:
        score += 2
        reasons.append('蠍/冥王星テーマが強い')
        chart_flags['pluto'] = True

    if _aspect_matches(aspects, 'Lilith', PERSONAL_BODIES | {'Chiron', 'Pluto', 'Neptune'}, 2.0) or _aspect_matches(aspects, 'Chiron', PERSONAL_BODIES | {'Lilith', 'Pluto', 'Neptune'}, 2.0):
        score += 2
        reasons.append('Lilith/Chironが深層読みに反応')
        chart_flags['lilith_chiron'] = True

    if structured_score >= 4:
        score += 2
        reasons.append('structured判定で一般論外れを補正')

    risk_ids = {str(f.get('id') or '') for f in (structure.get('risk_flags') or []) if isinstance(f, dict)}
    if any(x in observations_text for x in ['繰り返し', '周期', '毎回', '抜け出せない']) or any('cycle' in rid for rid in risk_ids):
        score += 5
        reasons.append('反復パターンの観測あり')
        source_flags['matched_observation_trigger'] = True
    if any(x in observations_text for x in ['説明できない', '一般論に当てはまらない', 'しっくりこない', '例外']):
        score += 4
        reasons.append('説明困難なズレを補正')
        source_flags['matched_observation_trigger'] = True

    has_user_trigger = source_flags['matched_text_trigger'] or source_flags['matched_observation_trigger']
    chart_flag_count = sum(1 for v in chart_flags.values() if v)
    if not has_user_trigger:
        if chart_flag_count >= 3 and (chart_flags['deep_house'] or chart_flags['pluto'] or chart_flags['lilith_chiron']):
            score = max(score, 12)
            score = min(score, 16)
            reasons.append('チャートのみ条件を満たすため軽く発火')
        else:
            score = min(score, 11)
            reasons.append('チャートのみ条件が弱いためインドはOFF')

    if score >= 20:
        level = 'strong'
    elif score >= 12:
        level = 'light'
    else:
        level = 'off'
    return score, level, reasons, source_flags, chart_flags

def _choose_distribution(western_fit: int, shichu_fit: int, structured_mode: str, theme_key: str, vedic_level: str) -> tuple[str, dict[str, int], list[str]]:
    reasons: list[str] = []
    if western_fit >= shichu_fit + 4:
        dominant = "western"
        reasons.append("西洋優勢")
    elif shichu_fit >= western_fit + 4:
        dominant = "shichu"
        reasons.append("四柱推命優勢")
    else:
        dominant = "balanced"
        reasons.append("両方を併用しやすい")

    dist = decide_distribution(dominant, vedic_level)

    if structured_mode == "structured":
        reasons.append("structured判定あり")
    if theme_key == "timing":
        dist["shichu"] += 5
        reasons.append("時期テーマなので四柱をやや加算")
    total = max(sum(dist.values()), 1)
    dist = {k: int(round(v / total * 100)) for k, v in dist.items()}
    diff = 100 - sum(dist.values())
    if diff:
        key = "western" if dist.get("western", 0) >= dist.get("shichu", 0) else "shichu"
        dist[key] = dist.get(key, 0) + diff
    return dominant, dist, reasons


def recommend_western_options(
    *,
    birth_date: str,
    birth_time: str | None = None,
    birth_place: str | None = None,
    prefecture: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    gender: str | None = None,
    house_system: str = "P",
    node_mode: str = "true",
    lilith_mode: str = "mean",
    consultation_text: str | None = None,
    observations_text: str | None = None,
    theme: str | None = None,
    day_change_at_23: bool = False,
) -> dict[str, Any]:
    unknowns: list[str] = []
    payload = build_payload_a(
        birth_date=birth_date,
        birth_time=birth_time,
        birth_place=birth_place,
        prefecture=prefecture,
        lat=lat,
        lon=lon,
        gender=gender or "female",
        house_system=house_system,
        node_mode=node_mode,
        lilith_mode=lilith_mode,
        include_asteroids=True,
        include_chiron=True,
        include_lilith=True,
        include_vertex=True,
        unknowns=unknowns,
    )
    western = calc_western_from_payload(payload, house_system=house_system)
    planets = [p for p in (western.get("planets") or []) if isinstance(p, dict)]
    aspects = [a for a in (western.get("aspects") or []) if isinstance(a, dict)]
    houses = [float(h.get("lon")) for h in (western.get("houses") or []) if isinstance(h, dict) and h.get("lon") is not None]
    planet_map = _as_planet_map(planets)
    structure = analyze_structure(planets, houses if len(houses) >= 12 else None)
    consultation_text = str(consultation_text or "")
    observations_text = str(observations_text or "")
    theme_key = str(theme or "overall").strip().lower()

    structured_score, structured_reasons = _structured_score(structure, aspects, planets)
    western_fit, western_reasons = _western_fit(planets, aspects, structure, theme_key, consultation_text)
    shichu_fit, shichu_reasons, shichu = _shichu_fit(payload, planets, theme_key, birth_time, consultation_text, day_change_at_23)
    structure_with_flags = dict(structure)
    structure_with_flags["risk_flags"] = western.get("risk_flags") or []

    structure_inputs = {
        "connection_density": max([int(v) for v in (structure.get("connections") or {}).values()] or [0]),
        "house_concentration": max([int(x) for x in (structure.get("house_counts") or [])] or [0]) >= 4,
        "hard_aspects": len(_body_aspects(aspects, {"Sun", "Moon", "Mercury", "Venus", "Mars", "ASC", "MC", "Saturn", "Uranus", "Pluto"}, 3.0, {"opposition", "square", "quincunx"})),
        "contradictions": len(_body_aspects(aspects, {"Sun", "Moon", "Mercury", "Venus", "Mars", "ASC", "MC", "Saturn", "Uranus", "Pluto"}, 2.2, {"opposition", "square"})) >= 2,
    }
    structured_eval = detect_structured_mode(structure_inputs)
    structured_mode = str(structured_eval.get("mode") or "general")


    chiron_aspects = _aspect_matches(aspects, "Chiron", PERSONAL_BODIES, 3.0)
    lilith_aspects = _aspect_matches(aspects, "Lilith", PERSONAL_BODIES | {"Chiron"}, 3.0)
    vertex_aspects = _aspect_matches(aspects, "Vertex", PERSONAL_BODIES, 2.0)
    asteroid_hits = []
    for asteroid in ASTEROID_BODIES:
        asteroid_hits.extend(_aspect_matches(aspects, asteroid, PERSONAL_BODIES, 2.0))

    chiron_house = int((planet_map.get("Chiron") or {}).get("house") or 0)
    lilith_house = int((planet_map.get("Lilith") or {}).get("house") or 0)
    vertex_house = int((planet_map.get("Vertex") or {}).get("house") or 0)

    include_chiron = bool(chiron_aspects or chiron_house in {1, 3, 6, 7, 8, 9, 10, 11})
    include_lilith = bool(lilith_aspects or lilith_house in {1, 7, 8, 9, 12} or structured_score >= 4)
    include_vertex = bool(vertex_aspects or vertex_house in {5, 7, 8})
    include_asteroids = bool((structured_score >= 3 and len(asteroid_hits) >= 1) or (structured_score >= 4 and len(asteroid_hits) >= 0))

    if not birth_time:
        include_vertex = False

    if theme_key in {"love", "relationship"}:
        include_vertex = include_vertex or bool(vertex_aspects)
        include_asteroids = include_asteroids or any((a.get("planet1") in {"Juno", "Vesta"} or a.get("planet2") in {"Juno", "Vesta"}) for a in asteroid_hits)
    if theme_key == "work":
        include_asteroids = include_asteroids or any((a.get("planet1") in {"Pallas", "Ceres"} or a.get("planet2") in {"Pallas", "Ceres"}) for a in asteroid_hits)
    if any(k in consultation_text for k in ["一般論", "当たら", "例外", "しっくりこな"]):
        include_asteroids = True
        include_lilith = include_lilith or bool(lilith_aspects)
    if structured_score >= 5:
        include_asteroids = True
        include_chiron = True

    _legacy_vedic_score, _legacy_vedic_level, _legacy_vedic_reasons, vedic_source_flags, chart_flags = _vedic_trigger(planets, aspects, structure_with_flags, theme_key, consultation_text, observations_text, structured_score)
    vedic_eval = detect_vedic_trigger(consultation_text, observations_text, chart_flags, structured_mode == 'structured')
    vedic_score = int(vedic_eval.get('score') or 0)
    vedic_level = str(vedic_eval.get('level') or 'off')
    vedic_reasons = list(vedic_eval.get('reasons') or [])

    dominant_fit, distribution, distribution_reasons = _choose_distribution(western_fit, shichu_fit, structured_mode, theme_key, vedic_level)
    suggested_astrology_system = "western"
    if dominant_fit in {"shichu", "balanced"}:
        suggested_astrology_system = "integrated_w_shichu"
    if vedic_level == "strong":
        suggested_astrology_system = "integrated" if dominant_fit == "western" else "integrated3"
    elif vedic_level == "light":
        suggested_astrology_system = "integrated" if dominant_fit == "western" else "integrated3"
    suggested_reading_style = structured_mode
    age_mode = detect_age_mode(birth_date)

    reasons: dict[str, list[str]] = {
        "fit": [],
        "structured": structured_reasons[:] + list(structured_eval.get("reasons") or []),
        "include_chiron": [],
        "include_lilith": [],
        "include_vertex": [],
        "include_asteroids": [],
        "distribution": distribution_reasons[:],
        "vedic_trigger": [],
    }
    reasons["fit"].append(f"西洋寄りスコア: {western_fit}")
    reasons["fit"].append(f"四柱推命寄りスコア: {shichu_fit}")
    reasons["fit"].append(f"インド発火スコア: {vedic_score} ({vedic_level})")
    reasons["fit"].extend(western_reasons[:2])
    reasons["fit"].extend(shichu_reasons[:2])
    if include_chiron:
        if chiron_aspects:
            reasons["include_chiron"].append(f"Chironが主要天体/角度と{len(chiron_aspects)}件タイト")
        if chiron_house in {1, 3, 6, 7, 8, 9, 10, 11}:
            reasons["include_chiron"].append(f"Chironが第{chiron_house}ハウス")
    if include_lilith:
        if lilith_aspects:
            reasons["include_lilith"].append(f"Lilithが主要天体/角度と{len(lilith_aspects)}件タイト")
        if lilith_house in {1, 7, 8, 9, 12}:
            reasons["include_lilith"].append(f"Lilithが第{lilith_house}ハウス")
        if structured_score >= 4:
            reasons["include_lilith"].append("structured判定が強いため補助採用")
    if include_vertex:
        if vertex_aspects:
            reasons["include_vertex"].append(f"Vertexが主要天体/角度と{len(vertex_aspects)}件タイト")
        if vertex_house in {5, 7, 8}:
            reasons["include_vertex"].append(f"Vertexが第{vertex_house}ハウス")
    elif not birth_time:
        reasons["include_vertex"].append("出生時刻不明のためVertex自動ONは抑制")
    if include_asteroids:
        if structured_reasons:
            reasons["include_asteroids"].append("構造型判定: " + " / ".join(structured_reasons[:3]))
        if asteroid_hits:
            reasons["include_asteroids"].append(f"小惑星が主要天体/角度と{len(asteroid_hits)}件タイト")
        if any(k in consultation_text for k in ["一般論", "当たら", "例外", "しっくりこな"]):
            reasons["include_asteroids"].append("一般論に当てはまりにくい相談内容")

    if vedic_reasons:
        reasons["vedic_trigger"].extend(vedic_reasons[:6])
    mode = structured_mode
    dominant_label = {
        "western": "西洋寄り",
        "shichu": "四柱推命寄り",
        "balanced": "バランス型",
    }.get(dominant_fit, "バランス型")
    return {
        "mode": mode,
        "score": structured_score,
        "fit_scores": {
            "western": western_fit,
            "shichu": shichu_fit,
            "vedic": vedic_score,
        },
        "dominant_fit": dominant_fit,
        "dominant_label": dominant_label,
        "distribution": distribution,
        "role_distribution": distribution,
        "suggested_astrology_system": suggested_astrology_system,
        "suggested_reading_style": suggested_reading_style,
        "age_mode": age_mode,
        "options": {
            "include_chiron": include_chiron,
            "include_lilith": include_lilith,
            "include_vertex": include_vertex,
            "include_asteroids": include_asteroids,
        },
        "reasons": reasons,
        "vedic_trigger": {
            "score": vedic_score,
            "level": vedic_level,
            "reasons": vedic_reasons[:],
        },
        "meta": {
            "birth_time_known": bool(str(birth_time or "").strip()),
            "unknowns": unknowns,
            "structured_reasons": structured_reasons,
            "structured_eval": structured_eval,
            "shichu_available": bool(shichu.get("day_master") or (shichu.get("normalized_data") or {}).get("pillars")),
            "vedic_source_flags": vedic_source_flags,
            "chart_flags": chart_flags,
            "age_mode": age_mode,
        },
    }
