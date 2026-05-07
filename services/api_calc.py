from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import yaml

from services.handoff_log import dumps_yaml
from services.shichusuimei_calc import calc_shichusuimei_from_payload
from services.transit_calc import calc_transits_long_term, calc_transits_single
from services.western_calc import calc_western_from_payload
from shared import _calc_payload_from_inputs

API_VERSION = "1.0"
ENGINE_NAME = "nanami-astro"
DEFAULT_TZ_NAME = "Asia/Tokyo"
DEFAULT_LAT = 35.6895
DEFAULT_LNG = 139.6917

WESTERN_MAJOR_BODIES = {"Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"}
WESTERN_SOFT_BODIES = {"Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC", "North Node", "Chiron", "Lilith", "Vertex"}
SHICHU_ELEMENTS = ("木", "火", "土", "金", "水")
TRANSIT_MAJOR_BODIES = {"Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"}


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "on", "yes", "y"}:
        return True
    if text in {"0", "false", "off", "no", "n", ""}:
        return False
    return default


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = _safe_text(value).strip()
    if not text:
        return None
    try:
        if len(text) == 10:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _normalize_period(value: Any) -> str:
    period = _safe_text(value).strip().lower() or "day"
    if period not in {"day", "month"}:
        raise ValueError("UNSUPPORTED_PERIOD")
    return period


def _normalize_day_boundary(value: Any) -> bool:
    text = _safe_text(value).strip().lower()
    return text in {"23:00", "23", "true", "on", "yes", "1"}


def _extract_writing(input_data: dict[str, Any]) -> dict[str, Any]:
    writing = input_data.get("writing") if isinstance(input_data.get("writing"), dict) else {}
    tone = writing.get("tone") if isinstance(writing.get("tone"), dict) else {}
    focus_areas = writing.get("focus_areas") if isinstance(writing.get("focus_areas"), list) else []
    return {
        "tone": {
            "sharpness": int(tone.get("sharpness") or 0),
            "warmth": int(tone.get("warmth") or 0),
            "mystical": int(tone.get("mystical") or 0),
        },
        "focus_areas": [str(x) for x in focus_areas if str(x).strip()],
    }


def _build_base_payload(input_data: dict[str, Any], *, day_change_at_23: bool = False) -> tuple[dict[str, Any], list[str]]:
    birth_date = _safe_text(input_data.get("birth_date")).strip()
    birth_time = _safe_text(input_data.get("birth_time")).strip() or None
    birth_place = _safe_text(input_data.get("birth_place")).strip() or None
    prefecture = _safe_text(input_data.get("prefecture")).strip() or None
    lat = input_data.get("lat")
    lon = input_data.get("lon")
    unknowns: list[str] = []
    if day_change_at_23:
        unknowns.append("日替わり境界は23:00切替")
    payload = _calc_payload_from_inputs(
        birth_date=birth_date,
        birth_time=birth_time,
        birth_place=birth_place,
        prefecture=prefecture,
        lat=lat if lat is not None else None,
        lon=lon if lon is not None else None,
        unknowns=unknowns,
    )
    payload["gender"] = _safe_text(input_data.get("gender") or "female").strip() or "female"
    return payload, unknowns


def _selected_planets(western: dict[str, Any]) -> dict[str, dict[str, Any]]:
    planets = western.get("planets") if isinstance(western, dict) else []
    out: dict[str, dict[str, Any]] = {}
    if isinstance(planets, list):
        for item in planets:
            if isinstance(item, dict) and item.get("name"):
                out[str(item["name"])] = item
    return out


def _planet_hits(aspects: list[dict[str, Any]], planet_name: str) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for item in aspects:
        if not isinstance(item, dict):
            continue
        if item.get("planet1") == planet_name or item.get("planet2") == planet_name:
            hits.append(item)
    return hits


def _aspect_basis(hits: list[dict[str, Any]], *, limit: int = 4) -> list[str]:
    basis: list[str] = []
    for item in hits[:limit]:
        p1 = _safe_text(item.get("planet1"))
        p2 = _safe_text(item.get("planet2"))
        asp = _safe_text(item.get("type"))
        orb = item.get("orb")
        orb_text = f" orb{float(orb):.1f}" if isinstance(orb, (int, float)) else ""
        basis.append(f"{p1}-{p2}:{asp}{orb_text}")
    return basis


def _build_western_tags(western: dict[str, Any]) -> list[dict[str, Any]]:
    planets = _selected_planets(western)
    aspects = western.get("aspects") if isinstance(western, dict) and isinstance(western.get("aspects"), list) else []
    angles = western.get("angles") if isinstance(western, dict) and isinstance(western.get("angles"), dict) else {}
    planet_count = len(planets)
    tags: list[dict[str, Any]] = []

    saturn_hits = _planet_hits(aspects, "Saturn")
    jupiter_hits = _planet_hits(aspects, "Jupiter")
    moon_hits = _planet_hits(aspects, "Moon")
    uranus_hits = _planet_hits(aspects, "Uranus")
    venus_hits = _planet_hits(aspects, "Venus")
    career_hits = [x for x in aspects if isinstance(x, dict) and ("MC" in {x.get("planet1"), x.get("planet2")} or "Sun" in {x.get("planet1"), x.get("planet2")})]

    tags.append({
        "id": "saturn_pressure",
        "label": "責任・制限・継続課題",
        "strength": min(100, len(saturn_hits) * 25),
        "category": "timing",
        "basis": _aspect_basis(saturn_hits),
        "writing_hint": "焦らず整える必要性として読む",
    })
    tags.append({
        "id": "jupiter_expansion",
        "label": "拡大・追い風・成長機会",
        "strength": min(100, len(jupiter_hits) * 20),
        "category": "opportunity",
        "basis": _aspect_basis(jupiter_hits),
        "writing_hint": "広がる可能性として読む",
    })
    tags.append({
        "id": "emotional_reset",
        "label": "感情の揺れ・安心の再調整",
        "strength": min(100, len(moon_hits) * 20),
        "category": "emotion",
        "basis": _aspect_basis(moon_hits),
        "writing_hint": "気持ちの動きを整えるテーマとして読む",
    })
    tags.append({
        "id": "uranus_disruption",
        "label": "変化・解放・予定変更",
        "strength": min(100, len(uranus_hits) * 25),
        "category": "change",
        "basis": _aspect_basis(uranus_hits),
        "writing_hint": "急な変化への対応力として読む",
    })
    tags.append({
        "id": "career_visibility",
        "label": "仕事面の可視化・対外性",
        "strength": min(100, len(career_hits) * 20 + (15 if angles.get("mc") is not None else 0)),
        "category": "career",
        "basis": _aspect_basis(career_hits),
        "writing_hint": "社会面で目立ちやすい流れとして読む",
    })
    tags.append({
        "id": "relationship_activation",
        "label": "対人・関係性の活性化",
        "strength": min(100, len(venus_hits) * 20),
        "category": "relationship",
        "basis": _aspect_basis(venus_hits),
        "writing_hint": "人間関係の動きとして読む",
    })

    if planet_count == 0:
        for tag in tags:
            tag["strength"] = 0
            tag["basis"] = []
    return tags


def _build_shichu_tags(shichu: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = shichu.get("normalized_data") if isinstance(shichu, dict) and isinstance(shichu.get("normalized_data"), dict) else {}
    structure = shichu.get("structure_report") if isinstance(shichu, dict) and isinstance(shichu.get("structure_report"), dict) else {}
    features = shichu.get("features") if isinstance(shichu, dict) and isinstance(shichu.get("features"), dict) else {}
    five = normalized.get("five_elements") if isinstance(normalized.get("five_elements"), dict) else {}
    visible = five.get("visible") if isinstance(five.get("visible"), dict) else {}
    ten = normalized.get("ten_gods") if isinstance(normalized.get("ten_gods"), dict) else {}
    day_master = _safe_text(structure.get("day_master"))
    strength = structure.get("strength_index") if isinstance(structure.get("strength_index"), dict) else {}
    daiun = normalized.get("daiun") if isinstance(normalized.get("daiun"), dict) else {}

    visible_counts = {k: int(visible.get(k) or 0) for k in SHICHU_ELEMENTS}
    max_elem = max(visible_counts, key=visible_counts.get) if visible_counts else ""
    min_elem = min(visible_counts, key=visible_counts.get) if visible_counts else ""
    max_count = visible_counts.get(max_elem, 0)
    min_count = visible_counts.get(min_elem, 0)

    def _tag(tag_id: str, label: str, strength_value: int, category: str, basis: list[str], hint: str) -> dict[str, Any]:
        return {
            "id": tag_id,
            "label": label,
            "strength": max(0, min(100, strength_value)),
            "category": category,
            "basis": basis,
            "writing_hint": hint,
        }

    imbalance_basis = []
    if max_elem:
        imbalance_basis.append(f"visible五行で{max_elem}が最多({max_count})")
    if min_elem:
        imbalance_basis.append(f"visible五行で{min_elem}が最少({min_count})")
    if day_master:
        imbalance_basis.append(f"日主={day_master}")

    ten_god_basis = []
    for key in ("year", "month", "day", "hour"):
        pillar = ten.get("pillars", {}).get(key, {}) if isinstance(ten.get("pillars"), dict) else {}
        if isinstance(pillar, dict):
            stem = _safe_text(pillar.get("stem"))
            tg = _safe_text(pillar.get("ten_god"))
            if stem and tg:
                ten_god_basis.append(f"{key}:{stem}/{tg}")

    current_daiun = []
    periods = daiun.get("periods") if isinstance(daiun.get("periods"), list) else []
    if periods:
        first = periods[0] if isinstance(periods[0], dict) else {}
        if first:
            current_daiun.append(
                f"{_safe_text(first.get('kanshi'))}:{_safe_text(first.get('start_age'))}〜{_safe_text(first.get('end_age'))}"
            )

    strength_score = float((strength.get("score") or 0) if isinstance(strength, dict) else 0)

    tags = [
        _tag(
            "five_element_imbalance",
            "五行バランスの偏り",
            80 if max_count - min_count >= 2 else 20 if max_count != min_count else 0,
            "structure",
            imbalance_basis if max_count != min_count else [],
            "偏りは強みと課題の両面として読む",
        ),
        _tag(
            "wealth_star_activation",
            "財の流れ・収入テーマ",
            70 if _safe_text((ten.get("pillars") or {}).get("year", {}).get("ten_god")) in {"正財", "偏財"} or _safe_text((ten.get("pillars") or {}).get("month", {}).get("ten_god")) in {"正財", "偏財"} else 0,
            "money",
            ten_god_basis if any("財" in b for b in ten_god_basis) else [],
            "財星が見えるところを収入・対価の動きとして読む",
        ),
        _tag(
            "officer_star_pressure",
            "責任・規律・ルール圧",
            70 if _safe_text((ten.get("pillars") or {}).get("year", {}).get("ten_god")) in {"正官", "偏官"} or _safe_text((ten.get("pillars") or {}).get("month", {}).get("ten_god")) in {"正官", "偏官"} else 0,
            "timing",
            ten_god_basis if any("官" in b for b in ten_god_basis) else [],
            "官星が出る場面は責任・締切・役割として読む",
        ),
        _tag(
            "resource_star_support",
            "支え・学び・回復",
            60 if _safe_text((ten.get("pillars") or {}).get("year", {}).get("ten_god")) in {"印綬", "偏印"} or _safe_text((ten.get("pillars") or {}).get("month", {}).get("ten_god")) in {"印綬", "偏印"} else 0,
            "support",
            ten_god_basis if any("印" in b for b in ten_god_basis) else [],
            "印星は回復や学びの支えとして読む",
        ),
        _tag(
            "output_star_expression",
            "表現・発信・アウトプット",
            60 if _safe_text((ten.get("pillars") or {}).get("year", {}).get("ten_god")) in {"食神", "傷官"} or _safe_text((ten.get("pillars") or {}).get("month", {}).get("ten_god")) in {"食神", "傷官"} else 0,
            "expression",
            ten_god_basis if any("食" in b or "傷" in b for b in ten_god_basis) else [],
            "食傷は表現・創作・言語化として読む",
        ),
        _tag(
            "luck_cycle_shift",
            "大運の切替・流れの転機",
            50 if current_daiun else 0,
            "timing",
            current_daiun,
            "大運は長期の流れとして読む",
        ),
    ]

    if not features and not normalized and not structure:
        for tag in tags:
            tag["strength"] = 0
            tag["basis"] = []
    return tags


def _build_transit_tags(transit: dict[str, Any]) -> list[dict[str, Any]]:
    today = transit.get("today_planets") if isinstance(transit, dict) and isinstance(transit.get("today_planets"), list) else []
    aspects = transit.get("aspects") if isinstance(transit, dict) and isinstance(transit.get("aspects"), list) else []
    long_term = transit.get("long_term") if isinstance(transit, dict) and isinstance(transit.get("long_term"), list) else []

    def _count_hits(planet_name: str) -> int:
        return sum(1 for item in aspects if isinstance(item, dict) and (item.get("transit_planet") == planet_name or item.get("natal_planet") == planet_name))

    pressure_hits = [item for item in long_term if isinstance(item, dict) and item.get("status") == "active" and _safe_text(item.get("transit_planet")) == "Saturn"]
    change_hits = [item for item in long_term if isinstance(item, dict) and _safe_text(item.get("transit_planet")) in {"Uranus", "Pluto"}]
    expansion_hits = [item for item in long_term if isinstance(item, dict) and _safe_text(item.get("transit_planet")) == "Jupiter"]
    emotional_hits = [item for item in aspects if isinstance(item, dict) and _safe_text(item.get("natal_planet")) == "Moon"]

    tags = [
        {
            "id": "timing_activation",
            "label": "今出やすいテーマ",
            "strength": 70 if today else 0,
            "category": "timing",
            "basis": [f"today_planets:{len(today)}"] if today else [],
            "writing_hint": "今日の空気として読む",
        },
        {
            "id": "pressure_period",
            "label": "圧・調整・詰まり",
            "strength": min(100, len(pressure_hits) * 25),
            "category": "timing",
            "basis": [f"{_safe_text(x.get('transit_planet'))}:{_safe_text(x.get('natal_planet'))}" for x in pressure_hits[:4]],
            "writing_hint": "無理に進めず整える時期として読む",
        },
        {
            "id": "change_window",
            "label": "変化の入口",
            "strength": min(100, len(change_hits) * 20),
            "category": "change",
            "basis": [f"{_safe_text(x.get('transit_planet'))}:{_safe_text(x.get('natal_planet'))}" for x in change_hits[:4]],
            "writing_hint": "変化は準備と切替の合図として読む",
        },
        {
            "id": "emotional_wave",
            "label": "感情の波",
            "strength": min(100, len(emotional_hits) * 25),
            "category": "emotion",
            "basis": [f"{_safe_text(x.get('transit_planet'))}:{_safe_text(x.get('natal_planet'))}:{_safe_text(x.get('aspect'))}" for x in emotional_hits[:4]],
            "writing_hint": "気分の上下として読む",
        },
        {
            "id": "expansion_window",
            "label": "拡大・追い風",
            "strength": min(100, len(expansion_hits) * 20),
            "category": "opportunity",
            "basis": [f"{_safe_text(x.get('transit_planet'))}:{_safe_text(x.get('natal_planet'))}" for x in expansion_hits[:4]],
            "writing_hint": "広がりやすい動きとして読む",
        },
    ]

    if not today and not aspects and not long_term:
        for tag in tags:
            tag["strength"] = 0
            tag["basis"] = []
    return tags


def _build_integration_tags(western: dict[str, Any], shichu: dict[str, Any], transit: dict[str, Any]) -> list[dict[str, Any]]:
    western_tags = _build_western_tags(western)
    shichu_tags = _build_shichu_tags(shichu)
    transit_tags = _build_transit_tags(transit)

    western_strong = any(tag.get("strength", 0) >= 50 for tag in western_tags)
    shichu_strong = any(tag.get("strength", 0) >= 50 for tag in shichu_tags)
    transit_strong = any(tag.get("strength", 0) >= 50 for tag in transit_tags)

    return [
        {
            "id": "structure_alignment",
            "label": "構造の一致",
            "strength": 70 if western_strong and shichu_strong else 0,
            "category": "integration",
            "basis": ["western_tags", "shichu_tags"] if western_strong and shichu_strong else [],
            "writing_hint": "両体系で強いテーマが重なるところを読む",
        },
        {
            "id": "structure_conflict",
            "label": "構造のズレ",
            "strength": 60 if western_strong and not shichu_strong else 0,
            "category": "integration",
            "basis": ["western_strong", "shichu_weak"] if western_strong and not shichu_strong else [],
            "writing_hint": "出やすい方向と内側の土台のズレを読む",
        },
        {
            "id": "timing_activation_of_core_theme",
            "label": "核テーマの時期反応",
            "strength": 70 if transit_strong else 0,
            "category": "integration",
            "basis": ["transit_tags"] if transit_strong else [],
            "writing_hint": "今動くテーマとして読む",
        },
        {
            "id": "western_shichu_double_emphasis",
            "label": "西洋×四柱の二重強調",
            "strength": 80 if western_strong and shichu_strong and transit_strong else 0,
            "category": "integration",
            "basis": ["western", "shichu", "transit"] if western_strong and shichu_strong and transit_strong else [],
            "writing_hint": "複数体系で同じ方向性が出るところを読む",
        },
        {
            "id": "risk_overlap",
            "label": "注意点の重なり",
            "strength": 50 if any(tag.get("id") == "saturn_pressure" and tag.get("strength", 0) >= 50 for tag in western_tags) and any(tag.get("id") == "pressure_period" and tag.get("strength", 0) >= 50 for tag in transit_tags) else 0,
            "category": "risk",
            "basis": ["western_saturn", "transit_pressure"] if any(tag.get("id") == "saturn_pressure" and tag.get("strength", 0) >= 50 for tag in western_tags) and any(tag.get("id") == "pressure_period" and tag.get("strength", 0) >= 50 for tag in transit_tags) else [],
            "writing_hint": "同じ注意が複数体系で出るなら先に整える",
        },
    ]


def _build_input_echo(input_data: dict[str, Any], *, day_change_at_23: bool = False, period: str = "day") -> dict[str, Any]:
    writing = _extract_writing(input_data)
    return {
        "name": _safe_text(input_data.get("name")).strip(),
        "birth_date": _safe_text(input_data.get("birth_date")).strip(),
        "birth_time": _safe_text(input_data.get("birth_time")).strip(),
        "birth_place": _safe_text(input_data.get("birth_place")).strip(),
        "prefecture": _safe_text(input_data.get("prefecture")).strip(),
        "lat": input_data.get("lat"),
        "lon": input_data.get("lon"),
        "gender": _safe_text(input_data.get("gender") or "female").strip() or "female",
        "target_date": _safe_text(input_data.get("target_date")).strip(),
        "period": period,
        "day_boundary": "23:00" if day_change_at_23 else "00:00",
        "writing": writing,
    }


def _build_yaml_doc(payload: dict[str, Any]) -> str:
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=120)


def _serialize_transit_single(transit: dict[str, Any], *, target_date: str | None = None) -> dict[str, Any]:
    out = dict(transit)
    if target_date:
        out["target_date"] = target_date
    return out


def _build_transit_api_data(
    base_payload: dict[str, Any],
    *,
    target_date: datetime | None,
    period: str,
) -> dict[str, Any]:
    natal = calc_western_from_payload(base_payload)
    natal_planets = natal.get("planets", []) if isinstance(natal, dict) else []
    lat = float(base_payload.get("lat") or DEFAULT_LAT)
    lng = float(base_payload.get("lng") or base_payload.get("lon") or DEFAULT_LNG)
    target_date_str = target_date.astimezone(timezone.utc).date().isoformat() if target_date else ""

    if period == "day":
        snapshot = calc_transits_single(natal_planets, target_date=target_date, lat=lat, lng=lng)
        return {
            "period": "day",
            "target_date": target_date_str,
            "natal": natal,
            "snapshot": _serialize_transit_single(snapshot, target_date=target_date_str),
            "long_term": calc_transits_long_term(natal_planets, lat=lat, lng=lng),
        }

    snapshots: list[dict[str, Any]] = []
    start_date = target_date or datetime.now(timezone.utc)
    for day_offset in range(31):
        day_dt = start_date + timedelta(days=day_offset)
        snap = calc_transits_single(natal_planets, target_date=day_dt, lat=lat, lng=lng)
        snapshots.append(_serialize_transit_single(snap, target_date=day_dt.date().isoformat()))

    return {
        "period": "month",
        "target_date": target_date_str,
        "natal": natal,
        "daily": snapshots,
        "long_term": calc_transits_long_term(natal_planets, lat=lat, lng=lng),
    }


def _wrap_response(
    *,
    endpoint: str,
    input_data: dict[str, Any],
    raw_data: dict[str, Any],
    interpreted_tags: dict[str, Any],
    handoff_yaml: str,
    period: str | None = None,
) -> dict[str, Any]:
    writing = _extract_writing(input_data)
    response: dict[str, Any] = {
        "ok": True,
        "meta": {
            "api_version": API_VERSION,
            "engine": ENGINE_NAME,
            "endpoint": endpoint,
        },
        "input": _build_input_echo(input_data, day_change_at_23=input_data.get("_day_change_at_23", False), period=period or "day"),
        "raw_data": raw_data,
        "interpreted_tags": interpreted_tags,
        "writing_hints": {
            "tone": writing["tone"],
            "focus_areas": writing["focus_areas"],
            "key_concepts": _collect_key_concepts(interpreted_tags),
        },
        "ai_prompt_context": {
            "role": "構造分析型の占星術鑑定",
            "instruction": "raw_dataを直接断定せず、interpreted_tagsを主軸に鑑定文を作成してください。",
            "caution": [
                "運命断定を避ける",
                "不安を煽らない",
                "basisがあるタグを優先する",
                "strengthが高いタグを優先する",
            ],
        },
        "handoff_yaml": handoff_yaml,
    }
    return response


def _collect_key_concepts(interpreted_tags: dict[str, Any]) -> list[str]:
    concepts: list[str] = []
    for group in interpreted_tags.values():
        if not isinstance(group, list):
            continue
        for tag in group:
            if not isinstance(tag, dict):
                continue
            label = _safe_text(tag.get("label"))
            if label and label not in concepts:
                concepts.append(label)
    return concepts[:8]


def _build_handoff_yaml_from_response(response: dict[str, Any]) -> str:
    payload = {
        "version": "api-calc-v1",
        "meta": response.get("meta", {}),
        "input": response.get("input", {}),
        "raw_data": response.get("raw_data", {}),
        "interpreted_tags": response.get("interpreted_tags", {}),
        "writing_hints": response.get("writing_hints", {}),
    }
    try:
        return dumps_yaml(payload)
    except Exception:
        return _build_yaml_doc(payload)


def calc_western_api(input_data: dict[str, Any]) -> dict[str, Any]:
    payload, unknowns = _build_base_payload(input_data)
    western = calc_western_from_payload(payload)
    raw_data = {"western": western, "shichu": None, "transit": None}
    interpreted_tags = {
        "western": _build_western_tags(western),
        "shichu": [],
        "transit": [],
        "integration": [],
    }
    response = _wrap_response(
        endpoint="western",
        input_data={**input_data, "_day_change_at_23": False},
        raw_data=raw_data,
        interpreted_tags=interpreted_tags,
        handoff_yaml="",
        period="day",
    )
    response["input"]["unknowns"] = unknowns
    response["handoff_yaml"] = _build_handoff_yaml_from_response(response)
    return response


def calc_shichu_api(input_data: dict[str, Any]) -> dict[str, Any]:
    day_change_at_23 = _normalize_day_boundary(input_data.get("day_boundary"))
    payload, unknowns = _build_base_payload(input_data, day_change_at_23=day_change_at_23)
    shichu = calc_shichusuimei_from_payload(payload, day_change_at_23=day_change_at_23)
    raw_data = {"western": None, "shichu": shichu, "transit": None}
    interpreted_tags = {
        "western": [],
        "shichu": _build_shichu_tags(shichu),
        "transit": [],
        "integration": [],
    }
    response = _wrap_response(
        endpoint="shichu",
        input_data={**input_data, "_day_change_at_23": day_change_at_23},
        raw_data=raw_data,
        interpreted_tags=interpreted_tags,
        handoff_yaml="",
        period="day",
    )
    response["input"]["unknowns"] = unknowns
    response["handoff_yaml"] = _build_handoff_yaml_from_response(response)
    return response


def calc_transit_api(input_data: dict[str, Any]) -> dict[str, Any]:
    day_change_at_23 = _normalize_day_boundary(input_data.get("day_boundary"))
    period = _normalize_period(input_data.get("period"))
    payload, unknowns = _build_base_payload(input_data, day_change_at_23=day_change_at_23)
    target_date = _parse_dt(input_data.get("target_date"))
    transit = _build_transit_api_data(payload, target_date=target_date, period=period)
    raw_data = {
        "western": transit.get("natal"),
        "shichu": None,
        "transit": transit,
    }
    interpreted_tags = {
        "western": _build_western_tags(transit.get("natal") or {}),
        "shichu": [],
        "transit": _build_transit_tags(transit),
        "integration": [],
    }
    response = _wrap_response(
        endpoint="transit",
        input_data={**input_data, "_day_change_at_23": day_change_at_23},
        raw_data=raw_data,
        interpreted_tags=interpreted_tags,
        handoff_yaml="",
        period=period,
    )
    response["input"]["unknowns"] = unknowns
    response["handoff_yaml"] = _build_handoff_yaml_from_response(response)
    return response


def calc_combined_api(input_data: dict[str, Any]) -> dict[str, Any]:
    day_change_at_23 = _normalize_day_boundary(input_data.get("day_boundary"))
    period = _normalize_period(input_data.get("period"))
    payload, unknowns = _build_base_payload(input_data, day_change_at_23=day_change_at_23)
    western = calc_western_from_payload(payload)
    shichu = calc_shichusuimei_from_payload(payload, day_change_at_23=day_change_at_23)
    target_date = _parse_dt(input_data.get("target_date"))
    transit = _build_transit_api_data(payload, target_date=target_date, period=period)
    raw_data = {
        "western": western,
        "shichu": shichu,
        "transit": transit,
    }
    interpreted_tags = {
        "western": _build_western_tags(western),
        "shichu": _build_shichu_tags(shichu),
        "transit": _build_transit_tags(transit),
        "integration": _build_integration_tags(western, shichu, transit),
    }
    response = _wrap_response(
        endpoint="combined",
        input_data={**input_data, "_day_change_at_23": day_change_at_23},
        raw_data=raw_data,
        interpreted_tags=interpreted_tags,
        handoff_yaml="",
        period=period,
    )
    response["input"]["unknowns"] = unknowns
    response["handoff_yaml"] = _build_handoff_yaml_from_response(response)
    return response


def api_error(code: str, message: str, *, status_code: int = 400) -> dict[str, Any]:
    return {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
        "status_code": status_code,
    }
