from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")

SOFT_ACTION_LINE = "少し整える時間を先に確保すると、流れを立て直しやすくなります。"

DEFAULT_CAUTION = [
    "勢いだけで決めると、後から認識のズレや摩擦が表に出やすい。",
    "感情で反応した直後に結論を出すと、必要以上に話をこじらせやすい。",
]

DEFAULT_AVOID = [
    "思いつきだけで一気に話や作業を進めること。",
    "細部に詰まりすぎて、本来の流れや優先順位を見失うこと。",
]

PLANET_JA = {
    "Sun": "太陽",
    "Moon": "月",
    "Mercury": "水星",
    "Venus": "金星",
    "Mars": "火星",
    "Jupiter": "木星",
    "Saturn": "土星",
    "Uranus": "天王星",
    "Neptune": "海王星",
    "Pluto": "冥王星",
}

SIGN_JA = {
    "Aries": "牡羊座",
    "Taurus": "牡牛座",
    "Gemini": "双子座",
    "Cancer": "蟹座",
    "Leo": "獅子座",
    "Virgo": "乙女座",
    "Libra": "天秤座",
    "Scorpio": "蠍座",
    "Sagittarius": "射手座",
    "Capricorn": "山羊座",
    "Aquarius": "水瓶座",
    "Pisces": "魚座",
}

ASPECT_JA = {
    "conjunction": "コンジャンクション",
    "opposition": "オポジション",
    "square": "スクエア",
    "trine": "トライン",
    "sextile": "セクスタイル",
}

AXIS_LABEL = {
    "overall": "全体",
    "love": "恋愛",
    "work": "仕事",
    "relationship": "人間関係",
}


def _calc_helpers():
    from services.transit_calc import calc_global_transit_snapshot
    return calc_global_transit_snapshot


def _safe_prompt_render(template: str, values: dict[str, Any]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))
    return rendered


def _truncate_text(text: str, max_len: int) -> str:
    value = str(text or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "…"


def _clean_item(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("summary", "text", "label", "title", "name"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""

    text = str(value or "").strip()
    if not text:
        return ""

    if text.startswith("{") and text.endswith("}"):
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                for key in ("summary", "type_translation_axis", "social_post"):
                    val = str(obj.get(key) or "").strip()
                    if val:
                        return val
        except Exception:
            pass

    return text


def _coerce_list(value: Any, fallback_items: list[str], *, min_items: int = 2, max_items: int = 3) -> list[str]:
    items: list[str] = []

    if isinstance(value, list):
        seq = value
    elif isinstance(value, str) and value.strip():
        seq = [value]
    else:
        seq = []

    for item in seq:
        cleaned = _clean_item(item)
        if cleaned and cleaned not in items:
            items.append(cleaned)

    for fb in fallback_items:
        cleaned_fb = _clean_item(fb)
        if cleaned_fb and cleaned_fb not in items:
            items.append(cleaned_fb)
        if len(items) >= min_items:
            break

    return items[:max_items]


def _parse_jsonish_response(raw: str, fallback: dict[str, object]) -> dict[str, object]:
    import ast
    import re

    cleaned = (raw or "").strip()

    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    def _normalize(text: str) -> str:
        t = (text or "").strip()
        start_obj = t.find("{")
        end_obj = t.rfind("}")
        if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
            t = t[start_obj : end_obj + 1].strip()
        t = t.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
        return t

    def _load_once(text: str):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, str):
                try:
                    parsed2 = json.loads(_normalize(parsed))
                    if isinstance(parsed2, dict):
                        return parsed2
                except Exception:
                    return None
        except Exception:
            return None
        return None

    norm = _normalize(cleaned)
    parsed = _load_once(norm)
    if parsed:
        return parsed

    try:
        lit = ast.literal_eval(norm)
        if isinstance(lit, dict):
            return lit
    except Exception:
        pass

    m = re.search(r'"summary"\s*:\s*"(.+?)"', norm, flags=re.DOTALL)
    if m:
        merged = dict(fallback)
        merged["summary"] = m.group(1).replace('\\"', '"').strip()
        merged["raw_text"] = raw or ""
        return merged

    merged = dict(fallback)
    merged["raw_text"] = raw or ""
    return merged


def _build_planet_summary(snapshot: dict[str, Any]) -> str:
    lines = []
    for p in snapshot.get("today_planets", [])[:10]:
        if not isinstance(p, dict):
            continue
        name = PLANET_JA.get(str(p.get("name") or "").strip(), str(p.get("name") or "").strip())
        sign = SIGN_JA.get(str(p.get("sign") or "").strip(), str(p.get("sign") or "").strip())
        degree = p.get("degree", 0)
        try:
            degree_text = f"{float(degree):.2f}°"
        except Exception:
            degree_text = str(degree)
        if name:
            lines.append(f"- {name}: {sign} {degree_text}".strip())
    return "\n".join(lines) if lines else "（データなし）"


def _build_aspect_summary(snapshot: dict[str, Any]) -> str:
    lines = []
    for a in snapshot.get("aspects", [])[:10]:
        if not isinstance(a, dict):
            continue
        pa = PLANET_JA.get(str(a.get("planet_a") or "").strip(), str(a.get("planet_a") or "").strip())
        pb = PLANET_JA.get(str(a.get("planet_b") or "").strip(), str(a.get("planet_b") or "").strip())
        aspect = ASPECT_JA.get(str(a.get("aspect") or "").strip(), str(a.get("aspect") or "").strip())
        orb = a.get("orb", 0)
        try:
            orb_text = f"{float(orb):.2f}°"
        except Exception:
            orb_text = str(orb)
        if pa and pb and aspect:
            lines.append(f"- {pa} {aspect} {pb} / orb {orb_text}")
    return "\n".join(lines) if lines else "（主要アスペクトなし）"


def _find_planet(snapshot: dict[str, Any], planet_name: str) -> dict[str, Any] | None:
    for p in snapshot.get("today_planets", []) or []:
        if isinstance(p, dict) and str(p.get("name") or "").strip() == planet_name:
            return p
    return None


def _axis_label(axis: str) -> str:
    return AXIS_LABEL.get(str(axis or "").strip().lower(), "全体")


def _pick_by_date(candidates: list[str], target_date: str) -> str:
    if not candidates:
        return ""
    seed = sum(ord(c) for c in (target_date or ""))
    return candidates[seed % len(candidates)]


def _aspect_meaning(aspect: str, planet_a: str, planet_b: str) -> str:
    pair = {planet_a, planet_b}
    if aspect == "square":
        if pair == {"Mars", "Saturn"}:
            return "動きたい気持ちと現実的な制約がぶつかりやすい"
        if pair == {"Mercury", "Neptune"}:
            return "言葉と感覚がずれやすく、誤解が増えやすい"
        return "力の向きが揃いにくく、摩擦やズレが出やすい"
    if aspect == "opposition":
        return "自分側と相手側の温度差や引っ張り合いが出やすい"
    if aspect == "trine":
        return "流れが自然につながりやすく、活かしやすい"
    if aspect == "sextile":
        return "少し意識するだけで追い風に変えやすい"
    if aspect == "conjunction":
        return "テーマが一点に集まり、体感が強く出やすい"
    return "空気の特徴を強めやすい"


def _extract_strong_aspects(snapshot: dict[str, Any], *, max_items: int = 2) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for a in snapshot.get("aspects", []) or []:
        if not isinstance(a, dict):
            continue
        aspect = str(a.get("aspect") or "").strip()
        if aspect not in ASPECT_JA:
            continue
        try:
            orb = float(a.get("orb", 99))
        except Exception:
            orb = 99.0
        if orb > 3.0:
            continue
        pa_raw = str(a.get("planet_a") or "").strip()
        pb_raw = str(a.get("planet_b") or "").strip()
        if not pa_raw or not pb_raw:
            continue
        rows.append(
            {
                "planets": f"{PLANET_JA.get(pa_raw, pa_raw)} × {PLANET_JA.get(pb_raw, pb_raw)}",
                "aspect": ASPECT_JA.get(aspect, aspect),
                "orb": round(orb, 2),
                "meaning": _aspect_meaning(aspect, pa_raw, pb_raw),
                "raw_aspect": aspect,
            }
        )
    rows.sort(key=lambda x: x["orb"])
    return rows[:max_items]


def _basis_from_planet(snapshot: dict[str, Any], planet_name: str, axis: str) -> str:
    p = _find_planet(snapshot, planet_name)
    if not p:
        return ""
    sign_raw = str(p.get("sign") or "").strip()
    sign = SIGN_JA.get(sign_raw, sign_raw)
    planet = PLANET_JA.get(planet_name, planet_name)
    axis_value = str(axis or "").strip().lower()

    if planet_name == "Mercury":
        if axis_value == "work":
            return f"{planet}が{sign}にあり、連絡や判断のテンポが仕事の進め方にそのまま出やすい配置。"
        return f"{planet}が{sign}にあり、言葉の選び方や認識のズレが今日の空気を左右しやすい配置。"
    if planet_name == "Venus":
        if axis_value in {"love", "relationship"}:
            return f"{planet}が{sign}にあり、対人の温度感や好意の伝え方が関係の流れを整えやすい配置。"
        return f"{planet}が{sign}にあり、柔らかい伝え方や心地よい距離感が空気を整えやすい配置。"
    if planet_name == "Mars":
        if axis_value == "work":
            return f"{planet}が{sign}にあり、行動の速さや進め方の癖が作業効率に強く出やすい配置。"
        return f"{planet}が{sign}にあり、動きたい気持ちや勢いの出方がそのまま表面化しやすい配置。"
    if planet_name == "Jupiter":
        return f"{planet}が{sign}にあり、少し広げる・信じてみる動きが追い風に変わりやすい配置。"
    if planet_name == "Saturn":
        return f"{planet}が{sign}にあり、曖昧なまま進めず現実的な線引きや責任整理が求められやすい配置。"
    if planet_name == "Moon":
        return f"{planet}が{sign}にあり、感情の反応や安心できる距離感が空気のベースを決めやすい配置。"
    if planet_name == "Sun":
        return f"{planet}が{sign}にあり、今どこへ力を向けたいかという全体の方向性が見えやすい配置。"
    return ""


def _build_astro_basis(snapshot: dict[str, Any], axis: str) -> list[str]:
    basis: list[str] = []
    strong_aspects = _extract_strong_aspects(snapshot, max_items=2)
    for item in strong_aspects:
        basis.append(
            f"{item['planets']}が{item['aspect']}（orb {item['orb']:.2f}°）で、{item['meaning']}。"
        )

    order = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn"]
    for planet_name in order:
        line = _basis_from_planet(snapshot, planet_name, axis)
        if line and line not in basis:
            basis.append(line)
        if len(basis) >= 4:
            break

    return basis[:4]


def _summary_headline(summary: str) -> str:
    text = str(summary or "").strip()
    if not text:
        return "今日は、空気の整え方がそのまま流れを左右しやすい日。"
    parts = [p.strip() for p in text.replace("\n", "").split("。") if p.strip()]
    if not parts:
        return text
    first = parts[0]
    if not first.endswith("日") and not first.endswith("配置"):
        first += "日。"
    else:
        first += "。"
    return first


def _build_social_levels(*, target_date: str, summary: str, axis: str, astro_basis: list[str], strong_aspects: list[dict[str, Any]]) -> dict[str, str]:
    headline = _summary_headline(summary)
    basis1 = astro_basis[0] if astro_basis else ""
    basis2 = astro_basis[1] if len(astro_basis) > 1 else ""
    axis_label = _axis_label(axis)

    light_candidates = [
        f"{headline}急ぐより、言葉や段取りを少し整えてから動くと{axis_label}の流れが安定しやすくなります。",
        f"{headline}勢いで決め切るより、一度落ち着いて優先順位を整えるほうが{axis_label}では噛み合いやすくなります。",
    ]

    if basis1:
        standard_candidates = [
            f"{headline}{basis1} だからこそ、今日は空気のまま反応するより、ひと呼吸おいて整えてから動くほうが{axis_label}を活かしやすい流れです。",
            f"{headline}{basis1} が背景にあるので、今日は結果を急ぐよりも伝え方や距離感を調整したほうが{axis_label}のズレを減らしやすくなります。",
        ]
    else:
        standard_candidates = light_candidates

    if strong_aspects:
        sa = strong_aspects[0]
        pro_candidates = [
            f"{sa['planets']}が{sa['aspect']}（orb {sa['orb']:.2f}°）。{sa['meaning']}ので、今日は勢い任せより調整力を使うほど{axis_label}が整いやすい日。",
            f"今日は{sa['planets']}の{sa['aspect']}がタイトで、{sa['meaning']}流れ。{basis2 or basis1 or '反応をそのまま出さず整えてから動く'}ことが{axis_label}の鍵になります。",
        ]
    elif basis1:
        pro_candidates = [
            f"今日は{basis1} {basis2 or ''} 占星術的には、空気を読むだけでなく伝え方を設計するほど{axis_label}が動きやすい日です。",
            f"{basis1} {basis2 or ''} そのため今日は、感覚で進むよりも意図して整えることが{axis_label}の安定につながりやすくなります。",
        ]
    else:
        pro_candidates = standard_candidates

    return {
        "light": _truncate_text(_pick_by_date(light_candidates, target_date), 120),
        "standard": _truncate_text(_pick_by_date(standard_candidates, target_date + "std"), 170),
        "pro": _truncate_text(_pick_by_date(pro_candidates, target_date + "pro"), 190),
    }


def _deterministic_theme_from_snapshot(
    *,
    snapshot: dict[str, Any],
    target_date: str,
    period: str,
    axis: str,
) -> dict[str, Any]:
    today_planets = snapshot.get("today_planets") or []
    aspects = snapshot.get("aspects") or []

    planet_names = [str(p.get("name") or "") for p in today_planets if isinstance(p, dict) and p.get("name")]
    aspect_names = [str(a.get("aspect") or "") for a in aspects if isinstance(a, dict)]

    has_moon = "Moon" in planet_names
    has_mercury = "Mercury" in planet_names
    has_venus = "Venus" in planet_names
    has_mars = "Mars" in planet_names
    has_jupiter = "Jupiter" in planet_names
    has_saturn = "Saturn" in planet_names

    has_square = "square" in aspect_names
    has_opposition = "opposition" in aspect_names
    has_trine = "trine" in aspect_names
    has_sextile = "sextile" in aspect_names
    has_conjunction = "conjunction" in aspect_names

    summary_parts: list[str] = []

    if has_square or has_opposition:
        summary_parts.append(
            "動きたい判断と慎重に整えたい感覚がぶつかりやすく、会話や段取りの場面で小さなズレが表に出やすい流れです。"
        )
    else:
        summary_parts.append(
            "全体としては流れを整えながら進めやすく、話し合いや作業の順番を少し調整するだけで噛み合いやすくなる流れです。"
        )

    if has_mercury and has_moon:
        summary_parts.append(
            "感情の反応が言葉に乗りやすいため、言い方や受け取り方ひとつで空気が大きく変わりやすい点が特徴です。"
        )
    elif has_mercury:
        summary_parts.append(
            "判断や連絡のテンポが早まりやすいぶん、説明不足のまま進めると認識差が残りやすくなります。"
        )
    elif has_moon:
        summary_parts.append(
            "気分や反応が先に立ちやすいため、結論より先に落ち着きを作ることが流れを整える鍵になります。"
        )

    if has_saturn and has_mars:
        summary_parts.append(
            "進めたい気持ちに対して現実的な制約や責任感がかかりやすく、勢いだけでは前に出にくい一方で、丁寧な再設計には向く時です。"
        )
    elif has_jupiter and (has_trine or has_sextile):
        summary_parts.append(
            "広げたい意欲を前向きに使いやすく、見通しを立てて共有すると周囲の協力も得やすくなります。"
        )

    summary = _truncate_text("".join(summary_parts), 220)

    core: list[str] = []
    push: list[str] = []
    caution: list[str] = []
    recommended: list[str] = []
    avoid: list[str] = []

    if has_square or has_opposition:
        core.append("勢いと慎重さのズレを整える")
        caution.append("正しさを急いでぶつけると、認識差が摩擦として残りやすい。")
        avoid.append("結論を急いで相手の反応を置き去りにすること。")

    if has_moon:
        core.append("感情の反応を先に落ち着かせる")
        caution.append("気分の波をそのまま判断に乗せると、必要以上に話が揺れやすい。")
        recommended.append("反応する前に、言葉と気持ちを一度切り分けて整理する。")

    if has_mercury:
        core.append("言葉の精度が空気を左右する")
        push.append("短く整理した説明ほど通りやすく、無駄な行き違いを減らしやすい。")
        recommended.append("連絡や説明は一度要点を絞ってから出し、確認の一文を添える。")

    if has_venus:
        push.append("柔らかい言い回しや配慮が関係調整にそのまま効きやすい。")
        recommended.append("意見を通す前に、相手が受け取りやすい形へ一段やわらげる。")

    if has_mars:
        push.append("手を動かしながら整える作業は進めやすく、停滞を崩すきっかけになりやすい。")
        caution.append("焦りが先に立つと、必要な確認を飛ばして後戻りが増えやすい。")
        avoid.append("思いつきの勢いで一気に着地まで持っていくこと。")

    if has_saturn:
        core.append("現実的な優先順位を引き直す")
        push.append("責任範囲や順番を整理すると、停滞していたことに筋道をつけやすい。")
        recommended.append("今やることと保留にすることを分け、負荷の置き場を明確にする。")

    if has_trine or has_sextile:
        push.append("小さな調整が全体の流れを整えやすく、協力や理解を得やすい。")

    if has_conjunction:
        core.append("テーマを一点に集めて扱う")
        caution.append("一点集中が強すぎると、周辺への目配りが抜けやすい。")

    if len(core) < 2:
        core.extend([
            "ズレを早めに微調整する",
            "感情と判断の間に余白を作る",
        ])

    if len(push) < 2:
        push.extend([
            "段取りを少し直すだけでも、結果と対話の噛み合いが良くなりやすい。",
            "急がず整える姿勢が、そのまま安定感として伝わりやすい。",
        ])

    if len(caution) < 2:
        caution.extend(DEFAULT_CAUTION)

    if len(recommended) < 2:
        recommended.extend([
            "一気に進める前に、目的・順番・伝え方の三点を整え直す。",
            SOFT_ACTION_LINE,
        ])

    if len(avoid) < 1:
        avoid.extend(DEFAULT_AVOID)

    astro_basis = _build_astro_basis(snapshot, axis)
    strong_aspects = _extract_strong_aspects(snapshot, max_items=2)
    social_levels = _build_social_levels(
        target_date=target_date,
        summary=summary,
        axis=axis,
        astro_basis=astro_basis,
        strong_aspects=strong_aspects,
    )

    axis_text = (
        "勢いの強さを見るより、反応の出し方・言葉の精度・調整力をどう使うかを見ると、"
        "タイプ差を出しやすい日です。"
    )

    return {
        "date": target_date,
        "period": period,
        "axis": axis,
        "summary": summary or "今日は、流れを急がず整えるほど全体が噛み合いやすくなる日です。",
        "core_themes": core[:3],
        "push": push[:3],
        "caution": caution[:3],
        "recommended_actions": recommended[:3],
        "avoid_actions": avoid[:2],
        "social_post": social_levels["standard"],
        "social_levels": social_levels,
        "astro_basis": astro_basis,
        "strong_aspects": strong_aspects,
        "type_translation_axis": _truncate_text(axis_text, 85),
        "source": "deterministic_fallback",
    }


def _normalize_daily_theme_result(result: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(fallback)
    if isinstance(result, dict):
        merged.update(result)

    merged["date"] = str(merged.get("date") or fallback["date"]).strip()
    merged["period"] = str(merged.get("period") or fallback["period"]).strip()
    merged["axis"] = str(merged.get("axis") or fallback["axis"]).strip()

    summary = _clean_item(merged.get("summary")) or fallback["summary"]
    type_axis = _clean_item(merged.get("type_translation_axis")) or fallback["type_translation_axis"]

    merged["summary"] = _truncate_text(summary, 220)
    merged["type_translation_axis"] = _truncate_text(type_axis, 85)

    merged["core_themes"] = _coerce_list(
        merged.get("core_themes"),
        fallback.get("core_themes", []),
        min_items=2,
        max_items=3,
    )
    merged["push"] = _coerce_list(
        merged.get("push"),
        fallback.get("push", []),
        min_items=2,
        max_items=3,
    )
    merged["caution"] = _coerce_list(
        merged.get("caution"),
        fallback.get("caution", DEFAULT_CAUTION),
        min_items=2,
        max_items=3,
    )
    merged["recommended_actions"] = _coerce_list(
        merged.get("recommended_actions"),
        fallback.get("recommended_actions", [SOFT_ACTION_LINE]),
        min_items=2,
        max_items=3,
    )
    merged["avoid_actions"] = _coerce_list(
        merged.get("avoid_actions"),
        fallback.get("avoid_actions", DEFAULT_AVOID),
        min_items=1,
        max_items=2,
    )

    if SOFT_ACTION_LINE not in merged["recommended_actions"]:
        merged["recommended_actions"].append(SOFT_ACTION_LINE)
        merged["recommended_actions"] = merged["recommended_actions"][:3]

    merged["astro_basis"] = _coerce_list(
        merged.get("astro_basis"),
        fallback.get("astro_basis", []),
        min_items=2,
        max_items=4,
    )

    strong_aspects = merged.get("strong_aspects")
    if not isinstance(strong_aspects, list):
        strong_aspects = fallback.get("strong_aspects", [])
    cleaned_aspects = []
    for item in strong_aspects or []:
        if not isinstance(item, dict):
            continue
        planets = str(item.get("planets") or "").strip()
        aspect = str(item.get("aspect") or "").strip()
        meaning = str(item.get("meaning") or "").strip()
        orb = item.get("orb", 0)
        try:
            orb = round(float(orb), 2)
        except Exception:
            orb = 0.0
        if planets and aspect:
            cleaned_aspects.append(
                {"planets": planets, "aspect": aspect, "orb": orb, "meaning": meaning}
            )
    merged["strong_aspects"] = cleaned_aspects[:2]

    fallback_social = fallback.get("social_levels", {}) if isinstance(fallback.get("social_levels"), dict) else {}
    social_levels = merged.get("social_levels")
    if not isinstance(social_levels, dict):
        social_levels = {}
    normalized_social = {
        "light": _truncate_text(
            _clean_item(social_levels.get("light")) or str(fallback_social.get("light") or ""),
            120,
        ),
        "standard": _truncate_text(
            _clean_item(social_levels.get("standard")) or _clean_item(merged.get("social_post")) or str(fallback_social.get("standard") or ""),
            170,
        ),
        "pro": _truncate_text(
            _clean_item(social_levels.get("pro")) or str(fallback_social.get("pro") or ""),
            190,
        ),
    }
    if not normalized_social["light"]:
        normalized_social["light"] = normalized_social["standard"]
    if not normalized_social["pro"]:
        normalized_social["pro"] = normalized_social["standard"]
    merged["social_levels"] = normalized_social
    merged["social_post"] = normalized_social["standard"]

    return merged


def _theme_quality_score(result: dict[str, Any]) -> int:
    score = 0
    summary = str(result.get("summary") or "").strip()
    basis = result.get("astro_basis") if isinstance(result.get("astro_basis"), list) else []
    social_levels = result.get("social_levels") if isinstance(result.get("social_levels"), dict) else {}

    if len(summary) >= 100:
        score += 2
    if len(basis) >= 2:
        score += 2
    if len(str(social_levels.get("standard") or "")) >= 90:
        score += 2
    if len(str(social_levels.get("pro") or "")) >= 90:
        score += 1
    return score


def _is_strong_theme(result: dict[str, Any]) -> bool:
    summary = str(result.get("summary") or "").strip()
    core = result.get("core_themes") if isinstance(result.get("core_themes"), list) else []
    push = result.get("push") if isinstance(result.get("push"), list) else []
    caution = result.get("caution") if isinstance(result.get("caution"), list) else []
    recommended = result.get("recommended_actions") if isinstance(result.get("recommended_actions"), list) else []
    avoid = result.get("avoid_actions") if isinstance(result.get("avoid_actions"), list) else []
    astro_basis = result.get("astro_basis") if isinstance(result.get("astro_basis"), list) else []
    social_levels = result.get("social_levels") if isinstance(result.get("social_levels"), dict) else {}

    if len(summary) < 100:
        return False
    if len(core) < 2 or len(push) < 2 or len(caution) < 2 or len(recommended) < 2 or len(avoid) < 1:
        return False
    if len(astro_basis) < 2:
        return False
    if len(str(social_levels.get("standard") or "")) < 90:
        return False
    return _theme_quality_score(result) >= 6


def _call_llm_json(prompt: str, *, max_output_tokens: int = 1100) -> str:
    try:
        from google import genai
        from google.genai import types
    except Exception:
        return ""

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return ""

    client = genai.Client(api_key=api_key)
    model = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()

    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.35,
            top_p=0.9,
            max_output_tokens=max_output_tokens,
            response_mime_type="application/json",
        ),
    )
    text = getattr(resp, "text", None)
    return (text or "").strip()


def _build_retry_prompt(base_prompt: str, weak_result: dict[str, Any]) -> str:
    weak_json = json.dumps(weak_result, ensure_ascii=False, indent=2)
    return (
        base_prompt
        + "\n\n# 再生成指示（品質不足のため修正）\n"
        + "前回の出力は浅く、具体性が不足しています。\n"
        + "以下の弱い出力をそのまま繰り返さず、次の点を改善して再生成してください。\n"
        + "・summary に場面、ズレ、原因を明確に入れる\n"
        + "・push / caution / recommended_actions を具体化する\n"
        + "・占星術の根拠になる basis を2件以上入れる\n"
        + "・JSON schema は厳守する\n\n"
        + "# 弱かった前回出力\n"
        + weak_json
    )


@router.get("/daily-theme", response_class=HTMLResponse)
def daily_theme_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="daily_theme.html",
        context={
            "request": request,
            "initial_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        },
    )


@router.post("/daily-theme/generate", response_class=JSONResponse)
async def daily_theme_generate(request: Request):
    try:
        calc_global_transit_snapshot = _calc_helpers()

        body = await request.json()
        date_str = str(body.get("date") or "").strip()
        period = str(body.get("period") or "daily").strip().lower()
        axis = str(body.get("axis") or "overall").strip().lower()
        lat = float(body.get("lat", 35.6895))
        lng = float(body.get("lng", 139.6917))

        target_date = None
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(status_code=400, detail="date は YYYY-MM-DD 形式で指定してください")

        snapshot = calc_global_transit_snapshot(target_date=target_date, lat=lat, lng=lng)

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl = (prompts_dir / "daily_theme.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")

        planet_summary = _build_planet_summary(snapshot)
        aspect_summary = _build_aspect_summary(snapshot)

        transit_date = str(snapshot.get("transit_date") or date_str or "").strip()

        prompt = _safe_prompt_render(
            tpl,
            {
                "common_rules": common_rules,
                "target_date": transit_date,
                "period": period,
                "axis": axis,
                "planet_summary": planet_summary,
                "aspect_summary": aspect_summary,
            },
        )

        fallback = _deterministic_theme_from_snapshot(
            snapshot=snapshot,
            target_date=transit_date,
            period=period,
            axis=axis,
        )

        raw = _call_llm_json(prompt, max_output_tokens=1100)
        parsed = _parse_jsonish_response(raw, fallback) if raw else fallback
        normalized = _normalize_daily_theme_result(parsed, fallback)

        if not _is_strong_theme(normalized):
            retry_prompt = _build_retry_prompt(prompt, normalized)
            raw_retry = _call_llm_json(retry_prompt, max_output_tokens=1200)
            retry_parsed = _parse_jsonish_response(raw_retry, fallback) if raw_retry else fallback
            retry_normalized = _normalize_daily_theme_result(retry_parsed, fallback)

            if _theme_quality_score(retry_normalized) >= _theme_quality_score(normalized):
                normalized = retry_normalized

        if not _is_strong_theme(normalized):
            normalized = _normalize_daily_theme_result(fallback, fallback)

        result_payload = dict(normalized)
        result_payload["transit_source"] = {
            "transit_date": transit_date,
            "planet_summary": planet_summary,
            "aspect_summary": aspect_summary,
        }
        return JSONResponse(content=result_payload)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/daily-theme/generate-types", response_class=JSONResponse)
async def daily_theme_generate_types(request: Request):
    try:
        body = await request.json()
        base_theme = body.get("theme") or {}
        date_str = str(body.get("date") or base_theme.get("date") or "").strip()
        period = str(body.get("period") or base_theme.get("period") or "daily").strip().lower()
        axis = str(body.get("axis") or base_theme.get("axis") or "overall").strip().lower()
        raw_type_lines = body.get("type_lines") or []

        if isinstance(raw_type_lines, str):
            raw_type_lines = [raw_type_lines]
        if not isinstance(raw_type_lines, list):
            raise HTTPException(status_code=400, detail="type_lines は配列で指定してください")

        parsed_types: list[dict[str, str]] = []
        for line in raw_type_lines:
            if not isinstance(line, str):
                continue
            cleaned = line.strip()
            if not cleaned:
                continue
            if ":" in cleaned:
                name, hint = cleaned.split(":", 1)
            elif "：" in cleaned:
                name, hint = cleaned.split("：", 1)
            else:
                name, hint = cleaned, ""
            name = name.strip()
            hint = hint.strip()
            if name:
                parsed_types.append({"type_name": name, "type_hint": hint})

        if not parsed_types:
            raise HTTPException(status_code=400, detail="タイプを1件以上入力してください")

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl_path = prompts_dir / "daily_type_forecast.txt"

        if not tpl_path.exists():
            items = []
            base_summary = str(base_theme.get("summary") or "").strip() or "今日は流れを整えるほど動きやすい日です。"
            base_social = (
                (base_theme.get("social_levels") or {}).get("standard")
                or str(base_theme.get("social_post") or "").strip()
            )
            for t in parsed_types:
                items.append({
                    "type_name": t["type_name"],
                    "type_hint": t["type_hint"],
                    "summary": base_summary,
                    "flow": "",
                    "likely_things": [],
                    "caution": [],
                    "advice": [SOFT_ACTION_LINE],
                    "social_post": base_social,
                })
            return JSONResponse(content={
                "date": date_str,
                "period": period,
                "axis": axis,
                "items": items,
                "source": "simple_fallback",
            })

        tpl = tpl_path.read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")

        prompt = _safe_prompt_render(
            tpl,
            {
                "common_rules": common_rules,
                "target_date": date_str,
                "period": period,
                "axis": axis,
                "base_theme_json": json.dumps(base_theme, ensure_ascii=False, indent=2),
                "type_lines": json.dumps(parsed_types, ensure_ascii=False, indent=2),
            },
        )

        raw = _call_llm_json(prompt, max_output_tokens=1400)
        result: dict[str, Any] = {}

        if raw:
            try:
                result = json.loads(raw)
                if not isinstance(result, dict):
                    result = {}
            except Exception:
                result = {}

        if not result:
            items = []
            base_summary = str(base_theme.get("summary") or "").strip() or "今日は流れを整えるほど動きやすい日です。"
            base_social = (
                (base_theme.get("social_levels") or {}).get("standard")
                or str(base_theme.get("social_post") or "").strip()
            )
            for t in parsed_types:
                items.append({
                    "type_name": t["type_name"],
                    "type_hint": t["type_hint"],
                    "summary": base_summary,
                    "flow": "",
                    "likely_things": [],
                    "caution": [],
                    "advice": [SOFT_ACTION_LINE],
                    "social_post": base_social,
                })
            result = {
                "date": date_str,
                "period": period,
                "axis": axis,
                "items": items,
                "source": "simple_fallback",
            }

        result.setdefault("date", date_str)
        result.setdefault("period", period)
        result.setdefault("axis", axis)
        if not isinstance(result.get("items"), list):
            result["items"] = []

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})
