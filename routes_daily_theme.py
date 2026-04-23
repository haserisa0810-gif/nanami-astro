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


REGION_POINTS = [
    {"key": "east", "label": "東日本", "city": "東京", "lat": 35.6762, "lng": 139.6503},
    {"key": "west", "label": "西日本", "city": "福岡", "lat": 33.5902, "lng": 130.4017},
]

HOUSE_THEME_LABELS = {
    1: "自分の出し方",
    2: "お金・価値観",
    3: "連絡・学び",
    4: "居場所・土台",
    5: "創造・恋愛",
    6: "仕事・整えること",
    7: "対人・パートナー",
    8: "深い共有・境界線",
    9: "遠くを見ること",
    10: "仕事・社会面",
    11: "仲間・広がり",
    12: "休息・内面整理",
}

SIGN_JA_SHORT = {
    "Ari": "牡羊座",
    "Tau": "牡牛座",
    "Gem": "双子座",
    "Can": "蟹座",
    "Leo": "獅子座",
    "Vir": "乙女座",
    "Lib": "天秤座",
    "Sco": "蠍座",
    "Sag": "射手座",
    "Cap": "山羊座",
    "Aqu": "水瓶座",
    "Pis": "魚座",
}


def _sign_label_ja(sign_raw: str) -> str:
    sign = str(sign_raw or "").strip()
    return SIGN_JA.get(sign, SIGN_JA_SHORT.get(sign, sign))


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


def _pick_post_style(target_date: str, axis: str) -> str:
    styles = ["theme", "relationship", "action", "astro", "short"]
    seed = sum(ord(c) for c in f"{target_date}:{axis}")
    return styles[seed % len(styles)]


def _normalize_sign_key(sign_raw: str) -> str:
    value = str(sign_raw or "").strip()
    if value in SIGN_JA:
        return value
    reverse_short = {k: v for k, v in SIGN_JA_SHORT.items()}
    if value in reverse_short:
        return reverse_short[value]
    if value in SIGN_JA.values():
        for k, v in SIGN_JA.items():
            if v == value:
                return k
    return value


def _sign_label_ja(sign_raw: str) -> str:
    key = _normalize_sign_key(sign_raw)
    return SIGN_JA.get(key, SIGN_JA_SHORT.get(str(sign_raw or "").strip(), str(sign_raw or "").strip()))


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




def _extract_sign_focus(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    weights = {
        "Sun": 3,
        "Moon": 4,
        "Mercury": 3,
        "Venus": 3,
        "Mars": 3,
        "Jupiter": 1,
        "Saturn": 1,
        "Uranus": 0,
        "Neptune": 0,
        "Pluto": 0,
    }

    sign_scores: dict[str, int] = {}
    sign_planets: dict[str, list[str]] = {}

    for p in snapshot.get("today_planets", []) or []:
        if not isinstance(p, dict):
            continue
        planet_name = str(p.get("name") or "").strip()
        sign_raw = _normalize_sign_key(str(p.get("sign") or "").strip())
        if not planet_name or not sign_raw:
            continue

        weight = weights.get(planet_name, 0)
        if weight <= 0:
            continue
        sign_scores[sign_raw] = sign_scores.get(sign_raw, 0) + weight
        sign_planets.setdefault(sign_raw, []).append(PLANET_JA.get(planet_name, planet_name))

    if not sign_scores:
        return None

    top_sign, top_score = max(sign_scores.items(), key=lambda x: x[1])
    planets = sign_planets.get(top_sign, [])
    has_luminary = ("太陽" in planets) or ("月" in planets)

    if top_score < 6 or not has_luminary:
        return None

    return {
        "sign_raw": top_sign,
        "sign_ja": _sign_label_ja(top_sign),
        "score": top_score,
        "planets": planets,
    }

def _basis_from_planet(snapshot: dict[str, Any], planet_name: str, axis: str) -> str:
    p = _find_planet(snapshot, planet_name)
    if not p:
        return ""
    sign_raw = str(p.get("sign") or "").strip()
    sign = _sign_label_ja(sign_raw)
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


def _pick_daily_theme_type(snapshot: dict[str, Any], target_date: str, axis: str) -> str:
    strong_aspects = _extract_strong_aspects(snapshot, max_items=3)
    sign_focus = _extract_sign_focus(snapshot)
    moon = _find_planet(snapshot, "Moon")
    mercury = _find_planet(snapshot, "Mercury")
    venus = _find_planet(snapshot, "Venus")
    mars = _find_planet(snapshot, "Mars")

    if strong_aspects:
        top = strong_aspects[0]
        try:
            orb = float(top.get("orb", 99))
        except Exception:
            orb = 99.0
        if orb <= 1.0:
            return "tight_aspect"

    if moon:
        moon_sign = _sign_label_ja(str(moon.get("sign") or "").strip())
        seed = sum(ord(c) for c in f"{target_date}:{moon_sign}:{axis}")
        return "moon_focus" if seed % 2 == 0 else "personal_planet"

    if mercury or venus or mars:
        return "personal_planet"
    if sign_focus:
        return "sign_focus"
    return "general"


def _summary_headline(summary: str) -> str:
    text = str(summary or "").strip()
    if not text:
        return "今日は、空気の整え方がそのまま流れを左右しやすい日。"
    parts = [p.strip() for p in text.replace("\n", "").split("。") if p.strip()]
    if not parts:
        return text
    first = parts[0]
    if first.endswith(("。", "日", "配置", "です", "ます")):
        return first.rstrip("。") + "。"
    return first + "。"


def _build_social_levels(*, target_date: str, summary: str, axis: str, astro_basis: list[str], strong_aspects: list[dict[str, Any]], sign_focus: dict[str, Any] | None = None, theme_type: str = "general") -> dict[str, str]:
    headline = _summary_headline(summary)
    axis_label = _axis_label(axis)
    post_style = _pick_post_style(target_date, axis)
    basis1 = astro_basis[0] if astro_basis else ""
    basis2 = astro_basis[1] if len(astro_basis) > 1 else ""
    sa = strong_aspects[0] if strong_aspects else None

    focus_line = f"今日は{sign_focus['sign_ja']}に天体が集中。" if sign_focus else ""
    moon = ""
    moon_obj = None
    # snapshot is not available here; derive from basis if possible is too weak, so keep optional only via summary/headline

    intro_candidates: dict[str, list[str]] = {
        "tight_aspect": [
            f"今日は{sa['planets']}が{sa['aspect']}（orb {sa['orb']:.2f}°）。" if sa else headline,
            headline,
        ],
        "moon_focus": [headline, focus_line or headline],
        "personal_planet": [headline, focus_line or headline],
        "sign_focus": [focus_line or headline, headline],
        "general": [headline, focus_line or headline],
    }
    intro = _pick_by_date([x for x in intro_candidates.get(theme_type, [headline]) if x], target_date + ":intro") or headline

    relation_line = f"言い方や距離感を整えるほど、{axis_label}のズレを減らしやすい日です。"
    action_line = f"今日は結果を急ぐより、順番と伝え方を整えるほうが{axis_label}が噛み合いやすくなります。"
    astro_line = basis1 or "今日は空気のまま反応せず、少し整えてから動くほうが流れを活かしやすい日です。"
    extra_line = basis2 or (f"{sa['meaning']}流れがあるので、勢いより調整力が効きやすくなります。" if sa else relation_line)

    short_candidates = [
        "\n".join([line for line in [intro, "流れが一方向に寄りやすい日。" if sign_focus else "空気の出方がそのまま結果に反映されやすい日。", action_line] if line]),
        "\n".join([line for line in [focus_line or intro, astro_line, "だからこそ、反応より調整を優先するとズレを減らしやすい。"] if line]),
    ]

    theme_candidates = [
        " ".join([line for line in [focus_line if theme_type == "sign_focus" else "", headline, action_line] if line]).strip(),
        " ".join([line for line in [intro, astro_line, relation_line] if line]).strip(),
    ]

    relationship_candidates = [
        " ".join([line for line in [intro, relation_line, extra_line] if line]).strip(),
        " ".join([line for line in [headline, "対人では温度差や認識差が出やすいので、言葉を一段やわらげるほど噛み合いやすくなります。", extra_line] if line]).strip(),
    ]

    action_candidates = [
        " ".join([line for line in [intro, action_line, extra_line] if line]).strip(),
        " ".join([line for line in [headline, "今日は一気に進めるより、目的と順番を整えてから動くほうが結果につながりやすい日です。", astro_line] if line]).strip(),
    ]

    astro_candidates = [
        " ".join([line for line in [intro, astro_line, relation_line] if line]).strip(),
        " ".join([line for line in [focus_line, f"{sa['planets']}の{sa['aspect']}がタイト。" if sa else astro_line, action_line] if line]).strip(),
    ]

    style_map = {
        "short": short_candidates,
        "theme": theme_candidates,
        "relationship": relationship_candidates,
        "action": action_candidates,
        "astro": astro_candidates,
    }

    standard_candidates = style_map.get(post_style, theme_candidates)
    light_candidates = [
        _truncate_text(_pick_by_date(short_candidates, target_date + ":short"), 120),
        _truncate_text(_pick_by_date(theme_candidates, target_date + ":theme"), 120),
    ]
    pro_candidates = [
        " ".join([line for line in [intro, astro_line, extra_line, relation_line] if line]).strip(),
        " ".join([line for line in [focus_line, headline, basis1, basis2, action_line] if line]).strip(),
    ]

    return {
        "light": _truncate_text(_pick_by_date(light_candidates, target_date + ":light"), 120),
        "standard": _truncate_text(_pick_by_date(standard_candidates, target_date + ":std"), 170),
        "pro": _truncate_text(_pick_by_date(pro_candidates, target_date + ":pro"), 190),
    }


def _jst_noon_utc(target_date_str: str) -> datetime:
    from datetime import timedelta
    jst = timezone(timedelta(hours=9))
    local_dt = datetime.strptime(target_date_str, "%Y-%m-%d").replace(hour=12, minute=0, second=0, microsecond=0, tzinfo=jst)
    return local_dt.astimezone(timezone.utc)


def _format_lon_as_sign_degree(lon: float | None) -> str:
    if lon is None:
        return "—"
    try:
        from services.western_calc import sign_of
        sign_short, deg = sign_of(float(lon))
        sign = SIGN_JA_SHORT.get(sign_short, sign_short)
        return f"{sign} {float(deg):.2f}°"
    except Exception:
        return "—"


def _house_focus_summary(axis: str, planet_house_map: dict[str, int | None]) -> str:
    axis_value = str(axis or "").strip().lower()
    focus_candidates = []
    if axis_value == "love":
        focus_candidates = [("Venus", "恋愛"), ("Moon", "感情"), ("Mars", "温度差")]
    elif axis_value == "work":
        focus_candidates = [("Mars", "動き方"), ("Mercury", "判断"), ("Saturn", "責任")]
    elif axis_value == "relationship":
        focus_candidates = [("Venus", "対人"), ("Moon", "反応"), ("Mercury", "言葉")]
    else:
        focus_candidates = [("Moon", "感情"), ("Mercury", "言葉"), ("Mars", "行動")]

    bits = []
    for planet_name, _ in focus_candidates:
        house_no = planet_house_map.get(planet_name)
        if isinstance(house_no, int):
            theme = HOUSE_THEME_LABELS.get(house_no, f"{house_no}ハウス")
            bits.append(theme)
    if not bits:
        return "今日は地域差が大きく出にくい配置です。"

    seen = []
    for b in bits:
        if b not in seen:
            seen.append(b)
    joined = " / ".join(seen[:2])
    return f"{joined}のテーマとして出やすい流れ。"



def _build_regional_sns_line(regional_houses: list[dict[str, Any]]) -> str:
    if not regional_houses or len(regional_houses) < 2:
        return ""
    east = regional_houses[0]
    west = regional_houses[1]
    east_text = str(east.get("focus_summary") or "").strip()
    west_text = str(west.get("focus_summary") or "").strip()

    def _shorten(text: str) -> str:
        return (
            text.replace("のテーマとして出やすい流れ。", "")
            .replace("今日は地域差が大きく出にくい配置です。", "大きな地域差は出にくい")
            .strip()
        )

    east_short = _shorten(east_text)
    west_short = _shorten(west_text)

    if east_short == west_short:
        return "地域差で見ると、東日本と西日本で大きな出方の違いは出にくい日です。"

    return f"地域差で見ると、東日本は{east_short}、西日本は{west_short}として出やすい日です。"


def _build_regional_houses(target_date_str: str, axis: str) -> list[dict[str, Any]]:
    try:
        import swisseph as swe
        from services.western_calc import PLANETS, configure_ephemeris, sign_of, house_of
    except Exception:
        return []

    utc_dt = _jst_noon_utc(target_date_str)
    ut = utc_dt.hour + utc_dt.minute / 60 + utc_dt.second / 3600
    jd = swe.julday(utc_dt.year, utc_dt.month, utc_dt.day, ut)

    engine_flag = configure_ephemeris()
    flags = engine_flag | swe.FLG_SPEED
    body_ids = {name: body_id for name, body_id in PLANETS}

    output: list[dict[str, Any]] = []
    for region in REGION_POINTS:
        try:
            cusps_raw, ascmc = swe.houses(jd, region["lat"], region["lng"], b"P")
            cusps = list(cusps_raw)[:12]
            asc = float(ascmc[0]) if ascmc else None
            mc = float(ascmc[1]) if ascmc else None
        except Exception:
            continue

        houses = []
        for idx, cusp in enumerate(cusps, start=1):
            try:
                sign_short, deg = sign_of(float(cusp))
                sign = SIGN_JA_SHORT.get(sign_short, sign_short)
                houses.append({
                    "house": idx,
                    "sign": sign,
                    "degree": round(float(deg), 2),
                    "label": HOUSE_THEME_LABELS.get(idx, f"{idx}ハウス"),
                })
            except Exception:
                houses.append({
                    "house": idx,
                    "sign": "",
                    "degree": 0.0,
                    "label": HOUSE_THEME_LABELS.get(idx, f"{idx}ハウス"),
                })

        planet_house_map: dict[str, int | None] = {}
        focus_planets = ["Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn"]
        focus_items = []
        for planet_name in focus_planets:
            body_id = body_ids.get(planet_name)
            if body_id is None:
                continue
            try:
                xx, _ = swe.calc_ut(jd, body_id, flags)
                lon = float(xx[0])
                house_no = house_of(lon, cusps)
                planet_house_map[planet_name] = house_no
                if house_no is not None:
                    focus_items.append({
                        "planet": PLANET_JA.get(planet_name, planet_name),
                        "house": int(house_no),
                        "theme": HOUSE_THEME_LABELS.get(int(house_no), f"{house_no}ハウス"),
                    })
            except Exception:
                planet_house_map[planet_name] = None

        output.append({
            "key": region["key"],
            "label": region["label"],
            "city": region["city"],
            "asc": _format_lon_as_sign_degree(asc),
            "mc": _format_lon_as_sign_degree(mc),
            "focus_summary": _house_focus_summary(axis, planet_house_map),
            "focus_planets": focus_items[:4],
            "houses": houses,
        })

    return output


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

    sign_focus = _extract_sign_focus(snapshot)
    theme_type = _pick_daily_theme_type(snapshot, target_date, axis)

    moon_obj = _find_planet(snapshot, "Moon")
    moon_sign = _sign_label_ja(str(moon_obj.get("sign") or "").strip()) if moon_obj else ""

    summary_parts: list[str] = []

    if theme_type == "sign_focus" and sign_focus:
        summary_parts.append(
            f"今日は{sign_focus['sign_ja']}に天体が集まりやすく、物事が一方向に動きやすい流れです。"
        )
    elif theme_type == "moon_focus" and moon_sign:
        summary_parts.append(
            f"今日は月が{moon_sign}にあり、感情の反応や安心できる距離感が空気を左右しやすい日です。"
        )
    elif theme_type == "tight_aspect" and aspects:
        pass

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
        sign_focus=sign_focus,
        theme_type=theme_type,
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
        "sign_focus": sign_focus,
        "theme_type": theme_type,
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

    sign_focus = merged.get("sign_focus")
    if isinstance(sign_focus, dict):
        sign_raw = str(sign_focus.get("sign_raw") or "").strip()
        sign_ja = str(sign_focus.get("sign_ja") or SIGN_JA.get(sign_raw, sign_raw)).strip()
        try:
            score = int(sign_focus.get("score", 0))
        except Exception:
            score = 0
        planets = sign_focus.get("planets")
        if not isinstance(planets, list):
            planets = []
        planets = [str(p).strip() for p in planets if str(p).strip()]
        if sign_ja and score > 0:
            merged["sign_focus"] = {
                "sign_raw": sign_raw,
                "sign_ja": sign_ja,
                "score": score,
                "planets": planets,
            }
        else:
            merged["sign_focus"] = None
    else:
        merged["sign_focus"] = None

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

        regional_houses = _build_regional_houses(transit_date, axis)

        fallback = _deterministic_theme_from_snapshot(
            snapshot=snapshot,
            target_date=transit_date,
            period=period,
            axis=axis,
        )
        regional_sns_line = _build_regional_sns_line(regional_houses)

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
        result_payload["regional_houses"] = regional_houses
        if regional_sns_line:
            result_payload["regional_sns_line"] = regional_sns_line
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
