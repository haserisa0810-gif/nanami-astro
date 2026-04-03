from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from services.astro_hint_builder import build_astro_hint_line

try:
    from services.text_formatter import format_ai_text as fix_punctuation
except Exception:
    def fix_punctuation(text: str) -> str:
        return text

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None  # type: ignore
    types = None  # type: ignore


DEFAULT_MODEL_NAME = "gemini-2.5-flash"
ALLOWED_MODEL_NAMES = {
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
}


def _normalize_requested_model(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    aliases = {
        "flash-lite": "gemini-2.5-flash-lite",
        "lite": "gemini-2.5-flash-lite",
        "flash": "gemini-2.5-flash",
        "pro": "gemini-2.5-pro",
    }
    normalized = aliases.get(raw, raw)
    if normalized in ALLOWED_MODEL_NAMES:
        return normalized
    return None


def _resolve_model_name(requested_model: Any = None) -> tuple[str, str]:
    requested = _normalize_requested_model(requested_model)
    if requested:
        return requested, "request"

    raw = _normalize_requested_model(os.getenv("GEMINI_MODEL"))
    if raw:
        return raw, "env"
    return DEFAULT_MODEL_NAME, "default"


MODEL_NAME, MODEL_SOURCE = _resolve_model_name()

_PROMPTS_DIR = (Path(__file__).resolve().parents[1] / "prompts").resolve()


def _read_prompt_file(name: str) -> str:
    p = (_PROMPTS_DIR / name).resolve()
    if _PROMPTS_DIR not in p.parents and p != _PROMPTS_DIR:
        raise ValueError("Invalid prompt path")
    if not p.exists():
        raise FileNotFoundError(f"Prompt template not found: {p}")
    return p.read_text(encoding="utf-8")


def _render_prompt(template: str, ctx: dict[str, Any]) -> str:
    class _D(dict):
        def __missing__(self, key: str) -> str:
            return ""

    return template.format_map(_D(ctx)).strip()


def _extract_text(resp: Any) -> str:
    if resp is None:
        return ""

    t = getattr(resp, "text", None)
    if isinstance(t, str) and t.strip():
        return t

    candidates = getattr(resp, "candidates", None)
    if isinstance(candidates, list) and candidates:
        c0 = candidates[0]
        content = getattr(c0, "content", None)
        parts = getattr(content, "parts", None)
        if isinstance(parts, list) and parts:
            buf: list[str] = []
            for p in parts:
                pt = getattr(p, "text", None)
                if isinstance(pt, str) and pt:
                    buf.append(pt)
            if buf:
                return "\n".join(buf)

    if isinstance(resp, dict):
        if isinstance(resp.get("text"), str):
            return resp["text"]
        try:
            cands = resp.get("candidates") or []
            if cands:
                parts = (((cands[0] or {}).get("content") or {}).get("parts") or [])
                buf = [p.get("text", "") for p in parts if isinstance(p, dict)]
                buf = [x for x in buf if isinstance(x, str) and x.strip()]
                if buf:
                    return "\n".join(buf)
        except Exception:
            pass

    return ""


def _safe_get_meta(astro_data: Any) -> dict[str, Any]:
    if not isinstance(astro_data, dict):
        return {}

    m = astro_data.get("_meta")
    if isinstance(m, dict) and m:
        return dict(m)

    m2 = astro_data.get("meta")
    if isinstance(m2, dict) and m2:
        return dict(m2)

    return {}


def _merge_meta(base: dict[str, Any], extra: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(base)
    if isinstance(extra, dict) and extra:
        out.update(extra)
    return out


def _limit_text(value: Any, max_chars: int = 2500) -> str:
    s = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + "\n...(truncated)"


def _extract_planets(astro_data: dict[str, Any]) -> list[dict[str, Any]]:
    def norm_planet(p: Any) -> dict[str, Any] | None:
        if not isinstance(p, dict):
            return None
        name = (p.get("name") or p.get("id") or p.get("planet") or "").strip()
        if not name:
            return None
        lon = p.get("abs_pos")
        if lon is None:
            lon = p.get("lon")
        if lon is None:
            lon = p.get("longitude")
        if lon is None:
            lon = p.get("position")
        try:
            lon_f = float(lon)
        except Exception:
            return None
        return {"name": name, "lon": lon_f, "sign": p.get("sign")}

    candidates: list[Any] = []
    if isinstance(astro_data.get("planets"), list):
        candidates = astro_data["planets"]
    elif isinstance(astro_data.get("western"), dict) and isinstance(astro_data["western"].get("planets"), list):
        candidates = astro_data["western"]["planets"]

    out: list[dict[str, Any]] = []
    for p in candidates:
        np = norm_planet(p)
        if np:
            out.append(np)
    return out


def _extract_house_cusps(astro_data: dict[str, Any]) -> list[float] | None:
    houses = astro_data.get("houses")
    if not isinstance(houses, list) and isinstance(astro_data.get("western"), dict):
        houses = astro_data["western"].get("houses")
    if not isinstance(houses, list):
        return None

    cusps: list[float] = []
    for h in houses:
        if not isinstance(h, dict):
            continue
        lon = h.get("lon")
        if lon is None:
            lon = h.get("abs_pos")
        if lon is None:
            lon = h.get("longitude")
        try:
            cusps.append(float(lon))
        except Exception:
            continue

    return cusps[:12] if len(cusps) >= 12 else None


def _build_structure_summary(astro_data: Any) -> str:
    try:
        if not isinstance(astro_data, dict):
            return ""

        derived: dict[str, Any] = {}
        try:
            from services.structure_engine import (  # type: ignore
                analyze_structure,
                derive_risk_flags,
                analyze_vedic_structure,
                derive_vedic_flags,
            )

            planets = _extract_planets(astro_data)
            cusps = _extract_house_cusps(astro_data)
            if planets:
                structure = analyze_structure(planets, cusps)
                derived["structure"] = structure
                derived["risk_flags"] = derive_risk_flags(structure)

            vedic_data = None
            if isinstance(astro_data.get("vedic"), dict):
                vedic_data = astro_data.get("vedic")
            elif astro_data.get("system") == "vedic":
                vedic_data = astro_data
            if isinstance(vedic_data, dict):
                vedic_structure = analyze_vedic_structure(vedic_data)
                derived["vedic_structure"] = vedic_structure
                derived["vedic_flags"] = derive_vedic_flags(vedic_structure)
        except Exception:
            pass

        keys = [
            "planets", "houses", "aspects", "angles", "skipped_bodies", "ephemeris",
            "nakshatra", "strength", "structure",
            "western", "vedic", "shichusuimei", "pillars",
            "summary", "features", "raw", "input", "normalized_data", "structure_report",
            "personA", "personB", "synastry",
        ]
        picked: dict[str, Any] = {}
        for k in keys:
            v = astro_data.get(k)
            if v is not None:
                picked[k] = v

        if derived:
            picked["_derived"] = derived

        if astro_data.get("system") == "shichusuimei" or astro_data.get("module") == "shichusuimei":
            summary = astro_data.get("summary") if isinstance(astro_data.get("summary"), dict) else {}
            raw = astro_data.get("raw") if isinstance(astro_data.get("raw"), dict) else {}
            raw_options = raw.get("options") if isinstance(raw.get("options"), dict) else {}
            raw_pillars = raw.get("pillars") if isinstance(raw.get("pillars"), dict) else {}
            input_data = astro_data.get("input") if isinstance(astro_data.get("input"), dict) else {}
            assumptions = input_data.get("assumptions") if isinstance(input_data.get("assumptions"), dict) else {}
            digest = {
                "day_kanshi": summary.get("day_kanshi") or raw_pillars.get("day"),
                "hour_kanshi": summary.get("hour_kanshi") or raw_pillars.get("hour"),
                "year_kanshi": summary.get("year_kanshi") or raw_pillars.get("year"),
                "month_kanshi": summary.get("month_kanshi") or raw_pillars.get("month"),
                "day_change_at_23": assumptions.get("day_change_at_23", raw_options.get("day_change_at_23")),
                "day_boundary_rule": assumptions.get("day_boundary_rule") or raw_options.get("day_boundary") or "00:00切替",
            }
            picked = {"shichusuimei_digest": digest, **picked}

        if not picked:
            top_keys = [k for k in astro_data.keys() if k not in ("meta", "_meta")]
            return f"available_keys: {top_keys[:60]}"

        return json.dumps(picked, ensure_ascii=False)

    except Exception:
        return ""


def _build_free_reading_key_data(astro_data: dict[str, Any]) -> str:
    try:
        if not isinstance(astro_data, dict):
            return ""

        planets = astro_data.get("planets")
        if not isinstance(planets, list) and isinstance(astro_data.get("western"), dict):
            planets = astro_data["western"].get("planets")
        if not isinstance(planets, list):
            planets = []

        aspects = astro_data.get("aspects")
        if not isinstance(aspects, list) and isinstance(astro_data.get("western"), dict):
            aspects = astro_data["western"].get("aspects")
        if not isinstance(aspects, list):
            aspects = []

        priority = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"]
        picked: list[str] = []
        by_name: dict[str, dict[str, Any]] = {}
        for item in planets:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if name and name not in by_name:
                by_name[name] = item

        for name in priority:
            item = by_name.get(name)
            if not item:
                continue
            sign = item.get("sign") or "-"
            house = item.get("house")
            house_text = f" / {int(house)}ハウス" if isinstance(house, (int, float)) else ""
            retro = " / 逆行" if item.get("retrograde") else ""
            picked.append(f"{name}: {sign}{house_text}{retro}")

        major_names = {"Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"}
        aspect_lines: list[str] = []
        for a in aspects:
            if not isinstance(a, dict):
                continue
            p1 = str(a.get("planet1") or "").strip()
            p2 = str(a.get("planet2") or "").strip()
            if p1 not in major_names and p2 not in major_names:
                continue
            atype = str(a.get("type") or "").strip()
            orb = a.get("orb")
            orb_text = f" orb {float(orb):.2f}" if isinstance(orb, (int, float)) else ""
            if p1 and p2 and atype:
                aspect_lines.append(f"{p1} - {p2}: {atype}{orb_text}")
            if len(aspect_lines) >= 6:
                break

        lines: list[str] = []
        if picked:
            lines.append("【この人の主要配置】")
            lines.extend(picked)
        if aspect_lines:
            lines.append("【主要アスペクト】")
            lines.extend(aspect_lines)

        return "\n".join(lines).strip()
    except Exception:
        return ""


def _major_planet_digest(data: dict[str, Any]) -> list[str]:
    planets = data.get("planets")
    if not isinstance(planets, list) and isinstance(data.get("western"), dict):
        planets = data["western"].get("planets")
    if not isinstance(planets, list):
        planets = []

    priority = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"]
    rows: list[str] = []
    by_name: dict[str, dict[str, Any]] = {}
    for item in planets:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            if name and name not in by_name:
                by_name[name] = item
    for name in priority:
        item = by_name.get(name)
        if not item:
            continue
        sign = item.get("sign") or "-"
        house = item.get("house")
        house_text = f"/{int(house)}H" if isinstance(house, (int, float)) else ""
        rows.append(f"{name}:{sign}{house_text}")
    return rows[:9]


def _major_aspect_digest(data: dict[str, Any], limit: int = 8) -> list[str]:
    aspects = data.get("aspects")
    if not isinstance(aspects, list) and isinstance(data.get("western"), dict):
        aspects = data["western"].get("aspects")
    if not isinstance(aspects, list):
        aspects = []

    rows: list[str] = []
    for a in aspects:
        if not isinstance(a, dict):
            continue
        p1 = str(a.get("planet1") or "").strip()
        p2 = str(a.get("planet2") or "").strip()
        t = str(a.get("type") or a.get("aspect") or "").strip()
        if p1 and p2 and t:
            rows.append(f"{p1}-{p2}:{t}")
        if len(rows) >= limit:
            break
    return rows


def _build_prompt_astro_digest(astro_data: dict[str, Any], *, compat_mode: bool = False) -> str:
    try:
        if compat_mode:
            pa = astro_data.get("personA") if isinstance(astro_data.get("personA"), dict) else {}
            pb = astro_data.get("personB") if isinstance(astro_data.get("personB"), dict) else {}
            digest = {
                "personA_core": _major_planet_digest(pa),
                "personA_aspects": _major_aspect_digest(pa, limit=6),
                "personB_core": _major_planet_digest(pb),
                "personB_aspects": _major_aspect_digest(pb, limit=6),
            }
            return json.dumps(digest, ensure_ascii=False)

        digest: dict[str, Any] = {
            "major_positions": _major_planet_digest(astro_data),
            "major_aspects": _major_aspect_digest(astro_data, limit=8),
        }
        if isinstance(astro_data.get("vedic"), dict):
            vedic = astro_data["vedic"]
            digest["vedic"] = {
                "nakshatra": vedic.get("nakshatra"),
                "strength": vedic.get("strength"),
            }
        if isinstance(astro_data.get("shichusuimei"), dict):
            s4 = astro_data["shichusuimei"]
            summary = s4.get("summary") if isinstance(s4.get("summary"), dict) else {}
            digest["shichusuimei"] = {
                "day_kanshi": summary.get("day_kanshi"),
                "hour_kanshi": summary.get("hour_kanshi"),
                "year_kanshi": summary.get("year_kanshi"),
                "month_kanshi": summary.get("month_kanshi"),
            }
        return json.dumps(digest, ensure_ascii=False)
    except Exception:
        return _limit_text(astro_data, 2200)


def _build_transit_summary(astro_data: dict[str, Any]) -> str:
    try:
        transit = astro_data.get("transit") or astro_data.get("transit_data")
        if not isinstance(transit, dict):
            return ""

        transit_date = transit.get("transit_date", "")
        today_planets = transit.get("today_planets") or []
        aspects = transit.get("aspects") or transit.get("layer_a") or []

        if not today_planets and not aspects:
            return ""

        lines: list[str] = []
        if transit_date:
            lines.append(f"トランジット日: {transit_date}")

        if today_planets:
            planet_strs = [
                f"{p['name']} {p.get('sign', '')} {p.get('degree', '')}°"
                for p in today_planets[:10]
                if isinstance(p, dict) and p.get("name")
            ]
            if planet_strs:
                lines.append("今日の天体: " + " / ".join(planet_strs))

        if aspects:
            asp_strs: list[str] = []
            for a in aspects[:10]:
                if not isinstance(a, dict):
                    continue
                t = a.get("transit_planet", "")
                n = a.get("natal_planet", "")
                asp = a.get("aspect", "")
                orb = a.get("orb", "")
                if t and n and asp:
                    asp_strs.append(f"T{t} {asp} N{n}(orb {orb}°)")
            if asp_strs:
                lines.append("有効アスペクト: " + " / ".join(asp_strs))

        return "\n".join(lines) if lines else ""
    except Exception:
        return ""


def _normalize_report_type(report_type: str | None) -> str:
    rt = (report_type or "").strip().lower()
    if rt in (
        "single_web",
        "single_line",
        "single_web_reader",
        "single_line_reader",
        "compat_web",
        "compat_line",
        "raw_prompt",
    ):
        return rt
    if rt in ("compatibility", "compat"):
        return "compat_web"
    if rt in ("single", ""):
        return "single_web"
    return "single_web"


def _parse_age_years(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        s = str(value).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _detect_available_systems(astro_data: dict[str, Any], astrology_system: str) -> dict[str, bool]:
    western = bool(astro_data.get("western") or astro_data.get("planets") or astrology_system == "western")
    vedic = bool(astro_data.get("vedic") or astrology_system == "vedic")
    shichu = bool(
        astro_data.get("shichusuimei")
        or astro_data.get("pillars")
        or astro_data.get("structure_report")
        or astrology_system in ("shichusuimei", "shichu")
    )
    if astrology_system == "integrated":
        western = True
        vedic = True
    if astrology_system in ("integrated3", "integrated_3"):
        western = True
        vedic = True
        shichu = True
    return {"western": western, "vedic": vedic, "shichu": shichu}


def _life_phase(age_years: Any) -> tuple[str, str]:
    age = _parse_age_years(age_years)
    if age is None:
        return ("現在地を見直す時期", "今までのやり方を棚卸しし、本当に残すものを選び直す時期")
    if age < 29:
        return ("基盤形成期", "経験を増やしながら、自分に合う土台や居場所を見極める時期")
    if age < 43:
        return ("拡張期", "役割や活動範囲を広げつつ、自分の強みを社会の中で形にしていく時期")
    if age < 56:
        return ("転換期", "これまで築いた現実的な力を土台にしながら、より本質的な選択へ重心を移す時期")
    return ("統合期", "積み上げてきた経験を整理し、不要なものを削ぎ落として、自分の核を生かす時期")


def _transit_focus(age_years: Any, available_systems: dict[str, bool]) -> str:
    age = _parse_age_years(age_years)
    if available_systems.get("vedic") and available_systems.get("shichu"):
        base = "広げるより、手元の選別と方向修正が効きやすい流れ"
    elif available_systems.get("vedic"):
        base = "外側の成果より、内側の納得感を整えるほど動きやすくなる流れ"
    elif available_systems.get("shichu"):
        base = "勢いよりも、生活リズムや現実条件を整えるほど結果が出やすい流れ"
    else:
        base = "対人や役割の整理を先に進めるほど、次の動きが見えやすくなる流れ"
    if age is not None and age >= 45:
        base += "。特に今は、増やすより削る判断が効きやすい"
    return base


def _line_fallback_text(meta2: dict[str, Any]) -> str:
    age = meta2.get("age_years")
    today = (meta2.get("today") or "").strip()
    phase_label, phase_theme = _life_phase(age)

    display_name = (
        (meta2.get("line_display_name") or "").strip()
        or (meta2.get("display_name") or "").strip()
        or (meta2.get("user_name") or "").strip()
    )
    if display_name in ("あなた", ""):
        display_name = ""

    header_lines: list[str] = []
    if display_name:
        header_lines.append(display_name)

    age_text = ""
    if age not in (None, "", "未計算"):
        age_text = f"{age}歳"
    if today and today != "未取得":
        age_text = f"{age_text}・{today}時点" if age_text else f"{today}時点"
    if age_text:
        header_lines.append(f"（{age_text}）")

    body = (
        "今は、無理に広げるよりも、自分に合うやり方を見極め直す方が流れに乗りやすい時期です。\n"
        f"人生の現在地でいうと『{phase_label}』にあたり、{phase_theme}。\n"
        "直近3〜6ヶ月は、新しいことを増やすより、続けるものと手放すものを整理するほど動きやすくなります。"
    )

    if header_lines:
        return "\n".join(header_lines) + "\n\n" + body
    return body


def _debug_model_info(requested_model: Any = None) -> str:
    resolved_model, source = _resolve_model_name(requested_model)
    env_model = os.getenv("GEMINI_MODEL")
    return (
        f"[debug] resolved_model={resolved_model}, "
        f"source={source}, "
        f"requested_model={repr(requested_model)}, "
        f"env_GEMINI_MODEL={repr(env_model)}, "
        f"default_model={DEFAULT_MODEL_NAME}"
    )


def _is_flash_model(model_name: str) -> bool:
    return model_name in {"gemini-2.5-flash", "gemini-2.5-flash-lite"}


def _looks_truncated(text: str) -> bool:
    body = (text or "").strip()
    if not body:
        return True
    if len(body) < 800:
        return True
    if body.endswith(("は", "が", "を", "に", "で", "と", "も", "や", "へ", "、", ",", "・")):
        return True
    if not body.endswith(("。", "！", "？", "」", "』", "】")):
        return True
    return False


def _expected_headers(compat_mode: bool) -> list[str]:
    return (
        [f"### {i})" for i in range(1, 9)] if compat_mode
        else [f"###{i}" for i in range(1, 9)]
    )


def _has_enough_sections(text: str, compat_mode: bool) -> bool:
    body = text or ""
    if compat_mode:
        count = 0
        for i in range(1, 9):
            if f"### {i})" in body or f"### {i}）" in body:
                count += 1
        return count >= 8
    count = 0
    for i in range(1, 9):
        if f"###{i}" in body or f"### {i}" in body:
            count += 1
    return count >= 8


def _needs_longform_flash_retry(text: str, *, is_web: bool, model_name: str, compat_mode: bool = False) -> bool:
    if not is_web or model_name != "gemini-2.5-flash":
        return False
    body = (text or "").strip()
    if len(body) < 1800:
        return True
    if compat_mode and not _has_enough_sections(body, True):
        return True
    if not any(body.endswith(ch) for ch in ("。", "！", "？", "」", "』", "】", ">")):
        return True
    return False


def _flash_web_boost_prompt(compat_mode: bool = False) -> str:
    if compat_mode:
        return (
            "【Gemini 2.5 Flash 専用の追加指示】\n"
            "- この相性鑑定は短くまとめないこと。\n"
            "- 必ず以下の8章をすべて出すこと。\n"
            "  1. 個体の関係特性\n"
            "  2. 感情的安心構造\n"
            "  3. 愛情表現と魅力認識\n"
            "  4. 親密性と距離感\n"
            "  5. 衝突発生メカニズム\n"
            "  6. 長期安定構造\n"
            "  7. 強い引力と変容作用\n"
            "  8. 実践的ヒント\n"
            "- 各章は最低でも4文以上。\n"
            "- 第8章で必ず最後まで完結させること。\n"
            "- 文章末尾を途中で終わらせないこと。\n"
        )
    return (
        "【Gemini 2.5 Flash 専用の追加指示】\n"
        "- この鑑定は短くまとめないこと。\n"
        "- 必ず以下の8章をすべて出すこと。\n"
        "  1. この人の核\n"
        "  2. 表に出る姿\n"
        "  3. 内側の本質とズレ\n"
        "  4. 現実での出方\n"
        "  5. 盲点と詰まりやすい癖\n"
        "  6. 扱い方のコツ\n"
        "  7. 人生の流れと現在地\n"
        "  8. これから3〜6ヶ月の流れ\n"
        "- 各章は最低でも5文以上で書くこと。\n"
        "- 箇条書きではなく、自然な日本語の段落で書くこと。\n"
        "- 全体の文字数は最低1800文字以上、理想は2200〜3200文字。\n"
        "- 途中で終わらせず、最後まで完結させること。\n"
        "- 具体例、状況描写、行動アドバイスを入れて厚みを出すこと。"
    )


def _extract_json_object(text: str) -> dict[str, Any]:
    s = (text or "").strip()
    if not s:
        return {}
    if s.startswith("```"):
        lines = s.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()
    try:
        data = json.loads(s)
        return data if isinstance(data, dict) else {}
    except Exception:
        pass
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            data = json.loads(s[start:end + 1])
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _call_model_once(
    *,
    client: Any,
    model_name: str,
    prompt: str,
    max_tokens: int,
    temperature: float,
    top_p: float = 0.95,
) -> tuple[str, Any]:
    config = types.GenerateContentConfig(
        temperature=temperature,
        top_p=top_p,
        max_output_tokens=max_tokens,
    )
    resp = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=config,
    )
    return (_extract_text(resp) or "").strip(), resp


def _log_usage(resp: Any, *, model_name: str, source: str, attempt: int, stage: str) -> None:
    usage = getattr(resp, "usage_metadata", None)
    if usage is None:
        return
    try:
        print("[ai_report] usage", {
            "model": model_name,
            "source": source,
            "attempt": attempt,
            "stage": stage,
            "prompt_token_count": getattr(usage, "prompt_token_count", None),
            "candidates_token_count": getattr(usage, "candidates_token_count", None),
            "total_token_count": getattr(usage, "total_token_count", None),
        })
    except Exception:
        pass


def _compat_names(astro_data: dict[str, Any], meta2: dict[str, Any]) -> tuple[str, str]:
    person_a = astro_data.get("personA") if isinstance(astro_data.get("personA"), dict) else {}
    person_b = astro_data.get("personB") if isinstance(astro_data.get("personB"), dict) else {}

    name_a = (
        person_a.get("name")
        or meta2.get("name")
        or meta2.get("user_name")
        or "A"
    )
    name_b = (
        person_b.get("name")
        or meta2.get("name_b")
        or "B"
    )
    return str(name_a).strip() or "A", str(name_b).strip() or "B"


def _compat_name_rules(name_a: str, name_b: str) -> str:
    return (
        "【登場人物名の固定ルール】\n"
        f"- PersonA → {name_a}さん\n"
        f"- PersonB → {name_b}さん\n"
        "- 「PersonA」「PersonB」「A」「B」という表記は本文で使わないこと。\n"
        f"- 必ず「{name_a}さん」「{name_b}さん」で書くこと。\n"
        "- 二人称で雑に省略せず、誰の話か曖昧にしないこと。\n"
    )


def _default_outline(*, compat_mode: bool = False) -> dict[str, Any]:
    section_titles = (
        [
            "個体の関係特性",
            "感情的安心構造",
            "愛情表現と魅力認識",
            "親密性と距離感",
            "衝突発生メカニズム",
            "長期安定構造",
            "強い引力と変容作用",
            "実践的ヒント",
        ] if compat_mode else [
            "この人の核",
            "表に出る姿",
            "内側の本質とズレ",
            "現実での出方",
            "盲点と詰まりやすい癖",
            "扱い方のコツ",
            "人生の流れと現在地",
            "これから3〜6ヶ月の流れ",
        ]
    )
    return {
        "voice": "落ち着いた分析調で、抽象だけでなく具体例も交えて書く",
        "central_theme": "主要配置から見た関係構造の理解",
        "sections": [
            {
                "id": i + 1,
                "title": title,
                "goal": "主要な読み筋を整理する",
                "points": ["配置の特徴を拾う", "関係の出方を具体化する"],
                "advice": ["現実で使える行動ヒントを1つ入れる", "断定しすぎない"],
            }
            for i, title in enumerate(section_titles)
        ],
    }


def _outline_prompt(*, ctx: dict[str, Any], single_web_prompt: str) -> str:
    compat_mode = bool(ctx.get("compat_mode"))
    name_rules = _compat_name_rules(
        str(ctx.get("name_a") or "A"),
        str(ctx.get("name_b") or "B"),
    ) if compat_mode else ""

    section_titles = (
        [
            "個体の関係特性", "感情的安心構造", "愛情表現と魅力認識", "親密性と距離感",
            "衝突発生メカニズム", "長期安定構造", "強い引力と変容作用", "実践的ヒント",
        ] if compat_mode else [
            "この人の核", "表に出る姿", "内側の本質とズレ", "現実での出方",
            "盲点と詰まりやすい癖", "扱い方のコツ", "人生の流れと現在地", "これから3〜6ヶ月の流れ",
        ]
    )
    sections_json = ",\n".join(
        [f'    {{"id": {i+1}, "title": "{title}", "goal": "...", "points": ["..."], "advice": ["..."]}}' for i, title in enumerate(section_titles)]
    )
    return (
        "あなたは占い鑑定文の設計者です。本文は書かず、有効なJSONのみ返してください。\n"
        "長文本文を2回に分けて書くための設計図を作ります。説明文は禁止です。\n\n"
        + name_rules
        + "\n【必須条件】\n"
        "- sections は必ず8件。\n"
        "- 各 section は title / goal / points / advice を持つ。\n"
        "- points と advice は各2〜4件の短文。\n"
        "- 占術別にバラさず統合した読み筋にする。\n"
        "- 名前が空なら呼びかけ不要。未来は断定しない。\n\n"
        + "【返却形式】\n{\n  \"voice\": \"文体方針\",\n  \"central_theme\": \"全体テーマ\",\n  \"sections\": [\n"
        + sections_json
        + "\n  ]\n}\n\n"
        + "【入力要約】\n"
        + f"digest: {ctx.get('astro_digest')}\n"
        + f"structure: {ctx.get('structure_summary')}\n"
        + f"meta: system={ctx.get('astrology_system')}, compat={compat_mode}, theme={ctx.get('theme')}, display={ctx.get('display_name')}, phase={ctx.get('life_phase_label')}, flow={ctx.get('transit_focus')}\n"
        + f"transit: {ctx.get('transit_summary')}\n"
    )


def _part_prompt(*, outline: dict[str, Any], ctx: dict[str, Any], part: int) -> str:
    compat_mode = bool(ctx.get("compat_mode"))
    if part == 1:
        section_range = "1〜4章"
        extra = (
            "- 1〜4章のみ書く。5〜8章には触れない。\n"
            "- 前半は人物像や関係の土台を具体化する。\n"
            "- 目安1200〜2200文字。\n"
        )
    else:
        section_range = "5〜8章"
        extra = (
            "- 5〜8章のみ書く。1〜4章を繰り返さない。\n"
            "- 後半は課題・扱い方・今後のヒントまで書き切る。\n"
            "- 第8章の最後まで完結させる。\n"
            "- 目安1200〜2200文字。\n"
        )

    scope_line = (
        "- 二者の相互作用として書く。相性の良し悪し判定ではなく、起きやすい仕組みを書く。\n"
        if compat_mode else
        "- 占術別の説明書きではなく、1人の人格として統合して描く。\n"
    )

    name_rules = _compat_name_rules(
        str(ctx.get("name_a") or "A"),
        str(ctx.get("name_b") or "B"),
    ) if compat_mode else ""

    return (
        "あなたは占い鑑定を行うプロの占い師です。以下の設計図に従い、このパートだけを完結した日本語本文として書いてください。\n\n"
        + name_rules
        + "\n【重要】\n"
        "- このパート以外の章は書かない。\n"
        "- 箇条書きは禁止。自然な段落だけで書く。\n"
        "- 短くまとめず、根拠のある具体表現を入れる。\n"
        "- 名前が空なら呼びかけ不要。敬称は付けない。\n"
        + scope_line
        + "- 章タイトルは本文内にそのまま表示してよい。\n"
        + extra
        + "\n【今回書く範囲】\n"
        + section_range
        + "\n\n【設計図(JSON)】\n"
        + _limit_text(json.dumps(outline, ensure_ascii=False), 3500)
        + "\n\n【補助情報】\n"
        + f"digest: {ctx.get('astro_digest')}\n"
        + f"display_name: {ctx.get('display_name')}\n"
        + f"name_a: {ctx.get('name_a')}\n"
        + f"name_b: {ctx.get('name_b')}\n"
        + f"life_phase: {ctx.get('life_phase_label')} / {ctx.get('life_phase_theme')}\n"
        + f"transit_focus: {ctx.get('transit_focus')}\n"
        + f"transit_summary: {ctx.get('transit_summary')}\n"
    )


def _completion_prompt(*, existing_text: str, ctx: dict[str, Any], compat_mode: bool) -> str:
    name_rules = _compat_name_rules(
        str(ctx.get("name_a") or "A"),
        str(ctx.get("name_b") or "B"),
    ) if compat_mode else ""
    return (
        "以下の本文は末尾が途中で切れています。"
        "重複せず、続きだけを自然につないで第8章の最後まで完結させてください。\n"
        "400〜900字程度で十分です。箇条書きは禁止です。\n\n"
        + name_rules
        + "\n【すでに出ている本文】\n"
        + _limit_text(existing_text, 3000)
    )


def _complete_tail_if_needed(
    *,
    client: Any,
    model_name: str,
    model_source: str,
    text: str,
    ctx: dict[str, Any],
    compat_mode: bool,
) -> str:
    body = (text or "").strip()
    if not body:
        return body
    if not _looks_truncated(body):
        return body

    try:
        prompt = _completion_prompt(existing_text=body, ctx=ctx, compat_mode=compat_mode)
        tail, resp = _call_model_once(
            client=client,
            model_name=model_name,
            prompt=prompt,
            max_tokens=1400,
            temperature=0.15,
        )
        _log_usage(resp, model_name=model_name, source=model_source, attempt=1, stage="tail")
        tail = (tail or "").strip()
        if tail:
            merged = (body + "\n" + tail).strip()
            if not _looks_truncated(merged):
                return merged
    except Exception:
        pass
    return body


def _generate_longform_in_parts(
    *,
    client: Any,
    model_name: str,
    model_source: str,
    ctx: dict[str, Any],
    single_web_prompt: str,
) -> str:
    outline_prompt = _outline_prompt(ctx=ctx, single_web_prompt=single_web_prompt)
    outline: dict[str, Any] = {}
    compat_mode = bool(ctx.get("compat_mode"))

    for attempt in range(2):
        try:
            outline_text, outline_resp = _call_model_once(
                client=client,
                model_name=model_name,
                prompt=outline_prompt,
                max_tokens=2200,
                temperature=0.1,
            )
            _log_usage(outline_resp, model_name=model_name, source=model_source, attempt=attempt + 1, stage="outline")
            outline = _extract_json_object(outline_text)
            sections = outline.get("sections") if isinstance(outline, dict) else None
            if isinstance(sections, list) and len(sections) >= 8:
                break
            outline = {}
            outline_prompt += "\n\n【再指示】有効なJSONだけを返してください。sections は必ず8件です。"
            time.sleep(0.4)
        except Exception:
            outline = {}
            time.sleep(0.6)

    if not outline:
        outline = _default_outline(compat_mode=compat_mode)

    parts: list[str] = []

    for part in (1, 2):
        part_prompt = _part_prompt(outline=outline, ctx=ctx, part=part)
        part_text = ""
        for attempt in range(3):
            try:
                candidate, part_resp = _call_model_once(
                    client=client,
                    model_name=model_name,
                    prompt=part_prompt,
                    max_tokens=4200,
                    temperature=0.15,
                )
                _log_usage(part_resp, model_name=model_name, source=model_source, attempt=attempt + 1, stage=f"part{part}")

                candidate = (candidate or "").strip()
                if part == 2:
                    candidate = _complete_tail_if_needed(
                        client=client,
                        model_name=model_name,
                        model_source=model_source,
                        text=candidate,
                        ctx=ctx,
                        compat_mode=compat_mode,
                    )

                too_short = len(candidate) < (1100 if part == 1 else 900)
                bad_tail = _looks_truncated(candidate)

                if too_short or bad_tail:
                    if attempt < 2:
                        part_prompt += (
                            "\n\n【再指示】\n"
                            "前回の出力は短すぎるか、途中で終わっています。"
                            "このパートだけを最初から書き直し、最後まで完結させてください。"
                            "具体例と関係の出方、行動ヒントを増やしてください。"
                        )
                        time.sleep(0.4)
                        continue

                part_text = candidate
                if part_text:
                    break
            except Exception:
                time.sleep(0.6)

        if not part_text:
            raise RuntimeError(f"part{part} generation failed")

        parts.append(part_text)

    if len(parts) != 2:
        raise RuntimeError("multipart generation incomplete")

    full = "\n\n".join(p for p in parts if p.strip()).strip()
    full = _complete_tail_if_needed(
        client=client,
        model_name=model_name,
        model_source=model_source,
        text=full,
        ctx=ctx,
        compat_mode=compat_mode,
    )

    if len(full) < 1800 or _looks_truncated(full):
        raise RuntimeError("combined text too short or truncated")

    if compat_mode and not _has_enough_sections(full, True):
        raise RuntimeError("compat sections incomplete")

    return fix_punctuation(full)


def generate_report(
    astro_data: dict[str, Any],
    *,
    style: str | None = None,
    report_type: str | None = None,
    meta: dict[str, Any] | None = None,
) -> str:
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return "GEMINI_API_KEY が未設定です"

    if genai is None or types is None:
        return "google-genai が読み込めません（requirements.txt を確認）"

    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        return f"Gemini client 初期化エラー: {e} / {_debug_model_info()}"

    base_meta = _safe_get_meta(astro_data)
    meta2 = _merge_meta(base_meta, meta)
    requested_model = _normalize_requested_model(meta2.get("ai_model")) if isinstance(meta2, dict) else None
    model_name, model_source = _resolve_model_name(requested_model)

    if style:
        meta2["output_style"] = style

    rt = _normalize_report_type(report_type)

    output_style = (style or meta2.get("output_style", "web") or "web").strip()
    detail_level = (meta2.get("detail_level", "standard") or "standard").strip()
    astrology_system = (meta2.get("astrology_system", "western") or "western").strip().lower()

    birth_date = meta2.get("birth_date", "未取得")
    today = meta2.get("today", "未取得")
    age_years = meta2.get("age_years", "未計算")
    era_title = meta2.get("era_title", "いまの転換期")
    theme = meta2.get("theme", "overall")
    user_message = meta2.get("message", "")
    observations_text = (meta2.get("observations_text", "") or "").strip()
    user_name = (meta2.get("user_name", "") or "").strip()
    display_name = user_name if user_name not in ("あなた",) else ""
    available_systems = _detect_available_systems(astro_data, astrology_system)
    life_phase_label, life_phase_theme = _life_phase(age_years)
    transit_focus = _transit_focus(age_years, available_systems)

    compat_mode = rt in {"compat_web", "compat_line"}
    name_a, name_b = _compat_names(astro_data, meta2)

    astro_digest = _build_prompt_astro_digest(astro_data, compat_mode=compat_mode)
    structure_summary = _limit_text(_build_structure_summary(astro_data), 1800 if compat_mode else 2600)
    transit_summary = _limit_text(_build_transit_summary(astro_data), 700)

    ctx: dict[str, Any] = {
        "astro_data": astro_digest,
        "astro_digest": astro_digest,
        "structure_summary": structure_summary,
        "astrology_system": astrology_system,
        "theme": theme,
        "user_message": user_message,
        "observations_text": observations_text,
        "birth_date": birth_date,
        "today": today,
        "age_years": age_years,
        "era_title": era_title,
        "detail_level": detail_level,
        "user_name": user_name or "あなた",
        "display_name": display_name,
        "available_systems": json.dumps(available_systems, ensure_ascii=False),
        "life_phase_label": life_phase_label,
        "life_phase_theme": life_phase_theme,
        "transit_focus": transit_focus,
        "transit_summary": transit_summary,
        "free_reading_key_data": _build_free_reading_key_data(astro_data),
        "astro_hint_line": build_astro_hint_line(astro_data),
        "compat_mode": compat_mode,
        "name_a": name_a,
        "name_b": name_b,
    }

    common_rules_tpl = _read_prompt_file("common_rules.txt")
    ctx["common_rules"] = _render_prompt(common_rules_tpl, ctx)

    single_web_tpl = _read_prompt_file("single_web.txt")
    single_line_tpl = _read_prompt_file("single_line.txt")
    single_web_reader_tpl = _read_prompt_file("single_web_reader.txt")
    single_line_reader_tpl = _read_prompt_file("single_line_reader.txt")
    compat_web_tpl = _read_prompt_file("compat_web.txt")
    compat_line_tpl = _read_prompt_file("compat_line.txt")

    single_web_prompt = _render_prompt(single_web_tpl, ctx)
    single_line_prompt = _render_prompt(single_line_tpl, ctx)
    single_web_reader_prompt = _render_prompt(single_web_reader_tpl, ctx)
    single_line_reader_prompt = _render_prompt(single_line_reader_tpl, ctx)
    compat_web_prompt = _render_prompt(compat_web_tpl, ctx)
    compat_line_prompt = _render_prompt(compat_line_tpl, ctx)

    if compat_mode:
        name_rules = _compat_name_rules(name_a, name_b)
        compat_web_prompt = name_rules + "\n" + compat_web_prompt
        compat_line_prompt = name_rules + "\n" + compat_line_prompt

    free_reading_prompt = ""
    if theme == "free_reading":
        free_reading_tpl = _read_prompt_file("free_reading_web.txt")
        free_reading_prompt = _render_prompt(free_reading_tpl, ctx)

    guard = ""
    if astrology_system == "integrated":
        guard = _read_prompt_file("guard_integrated.txt").strip()
    elif astrology_system == "integrated3":
        guard = _read_prompt_file("guard_integrated3.txt").strip()

    if guard:
        single_web_prompt += "\n\n" + guard
        single_line_prompt += "\n\n" + guard
        single_web_reader_prompt += "\n\n" + guard
        single_line_reader_prompt += "\n\n" + guard
        compat_web_prompt += "\n\n" + guard
        compat_line_prompt += "\n\n" + guard

    vedic_guard = ""
    if astrology_system == "vedic":
        vedic_guard = _read_prompt_file("guard_vedic.txt").strip()

    if rt == "raw_prompt":
        prompt = (meta2.get("message") or user_message or "").strip()
    elif rt == "compat_line" or (rt == "compat_web" and output_style == "line"):
        prompt = compat_line_prompt
    elif rt == "compat_web":
        prompt = compat_web_prompt
    elif rt == "single_line_reader":
        prompt = single_line_reader_prompt
    elif rt == "single_web_reader" and output_style == "line":
        prompt = single_line_reader_prompt
    elif rt == "single_web_reader":
        prompt = single_web_reader_prompt
    elif rt == "single_line" or (rt == "single_web" and output_style == "line"):
        prompt = single_line_prompt
    else:
        prompt = free_reading_prompt or single_web_prompt

    is_line = (output_style == "line" or "line" in rt)
    is_web = not is_line

    if vedic_guard:
        prompt = vedic_guard + "\n\n" + prompt

    if is_web and model_name == "gemini-2.5-flash":
        prompt = prompt.rstrip() + "\n\n" + _flash_web_boost_prompt(compat_mode)

    if is_web and model_name in {"gemini-2.5-flash-lite", "gemini-2.5-flash"} and theme != "free_reading":
        try:
            return _generate_longform_in_parts(
                client=client,
                model_name=model_name,
                model_source=model_source,
                ctx=ctx,
                single_web_prompt=single_web_prompt,
            )
        except Exception as e:
            print("[ai_report] multipart fallback", {"model": model_name, "error": str(e)})

    if is_line:
        max_tokens = 1400
    elif theme == "free_reading":
        max_tokens = 1400
    elif model_name == "gemini-2.5-flash":
        max_tokens = 7000
    else:
        max_tokens = 5200

    last_error = None

    for attempt in range(3):
        try:
            text1, resp = _call_model_once(
                client=client,
                model_name=model_name,
                prompt=prompt,
                max_tokens=max_tokens,
                temperature=0.15 if _is_flash_model(model_name) else 0.1,
            )
            _log_usage(resp, model_name=model_name, source=model_source, attempt=attempt + 1, stage="single")
            if not text1:
                raise RuntimeError("empty text")

            if compat_mode:
                text1 = _complete_tail_if_needed(
                    client=client,
                    model_name=model_name,
                    model_source=model_source,
                    text=text1,
                    ctx=ctx,
                    compat_mode=True,
                )

            if _needs_longform_flash_retry(text1, is_web=is_web, model_name=model_name, compat_mode=compat_mode) and attempt < 2:
                print("[ai_report] flash output too short", {
                    "model": model_name,
                    "chars": len(text1),
                    "attempt": attempt + 1,
                })
                prompt = (
                    prompt.rstrip()
                    + "\n\n【再指示】\n"
                    + "前回の出力は短すぎるか、文が途中で終わっています。必ず8章すべてを最後まで書き切り、"
                    + "全体で1800文字以上になるように、具体例・状況描写・行動アドバイスを増やして最初から書き直してください。"
                )
                time.sleep(0.5)
                continue

            return fix_punctuation(text1)
        except Exception as e:
            last_error = e
            time.sleep(0.8)

    if is_line:
        return fix_punctuation(_line_fallback_text(meta2))
    return f"AI生成エラー: {last_error} / {_debug_model_info(requested_model)}"
