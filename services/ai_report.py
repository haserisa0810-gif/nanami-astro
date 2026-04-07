from __future__ import annotations
import json
import os
import re
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

        western_data = astro_data.get("western") if isinstance(astro_data.get("western"), dict) else astro_data
        planets_src = western_data.get("planets") if isinstance(western_data.get("planets"), list) else []
        aspects_src = western_data.get("aspects") if isinstance(western_data.get("aspects"), list) else []
        houses_src = western_data.get("houses") if isinstance(western_data.get("houses"), list) else []
        skipped_src = western_data.get("skipped_bodies") if isinstance(western_data.get("skipped_bodies"), list) else []
        calc_engine = western_data.get("calc_engine") if isinstance(western_data.get("calc_engine"), dict) else {}
        options = western_data.get("options") if isinstance(western_data.get("options"), dict) else {}

        major_names = {"Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Saturn", "ASC", "MC"}
        optional_names = {"North Node", "South Node", "Lilith", "Chiron", "Ceres", "Pallas", "Juno", "Vesta", "Vertex"}

        def _planet_row(item: Any) -> dict[str, Any] | None:
            if not isinstance(item, dict):
                return None
            name = str(item.get("name") or "").strip()
            if not name:
                return None
            row: dict[str, Any] = {
                "name": name,
                "sign": item.get("sign"),
                "house": item.get("house"),
            }
            if item.get("retrograde"):
                row["retrograde"] = True
            return row

        major_planets: list[dict[str, Any]] = []
        optional_points: list[dict[str, Any]] = []
        for item in planets_src:
            row = _planet_row(item)
            if not row:
                continue
            name = str(row.get("name") or "")
            if name in major_names:
                major_planets.append(row)
            elif name in optional_names:
                optional_points.append(row)

        major_aspects: list[dict[str, Any]] = []
        optional_aspects: list[dict[str, Any]] = []
        for item in aspects_src:
            if not isinstance(item, dict):
                continue
            p1 = str(item.get("planet1") or "").strip()
            p2 = str(item.get("planet2") or "").strip()
            if not p1 or not p2:
                continue
            row = {
                "planet1": p1,
                "planet2": p2,
                "type": item.get("type") or item.get("aspect"),
                "orb": item.get("orb"),
            }
            if p1 in major_names and p2 in major_names:
                major_aspects.append(row)
            elif p1 in optional_names or p2 in optional_names:
                optional_aspects.append(row)

        house_digest: list[dict[str, Any]] = []
        for item in houses_src[:12]:
            if not isinstance(item, dict):
                continue
            house_digest.append({
                "house": item.get("house"),
                "sign": item.get("sign"),
            })

        skipped_bodies: list[dict[str, Any]] = []
        for item in skipped_src[:8]:
            if not isinstance(item, dict):
                continue
            skipped_bodies.append({
                "name": item.get("name"),
                "reason": item.get("reason"),
            })

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
                derived["risk_flags"] = derive_risk_flags(structure)[:5]
            vedic_data = None
            if isinstance(astro_data.get("vedic"), dict):
                vedic_data = astro_data.get("vedic")
            elif astro_data.get("system") == "vedic":
                vedic_data = astro_data
            if isinstance(vedic_data, dict):
                vedic_structure = analyze_vedic_structure(vedic_data)
                derived["vedic_structure"] = vedic_structure
                derived["vedic_flags"] = derive_vedic_flags(vedic_structure)[:5]
        except Exception:
            pass

        picked: dict[str, Any] = {
            "major_planets": major_planets[:12],
            "major_aspects": major_aspects[:18],
            "optional_points": optional_points[:10],
            "optional_aspects": optional_aspects[:10],
            "houses": house_digest,
            "options": options,
            "calc_engine": calc_engine,
            "skipped_bodies": skipped_bodies,
        }
        if derived:
            picked["_derived"] = derived
        if isinstance(astro_data.get("vedic"), dict):
            vedic = astro_data.get("vedic") or {}
            picked["vedic_digest"] = {
                "nakshatra": vedic.get("nakshatra"),
                "strength": vedic.get("strength"),
            }
        if astro_data.get("system") == "shichusuimei" or astro_data.get("module") == "shichusuimei" or isinstance(astro_data.get("shichusuimei"), dict):
            shichu = astro_data.get("shichusuimei") if isinstance(astro_data.get("shichusuimei"), dict) else astro_data
            summary = shichu.get("summary") if isinstance(shichu.get("summary"), dict) else {}
            raw = shichu.get("raw") if isinstance(shichu.get("raw"), dict) else {}
            raw_options = raw.get("options") if isinstance(raw.get("options"), dict) else {}
            raw_pillars = raw.get("pillars") if isinstance(raw.get("pillars"), dict) else {}
            input_data = shichu.get("input") if isinstance(shichu.get("input"), dict) else {}
            assumptions = input_data.get("assumptions") if isinstance(input_data.get("assumptions"), dict) else {}
            picked["shichusuimei_digest"] = {
                "day_kanshi": summary.get("day_kanshi") or raw_pillars.get("day"),
                "hour_kanshi": summary.get("hour_kanshi") or raw_pillars.get("hour"),
                "year_kanshi": summary.get("year_kanshi") or raw_pillars.get("year"),
                "month_kanshi": summary.get("month_kanshi") or raw_pillars.get("month"),
                "day_change_at_23": assumptions.get("day_change_at_23", raw_options.get("day_change_at_23")),
                "day_boundary_rule": assumptions.get("day_boundary_rule") or raw_options.get("day_boundary") or "00:00切替",
            }
        if not any(v for v in picked.values()):
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
    if len(body) < 500:
        return True
    if body.endswith(("は", "が", "を", "に", "で", "と", "も", "や", "へ", "、", ",", "・")):
        return True
    if not body.endswith(("。", "！", "？", "」", "』", "】")):
        return True
    return False
def _detail_policy(detail_level: str, *, is_line: bool, compat_mode: bool, theme: str, report_type: str) -> dict[str, int]:
    level = str(detail_level or "standard").strip().lower()
    if is_line:
        return {"min_chars": 180, "target_chars": 400, "max_tokens": 900}
    if "reader" in report_type:
        return {"min_chars": 500, "target_chars": 900, "max_tokens": 1400}
    if compat_mode:
        if level == "detailed":
            return {"min_chars": 1800, "target_chars": 2600, "max_tokens": 3200}
        if level == "short":
            return {"min_chars": 1200, "target_chars": 1800, "max_tokens": 2200}
        return {"min_chars": 1500, "target_chars": 2200, "max_tokens": 2600}
    if theme == "timing":
        if level == "detailed":
            return {"min_chars": 1400, "target_chars": 2200, "max_tokens": 2600}
        if level == "short":
            return {"min_chars": 1000, "target_chars": 1500, "max_tokens": 1800}
        return {"min_chars": 1200, "target_chars": 1800, "max_tokens": 2200}
    if level == "detailed":
        return {"min_chars": 1600, "target_chars": 2400, "max_tokens": 2800}
    if level == "short":
        return {"min_chars": 1000, "target_chars": 1600, "max_tokens": 1800}
    return {"min_chars": 1200, "target_chars": 1900, "max_tokens": 2200}

def _section_headers(theme: str, compat_mode: bool = False) -> list[str]:
    if compat_mode:
        return [
            "### 1. 個体の関係特性",
            "### 2. 感情的安心構造",
            "### 3. 愛情表現と魅力認識",
            "### 4. 親密性と距離感",
            "### 5. 衝突発生メカニズム",
            "### 6. 長期安定構造",
            "### 7. 強い引力と変容作用",
            "### 8. 実践的ヒント",
        ]
    if str(theme or "").strip().lower() == "timing":
        return [
            "### 1. 過去の流れ",
            "### 2. 現在地",
            "### 3. 近未来",
            "### 4. 当たりやすい動き方",
        ]
    return [
        "### 1. この人の核",
        "### 2. 表に出る姿",
        "### 3. 内側の本質とズレ",
        "### 4. 現実での出方",
        "### 5. 盲点と詰まりやすい癖",
        "### 6. 扱い方のコツ",
        "### 7. 人生の流れと現在地",
        "### 8. これから3〜6ヶ月の流れ",
    ]

def _has_required_headers(text: str, headers: list[str]) -> bool:
    body = text or ""
    found = sum(1 for h in headers if h in body)
    needed = len(headers)
    return found >= needed

def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    s = str(value or "").strip().lower()
    return s in {"1", "true", "on", "yes", "y"}


def _header_count(text: str, headers: list[str]) -> int:
    body = text or ""
    return sum(1 for h in headers if h in body)



def _extract_present_headers(text: str, headers: list[str]) -> list[str]:
    t = text or ""
    out: list[str] = []
    for h in headers:
        if h and h in t:
            out.append(h)
    return out


def _debug_preview(text: str, max_chars: int = 160) -> str:
    s = (text or "").replace("\n", " ").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + "..."


def _debug_candidate(stage: str, attempt: int, text: str, headers: list[str], policy: dict[str, int]) -> None:
    try:
        print("[ai_report][candidate]", {
            "stage": stage,
            "attempt": attempt,
            "chars": len(text or ""),
            "header_count": _header_count(text or "", headers),
            "present_headers": _extract_present_headers(text or "", headers),
            "looks_truncated": _looks_truncated(text or ""),
            "min_chars": policy.get("min_chars"),
            "preview": _debug_preview(text or ""),
        })
    except Exception:
        pass


def _debug_retry_decision(stage: str, attempt: int, reason: str, current_text: str, best_text: str) -> None:
    try:
        print("[ai_report][retry_decision]", {
            "stage": stage,
            "attempt": attempt,
            "reason": reason,
            "current_chars": len(current_text or ""),
            "best_chars": len(best_text or ""),
            "current_preview": _debug_preview(current_text or ""),
        })
    except Exception:
        pass


def _debug_final_choice(stage: str, chosen_text: str, headers: list[str], policy: dict[str, int], note: str) -> None:
    try:
        print("[ai_report][final_choice]", {
            "stage": stage,
            "note": note,
            "chars": len(chosen_text or ""),
            "header_count": _header_count(chosen_text or "", headers),
            "present_headers": _extract_present_headers(chosen_text or "", headers),
            "looks_truncated": _looks_truncated(chosen_text or ""),
            "min_chars": policy.get("min_chars"),
            "preview": _debug_preview(chosen_text or ""),
        })
    except Exception:
        pass

def _clean_intro(text: str) -> str:
    body = (text or "").strip()
    if not body:
        return ""
    lines = [ln.rstrip() for ln in body.splitlines()]
    drop_prefixes = (
        "この度は",
        "ご依頼いただきありがとうございます",
        "ありがとうございます",
        "あなたの星の配置から",
        "現在の状況とこれからの流れを",
        "---",
    )
    cleaned = []
    started = False
    for ln in lines:
        stripped = ln.strip()
        if not started:
            if not stripped:
                continue
            if stripped.startswith("### "):
                started = True
                cleaned.append(stripped)
                continue
            if stripped.startswith(drop_prefixes):
                continue
            # if a non-heading intro remains, skip until first heading for web reports
            continue
        cleaned.append(ln)
    return "\n".join(cleaned).strip() if cleaned else body




def _is_incomplete_web(text: str, headers: list[str] | None = None) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    if headers:
        found = _header_count(t, headers)
        needed = 7 if len(headers) >= 8 else len(headers)
        if found < needed:
            return True
    if not t.endswith(("。", "！", "？", "」", "』", "】")):
        return True
    return False


def _make_continue_prompt(*, previous_text: str, headers: list[str] | None = None) -> str:
    tpl = _read_prompt_file("continue_web.txt")
    header_text = " / ".join(headers or [])
    return _render_prompt(tpl, {"previous_text": previous_text, "headers": header_text})
def _block_header_groups(theme: str, compat_mode: bool, detail_level: str, is_line: bool) -> list[list[str]]:
    headers = _section_headers(theme, compat_mode=compat_mode)
    if is_line or not headers:
        return [headers]
    level = str(detail_level or "standard").strip().lower()
    if len(headers) <= 4:
        if level == "detailed":
            return [headers[:2], headers[2:4]]
        return [headers]
    if level == "detailed":
        return [headers[0:2], headers[2:4], headers[4:6], headers[6:8]]
    if level == "standard":
        return [headers[0:4], headers[4:8]]
    return [headers]


def _build_yaml_only_text(*, ctx: dict[str, Any], report_type: str, compat_mode: bool) -> str:
    title = "compatibility" if compat_mode else "single"
    parts = [
        f"mode: yaml_only",
        f"report_type: {report_type}",
        f"analysis_scope: {title}",
        f"theme: {ctx.get('theme') or 'overall'}",
        f"user_name: {ctx.get('user_name') or 'あなた'}",
        "",
        "astro_digest: |",
    ]
    for line in str(ctx.get("astro_digest") or "").splitlines()[:60]:
        parts.append(f"  {line}")
    structure = str(ctx.get("structure_summary") or "").strip()
    if structure:
        parts.extend(["", "structure_summary: |"])
    for line in structure.splitlines()[:80]:
        parts.append(f"  {line}")
    transit = str(ctx.get("transit_summary") or "").strip()
    if transit:
        parts.extend(["", "transit_summary: |"])
    for line in transit.splitlines()[:50]:
        parts.append(f"  {line}")
    return "\n".join(parts).strip()


def _call_model_once(*, client: Any, model_name: str, prompt: str, max_tokens: int, temperature: float, top_p: float = 0.95) -> tuple[str, Any]:
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


def _section_length_rule(detail_level: str, headers: list[str]) -> str:
    level = str(detail_level or "standard").lower()
    if len(headers) >= 8:
        if level == "detailed":
            return "各章は3〜5文で、章ごとに350〜650字程度を目安にしてください。"
        if level == "standard":
            return "各章は2〜4文で、章ごとに220〜450字程度を目安にしてください。"
        return "各章は2〜3文で、章ごとに150〜320字程度を目安にしてください。"
    return "各見出しを短すぎず、最後まで書き切ってください。"


def _build_single_prompt(base_prompt: str, *, headers: list[str], policy: dict[str, int], detail_level: str) -> str:
    extra = [
        "",
        "【出力の必須ルール】",
        "- 冒頭の挨拶文は禁止です。すぐに最初の見出しから書き始めてください。",
        f"- 最低{policy['min_chars']}文字以上、目安は{policy['target_chars']}文字前後です。",
        "- 見出しを省略せず、最後まで書き切ってください。",
        "- 途中で終わる文章は禁止です。",
        "- 同じ意味の言い換えで水増しせず、具体的な場面や行動描写で厚みを出してください。",
        _section_length_rule(detail_level, headers),
    ]
    if headers:
        extra.append("- 必ず次の見出しをこの順番で使ってください: " + " / ".join(headers))
    return base_prompt.rstrip() + "\n\n" + "\n".join(extra)


def _build_block_prompt(base_prompt: str, *, all_headers: list[str], block_headers: list[str], policy: dict[str, int], detail_level: str, is_first: bool) -> str:
    extra = [
        "",
        "【出力の必須ルール】",
        "- 冒頭の挨拶文は禁止です。すぐに見出しから書き始めてください。",
        "- 指定された見出しだけを書いてください。他の章は書かないでください。",
        "- 途中で終わる文章は禁止です。",
        "- 重複説明を避け、各章ごとに別の論点を担当させてください。",
        _section_length_rule(detail_level, block_headers),
        "- このブロックで使う見出し: " + " / ".join(block_headers),
        "- 全体の見出し構成: " + " / ".join(all_headers),
    ]
    if is_first:
        extra.append(f"- このブロックだけで最低{max(500, policy['min_chars']//2)}文字以上を目安にしてください。")
    else:
        extra.append(f"- このブロックだけで最低{max(450, policy['min_chars']//3)}文字以上を目安にしてください。")
    return base_prompt.rstrip() + "\n\n" + "\n".join(extra)


def _generate_single_report(*, client: Any, model_name: str, model_source: str, prompt: str, headers: list[str], policy: dict[str, int], detail_level: str) -> str:
    best_text = ""
    best_score = -1
    working_prompt = _build_single_prompt(prompt, headers=headers, policy=policy, detail_level=detail_level)
    for attempt in range(2):
        text1, resp = _call_model_once(
            client=client,
            model_name=model_name,
            prompt=working_prompt,
            max_tokens=policy["max_tokens"],
            temperature=0.2 if _is_flash_model(model_name) else 0.15,
        )
        _log_usage(resp, model_name=model_name, source=model_source, attempt=attempt + 1, stage="single")
        text1 = _clean_intro(text1)
        _debug_candidate("single", attempt + 1, text1, headers, policy)
        score = len(text1) + (_header_count(text1, headers) * 1000)
        if score > best_score:
            best_score = score
            best_text = text1
            _debug_final_choice("single-best-update", best_text, headers, policy, note="updated_best")
        enough = len(text1) >= policy["min_chars"] and _has_required_headers(text1, headers) and not _looks_truncated(text1)
        if enough:
            _debug_final_choice("single", text1, headers, policy, note="accepted_without_retry")
            return fix_punctuation(text1)
        reasons: list[str] = []
        if len(text1) < policy["min_chars"]:
            reasons.append(f"too_short<{policy['min_chars']}")
        if headers and not _has_required_headers(text1, headers):
            reasons.append("missing_headers")
        if _looks_truncated(text1):
            reasons.append("looks_truncated")
        _debug_retry_decision("single", attempt + 1, " / ".join(reasons) or "unknown", text1, best_text)
        working_prompt += "\n\n【再指示】前回の出力は短すぎる、または見出しが不足しています。冒頭挨拶を入れず、未出力の見出しも含めて最初から最後まで完全版を書き直してください。"
        time.sleep(0.4)
    if best_text:
        _debug_final_choice("single", best_text, headers, policy, note="return_best_after_retry")
        return fix_punctuation(best_text)
    raise RuntimeError("single generation failed")


def _generate_multipart_report(*, client: Any, model_name: str, model_source: str, prompt: str, headers: list[str], policy: dict[str, int], detail_level: str, compat_mode: bool, theme: str, is_line: bool) -> str:
    groups = _block_header_groups(theme, compat_mode, detail_level, is_line)
    parts: list[str] = []
    for idx, group in enumerate(groups, start=1):
        block_prompt = _build_block_prompt(prompt, all_headers=headers, block_headers=group, policy=policy, detail_level=detail_level, is_first=(idx == 1))
        block_best = ""
        for attempt in range(2):
            text1, resp = _call_model_once(
                client=client,
                model_name=model_name,
                prompt=block_prompt,
                max_tokens=max(1100, min(policy["max_tokens"], 1800 if detail_level == "detailed" else 1500)),
                temperature=0.2 if _is_flash_model(model_name) else 0.15,
            )
            _log_usage(resp, model_name=model_name, source=model_source, attempt=attempt + 1, stage=f"block{idx}")
            text1 = _clean_intro(text1)
            _debug_candidate(f"block{idx}", attempt + 1, text1, group, policy)
            if len(text1) > len(block_best):
                block_best = text1
            enough = _has_required_headers(text1, group) and not _looks_truncated(text1) and len(text1) >= (450 if detail_level == "detailed" else 280)
            if enough:
                parts.append(text1)
                break
            _debug_retry_decision(f"block{idx}", attempt + 1, "block_retry", text1, block_best)
            block_prompt += "\n\n【再指示】前回の出力は短すぎるか、指定見出しが不足しています。このブロックの見出しだけを、冒頭挨拶なしで最後まで書き切ってください。"
            time.sleep(0.35)
        else:
            if block_best:
                parts.append(block_best)
    combined = "\n\n".join(p for p in parts if p.strip())
    combined = _clean_intro(combined)
    if combined and len(combined) >= policy["min_chars"] and _header_count(combined, headers) >= max(4, len(headers) - 1):
        _debug_final_choice("multipart", combined, headers, policy, note="accepted_before_rescue")
        _debug_final_choice("multipart", combined, headers, policy, note="return_after_rescue")
    return fix_punctuation(combined)
    # one rescue pass for missing headers
    missing = [h for h in headers if h not in combined]
    if missing:
        rescue_prompt = prompt.rstrip() + "\n\n【不足見出しの追記】\n- 冒頭挨拶は禁止です。\n- 次の不足見出しだけを書いてください: " + " / ".join(missing)
        text2, resp2 = _call_model_once(
            client=client,
            model_name=model_name,
            prompt=rescue_prompt,
            max_tokens=max(900, min(policy["max_tokens"], 1400)),
            temperature=0.2 if _is_flash_model(model_name) else 0.15,
        )
        _log_usage(resp2, model_name=model_name, source=model_source, attempt=1, stage="rescue")
        text2 = _clean_intro(text2)
        if text2:
            combined = (combined + "\n\n" + text2).strip()
    return fix_punctuation(combined)


def generate_report(
    astro_data: dict[str, Any],
    *,
    style: str | None = None,
    report_type: str | None = None,
    meta: dict[str, Any] | None = None,
) -> str:
    base_meta = _safe_get_meta(astro_data)
    meta2 = _merge_meta(base_meta, meta)
    if style:
        meta2["output_style"] = style
    rt = _normalize_report_type(report_type)
    output_style = (style or meta2.get("output_style", "web") or "web").strip().lower()
    detail_level = (meta2.get("detail_level", "standard") or "standard").strip().lower()
    astrology_system = (meta2.get("astrology_system", "western") or "western").strip().lower()
    theme = (meta2.get("theme", "overall") or "overall").strip().lower()
    user_message = meta2.get("message", "")
    observations_text = (meta2.get("observations_text", "") or "").strip()
    user_name = (meta2.get("user_name") or meta2.get("name") or "")
    display_name = user_name if user_name not in ("", "あなた") else ""
    requested_model = _normalize_requested_model(meta2.get("ai_model")) if isinstance(meta2, dict) else None
    model_name, model_source = _resolve_model_name(requested_model)
    compat_mode = rt in {"compat_web", "compat_line"}
    is_line = (output_style == "line" or "line" in rt)
    astro_digest = _build_prompt_astro_digest(astro_data, compat_mode=compat_mode)
    structure_summary = _limit_text(_build_structure_summary(astro_data), 2400 if compat_mode else 3000)
    transit_summary = _limit_text(_build_transit_summary(astro_data), 1200)
    available_systems = _detect_available_systems(astro_data, astrology_system)
    age_years = meta2.get("age_years", "未計算")
    life_phase_label, life_phase_theme = _life_phase(age_years)
    transit_focus = _transit_focus(age_years, available_systems)
    ctx: dict[str, Any] = {
        "astro_data": astro_digest,
        "astro_digest": astro_digest,
        "structure_summary": structure_summary,
        "astrology_system": astrology_system,
        "theme": theme,
        "user_message": user_message,
        "observations_text": observations_text,
        "birth_date": meta2.get("birth_date", "未取得"),
        "today": meta2.get("today", "未取得"),
        "age_years": age_years,
        "era_title": meta2.get("era_title", "いまの転換期"),
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
    }
    common_rules_tpl = _read_prompt_file("common_rules.txt")
    ctx["common_rules"] = _render_prompt(common_rules_tpl, ctx)
    yaml_only = _truthy(meta2.get("yaml_only"))
    generate_ai_flag = meta2.get("generate_ai", True)
    if yaml_only or (generate_ai_flag is False):
        return _build_yaml_only_text(ctx=ctx, report_type=rt, compat_mode=compat_mode)
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return "GEMINI_API_KEY が未設定です"
    if genai is None or types is None:
        return "google-genai が読み込めません（requirements.txt を確認）"
    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        return f"Gemini client 初期化エラー: {e} / {_debug_model_info()}"
    single_web_template_name = "single_web_timing.txt" if theme == "timing" else "single_web.txt"
    prompt_map = {
        "single_web": _render_prompt(_read_prompt_file(single_web_template_name), ctx),
        "single_line": _render_prompt(_read_prompt_file("single_line.txt"), ctx),
        "single_web_reader": _render_prompt(_read_prompt_file("single_web_reader.txt"), ctx),
        "single_line_reader": _render_prompt(_read_prompt_file("single_line_reader.txt"), ctx),
        "compat_web": _render_prompt(_read_prompt_file("compat_web.txt"), ctx),
        "compat_line": _render_prompt(_read_prompt_file("compat_line.txt"), ctx),
    }
    if theme == "free_reading":
        prompt_map["single_web"] = _render_prompt(_read_prompt_file("free_reading_web.txt"), ctx)
    prompt = prompt_map.get(rt) or prompt_map["single_web"]
    if rt == "single_web" and output_style == "line":
        prompt = prompt_map["single_line"]
    elif rt == "single_web_reader" and output_style == "line":
        prompt = prompt_map["single_line_reader"]
    elif rt == "compat_web" and output_style == "line":
        prompt = prompt_map["compat_line"]
    policy = _detail_policy(detail_level, is_line=is_line, compat_mode=compat_mode, theme=theme, report_type=rt)
    headers = [] if is_line or "reader" in rt else _section_headers(theme, compat_mode=compat_mode)
    try:
        legacy_single_mode = (rt in {"single_web", "compat_web"}) and not is_line and "reader" not in rt
        if legacy_single_mode:
            print("=== AI REPORT LEGACY SINGLE COMPLETE ===", {
                "report_type": rt,
                "detail_level": detail_level,
                "theme": theme,
                "model": model_name,
            })
            text = _generate_single_report(
                client=client,
                model_name=model_name,
                model_source=model_source,
                prompt=prompt,
                headers=headers,
                policy=policy,
                detail_level=detail_level,
            )
            best_text = text or ""
            continue_round = 0
            while continue_round < 2 and _is_incomplete_web(best_text, headers=headers):
                continue_round += 1
                continue_prompt = _make_continue_prompt(previous_text=best_text, headers=headers)
                text2, resp2 = _call_model_once(
                    client=client,
                    model_name=model_name,
                    prompt=continue_prompt,
                    max_tokens=max(1400, policy["max_tokens"]),
                    temperature=0.15 if _is_flash_model(model_name) else 0.1,
                )
                _log_usage(resp2, model_name=model_name, source=model_source, attempt=continue_round, stage="continue")
                text2 = _clean_intro(text2)
                if text2.strip():
                    merged = (best_text.rstrip() + "\n\n" + text2.strip()).strip()
                else:
                    merged = best_text
                _debug_candidate("continue", continue_round, merged, headers, policy)
                if len(merged) > len(best_text):
                    best_text = merged
                else:
                    break
            text = best_text
        else:
            text = _generate_single_report(
                client=client,
                model_name=model_name,
                model_source=model_source,
                prompt=prompt,
                headers=headers,
                policy=policy,
                detail_level=detail_level,
            )
        if text and len(text.strip()) >= max(300, policy["min_chars"] // 2):
            return fix_punctuation(text)
        return fix_punctuation(text)
    except Exception as e:
        if is_line:
            return fix_punctuation(_line_fallback_text(meta2))
        return f"AI生成エラー: {e} / {_debug_model_info(requested_model)}"
