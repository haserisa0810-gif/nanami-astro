from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None  # type: ignore
    types = None  # type: ignore

try:
    from anthropic import Anthropic
except Exception:
    Anthropic = None  # type: ignore

try:
    from services.style_prompts import STYLE_PROMPTS
except Exception:
    try:
        from services.style_prompts import STYLE_PROMPTS  # type: ignore
    except Exception:
        STYLE_PROMPTS = {}  # type: ignore

try:
    from services.analysis_engine import detect_age_mode
    from services.prompt_builder import build_role_prompt, get_age_system_prompt
except Exception:
    from services.analysis_engine import detect_age_mode  # type: ignore
    from services.prompt_builder import build_role_prompt, get_age_system_prompt  # type: ignore

DEFAULT_AUTO_MODEL = "gemini-2.5-flash-lite"
PRO_MODEL = "gemini-2.5-pro"
FLASH_LITE_MODEL = "gemini-2.5-flash-lite"
FLASH_MODEL = "gemini-2.5-flash"
ALLOWED_MODEL_NAMES = {"gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-2.5-pro"}

CLAUDE_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_SONNET_MODEL = "claude-sonnet-4-6"
CLAUDE_ALLOWED_MODEL_NAMES = {"claude-haiku-4-5-20251001", "claude-4-5-haiku-latest", "claude-sonnet-4-5", "claude-sonnet-4-6"}

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


def _build_vedic_focus_summary(astro_data: dict[str, Any], meta2: dict[str, Any]) -> str:
    vedic = astro_data.get("vedic") if isinstance(astro_data, dict) else None
    if not isinstance(vedic, dict):
        return ""

    parts: list[str] = []

    asc = vedic.get("ascendant") or {}
    moon_nak = vedic.get("moon_nakshatra") or {}
    house_lords = vedic.get("house_lords_placement") or {}
    planets = vedic.get("planets") or []

    if isinstance(asc, dict):
        asc_sign = asc.get("rashi_name") or asc.get("sign") or ""
        asc_nak = asc.get("nakshatra_name") or ""
        if asc_sign or asc_nak:
            parts.append(f"ラグナは{asc_sign}、ナクシャトラは{asc_nak}。")

    if isinstance(moon_nak, dict):
        moon_nak_name = moon_nak.get("nakshatra_name") or ""
        moon_pada = moon_nak.get("pada") or ""
        moon_lord = moon_nak.get("lord") or ""
        if moon_nak_name:
            parts.append(f"月のナクシャトラは{moon_nak_name}、パーダは{moon_pada}、支配星は{moon_lord}。")

    if isinstance(house_lords, dict):
        for house_key in ("1", "12", "7", "9"):
            info = house_lords.get(house_key) or {}
            if isinstance(info, dict) and info:
                parts.append(
                    f"{house_key}室支配星は{info.get('lord', '')}で、第{info.get('placed_in_house', '?')}ハウス配置。"
                )

    if isinstance(planets, list):
        rahu_house = None
        ketu_house = None
        for pp in planets:
            if not isinstance(pp, dict):
                continue
            name = str(pp.get("name") or "").strip().lower()
            if name in {"rahu", "north node"}:
                rahu_house = pp.get("house_no")
            elif name in {"ketu", "south node"}:
                ketu_house = pp.get("house_no")
        if rahu_house or ketu_house:
            parts.append(f"ラーフは第{rahu_house or '?'}ハウス、ケートゥは第{ketu_house or '?'}ハウス。")

    return " ".join([pp for pp in parts if pp]).strip()


def _extract_shichu_status(astro_data: dict[str, Any], structure_summary_obj: dict[str, Any] | None = None) -> tuple[bool, str, str]:
    ss = structure_summary_obj or {}
    candidates = []
    if isinstance(astro_data.get("shichusuimei"), dict):
        candidates.append(astro_data.get("shichusuimei") or {})
    if isinstance(ss.get("shichusuimei"), dict):
        candidates.append(ss.get("shichusuimei") or {})
    if isinstance(astro_data.get("pillars"), dict):
        candidates.append({"pillars": astro_data.get("pillars")})
    if isinstance(ss.get("pillars"), dict):
        candidates.append({"pillars": ss.get("pillars")})

    for c in candidates:
        if not isinstance(c, dict):
            continue
        if c.get("day_master") or c.get("pillars") or (c.get("normalized_data") or {}).get("pillars"):
            return True, "available", "四柱推命データあり"
    return False, "missing", "四柱推命データなし"


def _build_prompt_control_bundle(meta2: dict[str, Any], auto_recommendation: dict[str, Any] | None) -> dict[str, Any]:
    auto_recommendation = auto_recommendation or {}
    age_mode = str(meta2.get("age_mode") or auto_recommendation.get("age_mode") or detect_age_mode(meta2.get("birth_date"))).strip().lower()

    role_distribution = auto_recommendation.get("role_distribution") or auto_recommendation.get("distribution") or meta2.get("role_distribution") or meta2.get("distribution")
    if not isinstance(role_distribution, dict):
        role_distribution = {"western": 45, "shichu": 45, "vedic": 10}

    vedic_level = str((auto_recommendation.get("vedic_trigger") or {}).get("level") or meta2.get("vedic_level") or "off").strip().lower()
    structured_mode = str(meta2.get("style") or meta2.get("reading_style") or auto_recommendation.get("suggested_reading_style") or "general").strip().lower()
    if structured_mode not in {"structured", "general"}:
        structured_mode = "general"

    return {
        "age_mode": age_mode,
        "structured_mode": structured_mode,
        "vedic_level": vedic_level,
        "age_system_prompt": get_age_system_prompt(age_mode),
        "role_prompt": build_role_prompt(role_distribution, age_mode=age_mode, structured_mode=structured_mode, vedic_level=vedic_level),
        "role_distribution": role_distribution,
    }



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
        _debug_structure_summary_input("_build_structure_summary", astro_data)
        if not isinstance(astro_data, dict):
            return ""
        derived: dict[str, Any] = {}
        try:
            from services.structure_engine import analyze_structure, derive_risk_flags  # type: ignore
            planets = _extract_planets(astro_data)
            cusps = _extract_house_cusps(astro_data)
            if planets:
                structure = analyze_structure(planets, cusps)
                derived["structure"] = structure
                derived["risk_flags"] = derive_risk_flags(structure)
        except Exception:
            pass
        keys = [
            "planets", "houses", "aspects", "angles", "skipped_bodies", "ephemeris",
            "nakshatra", "strength", "structure",
            "western", "vedic", "shichusuimei", "pillars",
            "personA", "personB", "synastry",
        ]
        picked: dict[str, Any] = {}
        for k in keys:
            v = astro_data.get(k)
            if v is not None:
                picked[k] = v
        if derived:
            picked["_derived"] = derived
        if not picked:
            top_keys = [k for k in astro_data.keys() if k not in ("meta", "_meta")]
            return f"available_keys: {top_keys[:60]}"
        return json.dumps(picked, ensure_ascii=False)
    except Exception:
        return ""


def _debug_structure_summary_input(label: str, astro_data: Any) -> None:
    try:
        data = astro_data if isinstance(astro_data, dict) else {}
        shichu = data.get("shichusuimei")
        print("[ai_report][structure_input]", {
            "label": label,
            "top_keys": list(data.keys())[:30],
            "has_shichusuimei": isinstance(shichu, dict),
            "shichu_keys": list(shichu.keys())[:20] if isinstance(shichu, dict) else [],
            "has_day_master": bool((shichu or {}).get("day_master")) if isinstance(shichu, dict) else False,
            "has_pillars": bool((shichu or {}).get("pillars")) if isinstance(shichu, dict) else False,
        })
    except Exception:
        pass


def _make_continue_prompt(*, previous_text: str) -> str:
    tpl = _read_prompt_file("continue_web.txt")
    return _render_prompt(tpl, {"previous_text": previous_text})


def _is_incomplete_web(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    return ("7. 実践アクション" not in t) or ("6. 成長と転換期" not in t)


def _normalize_report_type(report_type: str | None) -> str:
    rt = (report_type or "").strip().lower()
    if rt in (
        "single_web", "single_line", "single_web_reader", "single_line_reader", "compat_web", "compat_line",
    ):
        return rt
    if rt in ("compatibility", "compat"):
        return "compat_web"
    if rt in ("single", ""):
        return "single_web"
    return "single_web"


def _normalize_requested_model(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    aliases = {
        "lite": FLASH_LITE_MODEL,
        "flash-lite": FLASH_LITE_MODEL,
        "flash": FLASH_MODEL,
        "pro": PRO_MODEL,
    }
    if not raw or raw == "auto":
        return None
    normalized = aliases.get(raw, raw)
    return normalized if normalized in ALLOWED_MODEL_NAMES else None


def _normalize_requested_claude_model(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    aliases = {
        "haiku": CLAUDE_DEFAULT_MODEL,
        "sonnet": "claude-sonnet-4-6",
        "pro": "claude-sonnet-4-6",
    }
    if not raw or raw == "auto":
        return None
    normalized = aliases.get(raw, raw)
    return normalized if normalized in CLAUDE_ALLOWED_MODEL_NAMES else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def _choose_auto_model(meta2: dict[str, Any], astrology_system: str) -> tuple[str, str]:
    include_asteroids = _truthy(meta2.get("include_asteroids"))
    include_chiron = _truthy(meta2.get("include_chiron"))
    include_lilith = _truthy(meta2.get("include_lilith"))
    include_vertex = _truthy(meta2.get("include_vertex"))
    has_heavy_options = include_asteroids or include_chiron or include_lilith or include_vertex

    sys_name = (astrology_system or "western").strip().lower()
    if sys_name in {"integrated3", "integrated_3"}:
        return PRO_MODEL, "auto:integrated3"
    if sys_name == "integrated":
        if has_heavy_options:
            return PRO_MODEL, "auto:integrated+options"
        return FLASH_LITE_MODEL, "auto:integrated"
    if sys_name == "integrated_w_shichu":
        return FLASH_LITE_MODEL, "auto:integrated_w_shichu"
    return FLASH_LITE_MODEL, f"auto:{sys_name or 'western'}"


def _resolve_model_name(meta2: dict[str, Any], astrology_system: str) -> tuple[str, str]:
    if _truthy(meta2.get("allow_manual_ai_model")):
        requested = _normalize_requested_model(meta2.get("ai_model"))
        if requested:
            return requested, "manual"
    force_model = _normalize_requested_model(os.getenv("GEMINI_MODEL_FORCE"))
    if force_model:
        return force_model, "env_force"
    return _choose_auto_model(meta2, astrology_system)


def _should_use_claude(meta2: dict[str, Any], astrology_system: str) -> bool:
    requested_provider = str(meta2.get("ai_provider") or "").strip().lower()
    if requested_provider in {"gemini", "google"}:
        return False
    if requested_provider in {"claude", "anthropic"}:
        return True

    env_force = str(os.getenv("AI_PROVIDER_FORCE") or "").strip().lower()
    if env_force in {"gemini", "google"}:
        return False
    if env_force in {"claude", "anthropic"}:
        return True

    if _truthy(meta2.get("is_free_reading")) or _truthy(meta2.get("free_reading")):
        return False

    sys_name = (astrology_system or "western").strip().lower()
    return sys_name in {"western", "integrated", "integrated3", "integrated_3", "integrated_w_shichu"}


def _resolve_claude_model_name(meta2: dict[str, Any], astrology_system: str = "western") -> tuple[str, str]:
    if _truthy(meta2.get("allow_manual_ai_model")):
        requested = _normalize_requested_claude_model(meta2.get("ai_model"))
        if requested:
            return requested, "manual"

    force_model = _normalize_requested_claude_model(os.getenv("CLAUDE_MODEL_FORCE"))
    if force_model:
        return force_model, "env_force"

    sys_name = (astrology_system or "western").strip().lower()
    source_label = {
        "integrated": "auto:integrated",
        "integrated3": "auto:integrated3",
        "integrated_3": "auto:integrated3",
    }.get(sys_name, "auto:western")
    return CLAUDE_DEFAULT_MODEL, source_label


def _debug_model_info(model_name: str, model_source: str, fallback_used: bool = False, provider: str = "") -> str:
    return (
        f"[debug] provider={provider}, resolved_model={model_name}, "
        f"source={model_source}, fallback_used={fallback_used}, "
        f"env_GEMINI_MODEL_FORCE={repr(os.getenv('GEMINI_MODEL_FORCE'))}, "
        f"default_auto_model={DEFAULT_AUTO_MODEL}"
    )


def _select_prompt_files(astrology_system: str, rt: str, output_style: str, theme: str, use_claude: bool = False, reading_style: str = "general") -> dict[str, str]:
    sys_name = (astrology_system or "western").strip().lower()
    use_legacy_integrated = sys_name in {"integrated", "integrated3", "integrated_3"}
    if use_claude:
        _style_map = {
            "love":         "single_web_claude_love.txt",
            "work":         "single_web_claude_work.txt",
            "relationship": "single_web_claude_relationship.txt",
            "timing":       "single_web_claude_timing.txt",
        }
        if sys_name == "integrated_w_shichu":
            _w_shichu_style_map = {
                "love":         "single_web_claude_w_shichu_love.txt",
                "work":         "single_web_claude_w_shichu_work.txt",
                "relationship": "single_web_claude_w_shichu_relationship.txt",
                "timing":       "single_web_claude_w_shichu_timing.txt",
            }
            single_web_name = _w_shichu_style_map.get(reading_style, "single_web_claude_w_shichu.txt")
        else:
            single_web_name = _style_map.get(reading_style, "single_web_claude.txt")
    else:
        if theme == "free_reading":
            single_web_name = "free_reading_web.txt"
        elif theme == "timing":
            single_web_name = "single_web_timing.txt"
        else:
            single_web_name = "single_web.txt"
    single_web_reader_name = "single_web_reader.txt"
    if not use_claude and use_legacy_integrated:
        single_web_name = "single_web_legacy_integrated.txt"
        single_web_reader_name = "single_web_reader_legacy.txt"
    return {
        "single_web": single_web_name,
        "single_line": "single_line.txt",
        "single_web_reader": single_web_reader_name,
        "single_line_reader": "single_line_reader.txt",
        "compat_web": "compat_web.txt",
        "compat_line": "compat_line.txt",
    }


def _generate_once(*, client: Any, model_name: str, prompt: str, max_tokens: int) -> str:
    config = types.GenerateContentConfig(
        temperature=0.1,
        top_p=0.95,
        max_output_tokens=max_tokens,
    )
    resp = client.models.generate_content(model=model_name, contents=prompt, config=config)
    return (_extract_text(resp) or "").strip()


# システムプロンプトとして渡すペルソナ（Claudeのみ）
_CLAUDE_SYSTEM_PROMPT = (
    "あなたは占い師・星月七海（ほしつきななみ）です。"
    "「生きづらさを読み解く」がコンセプトです。"
    "読み手に寄り添いながら、星の配置を感覚的な言葉で丁寧に伝えてください。"
)


def _generate_once_claude(*, client: Any, model_name: str, prompt: str, max_tokens: int, system_prompt: str | None = None) -> str:
    resp = client.messages.create(
        model=model_name,
        max_tokens=max_tokens,
        temperature=0.75,
        system=(system_prompt or _CLAUDE_SYSTEM_PROMPT),
        messages=[{"role": "user", "content": prompt}],
    )
    buf: list[str] = []
    for block in getattr(resp, "content", []) or []:
        if getattr(block, "type", "") == "text":
            t = getattr(block, "text", "")
            if isinstance(t, str) and t.strip():
                buf.append(t)
    return "\n".join(buf).strip()


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
    detail_level = (meta2.get("detail_level", "standard") or "standard").strip()
    astrology_system = (meta2.get("astrology_system", "western") or "western").strip().lower()
    reading_style = str(meta2.get("style") or meta2.get("reading_style") or "general").strip().lower()

    birth_date = meta2.get("birth_date", "未取得")
    today = meta2.get("today", "未取得")
    age_years = meta2.get("age_years", "未計算")
    era_title = meta2.get("era_title", "いまの転換期")
    theme = meta2.get("theme", "overall")
    user_message = meta2.get("message", "")
    observations_text = (meta2.get("observations_text", "") or "").strip()
    user_name = meta2.get("user_name", "あなた")

    structure_summary = _build_structure_summary(astro_data)
    try:
        ss_obj = json.loads(structure_summary) if isinstance(structure_summary, str) and structure_summary.strip().startswith("{") else {}
        print("[ai_report][structure_summary]", {
            "has_shichusuimei": isinstance((ss_obj or {}).get("shichusuimei"), dict),
            "has_pillars": isinstance((ss_obj or {}).get("pillars"), dict),
            "keys": list((ss_obj or {}).keys())[:30] if isinstance(ss_obj, dict) else [],
        })
    except Exception:
        ss_obj = {}

    auto_recommendation = meta2.get("auto_recommendation") if isinstance(meta2.get("auto_recommendation"), dict) else {}
    prompt_controls = _build_prompt_control_bundle(meta2, auto_recommendation)
    has_shichu, shichu_status, shichu_reason = _extract_shichu_status(astro_data, ss_obj)

    auto_rec = meta2.get("auto_recommendation") or {}
    selected_systems = auto_rec.get("selected_systems") or []
    vedic_trigger = auto_rec.get("vedic_trigger") or {}
    vedic_level = str(vedic_trigger.get("level") or "off").strip().lower()
    vedic_score = int(vedic_trigger.get("score") or 0)
    vedic_reasons = vedic_trigger.get("reasons") or []
    manual_vedic_on = "vedic" in [str(x).lower() for x in selected_systems]
    must_use_vedic = (
        manual_vedic_on or (
            astrology_system in {"integrated", "integrated3", "integrated_3", "integrated_w_shichu", "vedic"}
            and vedic_level in {"light", "strong", "manual_on"}
        )
    )
    vedic_focus_summary = _build_vedic_focus_summary(astro_data, meta2)

    ctx: dict[str, Any] = {
        "astro_data": astro_data,
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
        "user_name": user_name,
        "auto_recommendation": json.dumps(auto_rec, ensure_ascii=False),
        "vedic_trigger_level": vedic_level,
        "vedic_trigger_score": vedic_score,
        "vedic_trigger_reasons": " / ".join([str(x) for x in vedic_reasons[:5]]),
        "must_use_vedic": "true" if must_use_vedic else "false",
        "vedic_focus_summary": vedic_focus_summary,
        "age_mode": prompt_controls["age_mode"],
        "has_shichu": str(bool(has_shichu)).lower(),
        "shichu_status": shichu_status,
        "shichu_reason": shichu_reason,
        "role_prompt": prompt_controls["role_prompt"],
        "age_system_prompt": prompt_controls["age_system_prompt"],
    }
    # 四柱推命データを明示的に取り出してプロンプトに渡す
    _shichu_raw = astro_data.get("shichusuimei") or astro_data.get("pillars") or ss_obj.get("shichusuimei") or ss_obj.get("pillars")
    ctx["shichu_data"] = json.dumps(_shichu_raw, ensure_ascii=False) if _shichu_raw else ""

    if must_use_vedic and vedic_focus_summary:
        ctx["user_message"] = (
            f"{user_message}\n\n"
            f"【今回の追加指示】\n"
            f"今回はインド占星術の視点を本文に必ず取り入れてください。\n"
            f"補助要約: {vedic_focus_summary}"
        )

    common_rules_tpl = _read_prompt_file("common_rules.txt")
    ctx["common_rules"] = _render_prompt(common_rules_tpl, ctx)

    use_claude = _should_use_claude(meta2, astrology_system)
    prompt_files = _select_prompt_files(astrology_system, rt, output_style, theme, use_claude=use_claude, reading_style=reading_style)
    prompt_prefix = (ctx.get("age_system_prompt", "") + "\n\n" + ctx.get("role_prompt", "")).strip()

    def _with_prefix(body: str) -> str:
        return (prompt_prefix + "\n\n" + body).strip() if prompt_prefix else body

    single_web_prompt = _with_prefix(_render_prompt(_read_prompt_file(prompt_files["single_web"]), ctx))
    single_line_prompt = _with_prefix(_render_prompt(_read_prompt_file(prompt_files["single_line"]), ctx))
    single_web_reader_prompt = _with_prefix(_render_prompt(_read_prompt_file(prompt_files["single_web_reader"]), ctx))
    single_line_reader_prompt = _with_prefix(_render_prompt(_read_prompt_file(prompt_files["single_line_reader"]), ctx))
    compat_web_prompt = _with_prefix(_render_prompt(_read_prompt_file(prompt_files["compat_web"]), ctx))
    compat_line_prompt = _with_prefix(_render_prompt(_read_prompt_file(prompt_files["compat_line"]), ctx))

    guard = ""
    if astrology_system == "integrated":
        guard = _read_prompt_file("guard_integrated.txt").strip()
    elif astrology_system in {"integrated3", "integrated_3"}:
        guard = _read_prompt_file("guard_integrated3.txt").strip()
    if guard:
        single_web_prompt += "\n\n" + guard
        single_line_prompt += "\n\n" + guard
        single_web_reader_prompt += "\n\n" + guard
        single_line_reader_prompt += "\n\n" + guard
        compat_web_prompt += "\n\n" + guard
        compat_line_prompt += "\n\n" + guard

    if astrology_system == "vedic":
        vedic_guard = _read_prompt_file("guard_vedic.txt").strip()
        if vedic_guard:
            single_web_prompt = vedic_guard + "\n\n" + single_web_prompt
            single_line_prompt = vedic_guard + "\n\n" + single_line_prompt
            single_web_reader_prompt = vedic_guard + "\n\n" + single_web_reader_prompt

    style_extra = STYLE_PROMPTS.get(reading_style, "")
    if style_extra:
        single_web_prompt += "\n\n" + style_extra
        single_line_prompt += "\n\n" + style_extra
        single_web_reader_prompt += "\n\n" + style_extra
        single_line_reader_prompt += "\n\n" + style_extra

    if rt == "compat_line" or (rt == "compat_web" and output_style == "line"):
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
        prompt = single_web_prompt

    if must_use_vedic:
        prompt += """

【最終強制指示】
今回はインド占星術発火ケースです。
本文を西洋占星術のみで完結させることを禁止します。
少なくとも2か所で、インド占星術の視点を本文に自然に統合してください。
特に「なぜ同じ流れが続きやすいか」「表面では説明しきれない背景」を補ってください。
可能なら vedic_focus_summary の内容を1回以上反映してください。
"""

    fallback_used = False

    if use_claude:
        api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
        if not api_key:
            return "ANTHROPIC_API_KEY が未設定です"
        if Anthropic is None:
            return "anthropic が読み込めません（requirements.txt を確認）"
        try:
            client = Anthropic(api_key=api_key)
        except Exception as e:
            return f"Claude client 初期化エラー: {e}"

        model_name, model_source = _resolve_claude_model_name(meta2, astrology_system)
        max_tokens = 3000 if ("line" in rt or output_style == "line") else 8192
        model_candidates = [model_name]
        provider_name = "claude"
        dynamic_system_prompt = (_CLAUDE_SYSTEM_PROMPT + "\n\n" + ctx.get("age_system_prompt", "") + "\n\n" + ctx.get("role_prompt", "")).strip()
    else:
        api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
        if not api_key:
            return "GEMINI_API_KEY が未設定です"
        if genai is None or types is None:
            return "google-genai が読み込めません（requirements.txt を確認）"
        try:
            client = genai.Client(api_key=api_key)
        except Exception as e:
            return f"Gemini client 初期化エラー: {e}"

        model_name, model_source = _resolve_model_name(meta2, astrology_system)
        max_tokens = 3000 if ("line" in rt or output_style == "line") else (8192 if model_name == PRO_MODEL else 5000)
        model_candidates = [model_name]
        if model_name == PRO_MODEL and not _truthy(meta2.get("disable_model_fallback")):
            model_candidates.append(FLASH_LITE_MODEL)
        provider_name = "gemini"

    last_error: Exception | None = None
    for candidate in model_candidates:
        for attempt in range(3):
            try:
                current_max_tokens = max_tokens if candidate == model_name else 4500
                if use_claude:
                    text1 = _generate_once_claude(client=client, model_name=candidate, prompt=prompt, max_tokens=current_max_tokens, system_prompt=dynamic_system_prompt)
                else:
                    text1 = _generate_once(client=client, model_name=candidate, prompt=prompt, max_tokens=current_max_tokens)
                if not text1:
                    raise RuntimeError("empty text")
                # Claude は 8192 トークンで1回出し切れるため続き生成をスキップ
                if not use_claude and output_style != "line" and ("web" in rt) and _is_incomplete_web(text1):
                    cont_prompt = _make_continue_prompt(previous_text=text1)
                    text2 = _generate_once(client=client, model_name=candidate, prompt=cont_prompt, max_tokens=current_max_tokens)
                    if text2:
                        return (text1 + "\n\n" + text2).strip()
                if candidate != model_name:
                    fallback_used = True
                return text1
            except Exception as e:
                last_error = e
                if attempt == 2:
                    break
                time.sleep(1.0)
        if candidate != model_name:
            fallback_used = True

    return f"AI生成エラー: {last_error} / {_debug_model_info(model_name, model_source, fallback_used=fallback_used, provider=provider_name)}"
