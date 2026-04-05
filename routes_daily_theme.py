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

SOFT_ACTION_LINE = "少し整える時間を意識すると、流れが落ち着きやすくなります。"
DEFAULT_CAUTION = [
    "勢いだけで決めず、ひと呼吸おいて全体の流れを見直す。",
    "直感と現実のどちらか一方に偏りすぎない。",
]
DEFAULT_AVOID = [
    "思いつきのまま一気に話を進めること。",
    "細部にこだわりすぎて全体のタイミングを逃すこと。",
]


def _calc_helpers():
    from services.transit_calc import calc_global_transit_snapshot
    return calc_global_transit_snapshot


def _safe_prompt_render(template: str, values: dict[str, Any]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))
    return rendered


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
            t = t[start_obj:end_obj + 1].strip()
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


def _sentences(text: str) -> list[str]:
    parts = []
    normalized = (text or "").replace("\n", "")
    for chunk in normalized.split("。"):
        chunk = chunk.strip()
        if chunk:
            parts.append(chunk + "。")
    return parts


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
                return str(obj.get("summary") or obj.get("type_translation_axis") or "").strip()
        except Exception:
            pass
    return text


def _coerce_list(value: Any, fallback_item: str = "") -> list[str]:
    items: list[str] = []
    if isinstance(value, list):
        seq = value
    elif isinstance(value, str) and value.strip():
        seq = [value]
    else:
        seq = []
    for item in seq:
        cleaned = _clean_item(item)
        if cleaned:
            items.append(cleaned)
    if not items and fallback_item:
        items = [fallback_item]
    return items[:2]


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
    has_moon = "Moon" in planet_names
    has_mercury = "Mercury" in planet_names
    has_venus = "Venus" in planet_names
    has_mars = "Mars" in planet_names

    aspect_names = [str(a.get("aspect") or "") for a in aspects if isinstance(a, dict)]
    has_square = "square" in aspect_names
    has_opposition = "opposition" in aspect_names
    has_trine = "trine" in aspect_names
    has_sextile = "sextile" in aspect_names

    summary_bits = []
    if has_square or has_opposition:
        summary_bits.append("動きたい気持ちと慎重さがぶつかりやすく、勢いだけで進めるとズレが出やすい日です。")
    else:
        summary_bits.append("流れを整えながら進めることで、やるべきことに集中しやすい日です。")
    if has_trine or has_sextile:
        summary_bits.append("対話や段取りを少し整えるだけで、物事が噛み合いやすくなります。")
    elif has_mercury or has_venus:
        summary_bits.append("言葉の選び方や距離感の取り方が、そのまま空気を左右しやすくなります。")
    if has_moon:
        summary_bits.append("感情の反応が表に出やすいので、先に落ち着きを作ることが大切です。")
    summary = "".join(summary_bits)[:120]

    core = []
    if has_moon:
        core.append("感情の揺れを先に整える")
    if has_mercury:
        core.append("言葉選びが空気を左右する")
    if not core:
        core = ["流れを急がず整える", "ズレを早めに微調整する"]

    push = []
    if has_trine or has_sextile:
        push.append("段取りの調整が結果につながる")
    if has_venus:
        push.append("柔らかい対話が通りやすい")
    if not push:
        push = ["小さな修正が効きやすい", "落ち着いた確認が追い風になる"]

    caution = []
    if has_square:
        caution.append("勢いで決めると摩擦が出やすい")
    if has_opposition:
        caution.append("相手基準に寄りすぎない")
    if not caution:
        caution = DEFAULT_CAUTION[:1]

    recommended = ["少し整える時間を意識する"]
    if has_mercury:
        recommended.append("言葉を短く整理して伝える")

    avoid = []
    if has_mars:
        avoid.append("思いつきで一気に進めること")
    if not avoid:
        avoid = DEFAULT_AVOID[:1]

    social = "今日は、急ぐより整えることが効きやすい日。感情や言葉の扱いを少し丁寧にするだけで、流れが落ち着きやすくなります。"

    axis_text = "勢いより調整力を見ると、タイプ別の違いを出しやすい日です。"

    return {
        "date": target_date,
        "period": period,
        "axis": axis,
        "summary": summary or "今日は、流れを急がず整えるほど動きやすくなる日です。",
        "core_themes": core[:2],
        "push": push[:2],
        "caution": caution[:2],
        "recommended_actions": recommended[:2],
        "avoid_actions": avoid[:2],
        "social_post": social,
        "type_translation_axis": axis_text,
        "source": "deterministic_fallback",
    }


def _normalize_daily_theme_result(result: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(fallback)
    if isinstance(result, dict):
        merged.update(result)

    summary = _clean_item(merged.get("summary")) or fallback["summary"]
    type_axis = _clean_item(merged.get("type_translation_axis")) or fallback["type_translation_axis"]
    social = _clean_item(merged.get("social_post")) or fallback["social_post"]

    merged["summary"] = summary
    merged["type_translation_axis"] = type_axis
    merged["social_post"] = social
    merged["core_themes"] = _coerce_list(merged.get("core_themes"), summary)
    merged["push"] = _coerce_list(merged.get("push"), summary)
    merged["caution"] = _coerce_list(merged.get("caution"), DEFAULT_CAUTION[0])
    merged["recommended_actions"] = _coerce_list(merged.get("recommended_actions"), SOFT_ACTION_LINE)
    merged["avoid_actions"] = _coerce_list(merged.get("avoid_actions"), DEFAULT_AVOID[0])

    if SOFT_ACTION_LINE not in merged["recommended_actions"]:
        merged["recommended_actions"].append(SOFT_ACTION_LINE)
    merged["recommended_actions"] = merged["recommended_actions"][:2]

    return merged


def _call_llm_json(prompt: str) -> str:
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
            temperature=0.3,
            top_p=0.9,
            max_output_tokens=900,
            response_mime_type="application/json",
        ),
    )
    text = getattr(resp, "text", None)
    return (text or "").strip()


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
        date_str = (body.get("date") or "").strip()
        period = (body.get("period") or "daily").strip().lower()
        axis = (body.get("axis") or "overall").strip().lower()
        lat = float(body.get("lat", 35.6895))
        lng = float(body.get("lng", 139.6917))

        target_date = None
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(status_code=400, detail="date は YYYY-MM-DD 形式で指定してください")

        snapshot = calc_global_transit_snapshot(target_date=target_date, lat=lat, lng=lng)

        planet_lines = []
        for p in snapshot.get("today_planets", [])[:10]:
            if isinstance(p, dict):
                planet_lines.append(f"- {p.get('name','')}: {p.get('sign','')} {float(p.get('degree',0)):.2f}°")
        planet_summary = "\n".join(planet_lines) if planet_lines else "（データなし）"

        asp_map = {
            "conjunction": "コンジャンクション",
            "opposition": "オポジション",
            "square": "スクエア",
            "trine": "トライン",
            "sextile": "セクスタイル",
        }
        aspect_lines = []
        for a in snapshot.get("aspects", [])[:10]:
            if isinstance(a, dict):
                aspect_lines.append(
                    f"- {a.get('planet_a','')} {asp_map.get(a.get('aspect',''), a.get('aspect',''))} {a.get('planet_b','')} / orb {float(a.get('orb', 0)):.2f}°"
                )
        aspect_summary = "\n".join(aspect_lines) if aspect_lines else "（主要アスペクトなし）"

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl = (prompts_dir / "daily_theme.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")

        prompt = _safe_prompt_render(
            tpl,
            {
                "common_rules": common_rules,
                "target_date": snapshot.get("transit_date", date_str or ""),
                "period": period,
                "axis": axis,
                "planet_summary": planet_summary,
                "aspect_summary": aspect_summary,
            },
        )

        fallback = _deterministic_theme_from_snapshot(
            snapshot=snapshot,
            target_date=snapshot.get("transit_date", date_str or ""),
            period=period,
            axis=axis,
        )

        raw = _call_llm_json(prompt)
        parsed = _parse_jsonish_response(raw, fallback) if raw else fallback
        result = _normalize_daily_theme_result(parsed, fallback)
        return JSONResponse(content=result)

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
        date_str = (body.get("date") or base_theme.get("date") or "").strip()
        period = (body.get("period") or base_theme.get("period") or "daily").strip().lower()
        axis = (body.get("axis") or base_theme.get("axis") or "overall").strip().lower()
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
            for t in parsed_types:
                items.append({
                    "type_name": t["type_name"],
                    "type_hint": t["type_hint"],
                    "summary": base_summary,
                    "flow": "",
                    "likely_things": [],
                    "caution": [],
                    "advice": [SOFT_ACTION_LINE],
                    "social_post": "",
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

        raw = _call_llm_json(prompt)
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
            base_social = str(base_theme.get("social_post") or "").strip()
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
