"""
日運（今日の空気）専用ルート。
main routes.py から分離して、他修正の影響を受けにくくする。
"""
from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from services.daily_theme_service import enrich_daily_theme_result


def _calc_helpers():
    from services.transit_calc import calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_global_transit_snapshot
    from services.western_calc import calc_western_from_payload
    return calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_global_transit_snapshot, calc_western_from_payload


def _parse_jsonish_response(raw: str, fallback: dict[str, object]) -> dict[str, object]:
    cleaned = (raw or "").strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    candidates: list[str] = []
    if cleaned:
        candidates.append(cleaned)
        start_obj = cleaned.find("{")
        end_obj = cleaned.rfind("}")
        if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
            candidates.append(cleaned[start_obj:end_obj+1])
        start_arr = cleaned.find("[")
        end_arr = cleaned.rfind("]")
        if start_arr != -1 and end_arr != -1 and end_arr > start_arr:
            candidates.append(cleaned[start_arr:end_arr+1])

    for cand in candidates:
        try:
            parsed = json.loads(cand)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {**fallback, "items": parsed}
        except Exception:
            pass

    merged = dict(fallback)
    merged.setdefault("raw_text", raw or "")
    merged["summary"] = cleaned or merged.get("summary") or "生成結果を取得できませんでした。"
    return merged

router = APIRouter()
templates = Jinja2Templates(directory="templates")


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


@router.post("/daily-theme/generate-types", response_class=JSONResponse)
async def daily_theme_generate_types(request: Request):
    """生成済みの『今日の空気』を元に、タイプ別運勢をまとめて生成する。"""
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
        tpl = (prompts_dir / "daily_type_forecast.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")
        prompt = tpl.format(
            common_rules=common_rules,
            target_date=date_str,
            period=period,
            axis=axis,
            base_theme_json=json.dumps(base_theme, ensure_ascii=False, indent=2),
            type_lines=json.dumps(parsed_types, ensure_ascii=False, indent=2),
        )

        from services.ai_report import generate_report as _gen
        raw = _gen(
            {
                "_meta": {
                    "output_style": "web",
                    "detail_level": "standard",
                    "astrology_system": "western",
                    "message": prompt,
                }
            },
            style="web",
            report_type="raw_prompt",
        )

        cleaned = (raw or "").strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        try:
            result = json.loads(cleaned)
        except Exception:
            result = {
                "date": date_str,
                "period": period,
                "axis": axis,
                "items": [
                    {
                        "type_name": t["type_name"],
                        "type_hint": t["type_hint"],
                        "summary": cleaned or "生成結果を取得できませんでした。",
                        "flow": "",
                        "likely_things": [],
                        "caution": [],
                        "advice": [],
                        "social_post": "",
                    }
                    for t in parsed_types
                ],
                "raw_text": raw or "",
            }

        if not isinstance(result, dict):
            result = {}
        result.setdefault("date", date_str)
        result.setdefault("period", period)
        result.setdefault("axis", axis)
        result.setdefault("items", [])
        if not isinstance(result.get("items"), list):
            result["items"] = []
        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/daily-theme/generate", response_class=JSONResponse)
async def daily_theme_generate(request: Request):
    """その日の全体トランジットから、占い師用の『今日の空気』を生成する。"""
    try:
        calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_global_transit_snapshot, calc_western_from_payload = _calc_helpers()

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
            planet_lines.append(f"- {p['name']}: {p.get('sign','')} {p.get('degree',0):.2f}°")
        planet_summary = "\n".join(planet_lines) if planet_lines else "（データなし）"

        asp_map = {
            "conjunction": "コンジャンクション",
            "opposition": "オポジション",
            "square": "スクエア",
            "trine": "トライン",
            "sextile": "セクスタイル",
        }
        aspect_lines = []
        for a in snapshot.get("aspects", [])[:12]:
            aspect_lines.append(
                f"- {a['planet_a']} {asp_map.get(a.get('aspect',''), a.get('aspect',''))} {a['planet_b']} / orb {a.get('orb', 0):.2f}°"
            )
        aspect_summary = "\n".join(aspect_lines) if aspect_lines else "（主要アスペクトなし）"

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl = (prompts_dir / "daily_theme.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")
        prompt = tpl.format(
            common_rules=common_rules,
            target_date=snapshot.get("transit_date", date_str or ""),
            period=period,
            axis=axis,
            planet_summary=planet_summary,
            aspect_summary=aspect_summary,
        )

        from services.ai_report import generate_report as _gen
        raw = _gen(
            {
                "_meta": {
                    "output_style": "web",
                    "detail_level": "standard",
                    "astrology_system": "western",
                    "message": prompt,
                }
            },
            style="web",
            report_type="raw_prompt",
        )

        result = _parse_jsonish_response(raw, {
            "period": period,
            "axis": axis,
            "date": snapshot.get("transit_date", date_str or ""),
            "summary": "生成結果を取得できませんでした。",
            "core_themes": [],
            "push": [],
            "caution": [],
            "recommended_actions": [],
            "avoid_actions": [],
            "social_post": "",
            "type_translation_axis": "",
        })

        result.setdefault("period", period)
        result.setdefault("axis", axis)
        result.setdefault("date", snapshot.get("transit_date", date_str or ""))
        result["source_transit"] = snapshot
        result = enrich_daily_theme_result(result)
        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))
