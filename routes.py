"""
routes.py
FastAPI のルート定義のみを担当する薄い層。
占術計算・レポート生成・ログ構築は analyze_engine に委譲する。
"""
from __future__ import annotations

import os
import traceback
from pathlib import Path
from typing import Any, Literal
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from auth import get_current_reader
from models import Astrologer, Order, OrderDelivery, OrderResultView
import json

from prefs import PREF_LABELS
from line_webhook import router as line_router
from routes_public_orders import router as public_orders_router
from routes_reader import router as reader_router
from routes_admin import router as admin_router
from routes_stripe import router as stripe_router
from routes_staff import router as staff_router
from fastapi.responses import JSONResponse
from datetime import datetime, timezone


def _startup_platform_safe() -> None:
    mode = (os.getenv("BOOTSTRAP_ON_STARTUP") or "safe").strip().lower()
    if mode in {"", "0", "false", "off", "skip", "disabled"}:
        print("startup bootstrap skipped")
        return
    try:
        from bootstrap_platform import init_db, seed_defaults
        from db import db_session
        init_db()
        with db_session() as db:
            seed_defaults(db)
        print("startup bootstrap completed")
    except Exception:
        print("startup bootstrap failed")
        traceback.print_exc()
        if mode == "strict":
            raise


def _build_full_astrologer_summary(*args, **kwargs):
    from services.astrologer_summary import build_full_astrologer_summary
    return build_full_astrologer_summary(*args, **kwargs)


def _analyze_helpers():
    from analyze_engine import (
        build_payload_a,
        build_base_meta,
        format_reports,
        build_handoff_logs,
        run_compatibility,
        run_single,
    )
    return build_payload_a, build_base_meta, format_reports, build_handoff_logs, run_compatibility, run_single


def _calc_helpers():
    from services.transit_calc import calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_global_transit_snapshot
    from services.western_calc import calc_western_from_payload
    return calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_global_transit_snapshot, calc_western_from_payload

AstrologySystem = Literal["western", "vedic", "integrated", "shichusuimei", "integrated3"]
AnalysisType = Literal["single", "compatibility"]

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.include_router(line_router)

@app.on_event("startup")
def _startup_platform() -> None:
    _startup_platform_safe()

app.include_router(public_orders_router)
app.include_router(reader_router)
app.include_router(admin_router)
app.include_router(stripe_router)
app.include_router(staff_router)


# ── ヘルスチェック ────────────────────────────────────────────────────────────

@app.get("/healthz")
def healthz():
    return {"ok": True}


# ── ページ表示（GET） ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def form_page(
    request: Request,
    analysis_type: str = "single",
    astrology_system: str = "western",
):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "request": request,
            "prefs": PREF_LABELS,
            "analysis_type": analysis_type,
            "astrology_system": astrology_system,
            "google_maps_api_key": os.getenv("GOOGLE_MAPS_API_KEY", ""),
            "ai_text": "",
            "reader_text": "",
            "line_text": "",
            "unknowns": [],
            "inputs_json": {},
            "payload_json": {},
            "raw_json": {},
            "handoff_json": "",
            "handoff_yaml": "",
            "handoff_json_full": "",
            "handoff_yaml_full": "",
            "handoff_json_delta": "",
            "handoff_yaml_delta": "",
            "bias_guard": {},
        },
    )


@app.get("/analyze", response_class=HTMLResponse)
def analyze_page(
    request: Request,
    analysis_type: str = "single",
    astrology_system: str = "western",
):
    return form_page(request, analysis_type=analysis_type, astrology_system=astrology_system)


@app.get("/western", response_class=HTMLResponse)
def western_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="western")


@app.get("/vedic", response_class=HTMLResponse)
def vedic_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="vedic")


@app.get("/integrated", response_class=HTMLResponse)
def integrated_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="integrated3")


@app.get("/shichu", response_class=HTMLResponse)
def shichu_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="shichusuimei")


@app.get("/lite", response_class=HTMLResponse)
def lite_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="western")


@app.get("/guide", response_class=HTMLResponse)
def guide_page(request: Request):
    return templates.TemplateResponse(request=request, name="guide.html", context={"request": request})

@app.get("/daily-theme", response_class=HTMLResponse)
def daily_theme_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="daily_theme.html",
        context={
            "request": request,
            "initial_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        },
    )



@app.post("/daily-theme/generate-types", response_class=JSONResponse)
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

@app.get("/about", response_class=HTMLResponse)
def about_page(request: Request):
    return templates.TemplateResponse(request=request, name="about.html", context={"request": request})
    
@app.get("/thanks-stripe", response_class=HTMLResponse)
def thanks_page(request: Request):
    return templates.TemplateResponse(request=request, name="thanks-stripe.html", context={"request": request})    
# ── 占い師サマリー ────────────────────────────────────────────────────────────

@app.post("/astrologer-result", response_class=HTMLResponse)
async def astrologer_result(request: Request):
    form = await request.form()

    raw_json = form.get("result_json") or "{}"
    try:
        result = json.loads(raw_json) if isinstance(raw_json, str) else {}
    except Exception:
        result = {}

    structure_summary_json = form.get("structure_summary_json") or "{}"
    try:
        structure_summary = json.loads(structure_summary_json) if isinstance(structure_summary_json, str) else {}
    except Exception:
        structure_summary = {}

    reader_text = form.get("reader_text") or ""
    if not isinstance(reader_text, str):
        reader_text = ""

    summary = _build_full_astrologer_summary(
        result if isinstance(result, dict) else {},
        structure_summary if isinstance(structure_summary, dict) else {},
    )

    return templates.TemplateResponse(
        request=request,
        name="astrologer_result.html",
        context={
            "request": request,
            "result": result if isinstance(result, dict) else {},
            "summary": summary,
            "structure_summary": structure_summary if isinstance(structure_summary, dict) else {},
            "reader_text": reader_text.strip(),
        },
    )


# ── 解析実行（POST） ──────────────────────────────────────────────────────────

@app.post("/analyze", response_class=HTMLResponse)
def analyze(
    request: Request,
    # A
    name: str | None = Form(None),
    birth_date: str = Form(...),
    birth_time: str | None = Form(None),
    birth_place: str | None = Form(None),
    prefecture: str | None = Form(None),
    lat: float | None = Form(None),
    lon: float | None = Form(None),
    from_order_code: str | None = Form(None),
    gender: str = Form("female"),
    # 共通
    analysis_type: AnalysisType = Form("single"),
    astrology_system: AstrologySystem = Form("western"),
    theme: str = Form("overall"),
    message: str | None = Form(None),
    observations_text: str | None = Form(None),
    output_style: str = Form("normal"),
    detail_level: str = Form("standard"),
    ai_model: str | None = Form(None),
    house_system: str = Form("P"),
    node_mode: str = Form("true"),
    lilith_mode: str = Form("mean"),
    include_asteroids: str | None = Form(None),
    include_chiron: str | None = Form(None),
    include_lilith: str | None = Form(None),
    include_vertex: str | None = Form(None),
    include_reader: str | None = Form(None),
    # 四柱推命オプション
    day_change_at_23: bool = Form(False),
    # トランジット
    include_transit: str | None = Form(None),
    # B（相性）
    name_b: str | None = Form(None),
    birth_date_b: str | None = Form(None),
    birth_time_b: str | None = Form(None),
    birth_place_b: str | None = Form(None),
    prefecture_b: str | None = Form(None),
    lat_b: float | None = Form(None),
    lon_b: float | None = Form(None),
    gender_b: str = Form("female"),
):
    build_payload_a, build_base_meta, format_reports, build_handoff_logs, run_compatibility, run_single = _analyze_helpers()
    calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_global_transit_snapshot, calc_western_from_payload = _calc_helpers()
    # チェックボックスをboolに変換
    include_asteroids = include_asteroids is not None
    include_chiron    = include_chiron is not None
    include_lilith    = include_lilith is not None
    include_vertex    = include_vertex is not None
    include_reader    = include_reader is not None
    include_transit   = include_transit is not None

    unknowns: list[str] = []

    # ── ペイロード・メタ構築 ──────────────────────────────────────────────────
    payload_a = build_payload_a(
        birth_date=birth_date,
        birth_time=birth_time,
        birth_place=birth_place,
        prefecture=prefecture,
        lat=lat,
        lon=lon,
        gender=gender,
        house_system=house_system,
        node_mode=node_mode,
        lilith_mode=lilith_mode,
        include_asteroids=include_asteroids,
        include_chiron=include_chiron,
        include_lilith=include_lilith,
        include_vertex=include_vertex,
        unknowns=unknowns,
    )

    base_meta = build_base_meta(
        birth_date=birth_date,
        output_style=output_style,
        detail_level=detail_level,
        house_system=house_system,
        node_mode=node_mode,
        lilith_mode=lilith_mode,
        include_asteroids=include_asteroids,
        include_chiron=include_chiron,
        include_lilith=include_lilith,
        include_vertex=include_vertex,
        include_reader=include_reader,
        theme=theme,
        message=message,
        observations_text=observations_text,
        analysis_type=analysis_type,
        astrology_system=astrology_system,
        ai_model=ai_model,
        day_change_at_23=day_change_at_23,
        name=name,
        name_b=name_b,
        gender=gender,
        gender_b=gender_b,
    )

    inputs_view: dict[str, Any] = {
        "analysis_type": analysis_type,
        "astrology_system": astrology_system,
        "name": name,
        "birth_date": birth_date,
        "birth_time": birth_time,
        "birth_place": birth_place,
        "prefecture": prefecture,
        "gender": gender,
        "name_b": name_b,
        "birth_date_b": birth_date_b,
        "birth_time_b": birth_time_b,
        "birth_place_b": birth_place_b,
        "prefecture_b": prefecture_b,
        "gender_b": gender_b,
        "output_style": output_style,
        "detail_level": detail_level,
        "ai_model": ai_model,
        "house_system": house_system,
        "node_mode": node_mode,
        "lilith_mode": lilith_mode,
        "include_asteroids": include_asteroids,
        "include_chiron": include_chiron,
        "include_lilith": include_lilith,
        "include_vertex": include_vertex,
        "include_reader": include_reader,
        "theme": theme,
        "message": message,
        "observations_text": (observations_text or "").strip(),
        "day_change_at_23": day_change_at_23,
    }

    astro_result: dict[str, Any] = {}

    try:
        # ── 分析実行 ──────────────────────────────────────────────────────────
        if analysis_type == "compatibility":
            if not birth_date_b:
                raise HTTPException(status_code=400, detail="相性分析では相手の生年月日が必要です。")

            # ── トランジット計算（run_compatibilityより先に実行） ─────────────
            transit_data = None
            if include_transit:
                try:
                    from services.western_calc import calc_western_from_payload  # type: ignore
                    _tmp_a = calc_western_from_payload(payload_a)
                    natal_a_tmp = _tmp_a.get("planets", [])
                    # B側のpayloadを構築
                    from shared import _calc_payload_from_inputs  # type: ignore
                    _unknowns_b: list = []
                    _payload_b = _calc_payload_from_inputs(
                        birth_date=birth_date_b or "",
                        birth_time=birth_time_b,
                        birth_place=birth_place_b,
                        prefecture=prefecture_b,
                        lat=lat_b, lon=lon_b,
                        unknowns=_unknowns_b,
                    )
                    _tmp_b = calc_western_from_payload(_payload_b)
                    natal_b_tmp = _tmp_b.get("planets", [])
                    # シナストリートランジット（A×今日・B×今日）
                    synastry_transit = calc_transits_synastry(natal_a_tmp, natal_b_tmp)
                    # A・Bそれぞれの長期トランジット
                    long_term_a = calc_transits_long_term(natal_a_tmp)
                    long_term_b = calc_transits_long_term(natal_b_tmp)
                    transit_data = {
                        **synastry_transit,
                        "long_term":   long_term_a,
                        "long_term_b": long_term_b,
                    }
                except Exception:
                    traceback.print_exc()
                    transit_data = {"error": "トランジット計算に失敗しました"}

            astro_result, payload_view, report_web, report_line, report_raw, report_reader = run_compatibility(
                payload_a=payload_a,
                birth_date_b=birth_date_b,
                birth_time_b=birth_time_b,
                birth_place_b=birth_place_b,
                prefecture_b=prefecture_b,
                lat_b=lat_b,
                lon_b=lon_b,
                gender_b=gender_b,
                house_system=house_system,
                node_mode=node_mode,
                lilith_mode=lilith_mode,
                include_asteroids=include_asteroids,
                include_chiron=include_chiron,
                include_lilith=include_lilith,
                include_vertex=include_vertex,
                include_reader=include_reader,
                base_meta=base_meta,
                unknowns=unknowns,
            )
            guard_meta: dict[str, Any] = {}
            report_raw = report_raw  # 相性分析では裏カルテなし

        else:
            # ── トランジット計算（run_singleより先に実行してAIに渡す） ────────
            transit_data = None
            if include_transit:
                try:
                    from services.western_calc import calc_western_from_payload  # type: ignore
                    _tmp = calc_western_from_payload(payload_a)
                    natal_planets_tmp = _tmp.get("planets", [])
                    today_transit = calc_transits_single(natal_planets_tmp)
                    long_term = calc_transits_long_term(natal_planets_tmp)
                    transit_data = {**today_transit, "long_term": long_term}
                except Exception:
                    traceback.print_exc()
                    transit_data = {"error": "トランジット計算に失敗しました"}

            astro_result, payload_view, report_web, report_line, report_raw, report_reader, guard_meta = run_single(
                astrology_system=astrology_system,
                payload_a=payload_a,
                base_meta=base_meta,
                message=message,
                include_reader=include_reader,
                day_change_at_23=day_change_at_23,
                transit_data=transit_data,
            )
            base_meta["bias_guard"] = guard_meta

        # ── レポート整形 ──────────────────────────────────────────────────────
        report_web, report_raw, report_reader, report_line = format_reports(
            report_web=report_web,
            report_raw=report_raw,
            report_reader=report_reader,
            report_line=report_line,
            detail_level=detail_level,
            output_style=output_style,
            include_reader=include_reader,
        )

        # ── YAMLログ構築 ──────────────────────────────────────────────────────
        bias_guard_obj = guard_meta if isinstance(guard_meta, dict) else {}

        logs = build_handoff_logs(
            inputs_view=inputs_view,
            payload_view=payload_view,
            unknowns=unknowns,
            astro_result=astro_result,
            report_web=report_web,
            report_raw=report_raw,
            report_reader=report_reader,
            report_line=report_line,
            observations_text=observations_text,
            bias_guard_obj=bias_guard_obj,
            transit=transit_data,
        )

        return templates.TemplateResponse(
            request=request,
            name="result.html",
            context={
                "request": request,
                "unknowns": unknowns,
                "inputs_json": inputs_view,
                "payload_json": payload_view,
                "raw_json": astro_result,
                "structure_summary_json": logs["structure_summary_json"],
                "ai_text": report_web,
                "raw_reader_text": report_raw,
                "reader_text": report_reader,
                "line_text": report_line,
                "include_reader": include_reader,
                "handoff_json":       logs["handoff_json"],
                "handoff_yaml":       logs["handoff_yaml"],
                "handoff_json_full":  logs["handoff_json_full"],
                "handoff_yaml_full":  logs["handoff_yaml_full"],
                "handoff_json_delta": logs["handoff_json_delta"],
                "handoff_yaml_delta": logs["handoff_yaml_delta"],
                "bias_guard": bias_guard_obj,
                "transit_data": transit_data,
                "from_order_code": (from_order_code or "").strip() or None,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/analyze/save-to-order")
def save_analysis_to_order(
    request: Request,
    from_order_code: str = Form(""),
    ai_text: str = Form(""),
    reader_text: str = Form(""),
    line_text: str = Form(""),
    inputs_json: str = Form("{}"),
    payload_json: str = Form("{}"),
    raw_json: str = Form("{}"),
    structure_summary_json: str = Form("{}"),
    handoff_yaml_full: str = Form(""),
):
    from db import db_session
    from sqlalchemy import select
    from services.result_builder import build_yaml_from_analysis, build_result_payload, render_result_html, render_report_html
    from services.yaml_log_service import create_yaml_log

    def _loads(value: str):
        try:
            return json.loads(value) if isinstance(value, str) else value
        except Exception:
            return {}

    resolved_order_code = (from_order_code or request.session.get("analyze_from_order_code") or "").strip()
    if not resolved_order_code:
        raise HTTPException(status_code=400, detail="order_code missing")

    with db_session() as db:
        order = db.scalar(select(Order).where(Order.order_code == resolved_order_code))
        if not order:
            raise HTTPException(status_code=404, detail='order not found')

        inputs_obj = _loads(inputs_json)
        payload_obj = _loads(payload_json)
        raw_obj = _loads(raw_json)
        structure_obj = _loads(structure_summary_json)

        def _good_text(*values):
            for value in values:
                txt = (value or "") if isinstance(value, str) else str(value or "")
                txt = txt.strip()
                if txt and "DEBUG" not in txt and "空文字" not in txt:
                    return txt
            return ""

        report_web = _good_text(
            ai_text,
            payload_obj.get("web_text") if isinstance(payload_obj, dict) else "",
            payload_obj.get("report_text") if isinstance(payload_obj, dict) else "",
            raw_obj.get("web_text") if isinstance(raw_obj, dict) else "",
            raw_obj.get("report_text") if isinstance(raw_obj, dict) else "",
            (raw_obj.get("reports") or {}).get("web") if isinstance(raw_obj.get("reports"), dict) else "",
            ((raw_obj.get("western") or {}).get("web_text") if isinstance(raw_obj.get("western"), dict) else ""),
            ((raw_obj.get("western") or {}).get("report_text") if isinstance(raw_obj.get("western"), dict) else ""),
        )
        report_reader = _good_text(reader_text)
        report_line = _good_text(line_text)
        horoscope_image_url = (
            raw_obj.get("chart_image_url") or raw_obj.get("wheel_image_url") or
            ((raw_obj.get("western") or {}).get("chart_image_url") if isinstance(raw_obj.get("western"), dict) else "") or
            ((raw_obj.get("western") or {}).get("wheel_image_url") if isinstance(raw_obj.get("western"), dict) else "") or ""
        )

        summary = {
            "saved_from": "analyze",
            "from_order_code": order.order_code,
            "order": {
                "horoscope_image_url": horoscope_image_url,
            },
            "reports": {
                "web": report_web,
                "reader": report_reader,
                "line": report_line,
            },
            "structure_summary": structure_obj,
            "raw_json": raw_obj,
            "payload_json": payload_obj,
        }

        yaml_body = build_yaml_from_analysis(
            order=order,
            inputs_json=inputs_obj,
            payload_json=payload_obj,
            raw_json=raw_obj,
            structure_summary_json=structure_obj,
            ai_text=ai_text or '',
            reader_text=reader_text or '',
            line_text=line_text or '',
            handoff_yaml_full=handoff_yaml_full or '',
        )
        yaml_log = create_yaml_log(
            db,
            order,
            yaml_body=yaml_body,
            summary=summary,
            created_by_type='system',
            created_by_id=None,
            log_type='generated',
            set_active=True,
        )
        db.flush()

        delivery = db.scalar(select(OrderDelivery).where(OrderDelivery.order_id == order.id).order_by(OrderDelivery.updated_at.desc(), OrderDelivery.id.desc()).limit(1))
        delivery_text = report_web
        if delivery:
            delivery.delivery_text = delivery_text
            delivery.is_draft = True
        else:
            db.add(OrderDelivery(order_id=order.id, reader_id=order.assigned_reader_id, delivery_text=delivery_text, is_draft=True))

        payload = build_result_payload(order, yaml_log, delivery_text=delivery_text)
        payload["raw_json"] = raw_obj
        payload["horoscope_image_url"] = horoscope_image_url or payload.get("horoscope_image_url") or ""

        view = db.scalar(select(OrderResultView).where(OrderResultView.order_id == order.id).order_by(OrderResultView.id.desc()).limit(1))
        if not view:
            view = OrderResultView(order_id=order.id)
            db.add(view)
        view.source_yaml_log_id = yaml_log.id
        view.result_payload_json = json.dumps(payload, ensure_ascii=False)
        view.result_html = render_result_html(payload)
        try:
            view.report_html = render_report_html(order, payload)
        except Exception:
            view.report_html = None
        view.horoscope_image_url = payload.get("horoscope_image_url") or None

        if order.status in {'received', 'paid', 'assigned'}:
            order.status = 'in_progress'
        db.commit()
    return RedirectResponse(url=f"/staff/orders/{resolved_order_code}", status_code=303)


# ── トランジット API ──────────────────────────────────────────────────────────

@app.post("/transit/single", response_class=JSONResponse)
async def transit_single(request: Request):
    """
    1人分のトランジット計算API。
    Body: { payload: {...出生情報}, date?: "YYYY-MM-DD" }
    """
    try:
        body = await request.json()
        payload = body.get("payload", {})
        date_str = body.get("date")

        target_date = None
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        natal_result = calc_western_from_payload(payload)
        natal_planets = natal_result.get("planets", [])

        lat = float(payload.get("lat", 35.6895))
        lng = float(payload.get("lng", 139.6917))

        transit = calc_transits_single(natal_planets, target_date=target_date, lat=lat, lng=lng)
        transit["natal_summary"] = [
            {"name": p["name"], "sign": p["sign"], "degree": round(p["degree"], 2)}
            for p in natal_planets
        ]
        return JSONResponse(content=transit)

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/transit/synastry", response_class=JSONResponse)
async def transit_synastry(request: Request):
    """
    2人分のトランジット計算API（3層）。
    Body: { payload_a: {...}, payload_b: {...}, date?: "YYYY-MM-DD" }
    """
    try:
        body = await request.json()
        payload_a = body.get("payload_a", {})
        payload_b = body.get("payload_b", {})
        date_str = body.get("date")

        target_date = None
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        natal_a = calc_western_from_payload(payload_a).get("planets", [])
        natal_b = calc_western_from_payload(payload_b).get("planets", [])

        lat = float(payload_a.get("lat", 35.6895))
        lng = float(payload_a.get("lng", 139.6917))

        result = calc_transits_synastry(natal_a, natal_b, target_date=target_date, lat=lat, lng=lng)
        return JSONResponse(content=result)

    except Exception as e:
        traceback.print_exc()
        return JSONResponse(content={"text": f"（サーバーエラー: {e}）"}, status_code=200)


@app.post("/transit/interpret", response_class=JSONResponse)
async def transit_interpret(request: Request):
    """
    トランジットデータをAIに解釈させて時期別ストーリーを返す。
    Body: { long_term: [...], today_aspects: [...], natal_planets: [...] }
    """
    try:
        import os, time
        from pathlib import Path

        body = await request.json()
        long_term = body.get("long_term", [])
        today_aspects = body.get("today_aspects", [])
        natal_planets = body.get("natal_planets", [])

        # natal_summary: 主要天体の位置を簡潔に
        major = ["Sun","Moon","Mercury","Venus","Mars","Jupiter","Saturn","ASC","MC"]
        natal_lines = []
        for p in natal_planets:
            if p.get("name") in major:
                natal_lines.append(f"{p['name']}: {p.get('sign','')} {p.get('house','')}ハウス")
        natal_summary = " / ".join(natal_lines) if natal_lines else "（データなし）"

        # long_term_summary: 時期・天体・アスペクト・出生天体を人間語に
        ASP_JP = {
            "conjunction": "が重なる", "opposition": "が向き合う",
            "square": "が摩擦を生む", "trine": "が流れをつくる", "sextile": "がチャンスをつくる"
        }
        PLANET_JP = {
            "Saturn": "土星（責任・制約・成熟）",
            "Jupiter": "木星（拡大・幸運・成長）",
            "Uranus": "天王星（変化・革新・解放）",
            "Neptune": "海王星（夢・直感・混乱）",
            "Pluto": "冥王星（変容・再生・深化）",
        }
        NATAL_JP = {
            "Sun": "あなた自身の核",
            "Moon": "感情・安心感",
            "Mercury": "思考・コミュニケーション",
            "Venus": "愛・価値観・お金",
            "Mars": "行動力・意欲",
            "Jupiter": "拡大・幸運のポイント",
            "Saturn": "責任・試練のポイント",
            "ASC": "自己表現・第一印象",
            "MC": "社会的な方向性・仕事",
            "North Node": "魂の方向性",
        }

        lt_lines = []
        for item in long_term[:15]:
            tp = PLANET_JP.get(item.get("transit_planet",""), item.get("transit_planet",""))
            asp = ASP_JP.get(item.get("aspect",""), item.get("aspect",""))
            np = NATAL_JP.get(item.get("natal_planet",""), item.get("natal_planet",""))
            start = item.get("start_date","")[:7].replace("-","年",1).replace("-","月")
            end   = item.get("end_date","")[:7].replace("-","年",1).replace("-","月")
            status = {"active":"進行中","upcoming":"まもなく","past":"終了"}.get(item.get("status",""),"")
            lt_lines.append(f"[{status}] {start}〜{end}: {tp}{asp}（{np}）")
        long_term_summary = "\n".join(lt_lines) if lt_lines else "（データなし）"

        # today_summary
        today_lines = []
        for a in today_aspects[:7]:
            tp = a.get("transit_planet","")
            np_label = NATAL_JP.get(a.get("natal_planet",""), a.get("natal_planet",""))
            asp = ASP_JP.get(a.get("aspect",""), a.get("aspect",""))
            today_lines.append(f"{PLANET_JP.get(tp,tp)}{asp}（{np_label}）orb{a.get('orb',0):.1f}°")
        today_summary = "\n".join(today_lines) if today_lines else "（データなし）"

        # プロンプト読み込み
        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl = (prompts_dir / "transit_interpret.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")

        prompt = tpl.format(
            common_rules=common_rules,
            natal_summary=natal_summary,
            long_term_summary=long_term_summary,
            today_summary=today_summary,
        )

        # generate_report の raw_prompt モードでそのまま送信
        try:
            from services.ai_report import generate_report as _gen
            _astro = {
                "_meta": {
                    "output_style": "web",
                    "detail_level": "standard",
                    "astrology_system": "western",
                    "user_name": "",
                    "display_name": "",
                    "birth_date": "",
                    "today": "",
                    "age_years": "",
                    "theme": "overall",
                    "message": prompt,
                    "observations_text": "",
                }
            }
            text = _gen(_astro, style="web", report_type="raw_prompt")
            if not text or "生成エラー" in text or text.startswith("GEMINI"):
                text = f"（AI生成エラー: {text}）"
        except Exception as ai_err:
            text = f"（AI生成エラー: {ai_err}）"

        return JSONResponse(content={"text": text})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/daily-theme/generate", response_class=JSONResponse)
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

        cleaned = (raw or "").strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        result: dict[str, Any]
        try:
            result = json.loads(cleaned)
        except Exception:
            result = {
                "period": period,
                "axis": axis,
                "date": snapshot.get("transit_date", date_str or ""),
                "summary": cleaned or "生成結果を取得できませんでした。",
                "core_themes": [],
                "push": [],
                "caution": [],
                "recommended_actions": [],
                "avoid_actions": [],
                "social_post": "",
                "type_translation_axis": "",
                "raw_text": raw or "",
            }

        result.setdefault("period", period)
        result.setdefault("axis", axis)
        result.setdefault("date", snapshot.get("transit_date", date_str or ""))
        result["source_transit"] = snapshot
        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))
