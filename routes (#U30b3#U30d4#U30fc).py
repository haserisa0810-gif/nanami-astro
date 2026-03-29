"""
routes.py
FastAPI のルート定義のみを担当する薄い層。
占術計算・レポート生成・ログ構築は analyze_engine に委譲する。
"""
from __future__ import annotations

import os
import traceback
from typing import Any, Literal

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from auth import get_current_reader
from models import Astrologer
import json

from prefs import PREF_LABELS
from line_webhook import router as line_router
from routes_public_orders import router as public_orders_router
from routes_reader import router as reader_router
from routes_admin import router as admin_router
from routes_stripe import router as stripe_router
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
    from services.transit_calc import calc_transits_single, calc_transits_synastry, calc_transits_long_term
    from services.western_calc import calc_western_from_payload
    return calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_western_from_payload

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
    calc_transits_single, calc_transits_synastry, calc_transits_long_term, calc_western_from_payload = _calc_helpers()
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
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))


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
