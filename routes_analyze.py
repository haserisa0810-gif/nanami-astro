from __future__ import annotations

import traceback
from typing import Any, Literal

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from routes_shared import templates

router = APIRouter()

AstrologySystem = Literal["western", "vedic", "integrated", "shichusuimei", "integrated3", "integrated_w_shichu"]
AnalysisType = Literal["single", "compatibility"]


TRUE_VALUES = {"1", "true", "on", "yes", "y"}
FALSE_VALUES = {"0", "false", "off", "no", "n", ""}


def _analyze_helpers():
    from services.analyze_engine import (
        build_base_meta,
        build_handoff_logs,
        build_payload_a,
        format_reports,
        run_compatibility,
        run_single,
    )

    return build_payload_a, build_base_meta, format_reports, build_handoff_logs, run_compatibility, run_single


def _calc_helpers():
    from services.transit_calc import (
        calc_global_transit_snapshot,
        calc_transits_long_term,
        calc_transits_single,
        calc_transits_synastry,
    )
    from services.western_calc import calc_western_from_payload

    return (
        calc_transits_single,
        calc_transits_synastry,
        calc_transits_long_term,
        calc_global_transit_snapshot,
        calc_western_from_payload,
    )


def _parse_checkbox(value: Any, *, default: bool = False) -> bool:
    """Robust checkbox parser.

    HTML checkbox values vary by template and browser: omitted / on / true / 1.
    Also, some older templates may omit generate_ai entirely. In that case,
    we want generate_ai to default to True, not False.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return default


def _build_inputs_view(**kwargs: Any) -> dict[str, Any]:
    return dict(kwargs)


@router.post("/analyze", response_class=HTMLResponse)
def analyze(
    request: Request,
    name: str | None = Form(None),
    birth_date: str = Form(...),
    birth_time: str | None = Form(None),
    birth_place: str | None = Form(None),
    prefecture: str | None = Form(None),
    lat: float | None = Form(None),
    lon: float | None = Form(None),
    from_order_code: str | None = Form(None),
    gender: str = Form("female"),
    analysis_type: AnalysisType = Form("single"),
    astrology_system: AstrologySystem = Form("western"),
    theme: str = Form("overall"),
    message: str | None = Form(None),
    observations_text: str | None = Form(None),
    output_style: str = Form("normal"),
    reading_style: str = Form("general"),
    detail_level: str = Form("standard"),
    ai_provider: str | None = Form(None),
    ai_model: str | None = Form(None),
    generate_ai: str | None = Form(None),
    yaml_only: str | None = Form(None),
    house_system: str = Form("P"),
    node_mode: str = Form("true"),
    lilith_mode: str = Form("mean"),
    include_asteroids: str | None = Form(None),
    include_chiron: str | None = Form(None),
    include_lilith: str | None = Form(None),
    include_vertex: str | None = Form(None),
    include_reader: str | None = Form(None),
    day_change_at_23: bool = Form(False),
    include_transit: str | None = Form(None),
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
    (
        calc_transits_single,
        calc_transits_synastry,
        calc_transits_long_term,
        _calc_global_transit_snapshot,
        calc_western_from_payload,
    ) = _calc_helpers()

    include_asteroids_flag = _parse_checkbox(include_asteroids, default=False)
    include_chiron_flag = _parse_checkbox(include_chiron, default=False)
    include_lilith_flag = _parse_checkbox(include_lilith, default=False)
    include_vertex_flag = _parse_checkbox(include_vertex, default=False)
    include_reader_flag = _parse_checkbox(include_reader, default=False)
    include_transit_flag = _parse_checkbox(include_transit, default=False)

    # Important: AI本文はUIでチェックが消えたり name がズレたときでも、
    # 既定では ON 扱いにする。
    yaml_only_flag = _parse_checkbox(yaml_only, default=False)
    generate_ai_flag = _parse_checkbox(generate_ai, default=True)
    if yaml_only_flag:
        generate_ai_flag = False

    unknowns: list[str] = []

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
        include_asteroids=include_asteroids_flag,
        include_chiron=include_chiron_flag,
        include_lilith=include_lilith_flag,
        include_vertex=include_vertex_flag,
        unknowns=unknowns,
    )

    base_meta = build_base_meta(
        birth_date=birth_date,
        output_style=output_style,
        detail_level=detail_level,
        house_system=house_system,
        node_mode=node_mode,
        lilith_mode=lilith_mode,
        include_asteroids=include_asteroids_flag,
        include_chiron=include_chiron_flag,
        include_lilith=include_lilith_flag,
        include_vertex=include_vertex_flag,
        include_reader=include_reader_flag,
        theme=theme,
        message=message,
        observations_text=observations_text,
        analysis_type=analysis_type,
        astrology_system=astrology_system,
        ai_provider=ai_provider,
        ai_model=ai_model,
        day_change_at_23=day_change_at_23,
        name=name,
        name_b=name_b,
        gender=gender,
        gender_b=gender_b,
    )
    base_meta["generate_ai"] = generate_ai_flag
    base_meta["yaml_only"] = yaml_only_flag
    base_meta["style"] = reading_style

    inputs_view = _build_inputs_view(
        analysis_type=analysis_type,
        astrology_system=astrology_system,
        name=name,
        birth_date=birth_date,
        birth_time=birth_time,
        birth_place=birth_place,
        prefecture=prefecture,
        gender=gender,
        name_b=name_b,
        birth_date_b=birth_date_b,
        birth_time_b=birth_time_b,
        birth_place_b=birth_place_b,
        prefecture_b=prefecture_b,
        gender_b=gender_b,
        output_style=output_style,
        detail_level=detail_level,
        ai_provider=ai_provider,
        ai_model=ai_model,
        house_system=house_system,
        node_mode=node_mode,
        lilith_mode=lilith_mode,
        include_asteroids=include_asteroids_flag,
        include_chiron=include_chiron_flag,
        include_lilith=include_lilith_flag,
        include_vertex=include_vertex_flag,
        include_reader=include_reader_flag,
        include_transit=include_transit_flag,
        theme=theme,
        message=message,
        observations_text=(observations_text or "").strip(),
        day_change_at_23=day_change_at_23,
        generate_ai=generate_ai_flag,
        yaml_only=yaml_only_flag,
    )

    astro_result: dict[str, Any] = {}

    try:
        if analysis_type == "compatibility":
            if not birth_date_b:
                raise HTTPException(status_code=400, detail="相性分析では相手の生年月日が必要です。")

            transit_data = None
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
                include_asteroids=include_asteroids_flag,
                include_chiron=include_chiron_flag,
                include_lilith=include_lilith_flag,
                include_vertex=include_vertex_flag,
                include_reader=include_reader_flag,
                base_meta=base_meta,
                unknowns=unknowns,
            )
            guard_meta: dict[str, Any] = {}
        else:
            transit_data = None
            if include_transit_flag:
                try:
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
                include_reader=include_reader_flag,
                day_change_at_23=day_change_at_23,
                transit_data=transit_data,
            )
            base_meta["bias_guard"] = guard_meta

        report_web, report_raw, report_reader, report_line = format_reports(
            report_web=report_web,
            report_raw=report_raw,
            report_reader=report_reader,
            report_line=report_line,
            detail_level=detail_level,
            output_style=output_style,
            include_reader=include_reader_flag,
        )

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

        # YAML-only のときだけ YAML を本文欄に出す。
        # generate_ai が何らかの都合で false でも yaml_only でなければ、
        # 既存の本文があるならそれを優先して落とさない。
        if yaml_only_flag:
            report_web = "YAMLログのみ作成モードです。下の内容を確認してください。\n\n" + (
                logs.get("handoff_yaml_full") or logs.get("handoff_yaml") or ""
            )
            report_line = ""
            report_reader = ""

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
                "include_reader": include_reader_flag,
                "handoff_json": logs["handoff_json"],
                "handoff_yaml": logs["handoff_yaml"],
                "handoff_json_full": logs["handoff_json_full"],
                "handoff_yaml_full": logs["handoff_yaml_full"],
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
