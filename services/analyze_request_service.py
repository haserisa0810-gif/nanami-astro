from __future__ import annotations

import traceback
from typing import Any

from fastapi import HTTPException


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


def normalize_checkbox_flags(**flags: str | None) -> dict[str, bool]:
    return {key: value is not None for key, value in flags.items()}


def build_inputs_view(**kwargs: Any) -> dict[str, Any]:
    return dict(kwargs)


def build_compatibility_transit_data(
    *,
    payload_a: dict[str, Any],
    birth_date_b: str | None,
    birth_time_b: str | None,
    birth_place_b: str | None,
    prefecture_b: str | None,
    lat_b: float | None,
    lon_b: float | None,
    calc_transits_synastry,
    calc_transits_long_term,
    calc_western_from_payload,
) -> dict[str, Any]:
    from shared import _calc_payload_from_inputs  # type: ignore

    _tmp_a = calc_western_from_payload(payload_a)
    natal_a_tmp = _tmp_a.get("planets", [])
    _unknowns_b: list[str] = []
    _payload_b = _calc_payload_from_inputs(
        birth_date=birth_date_b or "",
        birth_time=birth_time_b,
        birth_place=birth_place_b,
        prefecture=prefecture_b,
        lat=lat_b,
        lon=lon_b,
        unknowns=_unknowns_b,
    )
    _tmp_b = calc_western_from_payload(_payload_b)
    natal_b_tmp = _tmp_b.get("planets", [])
    synastry_transit = calc_transits_synastry(natal_a_tmp, natal_b_tmp)
    long_term_a = calc_transits_long_term(natal_a_tmp)
    long_term_b = calc_transits_long_term(natal_b_tmp)
    return {
        **synastry_transit,
        "long_term": long_term_a,
        "long_term_b": long_term_b,
    }


def build_single_transit_data(
    *,
    payload_a: dict[str, Any],
    calc_transits_single,
    calc_transits_long_term,
    calc_western_from_payload,
) -> dict[str, Any]:
    _tmp = calc_western_from_payload(payload_a)
    natal_planets_tmp = _tmp.get("planets", [])
    today_transit = calc_transits_single(natal_planets_tmp)
    long_term = calc_transits_long_term(natal_planets_tmp)
    return {**today_transit, "long_term": long_term}


def run_analyze_request(*, form_data: dict[str, Any]) -> dict[str, Any]:
    build_payload_a, build_base_meta, format_reports, build_handoff_logs, run_compatibility, run_single = _analyze_helpers()
    (
        calc_transits_single,
        calc_transits_synastry,
        calc_transits_long_term,
        _calc_global_transit_snapshot,
        calc_western_from_payload,
    ) = _calc_helpers()

    flags = normalize_checkbox_flags(
        include_asteroids=form_data.get("include_asteroids"),
        include_chiron=form_data.get("include_chiron"),
        include_lilith=form_data.get("include_lilith"),
        include_vertex=form_data.get("include_vertex"),
        include_reader=form_data.get("include_reader"),
        include_transit=form_data.get("include_transit"),
    )
    include_asteroids = flags["include_asteroids"]
    include_chiron = flags["include_chiron"]
    include_lilith = flags["include_lilith"]
    include_vertex = flags["include_vertex"]
    include_reader = flags["include_reader"]
    include_transit = flags["include_transit"]

    birth_date = form_data["birth_date"]
    birth_time = form_data.get("birth_time")
    birth_place = form_data.get("birth_place")
    prefecture = form_data.get("prefecture")
    lat = form_data.get("lat")
    lon = form_data.get("lon")
    from_order_code = form_data.get("from_order_code")
    gender = form_data.get("gender", "female")
    analysis_type = form_data.get("analysis_type", "single")
    astrology_system = form_data.get("astrology_system", "western")
    theme = form_data.get("theme", "overall")
    message = form_data.get("message")
    observations_text = form_data.get("observations_text")
    output_style = form_data.get("output_style", "normal")
    reading_style = form_data.get("reading_style", "general")
    detail_level = form_data.get("detail_level", "standard")
    ai_provider = form_data.get("ai_provider")
    ai_model = form_data.get("ai_model")
    house_system = form_data.get("house_system", "P")
    node_mode = form_data.get("node_mode", "true")
    lilith_mode = form_data.get("lilith_mode", "mean")
    day_change_at_23 = bool(form_data.get("day_change_at_23", False))
    name = form_data.get("name")
    name_b = form_data.get("name_b")
    birth_date_b = form_data.get("birth_date_b")
    birth_time_b = form_data.get("birth_time_b")
    birth_place_b = form_data.get("birth_place_b")
    prefecture_b = form_data.get("prefecture_b")
    lat_b = form_data.get("lat_b")
    lon_b = form_data.get("lon_b")
    gender_b = form_data.get("gender_b", "female")

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
        include_asteroids=include_asteroids,
        include_chiron=include_chiron,
        include_lilith=include_lilith,
        include_vertex=include_vertex,
        unknowns=unknowns,
    )

    base_meta = build_base_meta(
        birth_date=birth_date,
        output_style=output_style,
        reading_style=reading_style,
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
        ai_provider=ai_provider,
        ai_model=ai_model,
        day_change_at_23=day_change_at_23,
        name=name,
        name_b=name_b,
        gender=gender,
        gender_b=gender_b,
    )

    base_meta["style"] = reading_style

    if form_data.get("auto_recommendation") and isinstance(form_data.get("auto_recommendation"), dict):
        base_meta["auto_recommendation"] = form_data.get("auto_recommendation")

    inputs_view = build_inputs_view(
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
        reading_style=reading_style,
        detail_level=detail_level,
        ai_provider=ai_provider,
        ai_model=ai_model,
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
        observations_text=(observations_text or "").strip(),
        day_change_at_23=day_change_at_23,
    )

    astro_result: dict[str, Any] = {}

    try:
        if analysis_type == "compatibility":
            if not birth_date_b:
                raise HTTPException(status_code=400, detail="相性分析では相手の生年月日が必要です。")

            transit_data = None
            if include_transit:
                try:
                    transit_data = build_compatibility_transit_data(
                        payload_a=payload_a,
                        birth_date_b=birth_date_b,
                        birth_time_b=birth_time_b,
                        birth_place_b=birth_place_b,
                        prefecture_b=prefecture_b,
                        lat_b=lat_b,
                        lon_b=lon_b,
                        calc_transits_synastry=calc_transits_synastry,
                        calc_transits_long_term=calc_transits_long_term,
                        calc_western_from_payload=calc_western_from_payload,
                    )
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
        else:
            transit_data = None
            if include_transit:
                try:
                    transit_data = build_single_transit_data(
                        payload_a=payload_a,
                        calc_transits_single=calc_transits_single,
                        calc_transits_long_term=calc_transits_long_term,
                        calc_western_from_payload=calc_western_from_payload,
                    )
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

        report_web, report_raw, report_reader, report_line = format_reports(
            report_web=report_web,
            report_raw=report_raw,
            report_reader=report_reader,
            report_line=report_line,
            detail_level=detail_level,
            output_style=output_style,
        reading_style=reading_style,
            include_reader=include_reader,
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

        return {
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
            "handoff_json": logs["handoff_json"],
            "handoff_yaml": logs["handoff_yaml"],
            "handoff_json_full": logs["handoff_json_full"],
            "handoff_yaml_full": logs["handoff_yaml_full"],
            "handoff_json_delta": logs["handoff_json_delta"],
            "handoff_yaml_delta": logs["handoff_yaml_delta"],
            "bias_guard": bias_guard_obj,
            "transit_data": transit_data,
            "from_order_code": (from_order_code or "").strip() or None,
        }
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=str(e))
