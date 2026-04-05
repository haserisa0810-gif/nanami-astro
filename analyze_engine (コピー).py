"""
analyze_engine.py
占術計算・レポート生成・バイアスガード・YAMLログ構築を担当するモジュール。
ルーティングやHTTPリクエスト/レスポンスには関与しない。
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from fastapi import HTTPException

from services.western_calc import calc_western_from_payload
from services.ai_report import generate_report
from services.bias_guard import (
    derive_risk_flags_from_astro,
    validate_generated_text,
    build_fix_instructions,
    compact_guard_meta,
)
from services.handoff_log import build_handoff, dumps_json, dumps_yaml
from shared import (
    _age_years,
    _attach_meta,
    _calc_payload_from_inputs,
    apply_detail_level,
    format_by_style,
)


def _should_force_pro_model(
    *,
    base_meta: dict[str, Any],
    astrology_system: str,
    include_reader: bool,
    transit_data: dict[str, Any] | None = None,
) -> bool:
    """
    モデル自動切替は最小限にする。
    明示指定がある場合は必ずそれを優先し、勝手に Pro へ寄せない。
    """
    requested_model = str(base_meta.get("ai_model") or "").strip().lower()
    if requested_model in {
        "gemini-2.5-pro", "pro",
        "gemini-2.5-flash", "flash",
        "gemini-2.5-flash-lite", "flash-lite", "lite",
    }:
        return requested_model in {"gemini-2.5-pro", "pro"}

    detail_level = str(base_meta.get("detail_level") or "").strip().lower()

    # 未指定時だけ本当に重い条件で Pro を使う
    return (
        detail_level == "max"
        and astrology_system in {"integrated3", "integrated_3"}
        and include_reader
        and isinstance(transit_data, dict)
        and bool(transit_data)
        and not transit_data.get("error")
    )


def _with_effective_ai_model(
    base_meta: dict[str, Any],
    *,
    astrology_system: str,
    include_reader: bool,
    transit_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective = dict(base_meta)
    if _should_force_pro_model(
        base_meta=base_meta,
        astrology_system=astrology_system,
        include_reader=include_reader,
        transit_data=transit_data,
    ):
        effective["ai_model"] = "gemini-2.5-pro"
    return effective


# ── 占術計算 ─────────────────────────────────────────────────────────────────

def _calc_vedic(payload: dict[str, Any]) -> dict[str, Any]:
    import logging
    logging.getLogger(__name__).warning("VEDIC_CALC_CALLED")
    try:
        from services.vedic_calc import calc_vedic_from_payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"vedic_calc が読み込めません: {e}")
    return calc_vedic_from_payload(payload)


def run_astro_calc(
    astrology_system: str,
    payload_a: dict[str, Any],
    day_change_at_23: bool = False,
) -> dict[str, Any]:
    """占術タイプに応じて計算を実行し、生の結果を返す。"""
    if astrology_system == "vedic":
        return _calc_vedic(payload_a)

    if astrology_system == "integrated":
        return {
            "western": calc_western_from_payload(payload_a),
            "vedic": _calc_vedic(payload_a),
        }

    if astrology_system == "shichusuimei":
        from services.shichusuimei_calc import calc_shichusuimei_from_payload
        return calc_shichusuimei_from_payload(
            payload_a,
            tz_name="Asia/Tokyo",
            day_change_at_23=day_change_at_23,
        )

    if astrology_system == "integrated3":
        from services.shichusuimei_calc import calc_shichusuimei_from_payload
        return {
            "western": calc_western_from_payload(payload_a),
            "vedic": _calc_vedic(payload_a),
            "shichusuimei": calc_shichusuimei_from_payload(
                payload_a,
                tz_name="Asia/Tokyo",
                day_change_at_23=day_change_at_23,
            ),
        }

    # デフォルト: western
    return calc_western_from_payload(payload_a)


# ── ペイロード構築 ────────────────────────────────────────────────────────────

def build_payload_a(
    birth_date: str,
    birth_time: str | None,
    birth_place: str | None,
    prefecture: str | None,
    lat: float | None,
    lon: float | None,
    gender: str,
    house_system: str,
    node_mode: str,
    lilith_mode: str,
    include_asteroids: bool,
    include_chiron: bool,
    include_lilith: bool,
    include_vertex: bool,
    unknowns: list[str],
) -> dict[str, Any]:
    payload = _calc_payload_from_inputs(
        birth_date=birth_date,
        birth_time=birth_time,
        birth_place=birth_place,
        prefecture=prefecture,
        lat=lat,
        lon=lon,
        unknowns=unknowns,
    )
    payload["gender"] = gender
    payload["house_system"] = house_system
    payload["node_mode"] = node_mode
    payload["lilith_mode"] = lilith_mode
    payload["include_asteroids"] = include_asteroids
    payload["include_chiron"] = include_chiron
    payload["include_lilith"] = include_lilith
    payload["include_vertex"] = include_vertex
    return payload


def build_base_meta(
    birth_date: str,
    output_style: str,
    detail_level: str,
    house_system: str,
    node_mode: str,
    lilith_mode: str,
    include_asteroids: bool,
    include_chiron: bool,
    include_lilith: bool,
    include_vertex: bool,
    include_reader: bool,
    theme: str,
    message: str | None,
    observations_text: str | None,
    analysis_type: str,
    astrology_system: str,
    ai_model: str | None,
    day_change_at_23: bool,
    name: str | None,
    name_b: str | None,
    gender: str,
    gender_b: str,
) -> dict[str, Any]:
    today = date.today().isoformat()
    return {
        "birth_date": birth_date,
        "today": today,
        "age_years": _age_years(birth_date, today),
        "output_style": output_style,
        "detail_level": detail_level,
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
        "analysis_type": analysis_type,
        "astrology_system": astrology_system,
        "ai_model": ai_model,
        "shichusuimei": {
            "tz_name": "Asia/Tokyo",
            "day_change_at_23": day_change_at_23,
        },
        "name": name,
        "name_b": name_b,
        "gender": gender,
        "gender_b": gender_b,
    }


# ── 相性分析 ─────────────────────────────────────────────────────────────────

def run_compatibility(
    payload_a: dict[str, Any],
    birth_date_b: str,
    birth_time_b: str | None,
    birth_place_b: str | None,
    prefecture_b: str | None,
    lat_b: float | None,
    lon_b: float | None,
    gender_b: str,
    house_system: str,
    node_mode: str,
    lilith_mode: str,
    include_asteroids: bool,
    include_chiron: bool,
    include_lilith: bool,
    include_vertex: bool,
    include_reader: bool,
    base_meta: dict[str, Any],
    unknowns: list[str],
) -> tuple[dict[str, Any], dict[str, Any], str, str, str]:
    """
    相性分析を実行する。
    戻り値: (astro_result, payload_view, report_web, report_line, report_raw, report_reader)
    """
    unknowns_b: list[str] = []
    payload_b = _calc_payload_from_inputs(
        birth_date=birth_date_b,
        birth_time=birth_time_b,
        birth_place=birth_place_b,
        prefecture=prefecture_b,
        lat=lat_b,
        lon=lon_b,
        unknowns=unknowns_b,
    )
    payload_b["gender"] = gender_b
    payload_b["house_system"] = house_system
    payload_b["node_mode"] = node_mode
    payload_b["lilith_mode"] = lilith_mode
    payload_b["include_asteroids"] = include_asteroids
    payload_b["include_chiron"] = include_chiron
    payload_b["include_lilith"] = include_lilith
    payload_b["include_vertex"] = include_vertex
    unknowns.extend([f"相手: {u}" for u in unknowns_b])

    astro_a = _attach_meta(calc_western_from_payload(payload_a), base_meta)
    astro_b = _attach_meta(calc_western_from_payload(payload_b), {**base_meta, "birth_date": birth_date_b})

    effective_meta = _with_effective_ai_model(
        {**base_meta, "analysis_type": "compatibility", "astrology_system": "western"},
        astrology_system="western",
        include_reader=include_reader,
        transit_data=None,
    )

    astro_result: dict[str, Any] = {"personA": astro_a, "personB": astro_b}
    astro_result = _attach_meta(
        astro_result,
        effective_meta,
    )

    report_web    = generate_report(astro_result, style="web", report_type="compat_web")
    report_line   = ""
    report_raw    = "（相性分析では占い師メモは未対応）" if include_reader else ""
    report_reader = ""
    payload_view: dict[str, Any] = {"personA": payload_a, "personB": payload_b}

    return astro_result, payload_view, report_web, report_line, report_raw, report_reader


# ── 通常鑑定 ─────────────────────────────────────────────────────────────────

def run_single(
    astrology_system: str,
    payload_a: dict[str, Any],
    base_meta: dict[str, Any],
    message: str | None,
    include_reader: bool,
    day_change_at_23: bool,
    transit_data: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str, str, str, dict[str, Any]]:
    """
    通常鑑定（1名）を実行する。
    戻り値: (astro_result, payload_view, report_web, report_line, report_raw, report_reader, guard_meta)
    report_raw    = ""（廃止：API節約のためraw版を削除）
    report_reader = 構造版メモ（single_web_reader）
    """
    effective_meta = _with_effective_ai_model(
        base_meta,
        astrology_system=astrology_system,
        include_reader=include_reader,
        transit_data=transit_data,
    )

    astro_result = run_astro_calc(astrology_system, payload_a, day_change_at_23)
    astro_result = _attach_meta(astro_result, effective_meta)

    # transit データをAIプロンプトに渡せるよう埋め込む
    if isinstance(transit_data, dict):
        astro_result["transit"] = transit_data

    report_web  = generate_report(astro_result, style="web", report_type="single_web")
    report_line = ""

    # 占い師メモ：構造版のみ（raw版は廃止・API節約）
    report_raw    = ""
    report_reader = generate_report(astro_result, style="web", report_type="single_web_reader") if include_reader else ""

    # バイアスガード（構造版のみ対象。裏カルテはシビアな内容を意図的に許容）
    risk_flags = derive_risk_flags_from_astro(astro_result)
    guard_meta: dict[str, Any] = {"status": "ok", "ok": True, "issues": [], "retries": 0}

    if include_reader:
        for attempt in range(2):
            gr = validate_generated_text(text=report_reader, report_type="single_web_reader", risk_flags=risk_flags)
            if gr.ok:
                guard_meta = compact_guard_meta(gr)
                break
            fix = build_fix_instructions(gr, "single_web_reader")
            report_reader = generate_report(
                astro_result,
                style="web",
                report_type="single_web_reader",
                meta={"message": ((message or "") + "\n\n" + fix).strip()},
            )
            gr.retries = attempt + 1
            guard_meta = compact_guard_meta(gr)

    grc = validate_generated_text(text=report_web, report_type="single_web", risk_flags=risk_flags)
    if not grc.ok:
        fix = build_fix_instructions(grc, "single_web")
        report_web = generate_report(
            astro_result,
            style="web",
            report_type="single_web",
            meta={"message": ((message or "") + "\n\n" + fix).strip()},
        )

    payload_view = {**payload_a, "day_change_at_23": day_change_at_23}
    return astro_result, payload_view, report_web, report_line, report_raw, report_reader, guard_meta


# ── レポート整形 ──────────────────────────────────────────────────────────────

def format_reports(
    report_web: str,
    report_raw: str,
    report_reader: str,
    report_line: str,
    detail_level: str,
    output_style: str,
    include_reader: bool,
) -> tuple[str, str, str, str]:
    """表示用に整形して (report_web, report_raw, report_reader, report_line) を返す。"""
    report_web    = format_by_style(apply_detail_level(report_web, detail_level), output_style)
    report_raw    = format_by_style(apply_detail_level(report_raw, "standard"), "web")
    report_reader = format_by_style(apply_detail_level(report_reader, "standard"), "web")
    report_line   = format_by_style(apply_detail_level(report_line, "short"), "line")

    if not (report_web or "").strip():
        report_web = "（DEBUG）generate_report が空文字を返しました。Cloud Run ログを確認してください。"
    if report_line is None:
        report_line = ""
    if include_reader:
        if not (report_raw or "").strip():
            report_raw = ""  # raw版廃止済み
        if not (report_reader or "").strip():
            report_reader = "（DEBUG）readerレポートが空文字を返しました。"
    else:
        report_raw    = ""
        report_reader = ""

    return report_web, report_raw, report_reader, report_line


# ── YAMLログ構築 ──────────────────────────────────────────────────────────────

def build_handoff_logs(
    inputs_view: dict[str, Any],
    payload_view: dict[str, Any],
    unknowns: list[str],
    astro_result: dict[str, Any],
    report_web: str,
    report_raw: str,
    report_reader: str,
    report_line: str,
    observations_text: str | None,
    bias_guard_obj: dict[str, Any],
    transit: dict[str, Any] | None = None,
) -> dict[str, str]:
    """
    mini / full / delta の YAML / JSON ログを構築して辞書で返す。
    キー: handoff_json, handoff_yaml, handoff_json_full, handoff_yaml_full,
          handoff_json_delta, handoff_yaml_delta
    """
    try:
        from services.ai_report import _build_structure_summary  # type: ignore
    except Exception:
        _build_structure_summary = None  # type: ignore

    summary_obj: Any = None
    try:
        if _build_structure_summary is not None:
            s = _build_structure_summary(astro_result)
            summary_obj = json.loads(s) if isinstance(s, str) and s.strip().startswith("{") else s
    except Exception:
        summary_obj = None

    reports_obj = {"web": report_web, "raw": report_raw, "reader": report_reader, "line": report_line}
    common = dict(
        inputs_view=inputs_view,
        payload_view=payload_view,
        unknowns=unknowns,
        structure_summary=summary_obj,
        reports=reports_obj,
        observations_text=(observations_text or ""),
        bias_guard=bias_guard_obj,
    )

    # build_handoff に transit 引数がない版との互換対応
    # miniでは past を除いた long_term のみ渡す
    def _slim_t(t):
        if not isinstance(t, dict):
            return t
        import copy
        tc = copy.deepcopy(t)
        if isinstance(tc.get("long_term"), list):
            tc["long_term"] = [x for x in tc["long_term"]
                               if isinstance(x, dict) and x.get("status") in ("active","upcoming")][:15]
        if isinstance(tc.get("aspects"), list):
            tc["aspects"] = tc["aspects"][:10]
        return tc

    handoff_mini  = build_handoff(**common, mode="mini")
    handoff_full  = build_handoff(**common, mode="full")
    handoff_delta = build_handoff(**common, mode="delta", prev=None)

    # transit を構築後に直接注入
    if transit is not None:
        handoff_mini["transit"]  = _slim_t(transit)
        handoff_full["transit"]  = transit

    return {
        "handoff_json":       dumps_json(handoff_mini),
        "handoff_yaml":       dumps_yaml(handoff_mini),
        "handoff_json_full":  dumps_json(handoff_full),
        "handoff_yaml_full":  dumps_yaml(handoff_full),
        "handoff_json_delta": dumps_json(handoff_delta),
        "handoff_yaml_delta": dumps_yaml(handoff_delta),
        "structure_summary_json": summary_obj,
    }
