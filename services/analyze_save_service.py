from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException

from models import IntakeDraft, Order, OrderDelivery, OrderResultView


def loads_jsonish(value: str):
    try:
        return json.loads(value) if isinstance(value, str) else value
    except Exception:
        return {}


def good_text(*values):
    for value in values:
        txt = (value or "") if isinstance(value, str) else str(value or "")
        txt = txt.strip()
        if txt and "DEBUG" not in txt and "空文字" not in txt:
            return txt
    return ""


def save_analysis_result_to_order(
    *,
    request,
    from_order_code: str,
    ai_text: str,
    reader_text: str,
    line_text: str,
    inputs_json: str,
    payload_json: str,
    raw_json: str,
    structure_summary_json: str,
    handoff_yaml_full: str,
) -> str:
    from db import db_session
    from sqlalchemy import select
    from services.draft_service import link_report_to_draft
    from services.report_service import attach_report_to_order, ensure_report, update_report_ai, update_report_yaml
    from services.result_builder import (
        build_result_payload,
        build_yaml_from_analysis,
        render_report_html,
        render_result_html,
    )
    from services.yaml_log_service import create_yaml_log

    resolved_order_code = (from_order_code or request.session.get("analyze_from_order_code") or "").strip()
    if not resolved_order_code:
        raise HTTPException(status_code=400, detail="order_code missing")

    with db_session() as db:
        order = db.scalar(select(Order).where(Order.order_code == resolved_order_code))
        if not order:
            raise HTTPException(status_code=404, detail="order not found")

        inputs_obj = loads_jsonish(inputs_json)
        payload_obj = loads_jsonish(payload_json)
        raw_obj = loads_jsonish(raw_json)
        structure_obj = loads_jsonish(structure_summary_json)

        report_web = good_text(
            ai_text,
            payload_obj.get("web_text") if isinstance(payload_obj, dict) else "",
            payload_obj.get("report_text") if isinstance(payload_obj, dict) else "",
            raw_obj.get("web_text") if isinstance(raw_obj, dict) else "",
            raw_obj.get("report_text") if isinstance(raw_obj, dict) else "",
            (raw_obj.get("reports") or {}).get("web") if isinstance(raw_obj.get("reports"), dict) else "",
            ((raw_obj.get("western") or {}).get("web_text") if isinstance(raw_obj.get("western"), dict) else ""),
            ((raw_obj.get("western") or {}).get("report_text") if isinstance(raw_obj.get("western"), dict) else ""),
        )
        report_reader = good_text(reader_text)
        report_line = good_text(line_text)
        horoscope_image_url = (
            raw_obj.get("chart_image_url")
            or raw_obj.get("wheel_image_url")
            or ((raw_obj.get("western") or {}).get("chart_image_url") if isinstance(raw_obj.get("western"), dict) else "")
            or ((raw_obj.get("western") or {}).get("wheel_image_url") if isinstance(raw_obj.get("western"), dict) else "")
            or ""
        )

        summary = {
            "saved_from": "analyze",
            "from_order_code": order.order_code,
            "order": {"horoscope_image_url": horoscope_image_url},
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
            ai_text=ai_text or "",
            reader_text=reader_text or "",
            line_text=line_text or "",
            handoff_yaml_full=handoff_yaml_full or "",
        )
        yaml_log = create_yaml_log(
            db,
            order,
            yaml_body=yaml_body,
            summary=summary,
            created_by_type="system",
            created_by_id=None,
            log_type="generated",
            set_active=True,
        )
        db.flush()

        draft = db.scalar(select(IntakeDraft).where(IntakeDraft.promoted_order_id == order.id).order_by(IntakeDraft.id.desc()).limit(1))
        report = ensure_report(db, report_type="normal_web", order=order)
        update_report_yaml(db, report, yaml_payload=yaml_body, prompt_version="v2-draft-report")
        if draft:
            link_report_to_draft(db, draft, report)
        attach_report_to_order(db, report, order)

        delivery = db.scalar(
            select(OrderDelivery)
            .where(OrderDelivery.order_id == order.id)
            .order_by(OrderDelivery.updated_at.desc(), OrderDelivery.id.desc())
            .limit(1)
        )
        delivery_text = report_web
        if delivery:
            delivery.delivery_text = delivery_text
            delivery.is_draft = True
        else:
            db.add(
                OrderDelivery(
                    order_id=order.id,
                    reader_id=order.assigned_reader_id,
                    delivery_text=delivery_text,
                    is_draft=True,
                )
            )

        payload = build_result_payload(order, yaml_log, delivery_text=delivery_text)
        payload["raw_json"] = raw_obj
        payload["horoscope_image_url"] = horoscope_image_url or payload.get("horoscope_image_url") or ""

        view = db.scalar(
            select(OrderResultView)
            .where(OrderResultView.order_id == order.id)
            .order_by(OrderResultView.id.desc())
            .limit(1)
        )
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

        update_report_ai(
            db,
            report,
            sections={"web": report_web, "reader": report_reader, "line": report_line},
            result_payload=payload,
            result_html=view.result_html,
            model="gemini-2.5-flash",
            prompt_version="v2-draft-report",
        )
        if draft:
            link_report_to_draft(db, draft, report)
        order.primary_report_id = report.id

        if order.status in {"received", "paid", "assigned"}:
            order.status = "in_progress"
        db.commit()

    return resolved_order_code
