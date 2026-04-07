from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import IntakeDraft, Order, Report


def ensure_report(
    db: Session,
    *,
    report_type: str,
    draft: IntakeDraft | None = None,
    order: Order | None = None,
) -> Report:
    stmt = select(Report).where(Report.report_type == report_type)
    if draft is not None:
        stmt = stmt.where(Report.draft_id == draft.id)
    elif order is not None:
        stmt = stmt.where(Report.order_id == order.id)
    else:
        raise ValueError("draft or order required")
    report = db.scalar(stmt.order_by(Report.id.desc()))
    if report:
        return report
    report = Report(report_type=report_type, draft_id=draft.id if draft else None, order_id=order.id if order else None)
    db.add(report)
    db.flush()
    return report


def update_report_yaml(
    db: Session,
    report: Report,
    *,
    yaml_payload: str,
    prompt_version: str | None = None,
) -> Report:
    report.yaml_payload = yaml_payload
    report.yaml_status = "completed"
    if prompt_version:
        report.prompt_version = prompt_version
    db.flush()
    return report


def update_report_ai(
    db: Session,
    report: Report,
    *,
    sections: dict[str, Any] | None = None,
    result_payload: dict[str, Any] | None = None,
    result_html: str | None = None,
    model: str | None = None,
    prompt_version: str | None = None,
) -> Report:
    report.sections_json = json.dumps(sections or {}, ensure_ascii=False)
    report.result_payload_json = json.dumps(result_payload or {}, ensure_ascii=False)
    report.result_html = result_html
    report.ai_status = "completed"
    report.generated_at = datetime.utcnow()
    if model:
        report.model = model
    if prompt_version:
        report.prompt_version = prompt_version
    db.flush()
    return report


def attach_report_to_order(db: Session, report: Report, order: Order) -> Report:
    report.order_id = order.id
    order.primary_report_id = report.id
    db.flush()
    return report
