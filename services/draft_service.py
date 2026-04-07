from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import IntakeDraft, Menu, Order, Report


DEFAULT_DRAFT_TTL_DAYS = 14


def generate_draft_code() -> str:
    return f"D{uuid4().hex[:10].upper()}"


def create_or_update_draft_from_order_inputs(
    db: Session,
    *,
    menu: Menu | None,
    user_name: str | None,
    birth_date,
    user_contact: str | None = None,
    birth_time: str | None = None,
    birth_prefecture: str | None = None,
    birth_place: str | None = None,
    birth_lat: float | None = None,
    birth_lon: float | None = None,
    location_source: str | None = None,
    location_note: str | None = None,
    gender: str | None = None,
    consultation_text: str | None = None,
    source: str = "web",
    external_platform: str | None = None,
    external_order_ref: str | None = None,
    order_kind: str = "paid",
    requested_menu_code: str | None = None,
    generate_ai: bool = True,
    yaml_only: bool = False,
    existing_order: Order | None = None,
) -> IntakeDraft:
    draft = None
    if existing_order:
        draft = db.scalar(select(IntakeDraft).where(IntakeDraft.promoted_order_id == existing_order.id))
    if not draft and external_platform and external_order_ref:
        draft = db.scalar(
            select(IntakeDraft)
            .where(
                IntakeDraft.external_platform == external_platform,
                IntakeDraft.external_order_ref == external_order_ref,
            )
            .order_by(IntakeDraft.id.desc())
        )
    if not draft:
        draft = IntakeDraft(draft_code=generate_draft_code())
        db.add(draft)

    draft.source = source or draft.source or "web"
    draft.external_platform = external_platform or draft.external_platform
    draft.external_order_ref = external_order_ref or draft.external_order_ref
    draft.requested_menu_code = requested_menu_code or draft.requested_menu_code
    draft.menu_id = menu.id if menu else draft.menu_id
    draft.order_kind = order_kind or draft.order_kind or "paid"
    draft.user_name = (user_name or "").strip() or draft.user_name
    draft.user_contact = (user_contact or "").strip() or None
    draft.birth_date = birth_date
    draft.birth_time = (birth_time or "").strip() or None
    draft.birth_prefecture = (birth_prefecture or "").strip() or None
    draft.birth_place = (birth_place or "").strip() or None
    draft.birth_lat = birth_lat
    draft.birth_lon = birth_lon
    draft.location_source = (location_source or "").strip() or None
    draft.location_note = (location_note or "").strip() or None
    draft.gender = (gender or "").strip() or None
    draft.consultation_text = (consultation_text or "").strip() or None
    draft.generate_ai = bool(generate_ai and not yaml_only)
    draft.yaml_only = bool(yaml_only)
    draft.draft_status = "input_pending"
    draft.yaml_status = "pending"
    draft.ai_status = "not_requested" if yaml_only else "pending"
    draft.expires_at = datetime.utcnow() + timedelta(days=DEFAULT_DRAFT_TTL_DAYS)
    if existing_order:
        draft.promoted_order_id = existing_order.id
    db.flush()
    return draft


def promote_draft_to_order(db: Session, draft: IntakeDraft, order: Order) -> IntakeDraft:
    draft.promoted_order_id = order.id
    draft.draft_status = "promoted"
    draft.ai_status = "completed" if order.result_html else draft.ai_status
    db.flush()
    return draft


def link_report_to_draft(db: Session, draft: IntakeDraft, report: Report) -> None:
    report.draft_id = draft.id
    draft.latest_report_id = report.id
    if report.yaml_status == "completed":
        draft.yaml_status = "completed"
        if draft.draft_status in {"input_pending", "yaml_pending", "yaml_running"}:
            draft.draft_status = "yaml_completed"
    if report.ai_status == "completed":
        draft.ai_status = "completed"
        draft.draft_status = "ai_completed"
    db.flush()
