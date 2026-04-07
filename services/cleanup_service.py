from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import delete, or_, select
from sqlalchemy.orm import Session

from models import IntakeDraft, Report


def cleanup_expired_drafts(db: Session, dry_run: bool = True) -> dict:
    now = datetime.utcnow()
    first_cutoff = now - timedelta(days=3)
    second_cutoff = now - timedelta(days=7)

    target_rows = db.scalars(
        select(IntakeDraft).where(
            IntakeDraft.promoted_order_id.is_(None),
            or_(
                (IntakeDraft.draft_status.in_(["input_pending", "abandoned", "failed"]) & (IntakeDraft.updated_at < first_cutoff)),
                (
                    (IntakeDraft.yaml_status == "completed")
                    & (IntakeDraft.ai_status.in_(["not_requested", "completed", "failed"]))
                    & (IntakeDraft.updated_at < second_cutoff)
                ),
            ),
        )
    ).all()
    result = {"count": len(target_rows), "draft_codes": [row.draft_code for row in target_rows]}
    if dry_run or not target_rows:
        return result
    ids = [row.id for row in target_rows]
    db.execute(delete(Report).where(Report.draft_id.in_(ids), Report.order_id.is_(None)))
    db.execute(delete(IntakeDraft).where(IntakeDraft.id.in_(ids)))
    db.flush()
    return result


def cleanup_orphan_reports(db: Session, dry_run: bool = True) -> dict:
    cutoff = datetime.utcnow() - timedelta(days=7)
    rows = db.scalars(
        select(Report).where(
            Report.order_id.is_(None),
            Report.draft_id.is_(None),
            Report.updated_at < cutoff,
        )
    ).all()
    result = {"count": len(rows), "report_ids": [r.id for r in rows]}
    if dry_run or not rows:
        return result
    db.execute(delete(Report).where(Report.id.in_([r.id for r in rows])))
    db.flush()
    return result
