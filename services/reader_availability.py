from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Astrologer

OPEN_STATUSES = {"open"}
CLOSED_STATUSES = {"paused", "full", "hidden"}


def normalize_line_status(value: str | None) -> str:
    v = (value or "open").strip().lower()
    if v in OPEN_STATUSES | CLOSED_STATUSES:
        return v
    return "open"


def is_reader_line_available(reader: Astrologer | None) -> bool:
    if reader is None:
        return False
    if (reader.status or "").strip().lower() != "active":
        return False
    if not bool(getattr(reader, "line_accepting_enabled", True)):
        return False
    return normalize_line_status(getattr(reader, "line_accepting_status", "open")) == "open"


def line_status_label(status: str | None) -> str:
    normalized = normalize_line_status(status)
    return {
        "open": "受付中",
        "paused": "一時停止",
        "full": "満枠",
        "hidden": "非表示",
    }.get(normalized, "受付中")


def list_line_available_readers(db: Session) -> list[Astrologer]:
    readers = db.scalars(
        select(Astrologer)
        .where(Astrologer.status == "active")
        .order_by(Astrologer.display_name.asc(), Astrologer.id.asc())
    ).all()
    return [reader for reader in readers if is_reader_line_available(reader)]


def get_line_available_reader(db: Session, preferred_reader_id: int | None = None, preferred_reader_email: str | None = None) -> Astrologer | None:
    if preferred_reader_id:
        reader = db.scalar(select(Astrologer).where(Astrologer.id == preferred_reader_id, Astrologer.status == "active"))
        if is_reader_line_available(reader):
            return reader
    if preferred_reader_email:
        reader = db.scalar(select(Astrologer).where(Astrologer.login_email == preferred_reader_email, Astrologer.status == "active"))
        if is_reader_line_available(reader):
            return reader
    readers = list_line_available_readers(db)
    return readers[0] if readers else None


def line_unavailable_message(reader: Astrologer | None = None) -> str:
    if reader and (reader.line_accepting_message or "").strip():
        return reader.line_accepting_message.strip()
    return "現在、LINE受付を停止しています。時間をおいて再度お試しください。"
