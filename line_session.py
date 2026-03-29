from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select

from db import SessionLocal
from models import LineSession

_SESSION_TTL_SECONDS = int(os.getenv("LINE_SESSION_TTL_SECONDS", str(60 * 60 * 6)))


def _now() -> datetime:
    return datetime.utcnow()


def _expires_at() -> datetime:
    return _now() + timedelta(seconds=_SESSION_TTL_SECONDS)


def _normalize_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = dict(payload or {})
    data.pop("updated_at", None)
    return data


def _session_to_dict(row: LineSession | None) -> dict[str, Any]:
    if not row:
        return {}
    try:
        data = json.loads(row.session_json or "{}")
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    data["updated_at"] = int((row.updated_at or _now()).timestamp())
    return data


def _delete_expired(db) -> None:
    now = _now()
    expired_rows = db.scalars(select(LineSession).where(LineSession.expires_at.is_not(None), LineSession.expires_at < now)).all()
    for row in expired_rows:
        db.delete(row)
    if expired_rows:
        db.commit()


def get_session(user_id: str | None) -> dict[str, Any]:
    if not user_id:
        return {}
    with SessionLocal() as db:
        _delete_expired(db)
        row = db.scalar(select(LineSession).where(LineSession.line_user_id == user_id))
        if not row:
            return {}
        if row.expires_at and row.expires_at < _now():
            db.delete(row)
            db.commit()
            return {}
        return _session_to_dict(row)


def upsert_session(user_id: str | None, values: dict[str, Any]) -> dict[str, Any]:
    merged = _normalize_payload(values)
    if not user_id:
        return merged

    with SessionLocal() as db:
        _delete_expired(db)
        row = db.scalar(select(LineSession).where(LineSession.line_user_id == user_id))
        current = _session_to_dict(row)
        current.pop("updated_at", None)
        for key, value in (merged or {}).items():
            if value in (None, ""):
                continue
            current[key] = value

        if row is None:
            row = LineSession(
                line_user_id=user_id,
                state=str(current.get("state") or "idle"),
                session_json=json.dumps(current, ensure_ascii=False),
                expires_at=_expires_at(),
            )
            db.add(row)
        else:
            row.state = str(current.get("state") or "idle")
            row.session_json = json.dumps(current, ensure_ascii=False)
            row.expires_at = _expires_at()
        db.commit()
        db.refresh(row)
        return _session_to_dict(row)


def clear_session(user_id: str | None) -> None:
    if not user_id:
        return
    with SessionLocal() as db:
        row = db.scalar(select(LineSession).where(LineSession.line_user_id == user_id))
        if row:
            db.delete(row)
            db.commit()
