from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import AppSetting


def _ensure_app_settings_table(db: Session) -> bool:
    try:
        AppSetting.__table__.create(bind=db.get_bind(), checkfirst=True)
        return True
    except Exception as exc:
        print('app_settings ensure table error:', repr(exc))
        return False


def _normalize_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "on", "yes", "open", "enabled", "active"}


def get_setting(db: Session, key: str, default: str | None = None) -> str | None:
    if not _ensure_app_settings_table(db):
        return default
    try:
        row = db.scalar(select(AppSetting).where(AppSetting.key == key))
        return row.value if row else default
    except Exception as exc:
        print('app_settings get_setting error:', repr(exc))
        return default


def set_setting(db: Session, key: str, value: str | None) -> AppSetting:
    if not _ensure_app_settings_table(db):
        raise RuntimeError('app_settings table is unavailable')
    try:
        row = db.scalar(select(AppSetting).where(AppSetting.key == key))
        normalized = '' if value is None else str(value)
        if row:
            row.value = normalized
        else:
            row = AppSetting(key=key, value=normalized)
            db.add(row)
            db.flush()
        return row
    except Exception as exc:
        print('app_settings set_setting error:', repr(exc))
        raise


def get_line_bot_settings(db: Session, env_mode: str | None = None) -> dict:
    mode_default = (env_mode or 'order').strip().lower() or 'order'
    try:
        line_bot_enabled = _normalize_bool(get_setting(db, 'line_bot_enabled', 'true'), True)
        line_order_accepting = _normalize_bool(get_setting(db, 'line_order_accepting', 'true'), True)
        line_bot_mode = (get_setting(db, 'line_bot_mode', mode_default) or mode_default).strip().lower() or mode_default
    except Exception as exc:
        print('app_settings get_line_bot_settings fallback:', repr(exc))
        line_bot_enabled = True
        line_order_accepting = True
        line_bot_mode = mode_default
    return {
        'line_bot_enabled': line_bot_enabled,
        'line_order_accepting': line_order_accepting,
        'line_bot_mode': line_bot_mode,
    }
