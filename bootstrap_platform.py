from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import inspect, select, text
from sqlalchemy.orm import Session

import models  # noqa: F401  # モデルをBase.metadataへ登録
from auth import hash_password
from db import Base, SessionLocal, engine
from models import AdminUser, AppSetting, Astrologer, Menu

load_dotenv()


def _safe_seed_password(raw: str | None) -> str:
    return (raw or "").strip()


def _can_hash_password(raw: str | None) -> bool:
    value = _safe_seed_password(raw)
    return bool(value) and len(value.encode("utf-8")) <= 72


def _true_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _ensure_order_free_columns() -> None:
    inspector = inspect(engine)
    try:
        columns = {col["name"] for col in inspector.get_columns("orders")}
    except Exception:
        return
    required = {
        "order_kind": "ALTER TABLE orders ADD COLUMN order_kind VARCHAR(20) DEFAULT 'paid'",
        "free_reading_code": "ALTER TABLE orders ADD COLUMN free_reading_code VARCHAR(32)",
        "source_free_order_id": "ALTER TABLE orders ADD COLUMN source_free_order_id INTEGER",
        "ai_status": "ALTER TABLE orders ADD COLUMN ai_status VARCHAR(20) DEFAULT 'queued'",
        "result_payload_json": "ALTER TABLE orders ADD COLUMN result_payload_json TEXT",
        "result_html": "ALTER TABLE orders ADD COLUMN result_html TEXT",
        "free_result_text": "ALTER TABLE orders ADD COLUMN free_result_text TEXT",
    }
    with engine.begin() as conn:
        for name, ddl in required.items():
            if name not in columns:
                conn.execute(text(ddl))
        conn.execute(text("UPDATE orders SET order_kind = COALESCE(NULLIF(order_kind, ''), 'paid')"))
        conn.execute(text("UPDATE orders SET ai_status = CASE WHEN order_kind='free' THEN COALESCE(NULLIF(ai_status,''), 'queued') ELSE ai_status END"))
        try:
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_orders_free_reading_code ON orders (free_reading_code)"))
        except Exception:
            pass
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_orders_order_kind ON orders (order_kind)"))
        except Exception:
            pass
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_orders_ai_status ON orders (ai_status)"))
        except Exception:
            pass

def _ensure_order_location_columns() -> None:
    inspector = inspect(engine)
    try:
        columns = {col["name"] for col in inspector.get_columns("orders")}
    except Exception:
        return
    required = {
        "birth_prefecture": "ALTER TABLE orders ADD COLUMN birth_prefecture VARCHAR(50)",
        "birth_lat": "ALTER TABLE orders ADD COLUMN birth_lat FLOAT",
        "birth_lon": "ALTER TABLE orders ADD COLUMN birth_lon FLOAT",
        "location_source": "ALTER TABLE orders ADD COLUMN location_source VARCHAR(50)",
        "location_note": "ALTER TABLE orders ADD COLUMN location_note VARCHAR(255)",
    }
    with engine.begin() as conn:
        for name, ddl in required.items():
            if name not in columns:
                conn.execute(text(ddl))


def _ensure_order_staff_memo_column() -> None:
    inspector = inspect(engine)
    try:
        columns = {col["name"] for col in inspector.get_columns("orders")}
    except Exception:
        return
    if "staff_memo" in columns:
        return
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE orders ADD COLUMN staff_memo TEXT"))



def _ensure_astrologer_line_columns() -> None:
    inspector = inspect(engine)
    try:
        columns = {col["name"] for col in inspector.get_columns("astrologers")}
    except Exception:
        return
    required = {
        "line_accepting_enabled": "ALTER TABLE astrologers ADD COLUMN line_accepting_enabled BOOLEAN DEFAULT true",
        "line_accepting_status": "ALTER TABLE astrologers ADD COLUMN line_accepting_status VARCHAR(20) DEFAULT 'open'",
        "line_accepting_message": "ALTER TABLE astrologers ADD COLUMN line_accepting_message TEXT",
    }
    with engine.begin() as conn:
        for name, ddl in required.items():
            if name not in columns:
                conn.execute(text(ddl))
        conn.execute(text("UPDATE astrologers SET line_accepting_enabled = COALESCE(line_accepting_enabled, true)"))
        conn.execute(text("UPDATE astrologers SET line_accepting_status = COALESCE(NULLIF(line_accepting_status, ''), 'open')"))



def _ensure_yaml_log_columns() -> None:
    inspector = inspect(engine)
    try:
        columns = {col["name"] for col in inspector.get_columns("yaml_logs")}
    except Exception:
        return
    required = {
        "log_type": "ALTER TABLE yaml_logs ADD COLUMN log_type VARCHAR(30) DEFAULT 'generated'",
        "version_no": "ALTER TABLE yaml_logs ADD COLUMN version_no INTEGER DEFAULT 1",
        "is_active": "ALTER TABLE yaml_logs ADD COLUMN is_active BOOLEAN DEFAULT true",
    }
    with engine.begin() as conn:
        for name, ddl in required.items():
            if name not in columns:
                conn.execute(text(ddl))
        conn.execute(text("UPDATE yaml_logs SET log_type = COALESCE(NULLIF(log_type, ''), 'generated')"))
        conn.execute(text("UPDATE yaml_logs SET version_no = COALESCE(version_no, 1)"))
        conn.execute(text("UPDATE yaml_logs SET is_active = COALESCE(is_active, true)"))



def _ensure_staff_security_columns() -> None:
    inspector = inspect(engine)
    with engine.begin() as conn:
        try:
            admin_columns = {col["name"] for col in inspector.get_columns("admin_users")}
        except Exception:
            admin_columns = set()
        if "is_temp_password" not in admin_columns:
            conn.execute(text("ALTER TABLE admin_users ADD COLUMN is_temp_password BOOLEAN DEFAULT true"))
        try:
            reader_columns = {col["name"] for col in inspector.get_columns("astrologers")}
        except Exception:
            reader_columns = set()
        required_reader = {
            "is_temp_password": "ALTER TABLE astrologers ADD COLUMN is_temp_password BOOLEAN DEFAULT true",
            "stripe_account_id": "ALTER TABLE astrologers ADD COLUMN stripe_account_id VARCHAR(255)",
            "stripe_onboarding_completed": "ALTER TABLE astrologers ADD COLUMN stripe_onboarding_completed BOOLEAN DEFAULT false",
            "stripe_charges_enabled": "ALTER TABLE astrologers ADD COLUMN stripe_charges_enabled BOOLEAN DEFAULT false",
            "stripe_payouts_enabled": "ALTER TABLE astrologers ADD COLUMN stripe_payouts_enabled BOOLEAN DEFAULT false",
        }
        for name, ddl in required_reader.items():
            if name not in reader_columns:
                conn.execute(text(ddl))
        conn.execute(text("UPDATE admin_users SET is_temp_password = COALESCE(is_temp_password, true)"))
        conn.execute(text("UPDATE astrologers SET is_temp_password = COALESCE(is_temp_password, true)"))
        conn.execute(text("UPDATE astrologers SET stripe_onboarding_completed = COALESCE(stripe_onboarding_completed, false)"))
        conn.execute(text("UPDATE astrologers SET stripe_charges_enabled = COALESCE(stripe_charges_enabled, false)"))
        conn.execute(text("UPDATE astrologers SET stripe_payouts_enabled = COALESCE(stripe_payouts_enabled, false)"))
        try:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_astrologers_stripe_account_id ON astrologers (stripe_account_id)"))
        except Exception:
            pass


def _ensure_order_result_view_table() -> None:
    inspector = inspect(engine)
    try:
        tables = set(inspector.get_table_names())
    except Exception:
        return
    if "order_result_views" in tables:
        return
    from models import OrderResultView
    OrderResultView.__table__.create(bind=engine, checkfirst=True)

def _ensure_order_result_view_columns() -> None:
    inspector = inspect(engine)
    try:
        columns = {col["name"] for col in inspector.get_columns("order_result_views")}
    except Exception:
        return
    required = {
        "report_html": "ALTER TABLE order_result_views ADD COLUMN report_html TEXT",
        "report_generated_at": "ALTER TABLE order_result_views ADD COLUMN report_generated_at TIMESTAMP",
    }
    with engine.begin() as conn:
        for name, ddl in required.items():
            if name not in columns:
                conn.execute(text(ddl))

def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _ensure_order_free_columns()
    _ensure_order_location_columns()
    _ensure_order_staff_memo_column()
    _ensure_astrologer_line_columns()
    _ensure_staff_security_columns()
    _ensure_yaml_log_columns()
    _ensure_order_result_view_table()
    _ensure_order_result_view_columns()


def seed_defaults(db: Session) -> None:
    menus = [
        ("無料鑑定", "無料の簡易鑑定", 0, 0),
        ("恋愛鑑定", "恋愛の流れ・相性・動くべき時期を鑑定", 3000, 48),
        ("仕事鑑定", "転職・適職・人間関係・今後の流れを鑑定", 5000, 48),
        ("総合鑑定", "恋愛・仕事・全体運をまとめて鑑定", 10000, 72),
    ]
    for name, description, price, lead in menus:
        existing = db.scalar(select(Menu).where(Menu.name == name))
        if not existing:
            db.add(
                Menu(
                    name=name,
                    description=description,
                    price=price,
                    lead_time_hours=lead,
                    is_active=True,
                )
            )

    admin_email = os.getenv("DEFAULT_ADMIN_EMAIL", "admin@example.com")
    admin_password = _safe_seed_password(os.getenv("DEFAULT_ADMIN_PASSWORD", "admin1234"))
    admin_name = os.getenv("DEFAULT_ADMIN_NAME", "運営管理者")
    if not db.scalar(select(AdminUser).where(AdminUser.login_email == admin_email)) and _can_hash_password(admin_password):
        try:
            db.add(
                AdminUser(
                    login_email=admin_email,
                    password_hash=hash_password(admin_password),
                    display_name=admin_name,
                    is_active=True,
                    is_temp_password=True,
                )
            )
        except Exception as exc:
            print(f"[bootstrap] skip default admin seed: {exc}")
    elif not _can_hash_password(admin_password):
        print("[bootstrap] skip default admin seed: invalid password length")

    default_settings = {
        "line_session_prune_minutes": str(int(os.getenv("LINE_SESSION_TTL_SECONDS", str(60 * 60 * 6))) // 60),
        "line_bot_enabled": "true",
        "line_order_accepting": "true",
        "line_bot_mode": (os.getenv("LINE_BOT_MODE") or "order").strip().lower() or "order",
    }
    for key, value in default_settings.items():
        if not db.scalar(select(AppSetting).where(AppSetting.key == key)):
            db.add(AppSetting(key=key, value=value))

    reader_email = os.getenv("DEFAULT_READER_EMAIL", "reader@example.com")
    reader_password = _safe_seed_password(os.getenv("DEFAULT_READER_PASSWORD", "reader1234"))
    reader_name = os.getenv("DEFAULT_READER_NAME", "七海先生")
    enable_default_reader_seed = _true_env("ENABLE_DEFAULT_READER_SEED", "false")
    if enable_default_reader_seed and not db.scalar(select(Astrologer).where(Astrologer.login_email == reader_email)) and _can_hash_password(reader_password):
        try:
            db.add(
                Astrologer(
                    display_name=reader_name,
                    login_email=reader_email,
                    password_hash=hash_password(reader_password),
                    is_temp_password=True,
                    status="active",
                    commission_rate=60.00,
                    line_accepting_enabled=True,
                    line_accepting_status="open",
                )
            )
        except Exception as exc:
            print(f"[bootstrap] skip default reader seed: {exc}")
    elif enable_default_reader_seed and not _can_hash_password(reader_password):
        print("[bootstrap] skip default reader seed: invalid password length")

    db.commit()


def main() -> None:
    print(f"[bootstrap] DATABASE_URL={os.getenv('DATABASE_URL', '').split('@')[0] if os.getenv('DATABASE_URL') else 'env-not-set'}")
    init_db()
    with SessionLocal() as db:
        seed_defaults(db)
    print("bootstrap completed")


if __name__ == "__main__":
    main()
