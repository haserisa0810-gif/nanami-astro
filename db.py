from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

load_dotenv()


def _build_database_url() -> str:
    raw = (os.getenv("DATABASE_URL") or "").strip()
    if raw:
        if raw.startswith("postgres://"):
            return "postgresql+psycopg://" + raw[len("postgres://"):]
        if raw.startswith("postgresql://") and "+psycopg" not in raw:
            return "postgresql+psycopg://" + raw[len("postgresql://"):]
        return raw

    # Cloud Run / Cloud SQL を個別環境変数で構成したい場合の救済
    db_user = (os.getenv("DB_USER") or "").strip()
    db_password = os.getenv("DB_PASSWORD") or ""
    db_name = (os.getenv("DB_NAME") or "").strip()
    db_host = (os.getenv("DB_HOST") or "").strip()
    instance_connection_name = (os.getenv("INSTANCE_CONNECTION_NAME") or "").strip()

    if db_user and db_name:
        password_part = f":{quote_plus(db_password)}" if db_password else ""
        if instance_connection_name:
            return (
                f"postgresql+psycopg://{quote_plus(db_user)}{password_part}@/{db_name}"
                f"?host=/cloudsql/{instance_connection_name}"
            )
        host = db_host or "127.0.0.1"
        return f"postgresql+psycopg://{quote_plus(db_user)}{password_part}@{host}:5432/{db_name}"

    return "sqlite:///./app.db"


DATABASE_URL = _build_database_url()

connect_args = {}
engine_kwargs = {"future": True, "echo": False, "pool_pre_ping": True}

if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, connect_args=connect_args, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def db_session() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
