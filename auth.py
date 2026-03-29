from __future__ import annotations

import os
from typing import Any

from fastapi import Depends, HTTPException, Request, Response, status
from itsdangerous import BadSignature, URLSafeSerializer
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from db import get_db
from models import AdminUser, Astrologer

pwd_context = CryptContext(schemes=["bcrypt_sha256"], deprecated="auto")
admin_serializer = URLSafeSerializer(os.getenv("ADMIN_SESSION_SECRET", "dev-admin-secret"), salt="admin-session")
reader_serializer = URLSafeSerializer(os.getenv("READER_SESSION_SECRET", "dev-reader-secret"), salt="reader-session")

ADMIN_COOKIE = "admin_session"
READER_COOKIE = "reader_session"
SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", "2592000"))  # 30 days


def _normalize_password(password: str) -> str:
    if password is None:
        return ""
    return str(password)


def hash_password(password: str) -> str:
    return pwd_context.hash(_normalize_password(password))


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(_normalize_password(password), password_hash)


def _read_cookie(request: Request, cookie_name: str, serializer: URLSafeSerializer) -> dict[str, Any] | None:
    raw = request.cookies.get(cookie_name)
    if not raw:
        return None
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else None
    except BadSignature:
        return None


def _cookie_secure() -> bool:
    value = (os.getenv("COOKIE_SECURE") or "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    base_url = (os.getenv("BASE_URL") or "").strip().lower()
    if base_url.startswith("https://"):
        return True
    return bool(os.getenv("K_SERVICE"))


def _set_login_cookie(response: Response, cookie_name: str, token: str) -> None:
    response.set_cookie(
        key=cookie_name,
        value=token,
        httponly=True,
        samesite="lax",
        secure=_cookie_secure(),
        max_age=SESSION_MAX_AGE,
        path="/",
    )


def set_admin_login(response: Response, admin: AdminUser) -> None:
    token = admin_serializer.dumps({"id": admin.id, "email": admin.login_email})
    _set_login_cookie(response, ADMIN_COOKIE, token)


def clear_admin_login(response: Response) -> None:
    response.delete_cookie(ADMIN_COOKIE, path="/")


def set_reader_login(response: Response, reader: Astrologer) -> None:
    token = reader_serializer.dumps({"id": reader.id, "email": reader.login_email})
    _set_login_cookie(response, READER_COOKIE, token)


def clear_reader_login(response: Response) -> None:
    response.delete_cookie(READER_COOKIE, path="/")


def _allow_temp_password_path(path: str, allowed_prefixes: set[str]) -> bool:
    normalized = (path or '').strip() or '/'
    return any(normalized.startswith(prefix) for prefix in allowed_prefixes)


def get_current_admin(request: Request, db: Session = Depends(get_db)) -> AdminUser:
    data = _read_cookie(request, ADMIN_COOKIE, admin_serializer)
    if not data:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="admin login required", headers={"Location": "/admin/login"})
    admin = db.get(AdminUser, int(data["id"])) if data.get("id") else None
    if not admin or not admin.is_active:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="admin login required", headers={"Location": "/admin/login"})
    if getattr(admin, 'is_temp_password', False) and not _allow_temp_password_path(request.url.path, {'/admin/account', '/admin/logout'}):
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="password change required", headers={"Location": "/admin/account?force_password=1"})
    return admin


def get_current_reader(request: Request, db: Session = Depends(get_db)) -> Astrologer:
    data = _read_cookie(request, READER_COOKIE, reader_serializer)
    if not data:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="reader login required", headers={"Location": "/reader/login"})
    reader = db.get(Astrologer, int(data["id"])) if data.get("id") else None
    if not reader or reader.status != "active":
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="reader login required", headers={"Location": "/reader/login"})
    if getattr(reader, 'is_temp_password', False) and not _allow_temp_password_path(request.url.path, {'/reader/profile', '/reader/logout'}):
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="password change required", headers={"Location": "/reader/profile?force_password=1"})
    return reader



def clear_all_logins(response: Response) -> None:
    clear_admin_login(response)
    clear_reader_login(response)


def get_current_staff(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    admin_data = _read_cookie(request, ADMIN_COOKIE, admin_serializer)
    if admin_data and admin_data.get("id"):
        admin = db.get(AdminUser, int(admin_data["id"]))
        if admin and admin.is_active:
            return {
                "role": "admin",
                "user": admin,
                "display_name": admin.display_name,
                "login_email": admin.login_email,
                "id": admin.id,
            }

    reader_data = _read_cookie(request, READER_COOKIE, reader_serializer)
    if reader_data and reader_data.get("id"):
        reader = db.get(Astrologer, int(reader_data["id"]))
        if reader and reader.status == "active":
            return {
                "role": "reader",
                "user": reader,
                "display_name": reader.display_name,
                "login_email": reader.login_email,
                "id": reader.id,
            }

    raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, detail="staff login required", headers={"Location": "/login"})
