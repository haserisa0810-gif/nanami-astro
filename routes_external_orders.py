from __future__ import annotations

from datetime import date, datetime, time, timedelta
import base64
import hashlib
import hmac
import json
import os
import secrets

from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from google.cloud import storage
from sqlalchemy.orm import Session

from auth import get_current_staff
from db import get_db
from models import ExternalOrder, Order, OrderResultView
from services.notification_service import send_mail

router = APIRouter()
templates = Jinja2Templates(directory="templates")

EXTERNAL_STATUSES = {
    "draft": "下書き",
    "html_uploaded": "HTML登録済み",
    "url_issued": "URL発行済み",
    "mail_sent": "メール送信済み",
    "delivered": "納品済み",
}
SOURCE_OPTIONS = ["coconala", "manual"]
GENDER_OPTIONS = ["female", "male", "other", "unknown"]


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _bucket_name() -> str:
    return os.getenv("EXTERNAL_REPORTS_BUCKET", "").strip()


def _storage_client() -> storage.Client:
    return storage.Client()


def _object_name(order_code: str) -> str:
    return f"external_reports/{order_code}/report.html"


def _build_public_url(request: Request, token: str) -> str:
    base = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
    if base:
        return f"{base}/report/share/{token}"
    return str(request.url_for("external_report_share", token=token))


def _html_object_name(order: ExternalOrder) -> str | None:
    raw = (order.html_storage_path or "").strip()
    return raw or None


def _html_exists(order: ExternalOrder) -> bool:
    object_name = _html_object_name(order)
    bucket_name = _bucket_name()
    if not object_name or not bucket_name:
        return False
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.exists()


def _download_html(order: ExternalOrder) -> str | None:
    object_name = _html_object_name(order)
    bucket_name = _bucket_name()
    if not object_name or not bucket_name:
        return None
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")


def _generate_token() -> str:
    return secrets.token_urlsafe(32)


def _staff_report_bucket_name() -> str:
    return (os.getenv("STAFF_REPORTS_BUCKET") or os.getenv("EXTERNAL_REPORTS_BUCKET") or "").strip()


def _staff_report_object_name(order_code: str) -> str:
    return f"staff_reports/{order_code}/report.html"


def _download_staff_report_html(order_code: str) -> str | None:
    bucket_name = _staff_report_bucket_name()
    if not bucket_name:
        return None
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(_staff_report_object_name(order_code))
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")


def _staff_report_share_secret() -> str:
    return (
        os.getenv("REPORT_SHARE_SECRET")
        or os.getenv("SECRET_KEY")
        or os.getenv("SESSION_SECRET")
        or "nanami-astro-staff-report-share"
    )


def _decode_staff_share_token(token: str) -> str | None:
    if "." not in token:
        return None
    encoded, signature = token.split(".", 1)
    padding = "=" * (-len(encoded) % 4)
    try:
        payload = base64.urlsafe_b64decode(f"{encoded}{padding}").decode("utf-8")
    except Exception:
        return None
    expected = base64.urlsafe_b64encode(
        hmac.new(_staff_report_share_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        data = json.loads(payload)
    except Exception:
        return None
    if not isinstance(data, dict) or data.get("t") != "staff_report":
        return None
    order_code = str(data.get("o") or "").strip()
    return order_code or None


def _generate_order_code(db: Session) -> str:
    prefix = f"EXT-{datetime.utcnow().strftime('%Y%m%d')}-"
    rows = db.scalars(select(ExternalOrder.order_code).where(ExternalOrder.order_code.like(f"{prefix}%"))).all()
    used = set()
    for code in rows:
        try:
            used.add(int(code.rsplit('-', 1)[-1]))
        except Exception:
            continue
    seq = 1
    while seq in used:
        seq += 1
    return f"{prefix}{seq:04d}"


def _parse_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _parse_datetime_local(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _coerce_price(value: str | None) -> int | None:
    raw = (value or "").strip().replace(',', '')
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _apply_filters(stmt, q: str, status: str, source_type: str, staff_name: str, created_from: str, created_to: str):
    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            ExternalOrder.order_code.ilike(like)
            | ExternalOrder.customer_name.ilike(like)
            | ExternalOrder.customer_email.ilike(like)
            | ExternalOrder.menu_name.ilike(like)
        )
    if status:
        stmt = stmt.where(ExternalOrder.status == status)
    if source_type:
        stmt = stmt.where(ExternalOrder.source_type == source_type)
    if staff_name:
        stmt = stmt.where(ExternalOrder.staff_name == staff_name)
    dt_from = _parse_date(created_from)
    if dt_from:
        stmt = stmt.where(ExternalOrder.created_at >= datetime.combine(dt_from, time.min))
    dt_to = _parse_date(created_to)
    if dt_to:
        stmt = stmt.where(ExternalOrder.created_at < datetime.combine(dt_to + timedelta(days=1), time.min))
    return stmt


def _default_subject() -> str:
    return "【星月七海】鑑定書をお届けします"


def _default_body(order: ExternalOrder) -> str:
    url = (order.public_url or "").strip()
    return f"""{order.customer_name} 様

このたびはお申し込みいただきありがとうございます。
鑑定書のご用意が整いました。

以下のURLよりご確認ください。
{url}

ご不明点がありましたら、このメールへご返信ください。"""


@router.get("/staff/external-orders", response_class=HTMLResponse)
def external_order_list(request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    q = (request.query_params.get("q") or "").strip()
    status = (request.query_params.get("status") or "").strip()
    source_type = (request.query_params.get("source_type") or "").strip()
    staff_name = (request.query_params.get("staff_name") or "").strip()
    created_from = (request.query_params.get("created_from") or "").strip()
    created_to = (request.query_params.get("created_to") or "").strip()

    stmt = select(ExternalOrder).order_by(ExternalOrder.created_at.desc())
    stmt = _apply_filters(stmt, q, status, source_type, staff_name, created_from, created_to)
    orders = db.scalars(stmt).all()
    staff_names = [x for x in db.scalars(select(ExternalOrder.staff_name).distinct().order_by(ExternalOrder.staff_name.asc())).all() if x]
    return templates.TemplateResponse(
        request=request,
        name="external_orders_list.html",
        context={
            "request": request,
            "staff": staff,
            "orders": orders,
            "order_count": len(orders),
            "status_labels": EXTERNAL_STATUSES,
            "filters": {
                "q": q, "status": status, "source_type": source_type,
                "staff_name": staff_name, "created_from": created_from, "created_to": created_to,
            },
            "source_options": SOURCE_OPTIONS,
            "staff_names": staff_names,
            "success": request.query_params.get("success"),
            "error": request.query_params.get("error"),
        },
    )


@router.get("/staff/external-orders/new", response_class=HTMLResponse)
def external_order_new(request: Request, staff: dict = Depends(get_current_staff)):
    return templates.TemplateResponse(
        request=request,
        name="external_order_new.html",
        context={
            "request": request,
            "staff": staff,
            "source_options": SOURCE_OPTIONS,
            "gender_options": GENDER_OPTIONS,
            "default_staff_name": staff.get("display_name") or "",
            "error": request.query_params.get("error"),
        },
    )


@router.post("/staff/external-orders")
async def external_order_create(
    request: Request,
    customer_name: str = Form(...),
    customer_email: str = Form(...),
    birth_date: str = Form(""),
    birth_time: str = Form(""),
    gender: str = Form(""),
    prefecture: str = Form(""),
    birth_place: str = Form(""),
    consultation_text: str = Form(""),
    source_type: str = Form("manual"),
    menu_name: str = Form(""),
    price: str = Form(""),
    staff_name: str = Form(""),
    yaml_log_text: str = Form(""),
    html_file: UploadFile | None = File(None),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    name = customer_name.strip()
    email = customer_email.strip()
    if not name or not email:
        return _redirect("/staff/external-orders/new?error=required")

    order = ExternalOrder(
        order_code=_generate_order_code(db),
        source_type=source_type if source_type in SOURCE_OPTIONS else "manual",
        customer_name=name,
        customer_email=email,
        birth_date=_parse_date(birth_date),
        birth_time=(birth_time or "").strip() or None,
        gender=(gender or "").strip() or None,
        prefecture=(prefecture or "").strip() or None,
        birth_place=(birth_place or "").strip() or None,
        consultation_text=(consultation_text or "").strip() or None,
        menu_name=(menu_name or "").strip() or None,
        price=_coerce_price(price),
        staff_name=(staff_name or staff.get("display_name") or "").strip() or None,
        yaml_log_text=yaml_log_text or None,
        status="draft",
    )
    db.add(order)
    db.flush()

    if html_file and html_file.filename:
        await _save_html_upload(order, html_file)

    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=created")


async def _save_html_upload(order: ExternalOrder, html_file: UploadFile) -> None:
    filename = (html_file.filename or "report.html").strip()
    if not filename.lower().endswith(".html"):
        raise HTTPException(status_code=400, detail="HTMLファイルのみアップロードできます")

    content = await html_file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="HTMLファイルは5MB以下にしてください")

    bucket_name = _bucket_name()
    if not bucket_name:
        raise HTTPException(status_code=500, detail="EXTERNAL_REPORTS_BUCKET が未設定です")

    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(_object_name(order.order_code))
    blob.upload_from_string(content, content_type="text/html; charset=utf-8")

    order.html_storage_path = blob.name
    order.html_original_name = filename
    order.html_uploaded_at = datetime.utcnow()
    if order.status == "draft":
        order.status = "html_uploaded"


@router.get("/staff/external-orders/{order_id}", response_class=HTMLResponse)
def external_order_detail(order_id: int, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not order.mail_subject:
        order.mail_subject = _default_subject()
    if not order.mail_body:
        order.mail_body = _default_body(order)
        db.commit()
    return templates.TemplateResponse(
        request=request,
        name="external_order_detail.html",
        context={
            "request": request,
            "staff": staff,
            "order": order,
            "status_labels": EXTERNAL_STATUSES,
            "source_options": SOURCE_OPTIONS,
            "gender_options": GENDER_OPTIONS,
            "success": request.query_params.get("success"),
            "error": request.query_params.get("error"),
            "has_html": _html_exists(order),
        },
    )


@router.post("/staff/external-orders/{order_id}/update")
def external_order_update(
    order_id: int,
    customer_name: str = Form(...),
    customer_email: str = Form(...),
    birth_date: str = Form(""),
    birth_time: str = Form(""),
    gender: str = Form(""),
    prefecture: str = Form(""),
    birth_place: str = Form(""),
    consultation_text: str = Form(""),
    source_type: str = Form("manual"),
    menu_name: str = Form(""),
    price: str = Form(""),
    staff_name: str = Form(""),
    yaml_log_text: str = Form(""),
    mail_subject: str = Form(""),
    mail_body: str = Form(""),
    expires_at: str = Form(""),
    db: Session = Depends(get_db),
    staff: dict = Depends(get_current_staff),
):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    order.customer_name = customer_name.strip()
    order.customer_email = customer_email.strip()
    order.birth_date = _parse_date(birth_date)
    order.birth_time = (birth_time or "").strip() or None
    order.gender = (gender or "").strip() or None
    order.prefecture = (prefecture or "").strip() or None
    order.birth_place = (birth_place or "").strip() or None
    order.consultation_text = (consultation_text or "").strip() or None
    order.source_type = source_type if source_type in SOURCE_OPTIONS else order.source_type
    order.menu_name = (menu_name or "").strip() or None
    order.price = _coerce_price(price)
    order.staff_name = (staff_name or staff.get("display_name") or "").strip() or None
    order.yaml_log_text = yaml_log_text or None
    order.mail_subject = (mail_subject or _default_subject()).strip()
    order.mail_body = (mail_body or "").strip() or _default_body(order)
    order.expires_at = _parse_datetime_local(expires_at)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=saved")


@router.post("/staff/external-orders/{order_id}/upload-html")
async def external_order_upload_html(order_id: int, html_file: UploadFile = File(...), db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    await _save_html_upload(order, html_file)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=html_uploaded")


@router.post("/staff/external-orders/{order_id}/issue-url")
def external_order_issue_url(order_id: int, request: Request, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not _html_exists(order):
        return _redirect(f"/staff/external-orders/{order.id}?error=no_html")
    if not order.public_token:
        order.public_token = _generate_token()
    order.public_url = _build_public_url(request, order.public_token)
    order.url_issued_at = datetime.utcnow()
    if order.status in {"draft", "html_uploaded"}:
        order.status = "url_issued"
    if not order.mail_body:
        order.mail_body = _default_body(order)
    else:
        order.mail_body = order.mail_body.replace("{public_url}", order.public_url)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=url_issued")


@router.post("/staff/external-orders/{order_id}/reissue-url")
def external_order_reissue_url(order_id: int, request: Request, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not _html_exists(order):
        return _redirect(f"/staff/external-orders/{order.id}?error=no_html")
    order.public_token = _generate_token()
    order.public_url = _build_public_url(request, order.public_token)
    order.url_issued_at = datetime.utcnow()
    if order.status in {"draft", "html_uploaded"}:
        order.status = "url_issued"
    if order.mail_body:
        order.mail_body = order.mail_body.replace("{public_url}", order.public_url)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=url_reissued")


@router.post("/staff/external-orders/{order_id}/send-mail")
def external_order_send_mail(
    order_id: int,
    mail_subject: str = Form(""),
    mail_body: str = Form(""),
    db: Session = Depends(get_db),
    staff: dict = Depends(get_current_staff),
):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not order.public_url:
        return _redirect(f"/staff/external-orders/{order.id}?error=no_public_url")

    order.mail_subject = (mail_subject or order.mail_subject or _default_subject()).strip()
    order.mail_body = (mail_body or order.mail_body or _default_body(order)).strip()

    ok = send_mail(order.mail_subject, order.mail_body, [order.customer_email])

    if ok:
        order.mail_sent_at = datetime.utcnow()
        if order.status in {"draft", "html_uploaded", "url_issued"}:
            order.status = "mail_sent"
        order.last_error = None
        db.commit()
        return _redirect(f"/staff/external-orders/{order.id}?success=mail_sent")

    order.last_error = "メール送信に失敗しました"
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?error=mail_failed")


@router.post("/staff/external-orders/{order_id}/mark-delivered")
def external_order_mark_delivered(order_id: int, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    order.status = "delivered"
    order.delivered_at = datetime.utcnow()
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=delivered")


@router.get("/staff/external-orders/{order_id}/preview", response_class=HTMLResponse)
def external_order_preview(order_id: int, request: Request, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    body = _download_html(order)
    if not body:
        raise HTTPException(status_code=404)
    return HTMLResponse(body)


@router.get("/report/share/{token}", response_class=HTMLResponse, name="external_report_share")
def external_report_share(token: str, db: Session = Depends(get_db)):
    order = db.scalar(select(ExternalOrder).where(ExternalOrder.public_token == token))
    if order:
        if order.expires_at and datetime.utcnow() > order.expires_at:
            raise HTTPException(status_code=403, detail="このURLは期限切れです")
        body = _download_html(order)
        if not body:
            raise HTTPException(status_code=404)
        headers = {
            "X-Robots-Tag": "noindex, nofollow, noarchive",
            "Cache-Control": "private, no-store",
        }
        return HTMLResponse(content=body, headers=headers)

    staff_order_code = _decode_staff_share_token(token)
    if staff_order_code:
        staff_order = db.scalar(select(Order).where(Order.order_code == staff_order_code))
        if not staff_order:
            raise HTTPException(status_code=404)
        body = _download_staff_report_html(staff_order.order_code)
        if not body:
            latest_view = db.scalar(
                select(OrderResultView)
                .where(OrderResultView.order_id == staff_order.id)
                .order_by(OrderResultView.updated_at.desc(), OrderResultView.created_at.desc())
            )
            if latest_view and latest_view.report_html:
                body = latest_view.report_html
        if not body:
            raise HTTPException(status_code=404)
        headers = {
            "X-Robots-Tag": "noindex, nofollow, noarchive",
            "Cache-Control": "private, no-store",
        }
        return HTMLResponse(content=body, headers=headers)

    raise HTTPException(status_code=404)
