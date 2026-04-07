from __future__ import annotations

from datetime import date, datetime
import email
from email.header import decode_header
from email.message import Message
import html
import imaplib
import json
import os
import re
import smtplib
from email.mime.text import MIMEText
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Depends, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session, selectinload

from db import get_db
from models import Menu, Order, OrderInputSnapshot
from services.draft_service import create_or_update_draft_from_order_inputs, promote_draft_to_order
from services.free_reading_service import FREE_RESULT_FOOTER, ensure_unique_free_reading_code, process_free_reading
from services.order_service import create_order, get_or_create_customer
from services.location import PREFECTURE_OPTIONS, resolve_birth_location

router = APIRouter()
templates = Jinja2Templates(directory="templates")


STORES_PAYMENT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stores_payments (
    id SERIAL PRIMARY KEY,
    stores_order_no TEXT NOT NULL UNIQUE,
    buyer_name TEXT,
    buyer_email TEXT,
    item_name TEXT,
    amount INTEGER,
    currency TEXT DEFAULT 'jpy',
    payment_method TEXT,
    payment_status TEXT NOT NULL DEFAULT 'unknown',
    ordered_at TIMESTAMP,
    paid_at TIMESTAMP,
    mail_kind TEXT,
    raw_message_id TEXT,
    mail_subject TEXT,
    mail_received_at TIMESTAMP,
    raw_body TEXT,
    source TEXT DEFAULT 'stores_email',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""
STORES_PAYMENT_INDEX_SQL = "CREATE INDEX IF NOT EXISTS ix_stores_payments_buyer_email ON stores_payments (buyer_email)"
STORES_FROM_DEFAULT = "hello@stores.jp"
STORES_OWNER_NOTICE_PATTERN = re.compile(r"アイテムが購入されました|初売上おめでとうございます", re.I)
STORES_ORDER_NO_PATTERN = re.compile(r"^\d{10}$")

COURSE_SLUG_PRICE_MAP = {
    "light": 3000,
    "standard": 5000,
    "premium": 10000,
}


def _ensure_stores_payment_table(db: Session) -> None:
    try:
        db.execute(text(STORES_PAYMENT_TABLE_SQL))
        db.execute(text(STORES_PAYMENT_INDEX_SQL))
        db.commit()
    except Exception:
        db.rollback()
        raise


def _normalize_free_link_code(text_value: str | None) -> str | None:
    value = (text_value or "").strip().upper()
    if not value:
        return None
    m = re.search(r"(F-[A-Z0-9-]+|A[A-Z0-9]{6,})", value)
    return m.group(1) if m else None


def _find_source_free_order(db: Session, code: str | None):
    normalized = _normalize_free_link_code(code)
    if not normalized:
        return None
    return db.scalar(
        select(Order).where(
            Order.order_kind == "free",
            or_(
                Order.free_reading_code == normalized,
                Order.order_code == normalized,
            ),
        )
    )


def _resolve_initial_menu_id(db: Session, menu_id: int | None = None, course: str | None = None) -> int | None:
    if menu_id:
        menu = db.get(Menu, menu_id)
        if menu and menu.is_active and menu.price > 0:
            return menu.id
    normalized_course = (course or "").strip().lower()
    target_price = COURSE_SLUG_PRICE_MAP.get(normalized_course)
    if not target_price:
        return None
    menu = db.scalar(
        select(Menu)
        .where(Menu.is_active == True, Menu.price == target_price)
        .order_by(Menu.id.asc())
    )
    return menu.id if menu else None


def _decode_header_value(value: str | None) -> str:
    if not value:
        return ""
    parts: list[str] = []
    for chunk, charset in decode_header(value):
        if isinstance(chunk, bytes):
            try:
                parts.append(chunk.decode(charset or "utf-8", errors="replace"))
            except Exception:
                parts.append(chunk.decode("utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts)


def _extract_message_text(msg: Message) -> str:
    texts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = (part.get_content_type() or "").lower()
            disposition = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            charset = part.get_content_charset() or "utf-8"
            try:
                content = payload.decode(charset, errors="replace")
            except Exception:
                content = payload.decode("utf-8", errors="replace")
            if content_type == "text/plain":
                texts.append(content)
            elif content_type == "text/html" and not texts:
                stripped = re.sub(r"<br\s*/?>", "\n", content, flags=re.I)
                stripped = re.sub(r"</p\s*>", "\n", stripped, flags=re.I)
                stripped = re.sub(r"<[^>]+>", " ", stripped)
                texts.append(html.unescape(stripped))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                texts.append(payload.decode(charset, errors="replace"))
            except Exception:
                texts.append(payload.decode("utf-8", errors="replace"))
    body = "\n".join(x for x in texts if x).strip()
    body = body.replace("\r\n", "\n").replace("\r", "\n")
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body


def _normalize_email(value: str | None) -> str | None:
    if not value:
        return None
    m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", value, flags=re.I)
    return m.group(1).strip().lower() if m else None


def _normalize_order_no(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    value = re.sub(r"(注文番号|オーダー番号|注文ID)\s*[：:]?\s*", "", value, flags=re.I)
    value = re.sub(r"[^\dA-Z\-]", "", value.upper())
    return value or None


def _extract_first(patterns: list[str], text_value: str) -> str | None:
    for pattern in patterns:
        m = re.search(pattern, text_value, flags=re.I | re.M)
        if m:
            return (m.group(1) or "").strip()
    return None


def _extract_amount(text_value: str) -> int | None:
    candidates = [
        r"(?:お支払い金額|合計金額|ご請求金額|金額|合計（税込）)\s*[：:]?\s*[¥￥]?\s*([0-9][0-9,]*)",
        r"[¥￥]\s*([0-9][0-9,]*)",
    ]
    for pattern in candidates:
        m = re.search(pattern, text_value, flags=re.I)
        if m:
            raw = m.group(1).replace(",", "")
            try:
                return int(raw)
            except Exception:
                return None
    return None


def _parse_mail_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo is not None:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _parse_stores_mail(subject: str, body: str, message_id: str | None, received_at: datetime | None) -> dict[str, Any] | None:
    merged = f"{subject}\n{body}"
    lowered = merged.lower()
    if "stores" not in lowered and "ストアーズ" not in merged and "オーダー番号" not in merged and "注文番号" not in merged:
        return None

    order_no = _normalize_order_no(_extract_first([
        r"オーダー番号[：:]\s*([0-9A-Z\-]+)",
        r"注文番号[：:]\s*([0-9A-Z\-]+)",
        r"注文ID[：:]\s*([0-9A-Z\-]+)",
        r"オーダー番号[：:\s]*([0-9A-Z\-]{6,})",
    ], merged))
    if not order_no:
        return None

    buyer_email = _normalize_email(_extract_first([
        r"メールアドレス\s*[：:]\s*([^\n]+)",
        r"購入者メールアドレス\s*[：:]\s*([^\n]+)",
        r"連絡先\s*[：:]\s*([^\n]+)",
    ], merged))

    buyer_name = _extract_first([
        r"(?:購入者名|お名前|氏名)\s*[：:]\s*([^\n]+)",
    ], merged)
    if buyer_name:
        buyer_name = re.sub(r"\s+", " ", buyer_name).strip()

    item_name = _extract_first([
        r"(?:商品名|購入商品|商品)\s*[：:]\s*([^\n]+)",
        r"^([^\n\t]+)\t",
    ], merged)
    payment_method = _extract_first([
        r"(?:支払い方法|決済方法)\s*[：:]\s*([^\n]+)",
    ], merged)
    amount = _extract_amount(merged)

    is_owner_notice = bool(STORES_OWNER_NOTICE_PATTERN.search(merged))
    mail_kind = "purchase_completed"
    payment_status = "ordered"

    if "入金完了" in merged or "お支払いが完了" in merged or "支払い完了" in merged:
        mail_kind = "payment_completed"
        payment_status = "paid"
    elif is_owner_notice or "購入完了" in merged or "注文を受け付けました" in merged or "注文がありました" in merged:
        mail_kind = "owner_notice" if is_owner_notice else "purchase_completed"
        payment_status = "paid" if is_owner_notice else "ordered"
    elif "キャンセル" in merged:
        mail_kind = "cancelled"
        payment_status = "cancelled"

    ordered_at = _parse_mail_datetime(_extract_first([
        r"(?:注文日時|ご注文日時|購入日時)\s*[：:]\s*([^\n]+)",
    ], merged))
    paid_at = _parse_mail_datetime(_extract_first([
        r"(?:入金日時|支払日時|お支払い日時)\s*[：:]\s*([^\n]+)",
    ], merged))

    if payment_status == "paid" and paid_at is None:
        paid_at = received_at
    if ordered_at is None:
        ordered_at = received_at

    return {
        "stores_order_no": order_no,
        "buyer_name": buyer_name,
        "buyer_email": buyer_email,
        "item_name": item_name,
        "amount": amount,
        "currency": "jpy",
        "payment_method": payment_method,
        "payment_status": payment_status,
        "ordered_at": ordered_at,
        "paid_at": paid_at,
        "mail_kind": mail_kind,
        "raw_message_id": message_id,
        "mail_subject": subject[:500],
        "mail_received_at": received_at,
        "raw_body": body,
        "source": "stores_email",
    }


def _upsert_stores_payment(db: Session, parsed: dict[str, Any]) -> None:
    existing = db.execute(
        text("""
            SELECT id, payment_status, paid_at, ordered_at, buyer_email, buyer_name, item_name, amount
            FROM stores_payments
            WHERE stores_order_no = :order_no
        """),
        {"order_no": parsed["stores_order_no"]},
    ).mappings().first()

    payload = {
        "order_no": parsed["stores_order_no"],
        "buyer_name": parsed.get("buyer_name"),
        "buyer_email": parsed.get("buyer_email"),
        "item_name": parsed.get("item_name"),
        "amount": parsed.get("amount"),
        "currency": parsed.get("currency") or "jpy",
        "payment_method": parsed.get("payment_method"),
        "payment_status": parsed.get("payment_status") or "unknown",
        "ordered_at": parsed.get("ordered_at"),
        "paid_at": parsed.get("paid_at"),
        "mail_kind": parsed.get("mail_kind"),
        "raw_message_id": parsed.get("raw_message_id"),
        "mail_subject": parsed.get("mail_subject"),
        "mail_received_at": parsed.get("mail_received_at"),
        "raw_body": parsed.get("raw_body"),
        "source": parsed.get("source") or "stores_email",
    }

    if not existing:
        db.execute(text("""
            INSERT INTO stores_payments (
                stores_order_no, buyer_name, buyer_email, item_name, amount, currency,
                payment_method, payment_status, ordered_at, paid_at, mail_kind,
                raw_message_id, mail_subject, mail_received_at, raw_body, source,
                created_at, updated_at
            ) VALUES (
                :order_no, :buyer_name, :buyer_email, :item_name, :amount, :currency,
                :payment_method, :payment_status, :ordered_at, :paid_at, :mail_kind,
                :raw_message_id, :mail_subject, :mail_received_at, :raw_body, :source,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
        """), payload)
        return

    def merged_value(new_value: Any, old_value: Any) -> Any:
        return new_value if new_value not in (None, "", []) else old_value

    next_status = existing["payment_status"] or "unknown"
    if parsed.get("payment_status") == "paid":
        next_status = "paid"
    elif parsed.get("payment_status") == "cancelled":
        next_status = "cancelled"
    elif next_status == "unknown":
        next_status = parsed.get("payment_status") or "unknown"

    db.execute(text("""
        UPDATE stores_payments SET
            buyer_name = :buyer_name,
            buyer_email = :buyer_email,
            item_name = :item_name,
            amount = :amount,
            currency = :currency,
            payment_method = :payment_method,
            payment_status = :payment_status,
            ordered_at = :ordered_at,
            paid_at = :paid_at,
            mail_kind = :mail_kind,
            raw_message_id = :raw_message_id,
            mail_subject = :mail_subject,
            mail_received_at = :mail_received_at,
            raw_body = :raw_body,
            source = :source,
            updated_at = CURRENT_TIMESTAMP
        WHERE stores_order_no = :order_no
    """), {
        "order_no": parsed["stores_order_no"],
        "buyer_name": merged_value(parsed.get("buyer_name"), existing["buyer_name"]),
        "buyer_email": merged_value(parsed.get("buyer_email"), existing["buyer_email"]),
        "item_name": merged_value(parsed.get("item_name"), existing["item_name"]),
        "amount": merged_value(parsed.get("amount"), existing["amount"]),
        "currency": parsed.get("currency") or "jpy",
        "payment_method": merged_value(parsed.get("payment_method"), None),
        "payment_status": next_status,
        "ordered_at": merged_value(parsed.get("ordered_at"), existing["ordered_at"]),
        "paid_at": merged_value(parsed.get("paid_at"), existing["paid_at"]),
        "mail_kind": merged_value(parsed.get("mail_kind"), None),
        "raw_message_id": merged_value(parsed.get("raw_message_id"), None),
        "mail_subject": merged_value(parsed.get("mail_subject"), None),
        "mail_received_at": merged_value(parsed.get("mail_received_at"), None),
        "raw_body": merged_value(parsed.get("raw_body"), None),
        "source": parsed.get("source") or "stores_email",
    })


def sync_stores_order_emails(db: Session, *, limit: int = 30) -> dict[str, int]:
    _ensure_stores_payment_table(db)

    host = os.getenv("STORES_MAIL_IMAP_HOST", "imap.gmail.com")
    port = int(os.getenv("STORES_MAIL_IMAP_PORT", "993"))
    username = os.getenv("STORES_MAIL_USERNAME")
    password = os.getenv("STORES_MAIL_PASSWORD")
    from_filter = os.getenv("STORES_MAIL_FROM_FILTER", STORES_FROM_DEFAULT)

    if not username or not password:
        return {"fetched": 0, "parsed": 0, "upserted": 0, "skipped": 0, "errors": 0}

    fetched = parsed_count = upserted = skipped = errors = 0
    conn = None
    try:
        conn = imaplib.IMAP4_SSL(host, port)
        conn.login(username, password)
        conn.select("INBOX")
        status, data = conn.search(None, "ALL")
        if status != "OK":
            return {"fetched": 0, "parsed": 0, "upserted": 0, "skipped": 0, "errors": 1}

        ids = data[0].split()
        recent_ids = list(reversed(ids[-limit:]))
        for msg_id in recent_ids:
            status, payload = conn.fetch(msg_id, "(RFC822)")
            if status != "OK" or not payload or not payload[0]:
                errors += 1
                continue
            fetched += 1
            raw_email = payload[0][1]
            msg = email.message_from_bytes(raw_email)
            from_value = _decode_header_value(msg.get("From"))
            subject = _decode_header_value(msg.get("Subject"))
            if from_filter and from_filter.lower() not in from_value.lower() and "stores" not in from_value.lower() and "stores" not in subject.lower():
                skipped += 1
                continue
            body = _extract_message_text(msg)
            received_at = _parse_mail_datetime(msg.get("Date"))
            message_id_value = _decode_header_value(msg.get("Message-Id") or msg.get("Message-ID")) or None
            parsed = _parse_stores_mail(subject, body, message_id_value, received_at)
            if not parsed:
                skipped += 1
                continue
            parsed_count += 1
            _upsert_stores_payment(db, parsed)
            upserted += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        if conn is not None:
            try:
                conn.logout()
            except Exception:
                pass

    return {"fetched": fetched, "parsed": parsed_count, "upserted": upserted, "skipped": skipped, "errors": errors}


def _get_stores_payment_row(db: Session, order_no: str):
    _ensure_stores_payment_table(db)
    return db.execute(
        text("SELECT * FROM stores_payments WHERE stores_order_no = :order_no"),
        {"order_no": order_no},
    ).mappings().first()


def _verify_stores_payment(db: Session, *, stores_order_no: str, payment_email: str | None) -> tuple[str, dict[str, Any] | None]:
    row = _get_stores_payment_row(db, stores_order_no)
    if not row:
        return "not_found", None

    buyer_email = _normalize_email(row.get("buyer_email"))
    normalized_payment_email = _normalize_email(payment_email)

    if buyer_email and normalized_payment_email and buyer_email != normalized_payment_email:
        return "needs_review", dict(row)

    payment_status = (row.get("payment_status") or "unknown").lower()
    if payment_status == "paid":
        if buyer_email or normalized_payment_email:
            return "verified", dict(row)
        return "verified_without_email", dict(row)
    if payment_status in {"ordered", "pending", "unknown"}:
        return "unpaid", dict(row)
    if payment_status == "cancelled":
        return "cancelled", dict(row)
    return "needs_review", dict(row)


def _smtp_send(*, to_email: str, subject: str, body: str) -> bool:
    host = (os.getenv("SMTP_HOST") or "smtp.gmail.com").strip()
    port = int((os.getenv("SMTP_PORT") or "587").strip())
    username = (os.getenv("SMTP_USERNAME") or os.getenv("MAIL_USERNAME") or "").strip()
    password = (os.getenv("SMTP_PASSWORD") or os.getenv("MAIL_PASSWORD") or "").strip()
    from_email = (os.getenv("SMTP_FROM_EMAIL") or username or "").strip()
    from_name = (os.getenv("SMTP_FROM_NAME") or "星月七海の星読み").strip()

    if not host or not from_email or not to_email:
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = to_email

    try:
        with smtplib.SMTP(host, port, timeout=20) as server:
            server.ehlo()
            if port in (587, 25):
                server.starttls()
                server.ehlo()
            if username and password:
                server.login(username, password)
            server.sendmail(from_email, [to_email], msg.as_string())
        return True
    except Exception:
        return False


def _build_absolute_base_url(request: Request) -> str:
    explicit = (os.getenv("PUBLIC_ORDER_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or os.getenv("BASE_URL") or "").strip().rstrip("/")
    if explicit:
        return explicit
    return str(request.base_url).rstrip("/")


def _build_intake_url(request: Request, *, course: str | None = None, line_user_id: str | None = None, line_name: str | None = None) -> str:
    base = _build_absolute_base_url(request)
    path = "/menu"
    if course and course.strip().lower() in COURSE_SLUG_PRICE_MAP:
        path = f"/menu/{course.strip().lower()}"
    params: dict[str, str] = {}
    if line_user_id:
        params["line_user_id"] = line_user_id.strip()
    if line_name:
        params["line_name"] = line_name.strip()
    query = f"?{urlencode(params)}" if params else ""
    return f"{base}{path}{query}"


def _notify_admin_new_paid_order(
    request: Request,
    *,
    order: Order,
    payment_row: dict[str, Any] | None,
    event_label: str = "新規受付",
) -> None:
    admin_to = (os.getenv("ADMIN_NOTIFY_EMAIL") or os.getenv("ALERT_EMAIL") or "").strip()
    if not admin_to:
        return
    staff_url = f"{_build_absolute_base_url(request)}/staff/orders/{order.order_code}"
    subject = f"【nanami-astro】{event_label} {order.order_code}"
    body = "\n".join([
        f"{event_label}がありました。",
        "",
        f"受付番号: {order.order_code}",
        f"お名前: {order.user_name}",
        f"メニュー: {order.menu.name if order.menu else '-'}",
        f"連絡先: {order.user_contact or '-'}",
        f"申込み経路: {order.source or '-'}",
        f"外部決済: {order.external_platform or '-'}",
        f"STORES注文番号: {order.external_order_ref or '-'}",
        f"LINE連携: {'あり' if getattr(order.customer, 'line_user_id', None) else 'なし'}",
        f"決済照合: {'owner通知ベース' if payment_row and not payment_row.get('buyer_email') else '通常'}",
        "",
        f"管理画面: {staff_url}",
    ])
    _smtp_send(to_email=admin_to, subject=subject, body=body)


def _send_user_form_link_email(request: Request, *, to_email: str, course: str | None, line_user_id: str | None, line_name: str | None, payment_order_ref: str | None = None) -> bool:
    intake_url = _build_intake_url(request, course=course, line_user_id=line_user_id, line_name=line_name)
    if payment_order_ref:
        sep = '&' if '?' in intake_url else '?'
        intake_url = f"{intake_url}{sep}payment_order_ref={payment_order_ref.strip()}"
    subject = "【星月七海の星読み】ご入力フォームのご案内"
    body = "\n".join([
        "ご購入ありがとうございます。",
        "",
        "以下のフォームから必要事項をご入力ください。",
        intake_url,
        "",
        "STORESの購入完了メールに記載の注文番号をご用意のうえ、ご入力をお願いいたします。",
        "",
        "※ LINEからご案内が届いている場合は、LINE内のフォームをご利用ください。",
    ])
    return _smtp_send(to_email=to_email, subject=subject, body=body)


@router.post("/internal/stores/mail-sync")
def stores_mail_sync(
    request: Request,
    db: Session = Depends(get_db),
    x_sync_token: str | None = Header(None),
):
    expected = os.getenv("STORES_MAIL_SYNC_TOKEN", "").strip()
    supplied = (x_sync_token or request.query_params.get("token") or "").strip()
    if not expected or supplied != expected:
        raise HTTPException(status_code=403, detail="forbidden")
    result = sync_stores_order_emails(db)
    return JSONResponse({"ok": True, "result": result})


@router.get("/menu", response_class=HTMLResponse)
def menu_page(
    request: Request,
    db: Session = Depends(get_db),
    free_reading_code: str | None = None,
    menu_id: int | None = None,
    course: str | None = None,
    payment_order_ref: str | None = None,
    payment_email: str | None = None,
    line_user_id: str | None = None,
    line_name: str | None = None,
):
    menus = db.scalars(select(Menu).where(Menu.is_active == True).order_by(Menu.price.asc())).all()
    initial_free_order = None
    if free_reading_code:
        initial_free_order = _find_source_free_order(db, free_reading_code)
    resolved_menu_id = _resolve_initial_menu_id(db, menu_id=menu_id, course=course)
    return templates.TemplateResponse(
        request=request,
        name="order_start.html",
        context={
            "request": request,
            "menus": menus,
            "error": None,
            "prefecture_options": PREFECTURE_OPTIONS,
            "initial_free_order": initial_free_order,
            "initial_free_reading_code": _normalize_free_link_code(free_reading_code) or free_reading_code,
            "initial_menu_id": resolved_menu_id,
            "initial_course": (course or "").strip().lower(),
            "initial_payment_order_ref": (payment_order_ref or "").strip(),
            "initial_payment_email": (payment_email or "").strip().lower(),
            "initial_line_user_id": (line_user_id or "").strip(),
            "initial_line_name": (line_name or "").strip(),
            "form_values": {},
        },
    )


@router.get("/order/start", response_class=HTMLResponse)
def order_start(
    request: Request,
    db: Session = Depends(get_db),
    free_reading_code: str | None = None,
    menu_id: int | None = None,
    course: str | None = None,
    payment_order_ref: str | None = None,
    payment_email: str | None = None,
    line_user_id: str | None = None,
    line_name: str | None = None,
):
    return menu_page(
        request,
        db,
        free_reading_code=free_reading_code,
        menu_id=menu_id,
        course=course,
        payment_order_ref=payment_order_ref,
        payment_email=payment_email,
        line_user_id=line_user_id,
        line_name=line_name,
    )


@router.get("/menu/{course}", response_class=HTMLResponse)
def menu_page_by_course(
    course: str,
    request: Request,
    db: Session = Depends(get_db),
    free_reading_code: str | None = None,
    payment_order_ref: str | None = None,
    payment_email: str | None = None,
    line_user_id: str | None = None,
    line_name: str | None = None,
):
    normalized_course = (course or "").strip().lower()
    if normalized_course not in COURSE_SLUG_PRICE_MAP:
        raise HTTPException(status_code=404, detail="course not found")
    return menu_page(
        request,
        db,
        free_reading_code=free_reading_code,
        course=normalized_course,
        payment_order_ref=payment_order_ref,
        payment_email=payment_email,
        line_user_id=line_user_id,
        line_name=line_name,
    )


@router.post("/order/start", response_class=HTMLResponse)
def create_order_page(
    request: Request,
    menu_id: int = Form(...),
    user_name: str = Form(...),
    user_contact: str | None = Form(None),
    birth_date: str = Form(...),
    birth_time: str | None = Form(None),
    birth_prefecture: str | None = Form(None),
    birth_place: str | None = Form(None),
    gender: str | None = Form(None),
    consultation_text: str | None = Form(None),
    free_reading_code: str | None = Form(None),
    payment_order_ref: str | None = Form(None),
    payment_email: str | None = Form(None),
    base_order_ref: str | None = Form(None),
    line_user_id: str | None = Form(None),
    line_name: str | None = Form(None),
    course: str | None = Form(None),
    db: Session = Depends(get_db),
):
    menu = db.get(Menu, menu_id)
    if not menu or not menu.is_active:
        raise HTTPException(status_code=404, detail="menu not found")

    normalized_contact = (user_contact or "").strip().lower()
    normalized_payment_email = (payment_email or "").strip().lower()
    normalized_payment_order_ref = _normalize_order_no(payment_order_ref or base_order_ref or "")
    normalized_line_user_id = (line_user_id or "").strip() or None
    normalized_line_name = (line_name or "").strip() or None

    def render_form_error(message: str, status_code: int = 400):
        menus = db.scalars(select(Menu).where(Menu.is_active == True).order_by(Menu.price.asc())).all()
        submitted_course = (course or "").strip().lower()
        resolved_menu_id = _resolve_initial_menu_id(db, menu_id=menu_id, course=submitted_course) or menu_id
        return templates.TemplateResponse(
            request=request,
            name="order_start.html",
            context={
                "request": request,
                "menus": menus,
                "error": message,
                "prefecture_options": PREFECTURE_OPTIONS,
                "initial_free_reading_code": _normalize_free_link_code(free_reading_code) or free_reading_code,
                "initial_free_order": None,
                "initial_menu_id": resolved_menu_id,
                "initial_course": submitted_course,
                "initial_payment_order_ref": normalized_payment_order_ref or "",
                "initial_payment_email": normalized_payment_email,
                "initial_line_user_id": normalized_line_user_id or "",
                "initial_line_name": normalized_line_name or "",
                "form_values": {
                    "menu_id": menu_id,
                    "user_name": user_name,
                    "user_contact": normalized_contact,
                    "birth_date": birth_date,
                    "birth_time": birth_time,
                    "birth_prefecture": birth_prefecture,
                    "birth_place": birth_place,
                    "gender": gender,
                    "consultation_text": consultation_text,
                    "payment_order_ref": normalized_payment_order_ref or "",
                    "payment_email": normalized_payment_email,
                    "free_reading_code": free_reading_code,
                    "line_user_id": normalized_line_user_id or "",
                    "line_name": normalized_line_name or "",
                    "course": submitted_course,
                },
            },
            status_code=status_code,
        )

    if not normalized_payment_order_ref:
        return render_form_error("STORES注文番号が未入力です。購入完了メールに記載の注文番号を入力してください。")
    if not STORES_ORDER_NO_PATTERN.fullmatch(normalized_payment_order_ref):
        return render_form_error("STORES注文番号は10桁の数字で入力してください。")
    if not normalized_contact:
        return render_form_error("ホームページからのお申込みは連絡先メールアドレスが必須です。")
    if "@" not in normalized_contact:
        return render_form_error("正しいご連絡先メールアドレスを入力してください。")
    if not normalized_payment_email:
        return render_form_error("STORES購入時メールアドレスが未入力です。購入完了メールに記載のメールアドレスを入力してください。")
    if "@" not in normalized_payment_email:
        return render_form_error("STORES購入時メールアドレスの形式が正しくありません。")

    existing_order = db.scalar(
        select(Order).where(
            Order.external_platform == "stores",
            Order.external_order_ref == normalized_payment_order_ref,
            Order.user_contact == normalized_contact,
        )
    )

    try:
        birth_date_obj = date.fromisoformat(birth_date)
    except ValueError:
        return render_form_error("生年月日の形式が正しくありません。")

    if os.getenv("STORES_MAIL_SYNC_ON_SUBMIT", "1") == "1":
        try:
            sync_stores_order_emails(db, limit=int(os.getenv("STORES_MAIL_SYNC_SUBMIT_LIMIT", "20")))
        except Exception:
            db.rollback()

    verification_status, payment_row = _verify_stores_payment(
        db,
        stores_order_no=normalized_payment_order_ref,
        payment_email=normalized_payment_email or None,
    )
    if verification_status == "not_found":
        return render_form_error("この注文番号はまだ確認できません。決済後のメール取り込みが終わってから再度お試しください。")
    if verification_status == "unpaid":
        return render_form_error("ご注文は確認できましたが、まだ入金確認前です。入金完了後にもう一度お試しください。")
    if verification_status == "cancelled":
        return render_form_error("この注文はキャンセル扱いになっています。別の注文番号をご確認ください。")
    if verification_status == "needs_review":
        return render_form_error("注文番号は見つかりましたが、購入時メールアドレスが一致しません。購入時のメールアドレスをご確認ください。")

    location = resolve_birth_location((birth_prefecture or "").strip() or None, (birth_place or "").strip() or None)
    customer = get_or_create_customer(
        db,
        display_name=(normalized_line_name or user_name.strip()),
        line_user_id=normalized_line_user_id,
        email=normalized_contact,
    )
    linked_free_reading_code = (free_reading_code or "").strip().upper() or None
    source_free_order = _find_source_free_order(db, linked_free_reading_code)

    if existing_order:
        immutable_statuses = {"assigned", "in_progress", "delivered", "completed", "cancelled"}
        if (existing_order.status or "").strip() in immutable_statuses:
            return render_form_error(
                f"この注文番号はすでに受付済みです。受付番号は {existing_order.order_code} です。鑑定準備が進んでいるため、フォームの再送信では更新できません。",
                status_code=409,
            )

        existing_order.menu_id = menu.id
        existing_order.price = menu.price
        existing_order.user_name = user_name.strip()
        existing_order.user_contact = normalized_contact or None
        existing_order.birth_date = birth_date_obj
        existing_order.birth_time = (birth_time or "").strip() or None
        existing_order.birth_prefecture = location.get("birth_prefecture")
        existing_order.birth_place = location.get("birth_place")
        existing_order.birth_lat = location.get("birth_lat")
        existing_order.birth_lon = location.get("birth_lon")
        existing_order.location_source = location.get("location_source")
        existing_order.location_note = location.get("location_note")
        existing_order.gender = (gender or "").strip() or None
        existing_order.consultation_text = (consultation_text or "").strip() or None
        existing_order.customer = customer
        existing_order.source = "self"
        existing_order.external_platform = "stores"
        existing_order.external_order_ref = normalized_payment_order_ref
        existing_order.status = "paid"
        if source_free_order:
            existing_order.source_free_order_id = source_free_order.id
        db.add(OrderInputSnapshot(
            order_id=existing_order.id,
            inputs_json=json.dumps({
                "user_name": user_name,
                "user_contact": normalized_contact,
                "birth_date": birth_date,
                "birth_time": birth_time,
                "birth_prefecture": birth_prefecture,
                "birth_place": birth_place,
                "birth_lat": location.get("birth_lat"),
                "birth_lon": location.get("birth_lon"),
                "location_source": location.get("location_source"),
                "gender": gender,
                "consultation_text": consultation_text,
                "menu_id": menu_id,
                "payment_order_ref": normalized_payment_order_ref,
                "payment_email": normalized_payment_email,
                "line_user_id": normalized_line_user_id,
                "line_name": normalized_line_name,
                "stores_payment_snapshot": payment_row,
                "verification_status": verification_status,
                "form_resubmitted": True,
            }, ensure_ascii=False, default=str),
            payload_json=None,
            unknowns_json=None,
        ))
        draft = create_or_update_draft_from_order_inputs(
            db,
            menu=menu,
            user_name=user_name.strip(),
            user_contact=normalized_contact or None,
            birth_date=birth_date_obj,
            birth_time=(birth_time or "").strip() or None,
            birth_prefecture=location.get("birth_prefecture"),
            birth_place=location.get("birth_place"),
            birth_lat=location.get("birth_lat"),
            birth_lon=location.get("birth_lon"),
            location_source=location.get("location_source"),
            location_note=location.get("location_note"),
            gender=(gender or "").strip() or None,
            consultation_text=(consultation_text or "").strip() or None,
            source="self",
            external_platform="stores",
            external_order_ref=normalized_payment_order_ref,
            order_kind="paid",
            requested_menu_code=(course or "").strip().lower() or None,
            existing_order=existing_order,
        )
        promote_draft_to_order(db, draft, existing_order)
        existing_order.input_origin = existing_order.input_origin or "draft_promoted"
        db.commit()

        try:
            _notify_admin_new_paid_order(request, order=existing_order, payment_row=payment_row, event_label="申込み更新")
        except Exception:
            pass

        return RedirectResponse(url=f"/order/confirm?order_code={existing_order.order_code}&updated=1", status_code=303)

    order = create_order(
        db,
        menu=menu,
        user_name=user_name.strip(),
        user_contact=normalized_contact or None,
        birth_date=birth_date_obj,
        birth_time=(birth_time or "").strip() or None,
        birth_prefecture=location.get("birth_prefecture"),
        birth_place=location.get("birth_place"),
        birth_lat=location.get("birth_lat"),
        birth_lon=location.get("birth_lon"),
        location_source=location.get("location_source"),
        location_note=location.get("location_note"),
        gender=(gender or "").strip() or None,
        consultation_text=(consultation_text or "").strip() or None,
        customer=customer,
        source="self",
        external_platform="stores",
        external_order_ref=normalized_payment_order_ref,
        status="paid",
        inputs_json=json.dumps({
            "user_name": user_name,
            "user_contact": normalized_contact,
            "birth_date": birth_date,
            "birth_time": birth_time,
            "birth_prefecture": birth_prefecture,
            "birth_place": birth_place,
            "birth_lat": location.get("birth_lat"),
            "birth_lon": location.get("birth_lon"),
            "location_source": location.get("location_source"),
            "gender": gender,
            "consultation_text": consultation_text,
            "menu_id": menu_id,
            "payment_order_ref": normalized_payment_order_ref,
            "payment_email": normalized_payment_email,
            "line_user_id": normalized_line_user_id,
            "line_name": normalized_line_name,
            "stores_payment_snapshot": payment_row,
            "verification_status": verification_status,
        }, ensure_ascii=False, default=str),
    )
    if source_free_order:
        order.source_free_order_id = source_free_order.id
    draft = create_or_update_draft_from_order_inputs(
        db,
        menu=menu,
        user_name=user_name.strip(),
        user_contact=normalized_contact or None,
        birth_date=birth_date_obj,
        birth_time=(birth_time or "").strip() or None,
        birth_prefecture=location.get("birth_prefecture"),
        birth_place=location.get("birth_place"),
        birth_lat=location.get("birth_lat"),
        birth_lon=location.get("birth_lon"),
        location_source=location.get("location_source"),
        location_note=location.get("location_note"),
        gender=(gender or "").strip() or None,
        consultation_text=(consultation_text or "").strip() or None,
        source="self",
        external_platform="stores",
        external_order_ref=normalized_payment_order_ref,
        order_kind="paid",
        requested_menu_code=(course or "").strip().lower() or None,
        existing_order=order,
    )
    promote_draft_to_order(db, draft, order)
    order.input_origin = "draft_promoted"
    db.commit()

    try:
        _notify_admin_new_paid_order(request, order=order, payment_row=payment_row, event_label="申込み受付")
    except Exception:
        pass

    return RedirectResponse(url=f"/order/confirm?order_code={order.order_code}", status_code=303)


@router.post("/order/send-form-link")
def send_form_link(
    request: Request,
    course: str = Form(...),
    email_to: str = Form(...),
    payment_order_ref: str | None = Form(None),
    line_user_id: str | None = Form(None),
    line_name: str | None = Form(None),
):
    normalized_email = _normalize_email(email_to)
    if not normalized_email:
        raise HTTPException(status_code=400, detail="invalid email")
    normalized_course = (course or "").strip().lower()
    if normalized_course not in COURSE_SLUG_PRICE_MAP:
        raise HTTPException(status_code=400, detail="invalid course")
    ok = _send_user_form_link_email(
        request,
        to_email=normalized_email,
        course=normalized_course,
        payment_order_ref=_normalize_order_no(payment_order_ref or ""),
        line_user_id=(line_user_id or "").strip() or None,
        line_name=(line_name or "").strip() or None,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="send failed")
    return JSONResponse({"ok": True})


@router.get("/free-reading/start", response_class=HTMLResponse)
def free_reading_start_page(request: Request):
    return templates.TemplateResponse(request=request, name="free_start.html", context={"request": request, "error": None, "prefecture_options": PREFECTURE_OPTIONS})


@router.post("/free-reading/start")
def free_reading_create(
    request: Request,
    background_tasks: BackgroundTasks,
    user_name: str = Form(...),
    user_contact: str | None = Form(None),
    birth_date: str = Form(...),
    birth_time: str | None = Form(None),
    birth_prefecture: str | None = Form(None),
    birth_place: str | None = Form(None),
    gender: str | None = Form(None),
    consultation_text: str | None = Form(None),
    db: Session = Depends(get_db),
):
    normalized_contact = (user_contact or "").strip()

    try:
        birth_date_obj = date.fromisoformat(birth_date)
    except ValueError:
        return templates.TemplateResponse(
            request=request,
            name="free_start.html",
            context={"request": request, "error": "生年月日の形式が正しくありません。", "prefecture_options": PREFECTURE_OPTIONS},
            status_code=400,
        )

    menu = db.scalar(select(Menu).where(Menu.name == "無料鑑定"))
    if not menu:
        raise HTTPException(status_code=500, detail="無料鑑定メニューが見つかりません")

    customer = None
    location = resolve_birth_location((birth_prefecture or "").strip() or None, (birth_place or "").strip() or None)
    if user_contact:
        customer = get_or_create_customer(db, display_name=user_name.strip(), email=user_contact.strip() if "@" in user_contact else None)

    order = create_order(
        db,
        menu=menu,
        user_name=user_name.strip(),
        user_contact=normalized_contact or None,
        birth_date=birth_date_obj,
        birth_time=(birth_time or "").strip() or None,
        birth_prefecture=location.get("birth_prefecture"),
        birth_place=location.get("birth_place"),
        birth_lat=location.get("birth_lat"),
        birth_lon=location.get("birth_lon"),
        location_source=location.get("location_source"),
        location_note=location.get("location_note"),
        gender=(gender or "").strip() or None,
        consultation_text=(consultation_text or "").strip() or None,
        customer=customer,
        source="self",
        status="received",
        inputs_json=json.dumps({
            "user_name": user_name,
            "user_contact": normalized_contact,
            "birth_date": birth_date,
            "birth_time": birth_time,
            "birth_prefecture": birth_prefecture,
            "birth_place": birth_place,
            "gender": gender,
            "consultation_text": consultation_text,
            "menu_id": menu.id,
        }, ensure_ascii=False),
    )
    order.order_kind = "free"
    order.price = 0
    order.free_reading_code = ensure_unique_free_reading_code(db)
    order.ai_status = "queued"
    draft = create_or_update_draft_from_order_inputs(
        db,
        menu=menu,
        user_name=user_name.strip(),
        user_contact=normalized_contact or None,
        birth_date=birth_date_obj,
        birth_time=(birth_time or "").strip() or None,
        birth_prefecture=location.get("birth_prefecture"),
        birth_place=location.get("birth_place"),
        birth_lat=location.get("birth_lat"),
        birth_lon=location.get("birth_lon"),
        location_source=location.get("location_source"),
        location_note=location.get("location_note"),
        gender=(gender or "").strip() or None,
        consultation_text=(consultation_text or "").strip() or None,
        source="self",
        external_platform=None,
        external_order_ref=None,
        order_kind="free",
        existing_order=order,
    )
    promote_draft_to_order(db, draft, order)
    order.input_origin = "draft_promoted"
    db.commit()
    background_tasks.add_task(process_free_reading, order.id)
    return RedirectResponse(url=f"/free-reading/{order.order_code}/wait", status_code=303)


@router.get("/free-reading/{order_code}/wait", response_class=HTMLResponse)
def free_reading_wait(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).where(Order.order_code == order_code, Order.order_kind == "free"))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    return templates.TemplateResponse(request=request, name="free_wait.html", context={"request": request, "order": order})


@router.get("/free-reading/{order_code}/status")
def free_reading_status(order_code: str, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).where(Order.order_code == order_code, Order.order_kind == "free"))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    return {
        "order_code": order.order_code,
        "free_reading_code": order.free_reading_code,
        "status": order.ai_status or "queued",
        "result_url": f"/free-reading/{order.order_code}/result" if (order.ai_status == "completed") else None,
    }


@router.get("/free-reading/{order_code}/result", response_class=HTMLResponse)
def free_reading_result(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(
        select(Order)
        .options(selectinload(Order.yaml_logs), selectinload(Order.result_views))
        .where(Order.order_code == order_code, Order.order_kind == "free")
    )
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    result_payload = None
    if order.result_payload_json:
        try:
            result_payload = json.loads(order.result_payload_json)
        except Exception:
            result_payload = None
    yaml_log = sorted(order.yaml_logs, key=lambda x: x.updated_at or x.created_at, reverse=True)
    latest_yaml = yaml_log[0] if yaml_log else None
    return templates.TemplateResponse(
        request=request,
        name="free_result.html",
        context={"request": request, "order": order, "result_payload": result_payload, "yaml_log": latest_yaml, "footer_message": FREE_RESULT_FOOTER},
    )


@router.get("/order/confirm", response_class=HTMLResponse)
def order_confirm(order_code: str, request: Request, updated: int = 0, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.menu)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    return templates.TemplateResponse(
        request=request,
        name="order_confirm.html",
        context={"request": request, "order": order, "updated": bool(updated)},
    )


@router.get("/result/{order_code}", response_class=HTMLResponse)
def order_result(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(
        select(Order)
        .options(selectinload(Order.deliveries), selectinload(Order.menu), selectinload(Order.yaml_logs), selectinload(Order.result_views))
        .where(Order.order_code == order_code)
    )
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    latest_delivery = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    delivery = latest_delivery[0] if latest_delivery else None
    yaml_log = sorted(order.yaml_logs, key=lambda x: x.updated_at or x.created_at, reverse=True)
    latest_yaml = yaml_log[0] if yaml_log else None
    result_view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    result_payload = None
    raw_payload = result_view.result_payload_json if result_view and result_view.result_payload_json else order.result_payload_json
    if raw_payload:
        try:
            result_payload = json.loads(raw_payload)
        except Exception:
            result_payload = None
    return templates.TemplateResponse(
        request=request,
        name="order_result.html",
        context={"request": request, "order": order, "delivery": delivery, "yaml_log": latest_yaml, "result_view": result_view, "result_payload": result_payload},
    )


@router.get("/report/{order_code}", response_class=HTMLResponse)
def order_report(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(
        select(Order)
        .options(selectinload(Order.result_views), selectinload(Order.deliveries), selectinload(Order.yaml_logs), selectinload(Order.menu))
        .where(Order.order_code == order_code)
    )
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    result_view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if result_view and result_view.report_html:
        return HTMLResponse(content=result_view.report_html)
    return order_result(order_code=order_code, request=request, db=db)
