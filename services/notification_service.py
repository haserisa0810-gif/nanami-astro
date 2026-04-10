import os
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Any

import httpx


def get_notify_emails() -> list[str]:
    emails = [
        x.strip()
        for x in (os.getenv("ADMIN_NOTIFY_EMAILS") or "").split(",")
        if x.strip()
    ]
    if not emails:
        single = (
            os.getenv("ADMIN_NOTIFY_EMAIL")
            or os.getenv("ALERT_EMAIL")
            or ""
        ).strip()
        if single:
            emails = [single]
    return emails


def _smtp_config() -> tuple[str, int, str, str, str, str]:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = (os.getenv("SMTP_USERNAME") or os.getenv("MAIL_USERNAME") or "").strip()
    smtp_pass = (os.getenv("SMTP_PASSWORD") or os.getenv("MAIL_PASSWORD") or "").strip()
    from_email = (os.getenv("SMTP_FROM_EMAIL") or smtp_user or "").strip()
    from_name = (os.getenv("SMTP_FROM_NAME") or "星月七海の星読み").strip()
    return smtp_host, smtp_port, smtp_user, smtp_pass, from_email, from_name


def send_mail(subject: str, body: str, to_emails: list[str]) -> bool:
    if not to_emails:
        return False

    smtp_host, smtp_port, smtp_user, smtp_pass, from_email, from_name = _smtp_config()
    if not smtp_user or not smtp_pass or not from_email:
        print("SMTP設定不足のためメール送信スキップ")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = ", ".join(to_emails)
    msg["Date"] = formatdate(localtime=True)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.ehlo()
            if smtp_port in (587, 25):
                server.starttls()
                server.ehlo()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, to_emails, msg.as_string())
        return True
    except Exception as e:
        print("メール送信エラー:", repr(e))
        return False


def _safe_get(obj: Any, name: str, default: Any = "-") -> Any:
    try:
        value = getattr(obj, name, default)
    except Exception:
        value = default
    return default if value in (None, "") else value


def _join_lines(lines: list[str]) -> str:
    return "\n".join(lines)


def _order_summary_lines(order: Any) -> list[str]:
    return [
        "■受付番号",
        str(_safe_get(order, "order_code")),
        "",
        "■STORES注文番号",
        str(_safe_get(order, "external_order_ref")),
        "",
        "■お名前",
        str(_safe_get(order, "user_name")),
        "",
        "■メニュー",
        str(_safe_get(order, "menu_id")),
        "",
        "■生年月日",
        str(_safe_get(order, "birth_date")),
        "",
        "■出生時間",
        str(_safe_get(order, "birth_time")),
        "",
        "■出生地",
        str(_safe_get(order, "birth_place")),
        "",
        "■連絡先",
        str(_safe_get(order, "user_contact")),
        "",
        "■相談内容",
        str(_safe_get(order, "consultation_text")),
        "",
        "■管理画面",
        f"https://pay.nanami-astro.com/admin/orders/{_safe_get(order, 'id', '')}",
    ]


def notify_new_order(order: Any) -> bool:
    emails = get_notify_emails()
    if not emails:
        return False

    subject = "【nanami-astro】鑑定依頼が届きました"
    body = _join_lines(["新しい鑑定依頼が届きました。", ""] + _order_summary_lines(order))
    return send_mail(subject, body, emails)


async def notify_line_order_correction(order: Any, correction_text: str | None = None) -> bool:
    emails = get_notify_emails()
    if not emails:
        return False

    subject = f"【nanami-astro】LINE注文修正 {_safe_get(order, 'order_code')}"
    body_lines = [
        "LINE経由で注文修正がありました。",
        "",
        *_order_summary_lines(order),
    ]
    if correction_text:
        body_lines += ["", "■修正内容", str(correction_text)]
    return send_mail(subject, _join_lines(body_lines), emails)


async def notify_new_line_reservation(order: Any) -> bool:
    emails = get_notify_emails()
    if not emails:
        return False

    subject = f"【nanami-astro】LINE予約が入りました {_safe_get(order, 'order_code')}"
    body = _join_lines(["LINE経由の新規予約がありました。", ""] + _order_summary_lines(order))
    return send_mail(subject, body, emails)


def _customer_line_id(order: Any) -> str:
    customer = getattr(order, "customer", None)
    customer_line_id = (getattr(customer, "line_user_id", None) or "").strip() if customer else ""
    if customer_line_id:
        return customer_line_id
    user_contact = (getattr(order, "user_contact", None) or "").strip()
    if user_contact.startswith("U"):
        return user_contact
    return ""


def _report_url(order: Any) -> str:
    return f"https://pay.nanami-astro.com/report/{_safe_get(order, 'order_code', '')}"


def _trim_text(text: str, limit: int = 4000) -> str:
    body = (text or "").strip()
    if len(body) <= limit:
        return body
    return body[: limit - 10].rstrip() + "\n\n（以下省略）"


async def _line_push(user_id: str | None, text: str) -> bool:
    if not user_id:
        print("NO userId for push")
        return False

    token = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
    if not token:
        print("LINE_CHANNEL_ACCESS_TOKEN is missing")
        return False

    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": (text or "")[:4900]}]}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, headers=headers, json=payload)
            print("LINE push status:", response.status_code, "body:", (response.text or "")[:300])
            return response.status_code < 400
    except Exception as exc:
        print("LINE push exception:", repr(exc))
        return False


def _latest_delivery_text(order: Any) -> str:
    deliveries = getattr(order, "deliveries", None) or []
    if deliveries:
        latest = sorted(
            deliveries,
            key=lambda d: getattr(d, "updated_at", None) or getattr(d, "created_at", None),
            reverse=True,
        )[0]
        text = (getattr(latest, "delivery_text", None) or "").strip()
        if text:
            return text
    return (getattr(order, "result_html", None) or "").strip()


async def notify_line_delivery(order: Any, mode: str | None = None) -> bool:
    user_id = _customer_line_id(order)
    if not user_id:
        print(f"LINE delivery skipped: no line_user_id for order {_safe_get(order, 'order_code')}")
        return False

    report_url = _report_url(order)
    delivery = _latest_delivery_text(order)
    body_lines = [
        f"{_safe_get(order, 'user_name', 'お客様')} 様",
        "",
        "鑑定が完了しました。",
        f"受付番号: {_safe_get(order, 'order_code')}",
        "",
        "▼鑑定書はこちら",
        report_url,
    ]
    if delivery:
        body_lines += ["", "▼鑑定本文", _trim_text(delivery, 3200)]
    message = _join_lines(body_lines)
    return await _line_push(user_id, message)


async def notify_delivery_email(order: Any, mode: str | None = None) -> bool:
    user_contact = str(_safe_get(order, "user_contact", "") or "").strip()
    customer = getattr(order, "customer", None)
    customer_email = (getattr(customer, "email", None) or "").strip() if customer else ""
    to_email = user_contact if "@" in user_contact and not user_contact.startswith("U") else customer_email
    if not to_email:
        return False

    subject = "【星月七海の星読み】鑑定結果のご案内"
    report_url = _report_url(order)
    delivery = _latest_delivery_text(order)
    body_lines = [
        f"{_safe_get(order, 'user_name', 'お客様')} 様",
        "",
        "鑑定が完了しました。",
        f"受付番号: {_safe_get(order, 'order_code')}",
        f"通知種別: {mode or 'delivery'}",
        "",
        "▼鑑定書はこちら",
        report_url,
    ]
    if delivery:
        body_lines += ["", "▼鑑定本文", _trim_text(delivery, 12000)]
    return send_mail(subject, _join_lines(body_lines), [to_email])
