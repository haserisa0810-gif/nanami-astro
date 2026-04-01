from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Any, Iterable
from urllib.parse import urljoin
import json

import httpx

from models import Order




def _base_url() -> str:
    return (os.getenv("BASE_URL") or "").strip().rstrip("/")


def _absolute_url(url: str | None) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    base = _base_url()
    if not base:
        return raw
    return urljoin(base + "/", raw.lstrip("/"))


def build_order_result_url(order: Order) -> str:
    base = _base_url()
    if not base:
        return f"/report/{order.order_code}"
    return f"{base}/report/{order.order_code}"


def _extract_result_payload(order: Order) -> dict[str, Any]:
    raw_payload = getattr(order, "result_payload_json", None) or ""
    if not raw_payload and getattr(order, "result_views", None):
        latest = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
        raw_payload = getattr(latest, "result_payload_json", None) or ""
    if not raw_payload:
        return {}
    try:
        data = json.loads(raw_payload)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _latest_delivery_text(order: Order) -> str:
    deliveries = getattr(order, "deliveries", None) or []
    if not deliveries:
        return ""
    latest = next(iter(sorted(deliveries, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    return ((getattr(latest, "delivery_text", None) or "").strip()) if latest else ""


def _planet_digest(payload: dict[str, Any]) -> str:
    items = []
    for p in (payload.get("planet_list") or [])[:6]:
        if not isinstance(p, dict):
            continue
        name = (p.get("name") or "").strip()
        sign = (p.get("sign") or "").strip()
        house = (p.get("house") or "").strip()
        if name:
            tail = " / ".join([x for x in [sign, house] if x])
            items.append(f"・{name}{(' : ' + tail) if tail else ''}")
    return "\n".join(items)


def _sections_digest(payload: dict[str, Any]) -> str:
    rows = []
    for sec in (payload.get("sections") or [])[:3]:
        if not isinstance(sec, dict):
            continue
        heading = (sec.get("heading") or "本文").strip()
        body = (sec.get("body") or "").strip()
        if body:
            rows.append(f"【{heading}】\n{body[:700]}")
    return "\n\n".join(rows)


def build_delivery_completed_user_message(order: Order, *, mode: str = "delivery") -> str:
    payload = _extract_result_payload(order)
    delivery_text = _latest_delivery_text(order)
    result_url = build_order_result_url(order)
    is_free_order = (getattr(order, "order_kind", "") or "").strip() == "free"
    free_reading_code = (getattr(order, "free_reading_code", None) or "").strip()

    # mode の解釈:
    #   "delivery"     → 本文テキストのみ送る（URLなし）
    #   "report_only"  → URLのみ送る（本文テキストなし）
    #   "delivery_with_report" → 本文テキスト＋URL両方送る
    #   "auto"         → 自動鑑定用（本文＋URL両方送る）
    include_text = mode in ("delivery", "delivery_with_report", "auto")
    include_url  = mode in ("report_only", "delivery_with_report", "auto")

    if mode == "auto":
        main_body = _sections_digest(payload) or delivery_text or (getattr(order, "free_result_text", None) or "").strip()
        opening = "鑑定結果をお送りします。"
    elif mode == "report_only":
        main_body = ""
        opening = "お待たせしました。鑑定書をお届けします。"
    else:
        main_body = delivery_text or _sections_digest(payload) or (getattr(order, "free_result_text", None) or "").strip()
        opening = "お待たせしました。鑑定が仕上がりました。心を込めてお届けします。"

    if include_text and not main_body:
        main_body = "鑑定結果ページをご確認ください。"

    planet_text = _planet_digest(payload) if include_text else ""
    extra_lines = []
    if planet_text:
        extra_lines.append("【主要な星配置】\n" + planet_text)
    if include_url:
        extra_lines.append("【ホロスコープ図・ハウス解説・鑑定書】\n" + result_url)
    if is_free_order and free_reading_code:
        extra_lines.append(
            "【無料鑑定ID】\n"
            f"{free_reading_code}\n\n"
            "有料鑑定をご希望の場合は、LINEのお申込み時にこの無料鑑定IDをそのまま送ってください。\n"
            "今回の内容を引き継いでご案内しやすくなります。"
        )

    parts = [f"{opening}\n予約番号【{order.order_code}】"]
    if include_text and main_body:
        parts.append(main_body.strip())
    if extra_lines:
        parts.append("\n\n".join(extra_lines))

    return "\n\n".join(parts).strip()


def build_delivery_chart_url(order: Order) -> str:
    payload = _extract_result_payload(order)
    return _absolute_url(payload.get("horoscope_image_url") or "")


def _chunk_text(text: str, limit: int = 4900) -> list[str]:
    body = (text or "").strip()
    if not body:
        return []
    chunks = []
    while body:
        if len(body) <= limit:
            chunks.append(body)
            break
        cut = body.rfind("\n", 0, limit)
        if cut < int(limit * 0.6):
            cut = limit
        chunks.append(body[:cut].strip())
        body = body[cut:].lstrip()
    return [c for c in chunks if c]

def _split_env_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace("\n", ",").split(",") if item.strip()]


def _gender_label(value: str | None) -> str:
    mapping = {
        "1": "女性",
        "2": "男性",
        "3": "その他",
        "4": "回答しない",
        "female": "女性",
        "male": "男性",
        "other": "その他",
        "prefer_not_to_say": "回答しない",
    }
    return mapping.get((value or "").strip(), (value or "未指定"))


def build_reservation_summary(order: Order) -> str:
    return (
        "LINEから新しい占い予約が入りました。\n\n"
        f"予約番号: {order.order_code}\n"
        f"メニュー: {order.menu.name if order.menu else '-'}\n"
        f"名前: {order.user_name or '-'}\n"
        f"生年月日: {order.birth_date.isoformat() if order.birth_date else '-'}\n"
        f"出生時刻: {order.birth_time or '不明'}\n"
        f"出生地: {order.birth_place or '不明'}\n"
        f"性別: {_gender_label(order.gender)}\n"
        f"LINE userId: {order.user_contact or '-'}\n"
        f"状態: {order.status}\n"
        f"受付日時: {order.created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    )


def build_payment_completed_summary(order: Order) -> str:
    return (
        "LINE予約の決済が完了しました。\n\n"
        f"予約番号: {order.order_code}\n"
        f"メニュー: {order.menu.name if order.menu else '-'}\n"
        f"名前: {order.user_name or '-'}\n"
        f"生年月日: {order.birth_date.isoformat() if order.birth_date else '-'}\n"
        f"出生時刻: {order.birth_time or '不明'}\n"
        f"出生地: {order.birth_place or '不明'}\n"
        f"性別: {_gender_label(order.gender)}\n"
        f"LINE userId: {order.user_contact or '-'}\n"
        f"決済日時: {(order.paid_at.strftime('%Y-%m-%d %H:%M:%S') + ' UTC') if order.paid_at else '-'}\n"
    )


def build_payment_completed_user_message(order: Order) -> str:
    return (
        'ご決済ありがとうございます。\n'
        f'予約番号【{order.order_code}】のお支払いを確認しました。\n\n'
        'ご予約内容は受け付け済みです。\n'
        '入力内容に修正がある場合のみ、このままLINEに返信してください。\n'
        '順次確認のうえ、ご案内します。'
    )


async def _push_line_messages(user_ids: Iterable[str], messages: list[dict[str, Any]]) -> None:
    user_ids = [x for x in user_ids if x]
    messages = [m for m in (messages or []) if isinstance(m, dict) and m.get("type")]
    if not user_ids or not messages:
        return
    token = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
    if not token:
        print("LINE notify skipped: LINE_CHANNEL_ACCESS_TOKEN missing")
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=20) as client:
        for user_id in user_ids:
            payload = {"to": user_id, "messages": messages[:5]}
            try:
                response = await client.post(url, headers=headers, json=payload)
                print("LINE notify status:", user_id, response.status_code, (response.text or "")[:200])
            except Exception as exc:
                print("LINE notify exception:", user_id, repr(exc))


async def _push_line_message(user_ids: Iterable[str], text: str) -> None:
    messages = [{"type": "text", "text": chunk} for chunk in _chunk_text(text)]
    await _push_line_messages(user_ids, messages)


def _send_email_message(recipients: list[str], subject: str, body: str, *, thread_id: str | None = None) -> None:
    if not recipients:
        return
    host = (os.getenv("SMTP_HOST") or "").strip()
    username = (os.getenv("SMTP_USERNAME") or "").strip()
    password = os.getenv("SMTP_PASSWORD") or ""
    from_email = (os.getenv("SMTP_FROM_EMAIL") or username or "").strip()
    if not host or not from_email:
        print("Email notify skipped: SMTP_HOST or SMTP_FROM_EMAIL missing")
        return
    port = int((os.getenv("SMTP_PORT") or "587").strip())
    use_ssl = (os.getenv("SMTP_USE_SSL") or "false").strip().lower() == "true"
    use_starttls = (os.getenv("SMTP_USE_STARTTLS") or "true").strip().lower() == "true"

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = from_email
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    # スレッド化: 予約番号ごとに固定のMessage-IDを使いIn-Reply-Toで紐づける
    if thread_id:
        domain = (from_email.split("@")[-1]) if "@" in from_email else "nanami-astro.com"
        base_msg_id = f"<{thread_id}@{domain}>"
        message["Message-ID"] = f"<{thread_id}.{subject[:10].encode('ascii','ignore').decode()}@{domain}>"
        message["In-Reply-To"] = base_msg_id
        message["References"] = base_msg_id

    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_cls(host, port, timeout=20) as smtp:
        smtp.ehlo()
        if use_starttls and not use_ssl:
            smtp.starttls()
            smtp.ehlo()
        if username:
            smtp.login(username, password)
        smtp.send_message(message)
        print("Email notify sent to", recipients)


async def notify_new_line_reservation(order: Order) -> None:
    body = build_reservation_summary(order)
    admin_line_ids = _split_env_list(os.getenv("ADMIN_NOTIFY_LINE_USER_IDS"))
    reader_line_ids = _split_env_list(os.getenv("READER_NOTIFY_LINE_USER_IDS"))
    admin_emails = _split_env_list(os.getenv("ADMIN_NOTIFY_EMAILS"))
    reader_emails = _split_env_list(os.getenv("READER_NOTIFY_EMAILS"))

    await _push_line_message(admin_line_ids + reader_line_ids, body)
    try:
        _send_email_message(admin_emails + reader_emails, f"LINE予約受付 【{order.order_code}】", body, thread_id=order.order_code)
    except Exception as exc:
        print("Email notify exception:", repr(exc))


async def notify_paid_line_order(order: Order) -> None:
    body = build_payment_completed_summary(order)
    user_message = build_payment_completed_user_message(order)
    admin_line_ids = _split_env_list(os.getenv("ADMIN_NOTIFY_LINE_USER_IDS"))
    reader_line_ids = _split_env_list(os.getenv("READER_NOTIFY_LINE_USER_IDS"))
    admin_emails = _split_env_list(os.getenv("ADMIN_NOTIFY_EMAILS"))
    reader_emails = _split_env_list(os.getenv("READER_NOTIFY_EMAILS"))
    customer_line_id = getattr(order.customer, 'line_user_id', None) or (order.user_contact if (order.user_contact or '').startswith('U') else None)

    await _push_line_message(admin_line_ids + reader_line_ids, body)
    if customer_line_id:
        await _push_line_message([customer_line_id], user_message)
    try:
        _send_email_message(admin_emails + reader_emails, f"決済完了 【{order.order_code}】", body, thread_id=order.order_code)
    except Exception as exc:
        print("Email notify exception:", repr(exc))


def build_correction_summary(order: Order, message_text: str) -> str:
    return (
        "LINE予約の修正連絡が届きました。\n\n"
        f"予約番号: {order.order_code}\n"
        f"名前: {order.user_name or '-'}\n"
        f"LINE userId: {order.user_contact or '-'}\n\n"
        "【修正内容】\n"
        f"{(message_text or '').strip() or '-'}\n"
    )


async def notify_line_order_correction(order: Order, message_text: str) -> None:
    body = build_correction_summary(order, message_text)
    admin_line_ids = _split_env_list(os.getenv("ADMIN_NOTIFY_LINE_USER_IDS"))
    reader_line_ids = _split_env_list(os.getenv("READER_NOTIFY_LINE_USER_IDS"))
    admin_emails = _split_env_list(os.getenv("ADMIN_NOTIFY_EMAILS"))
    reader_emails = _split_env_list(os.getenv("READER_NOTIFY_EMAILS"))

    await _push_line_message(admin_line_ids + reader_line_ids, body)
    try:
        _send_email_message(admin_emails + reader_emails, f"LINE予約修正 【{order.order_code}】", body, thread_id=order.order_code)
    except Exception as exc:
        print("Email notify exception:", repr(exc))


async def notify_line_delivery(order: Order, *, mode: str = "delivery") -> None:
    customer_line_id = getattr(order.customer, 'line_user_id', None) or (order.user_contact if (order.user_contact or '').startswith('U') else None)
    if not customer_line_id:
        print("LINE delivery notify skipped: customer line id missing", order.order_code)
        return

    text = build_delivery_completed_user_message(order, mode=mode)
    chart_url = build_delivery_chart_url(order)

    messages: list[dict[str, Any]] = []
    for chunk in _chunk_text(text):
        messages.append({"type": "text", "text": chunk})
    if chart_url.startswith("http://") or chart_url.startswith("https://"):
        messages.append({"type": "image", "originalContentUrl": chart_url, "previewImageUrl": chart_url})

    await _push_line_messages([customer_line_id], messages[:5])
