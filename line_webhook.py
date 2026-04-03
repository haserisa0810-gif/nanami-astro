"""
line_webhook.py
LINE Webhook のエンドポイント定義。
イベント受信・署名検証・LINE API 送受信のみを担当する。
テキスト解析は line_parser、占術計算は line_handler に委譲する。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request

try:
    from src.web.line_session import clear_session, get_session, upsert_session  # type: ignore
except Exception:
    from line_session import clear_session, get_session, upsert_session  # type: ignore

from line_parser import (
    help_text,
    merge_user_state,
    missing_fields_message,
    parse_line_text,
    should_reset,
    should_show_help,
)
from line_order_handler import (
    append_correction_note,
    apply_correction_to_order,
    correction_select_prompt,
    get_order_by_code,
    handle_order_message,
    should_start_order,
)
from services.notification_service import notify_line_order_correction, notify_new_line_reservation
from services.app_settings import get_line_bot_settings
from services.reader_availability import list_line_available_readers
from db import SessionLocal
from models import LineWebhookEvent

router = APIRouter()


def _event_dedup_key(event: dict[str, Any]) -> str | None:
    webhook_event_id = (event.get("webhookEventId") or "").strip()
    if webhook_event_id:
        return webhook_event_id

    delivery_context = event.get("deliveryContext") or {}
    is_redelivery = bool(delivery_context.get("isRedelivery"))
    event_type = (event.get("type") or "").strip()
    user_id = (event.get("source") or {}).get("userId") or ""
    timestamp = str(event.get("timestamp") or "")

    message = event.get("message") or {}
    message_id = str(message.get("id") or "")
    message_text = str(message.get("text") or "")

    postback = event.get("postback") or {}
    postback_data = str(postback.get("data") or "")

    if is_redelivery or message_id:
        raw = "|".join([event_type, user_id, timestamp, message_id, message_text, postback_data])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return None


def _register_line_event_if_new(event: dict[str, Any], user_id: str | None, event_type: str | None) -> bool:
    dedup_key = _event_dedup_key(event)
    if not dedup_key:
        return True

    with SessionLocal() as db:
        existing = db.query(LineWebhookEvent).filter(LineWebhookEvent.webhook_event_id == dedup_key).first()
        if existing:
            print("LINE duplicate event skipped:", dedup_key)
            return False
        db.add(
            LineWebhookEvent(
                webhook_event_id=dedup_key,
                line_user_id=user_id,
                event_type=event_type,
                raw_event_json=json.dumps(event, ensure_ascii=False),
            )
        )
        db.commit()
        return True



def _line_calc_helpers():
    from line_handler import build_astro_payload, build_report, format_reply, run_astro_calc
    return build_astro_payload, build_report, format_reply, run_astro_calc


def _verify_line_signature(body: bytes, channel_secret: str, signature: str) -> bool:
    mac = hmac.new(channel_secret.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature or "")


async def _line_push(user_id: str | None, text: str) -> None:
    if not user_id:
        print("NO userId for push")
        return

    token = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
    if not token:
        print("LINE_CHANNEL_ACCESS_TOKEN is missing")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": (text or "")[:4900]}]}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, headers=headers, json=payload)
            print("LINE push status:", response.status_code, "body:", (response.text or "")[:300])
    except Exception as exc:
        print("LINE push exception:", repr(exc))


async def _line_reply(reply_token: str | None, text: str, user_id: str | None = None) -> None:
    token = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
    if not token:
        print("LINE_CHANNEL_ACCESS_TOKEN is missing")
        return

    if not reply_token:
        print("NO replyToken, fallback to push")
        await _line_push(user_id, text)
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": (text or "")[:4900]}],
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(url, headers=headers, json=payload)
            print("LINE reply status:", response.status_code, "body:", (response.text or "")[:300])
            if response.status_code >= 400:
                print("LINE reply failed, fallback to push")
                await _line_push(user_id, text)
    except Exception as exc:
        print("LINE reply exception:", repr(exc))
        await _line_push(user_id, text)


async def _get_line_display_name(user_id: str | None) -> str | None:
    if not user_id:
        return None

    token = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
    if not token:
        return None

    url = f"https://api.line.me/v2/bot/profile/{user_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url, headers=headers)
            if response.status_code != 200:
                print("LINE profile status:", response.status_code, "body:", (response.text or "")[:300])
                return None
            return response.json().get("displayName")
    except Exception as exc:
        print("LINE profile exception:", repr(exc))
        return None


def _format_user_name(name: str | None) -> str:
    if not name:
        return "あなた"
    name = (name or "").strip()
    return name if (name and name.endswith("さん")) else (f"{name}さん" if name else "あなた")


def _looks_like_booking_payload(text: str | None) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    required = ["【コース】", "【お名前】", "【生年月日】"]
    has_required = all(k in t for k in required)
    has_detail = (
        "\n" in t
        or "【出生時間】" in t
        or "【出生地】" in t
        or "【性別】" in t
        or "【ご相談内容】" in t
    )
    return has_required and has_detail


@router.get("/line/webhook")
async def line_webhook_healthcheck() -> dict[str, Any]:
    return {"ok": True, "message": "LINE webhook is reachable"}


@router.post("/line/webhook")
async def line_webhook(request: Request) -> dict[str, Any]:
    body = await request.body()
    signature = request.headers.get("x-line-signature", "")

    print("LINE webhook body:", body[:500])
    print("LINE signature header:", signature)

    secret = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
    if secret and not _verify_line_signature(body, secret, signature):
        print("LINE signature invalid")
        raise HTTPException(status_code=401, detail="Invalid LINE signature")

    try:
        payload = json.loads(body.decode("utf-8") or "{}")
    except Exception as exc:
        print("LINE webhook json parse error:", repr(exc))
        payload = {}

    print("LINE payload parsed:", payload)

    events = payload.get("events", []) or []
    if not events:
        return {"ok": True}

    for event in events:
        reply_token = event.get("replyToken")
        user_id = event.get("source", {}).get("userId")
        event_type = event.get("type")
        print("LINE event type:", event_type, "userId:", user_id)

        if not _register_line_event_if_new(event, user_id, event_type):
            continue

        raw_text = None
        if event_type == "message":
            message = event.get("message", {}) or {}
            if message.get("type") != "text":
                print("LINE ignored non-text message:", event)
                continue
            raw_text = (message.get("text", "") or "").strip()
        elif event_type == "postback":
            postback = event.get("postback", {}) or {}
            data = (postback.get("data", "") or "").strip()
            print("LINE postback data:", data)
            if data in {"reserve", "start_order", "action=reserve"}:
                raw_text = "予約"
            elif data in {"restart", "action=restart"}:
                raw_text = "やり直し"
            elif data in {"cancel", "action=cancel"}:
                raw_text = "キャンセル"
            elif data in {"resume", "action=resume"}:
                raw_text = "続き"
            else:
                raw_text = data
        else:
            print("LINE ignored event type:", event_type, event)
            continue

        session = get_session(user_id)
        current_state = (session or {}).get("state") or "idle"
        env_mode = (os.getenv("LINE_BOT_MODE") or "order").strip().lower()
        runtime_settings = {
            "line_bot_enabled": True,
            "line_order_accepting": True,
            "line_bot_mode": env_mode or "order",
        }
        available_line_readers = []
        try:
            with SessionLocal() as db:
                runtime_settings = get_line_bot_settings(db, env_mode=env_mode)
                available_line_readers = list_line_available_readers(db)
        except Exception as exc:
            print("LINE settings fallback to env due to error:", repr(exc))
        bot_mode = runtime_settings.get("line_bot_mode") or env_mode or "order"
        line_bot_enabled = bool(runtime_settings.get("line_bot_enabled", True))
        line_order_accepting = bool(runtime_settings.get("line_order_accepting", True))

        if not line_bot_enabled or bot_mode == "off":
            await _line_reply(reply_token, '現在LINE受付を停止しています。再開までお待ちください。', user_id)
            continue

        looks_like_booking_payload = _looks_like_booking_payload(raw_text)
        force_order_flow = bool(
            should_start_order(raw_text)
            or looks_like_booking_payload
            or (current_state not in {None, '', 'idle'})
        )
        is_new_order_start = bool(
            should_start_order(raw_text)
            and current_state in {None, '', 'idle', 'completed'}
        )

        # 予約開始語が来たとき、または予約フロー継続中のみ予約処理へ
        if force_order_flow:
            if is_new_order_start and not line_order_accepting:
                await _line_reply(
                    reply_token,
                    '現在、新規のLINE予約受付を停止しています。再開後にもう一度「予約」と送ってください。',
                    user_id,
                )
                continue

            if is_new_order_start and not available_line_readers:
                await _line_reply(
                    reply_token,
                    '現在、受付可能な占い師がいません。時間をおいて再度「予約」と送ってください。',
                    user_id,
                )
                continue

            line_display_name = await _get_line_display_name(user_id)
            existing_order_code = (session or {}).get("order_code")

            if current_state == "completed" and raw_text not in {"続き", "やり直し", "キャンセル"} and not should_start_order(raw_text):
                order = get_order_by_code(existing_order_code)
                if order:
                    if order.status in {"in_progress", "delivered", "completed", "cancelled"}:
                        await _line_reply(
                            reply_token,
                            "鑑定をすでに開始しているため、内容の修正はお受けできません。\nご不明な点はご予約時のやり取りをご確認ください。",
                            user_id,
                        )
                        continue

                    correction_state = (session or {}).get("correction_state")

                    if raw_text in {"修正", "修正したい", "変更", "変更したい", "訂正", "間違えた", "間違い"} or correction_state is None:
                        upsert_session(user_id, {
                            "state": "completed",
                            "order_code": existing_order_code,
                            "correction_state": "field_select",
                        })
                        await _line_reply(reply_token, correction_select_prompt(), user_id)
                        continue

                    if correction_state == "field_select":
                        if raw_text in {"1", "2", "3", "4", "5", "6", "7"}:
                            from line_order_handler import _edit_prompt
                            draft = {
                                "user_name": order.user_name,
                                "birth_date": order.birth_date.isoformat() if order.birth_date else None,
                                "birth_time": order.birth_time,
                                "birth_place": order.birth_place,
                                "gender": order.gender,
                                "consultation_text": order.consultation_text,
                            }
                            upsert_session(user_id, {
                                "state": "completed",
                                "order_code": existing_order_code,
                                "correction_state": "field_input",
                                "correction_field": raw_text,
                            })
                            await _line_reply(reply_token, _edit_prompt(raw_text, draft), user_id)
                        else:
                            await _line_reply(reply_token, f"1〜7の番号で選んでください。\n\n{correction_select_prompt()}", user_id)
                        continue

                    if correction_state == "field_input":
                        field_code = (session or {}).get("correction_field", "")
                        if raw_text == "確認に戻る":
                            upsert_session(user_id, {
                                "state": "completed",
                                "order_code": existing_order_code,
                                "correction_state": "field_select",
                            })
                            await _line_reply(reply_token, correction_select_prompt(), user_id)
                            continue

                        success, label = apply_correction_to_order(existing_order_code, field_code, raw_text)
                        if success:
                            upsert_session(user_id, {
                                "state": "completed",
                                "order_code": existing_order_code,
                            })
                            await notify_line_order_correction(order, f"[{label}を修正]\n{raw_text}")
                            await _line_reply(
                                reply_token,
                                f"【{label}】を修正しました。\n他に修正がある場合は「修正したい」と送ってください。",
                                user_id,
                            )
                        else:
                            await _line_reply(reply_token, "入力内容を確認してもう一度送ってください。", user_id)
                        continue

                else:
                    clear_session(user_id)
                    await _line_reply(
                        reply_token,
                        'ご予約内容が見つかりませんでした。新しくご予約される場合は「予約」と送ってください。',
                        user_id,
                    )
                    continue

            reply_text, next_session, should_clear, created_order_code = handle_order_message(
                user_id, raw_text, session, line_display_name
            )
            if should_clear:
                clear_session(user_id)
            else:
                upsert_session(user_id, next_session)

            await _line_reply(reply_token, reply_text, user_id)

            if created_order_code:
                order = get_order_by_code(created_order_code)
                if order:
                    try:
                        await notify_new_line_reservation(order)
                    except Exception as exc:
                        print("notify_new_line_reservation error:", repr(exc))
            continue

        # 予約フロー以外は通常問い合わせとして受ける
        await _line_reply(
            reply_token,
            "お問い合わせありがとうございます。\n"
            "ご相談内容をそのままお送りください。\n"
            "ご予約をご希望の場合は「予約」と送っていただければご案内します。",
            user_id,
        )
        continue

        if should_reset(raw_text):
            clear_session(user_id)
            await _line_reply(reply_token, "入力内容をリセットしました。\n" + help_text(), user_id)
            continue

        if should_show_help(raw_text):
            await _line_reply(reply_token, help_text(), user_id)
            continue

        line_display_name = await _get_line_display_name(user_id)
        formatted_user_name = _format_user_name(line_display_name)

        parsed = parse_line_text(raw_text)
        session = get_session(user_id)
        merged = merge_user_state(session, parsed)
        upsert_session(user_id, merged)

        print("LINE parsed text:", parsed)
        print("LINE merged session:", merged)

        missing_msg = missing_fields_message(merged)
        if missing_msg:
            await _line_reply(reply_token, missing_msg)
            continue

        try:
            build_astro_payload, build_report, format_reply, run_astro_calc = _line_calc_helpers()
            astro_payload, unknowns = build_astro_payload(merged)
        except HTTPException as exc:
            print("LINE webhook payload build error:", repr(exc))
            upsert_session(user_id, merged)
            if exc.status_code == 400 and "出生地" in str(exc.detail):
                await _line_reply(reply_token, "都道府県の解釈ができませんでした。\n例: 宮城 / 東京都 / 福岡", user_id)
            else:
                await _line_reply(reply_token, help_text(), user_id)
            continue
        except Exception as exc:
            print("LINE webhook payload build error:", repr(exc))
            upsert_session(user_id, merged)
            await _line_reply(reply_token, help_text(), user_id)
            continue

        try:
            astro = run_astro_calc(astro_payload)
            text, _meta = build_report(astro, merged, formatted_user_name, line_display_name)
            reply_text = format_reply(text, unknowns)
            clear_session(user_id)
            await _line_reply(reply_token, reply_text, user_id)
        except Exception as exc:
            print("LINE webhook error:", repr(exc))
            upsert_session(user_id, merged)
            await _line_reply(
                reply_token,
                "すみません、鑑定の生成で一時的にエラーが出ました。\n"
                "入力内容は保持しています。少し待って、もう一度メッセージを送ってください。",
                user_id,
            )

    return {"ok": True}
