from __future__ import annotations

import asyncio
import json
import os
import smtplib
from email.mime.text import MIMEText
from typing import Any

import stripe
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from db import get_db
from models import Order, PaymentTransaction
from services.notification_service import notify_paid_line_order
from services.order_service import auto_assign_reader, update_order_status
from services.stripe_service import create_checkout_session, retrieve_checkout_session

router = APIRouter()


def send_admin_mail(subject: str, body: str) -> None:
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = (os.getenv('SMTP_USER') or '').strip()
    smtp_pass = (os.getenv('SMTP_PASS') or '').strip()
    to_email = (os.getenv('ADMIN_EMAIL') or '').strip()

    if not smtp_user or not smtp_pass or not to_email:
        print('MAIL SKIP: SMTP_USER / SMTP_PASS / ADMIN_EMAIL not fully configured')
        return

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = to_email

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        print(f'MAIL SENT: {subject} -> {to_email}')
    except Exception as e:
        print('MAIL ERROR:', repr(e))


def _webhook_secret() -> str:
    return os.getenv('STRIPE_WEBHOOK_SECRET', '')


def _run_async_notification(coro) -> None:
    import asyncio
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            loop.create_task(coro)
        else:
            asyncio.run(coro)
    except Exception as exc:
        print('Notification error:', repr(exc))


def _is_test_payment_bypass_enabled() -> bool:
    value = (os.getenv('STRIPE_ALLOW_TEST_SKIP_PAYMENT') or '').strip().lower()
    if value in {'1', 'true', 'yes', 'on'}:
        return True
    app_env = (os.getenv('APP_ENV') or '').strip().lower()
    return app_env in {'dev', 'development', 'local', 'staging', 'test'}


def _maybe_auto_assign_paid_order(db: Session, order: Order) -> None:
    enabled = (os.getenv('AUTO_ASSIGN_PAID_ORDERS') or 'true').strip().lower()
    if enabled in {'0', 'false', 'off', 'no'}:
        return
    preferred_reader_id_raw = (os.getenv('AUTO_ASSIGN_READER_ID') or '').strip()
    preferred_reader_id = int(preferred_reader_id_raw) if preferred_reader_id_raw.isdigit() else None
    preferred_reader_email = (os.getenv('AUTO_ASSIGN_READER_EMAIL') or '').strip() or None
    auto_assign_reader(
        db,
        order,
        preferred_reader_id=preferred_reader_id,
        preferred_reader_email=preferred_reader_email,
        actor_type='system',
        note='auto assigned immediately after payment',
    )


def _load_order(db: Session, order_code: str) -> Order | None:
    return db.scalar(
        select(Order)
        .options(selectinload(Order.menu), selectinload(Order.customer))
        .where(Order.order_code == order_code)
    )


def _upsert_payment_tx(
    db: Session,
    order: Order,
    *,
    provider_session_id: str | None,
    event: dict[str, Any] | None = None,
    payment_intent: str | None = None,
) -> PaymentTransaction:
    tx = None
    if provider_session_id:
        tx = db.scalar(
            select(PaymentTransaction).where(PaymentTransaction.provider_session_id == provider_session_id)
        )
    if not tx:
        tx = db.scalar(
            select(PaymentTransaction).where(
                PaymentTransaction.order_id == order.id,
                PaymentTransaction.provider == 'stripe',
            )
        )
    if not tx:
        tx = PaymentTransaction(
            order_id=order.id,
            provider='stripe',
            provider_payment_id=payment_intent,
            provider_session_id=provider_session_id,
            amount=order.price,
            currency='jpy',
            status='pending',
        )
        db.add(tx)

    tx.provider_session_id = provider_session_id or tx.provider_session_id
    tx.provider_payment_id = payment_intent or tx.provider_payment_id
    if event is not None:
        tx.raw_event_json = json.dumps(event, ensure_ascii=False)
    return tx


def _mark_order_paid(
    db: Session,
    order: Order,
    *,
    provider_session_id: str | None,
    payment_intent: str | None,
    note: str,
) -> bool:
    tx = _upsert_payment_tx(
        db,
        order,
        provider_session_id=provider_session_id,
        payment_intent=payment_intent,
        event={
            'type': 'stripe.checkout.session.sync',
            'data': {
                'object': {
                    'id': provider_session_id,
                    'payment_intent': payment_intent,
                    'metadata': {'order_code': order.order_code},
                }
            },
        },
    )

    already_paid = order.status in {'paid', 'assigned', 'in_progress', 'delivered', 'completed'}
    tx.status = 'paid'

    if not already_paid:
        update_order_status(db, order, to_status='paid', actor_type='system', note=note)
        _maybe_auto_assign_paid_order(db, order)

    order.stripe_checkout_session_id = provider_session_id or order.stripe_checkout_session_id
    order.stripe_payment_intent_id = payment_intent or order.stripe_payment_intent_id
    tx.paid_at = order.paid_at

    return (not already_paid) and order.source == 'line'


def _send_payment_completed_admin_mail(order: Order) -> None:
    try:
        menu_name = getattr(getattr(order, 'menu', None), 'name', '') or '未設定'
        customer_name = getattr(getattr(order, 'customer', None), 'name', '') or getattr(order, 'customer_name', '') or '未設定'
        email = getattr(getattr(order, 'customer', None), 'email', '') or getattr(order, 'email', '') or '未設定'
        line_user_id = getattr(order, 'line_user_id', '') or 'なし'

        subject = f"[nanami astro] 決済完了 {order.order_code}"
        body = (
            '決済が完了しました。\n\n'
            f'注文ID: {order.order_code}\n'
            f'メニュー: {menu_name}\n'
            f'顧客名: {customer_name}\n'
            f'メール: {email}\n'
            f'LINE user id: {line_user_id}\n'
            f'金額: {order.price}円\n'
            f'現在ステータス: {order.status}\n\n'
            '管理画面で確認してください。\n'
        )
        send_admin_mail(subject, body)
    except Exception as exc:
        print('MAIL BUILD ERROR:', repr(exc))


def _sync_checkout_session_to_order(
    db: Session,
    order: Order,
    session_payload: Any,
    *,
    note: str,
) -> dict[str, Any]:
    session_id = getattr(session_payload, 'id', None) or session_payload.get('id')
    payment_status = getattr(session_payload, 'payment_status', None) or session_payload.get('payment_status')
    status = getattr(session_payload, 'status', None) or session_payload.get('status')
    payment_intent = getattr(session_payload, 'payment_intent', None) or session_payload.get('payment_intent')

    notify_paid = False

    if payment_status == 'paid':
        notify_paid = _mark_order_paid(
            db,
            order,
            provider_session_id=session_id,
            payment_intent=payment_intent,
            note='stripe session status sync from success page',
        )
    else:
        tx = _upsert_payment_tx(
            db,
            order,
            provider_session_id=session_id,
            payment_intent=payment_intent,
        )
        tx.status = 'expired' if status == 'expired' else 'pending'
        order.stripe_checkout_session_id = session_id or order.stripe_checkout_session_id

    db.commit()
    db.refresh(order)

    if notify_paid:
        _run_async_notification(notify_paid_line_order(order))

    return {
        'order_code': order.order_code,
        'order_status': order.status,
        'checkout_status': status,
        'payment_status': payment_status,
        'paid': payment_status == 'paid',
        'assigned_reader_id': order.assigned_reader_id,
    }


@router.post('/api/stripe/checkout')
def stripe_checkout(payload: dict, db: Session = Depends(get_db)):
    order_code = str(payload.get('order_code') or '').strip()
    if not order_code:
        raise HTTPException(status_code=400, detail='order_code required')

    order = db.scalar(
        select(Order).options(selectinload(Order.menu)).where(Order.order_code == order_code)
    )
    if not order or not order.menu:
        raise HTTPException(status_code=404, detail='order not found')

    if order.status not in {'pending_payment', 'payment_failed', 'expired'}:
        raise HTTPException(status_code=400, detail='order is not payable')

    try:
        session = create_checkout_session(order, order.menu)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'Stripe checkout creation failed: {exc}')

    order.stripe_checkout_session_id = session.id

    tx = db.scalar(
        select(PaymentTransaction).where(PaymentTransaction.provider_session_id == session.id)
    )
    if not tx:
        db.add(
            PaymentTransaction(
                order_id=order.id,
                provider='stripe',
                provider_session_id=session.id,
                amount=order.price,
                currency='jpy',
                status='pending',
            )
        )

    db.commit()
    return {'checkout_url': session.url, 'session_id': session.id}


@router.get('/api/stripe/orders/{order_code}/session-status')
def stripe_order_session_status(
    order_code: str,
    session_id: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    order = _load_order(db, order_code)
    if not order:
        raise HTTPException(status_code=404, detail='order not found')

    effective_session_id = (session_id or order.stripe_checkout_session_id or '').strip()
    if not effective_session_id:
        return {
            'order_code': order.order_code,
            'order_status': order.status,
            'checkout_status': None,
            'payment_status': None,
            'paid': order.status in {'paid', 'assigned', 'in_progress', 'delivered', 'completed'},
        }

    try:
        session = retrieve_checkout_session(effective_session_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f'Unable to retrieve Stripe session: {exc}')

    return _sync_checkout_session_to_order(
        db,
        order,
        session,
        note='stripe session status sync from success page',
    )


@router.post('/api/test/orders/{order_code}/simulate-paid')
def simulate_paid_order(order_code: str, db: Session = Depends(get_db)):
    if not _is_test_payment_bypass_enabled():
        raise HTTPException(status_code=403, detail='test payment bypass disabled')

    order = _load_order(db, order_code)
    if not order:
        raise HTTPException(status_code=404, detail='order not found')

    if order.status in {'delivered', 'completed', 'refunded'}:
        raise HTTPException(status_code=400, detail='order cannot be moved to paid from current status')

    tx = db.scalar(
        select(PaymentTransaction).where(
            PaymentTransaction.order_id == order.id,
            PaymentTransaction.provider == 'stripe',
        )
    )
    if not tx:
        tx = PaymentTransaction(
            order_id=order.id,
            provider='stripe',
            provider_session_id=f'test_skip_{order.order_code}',
            amount=order.price,
            currency='jpy',
            status='pending',
            raw_event_json=json.dumps(
                {'type': 'test.simulate_paid', 'order_code': order.order_code},
                ensure_ascii=False,
            ),
        )
        db.add(tx)

    tx.status = 'paid'
    update_order_status(db, order, to_status='paid', actor_type='system', note='test payment bypass')
    order.stripe_checkout_session_id = order.stripe_checkout_session_id or f'test_skip_{order.order_code}'
    tx.paid_at = order.paid_at
    _maybe_auto_assign_paid_order(db, order)

    db.commit()
    db.refresh(order)

    if order.source == 'line':
        _run_async_notification(notify_paid_line_order(order))

    return {
        'ok': True,
        'order_code': order.order_code,
        'status': order.status,
        'assigned_reader_id': order.assigned_reader_id,
        'redirect_url': f'/thanks/{order.order_code}',
    }


@router.post('/api/stripe/webhook')
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.body()
    sig_header = request.headers.get('stripe-signature', '')
    webhook_secret = _webhook_secret()

    print(
        f"STRIPE WEBHOOK: payload_len={len(payload)}, "
        f"sig={sig_header[:30] if sig_header else 'NONE'}, "
        f"secret_set={bool(webhook_secret)}"
    )

    try:
        if webhook_secret:
            event = stripe.Webhook.construct_event(
                payload=payload,
                sig_header=sig_header,
                secret=webhook_secret,
            )
        else:
            event = json.loads(payload.decode('utf-8'))
    except Exception as exc:
        print(f"STRIPE WEBHOOK ERROR: {repr(exc)}")
        raise HTTPException(status_code=400, detail=f'Invalid webhook: {exc}')

    if hasattr(event, 'to_dict_recursive'):
        event = event.to_dict_recursive()

    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            raise HTTPException(status_code=400, detail='Invalid webhook: event is not parseable')
    elif not isinstance(event, dict):
        try:
            event = json.loads(json.dumps(event, default=str))
        except Exception:
            raise HTTPException(status_code=400, detail='Invalid webhook: event conversion failed')

    if not isinstance(event, dict):
        raise HTTPException(status_code=400, detail='Invalid webhook: event is not a dict')

    event_type = event.get('type')
    data_object = ((event.get('data') or {}).get('object') or {}) if isinstance(event.get('data'), dict) else {}
    metadata = data_object.get('metadata') or {} if isinstance(data_object, dict) else {}
    order_code = (
        metadata.get('order_code') or data_object.get('client_reference_id') or ''
    ).strip() if isinstance(data_object, dict) else ''

    order = None
    if order_code:
        order = _load_order(db, order_code)
    elif data_object.get('id'):
        order = db.scalar(
            select(Order)
            .options(selectinload(Order.menu), selectinload(Order.customer))
            .where(Order.stripe_checkout_session_id == data_object.get('id'))
        )

    if order:
        tx = _upsert_payment_tx(
            db,
            order,
            provider_session_id=data_object.get('id') if str(event_type or '').startswith('checkout.session') else None,
            event=event,
            payment_intent=data_object.get('payment_intent'),
        )

        notify_paid = False

        if event_type == 'checkout.session.completed':
            notify_paid = _mark_order_paid(
                db,
                order,
                provider_session_id=data_object.get('id'),
                payment_intent=data_object.get('payment_intent'),
                note='stripe checkout completed',
            )
            _send_payment_completed_admin_mail(order)
        elif event_type == 'checkout.session.expired':
            tx.status = 'expired'
            update_order_status(db, order, to_status='expired', actor_type='system', note='stripe checkout expired')
        elif str(event_type or '').startswith('payment_intent.payment_failed'):
            tx.status = 'failed'
            update_order_status(db, order, to_status='payment_failed', actor_type='system', note='stripe payment failed')
        elif str(event_type or '').startswith('charge.refunded'):
            tx.status = 'refunded'
            update_order_status(db, order, to_status='refunded', actor_type='system', note='stripe refund')

        db.commit()
        db.refresh(order)

        if notify_paid:
            _run_async_notification(notify_paid_line_order(order))

    return JSONResponse({'received': True})
