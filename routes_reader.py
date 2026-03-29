from __future__ import annotations

from datetime import datetime
import asyncio

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from auth import clear_reader_login, get_current_reader, set_reader_login, verify_password
from db import get_db
from models import Astrologer, Order, OrderDelivery, Payout, YamlLog
from routes_staff import _resolve_reader_login
from services.order_service import update_order_status
from services.yaml_log_service import upsert_yaml_log
from services.reader_availability import line_status_label, normalize_line_status
from services.notification_service import notify_line_delivery

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _run_async_notification(coro) -> None:
    try:
        asyncio.run(coro)
    except RuntimeError:
        print('Notification skipped: existing event loop')
    except Exception as exc:
        print('Notification error:', repr(exc))


def _sync_payout(db: Session, order: Order, reader: Astrologer) -> None:
    if order.status not in {"assigned", "in_progress", "delivered", "completed"}:
        return
    rate = float(reader.commission_rate)
    reader_amount = int(round(order.price * rate / 100.0))
    platform_amount = order.price - reader_amount
    payout = db.scalar(select(Payout).where(Payout.order_id == order.id, Payout.reader_id == reader.id))
    if payout:
        payout.gross_amount = order.price
        payout.commission_rate = rate
        payout.reader_amount = reader_amount
        payout.platform_amount = platform_amount
    else:
        db.add(Payout(order_id=order.id, reader_id=reader.id, gross_amount=order.price, commission_rate=rate, reader_amount=reader_amount, platform_amount=platform_amount, status="unpaid"))


@router.get("/reader/login", response_class=HTMLResponse)
def reader_login_page(request: Request):
    return templates.TemplateResponse(request=request, name="reader_login.html", context={"request": request, "error": None})


@router.post("/reader/login")
def reader_login(response: Response, request: Request, login_email: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    reader = _resolve_reader_login(db, login_email)
    if not reader or not verify_password(password, reader.password_hash):
        return templates.TemplateResponse(request=request, name="reader_login.html", context={"request": request, "error": "メールアドレスまたはパスワードが違います。"}, status_code=400)
    if reader.status != "active":
        return templates.TemplateResponse(request=request, name="reader_login.html", context={"request": request, "error": "このアカウントは現在利用できません。"}, status_code=400)
    target = '/reader/profile?force_password=1' if getattr(reader, 'is_temp_password', False) else '/reader/dashboard'
    redirect = RedirectResponse(url=target, status_code=303)
    set_reader_login(redirect, reader)
    return redirect


@router.get("/reader/logout")
def reader_logout():
    response = RedirectResponse(url="/reader/login", status_code=303)
    clear_reader_login(response)
    return response


@router.get("/reader/dashboard", response_class=HTMLResponse)
def reader_dashboard(request: Request, reader: Astrologer = Depends(get_current_reader), db: Session = Depends(get_db)):
    orders = db.scalars(select(Order).where(Order.assigned_reader_id == reader.id).order_by(Order.created_at.desc()).limit(10)).all()
    counts = {k: db.scalar(select(func.count()).select_from(Order).where(Order.assigned_reader_id == reader.id, Order.status == k)) or 0 for k in ["assigned", "in_progress", "delivered", "completed"]}
    planned = db.scalar(select(func.coalesce(func.sum(Payout.reader_amount), 0)).where(Payout.reader_id == reader.id)) or 0
    return templates.TemplateResponse(request=request, name="reader_dashboard.html", context={"request": request, "reader": reader, "orders": orders, "counts": counts, "planned": planned})


@router.get("/reader/orders", response_class=HTMLResponse)
def reader_orders(request: Request, reader: Astrologer = Depends(get_current_reader), db: Session = Depends(get_db)):
    orders = db.scalars(select(Order).options(selectinload(Order.menu)).where(Order.assigned_reader_id == reader.id).order_by(Order.created_at.desc())).all()
    return templates.TemplateResponse(request=request, name="reader_orders.html", context={"request": request, "reader": reader, "orders": orders})


@router.get("/reader/orders/{order_code}", response_class=HTMLResponse)
def reader_order_detail(order_code: str, request: Request, reader: Astrologer = Depends(get_current_reader), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.menu), selectinload(Order.deliveries), selectinload(Order.customer), selectinload(Order.yaml_logs)).where(Order.order_code == order_code, Order.assigned_reader_id == reader.id))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    latest = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    delivery = latest[0] if latest else None
    previous_logs = []
    if order.customer_id:
        previous_logs = db.scalars(select(YamlLog).where(YamlLog.customer_id == order.customer_id, YamlLog.order_id != order.id).order_by(YamlLog.created_at.desc()).limit(5)).all()
    return templates.TemplateResponse(request=request, name="reader_order_detail.html", context={"request": request, "reader": reader, "order": order, "delivery": delivery, 'previous_logs': previous_logs})


@router.post("/reader/orders/{order_code}/save")
def reader_save_delivery(order_code: str, request: Request, delivery_text: str = Form(...), action: str = Form("draft"), reader: Astrologer = Depends(get_current_reader), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.deliveries), selectinload(Order.menu), selectinload(Order.customer), selectinload(Order.result_views)).where(Order.order_code == order_code, Order.assigned_reader_id == reader.id))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    if order.status == "assigned":
        update_order_status(db, order, to_status="in_progress", actor_type="reader", actor_id=reader.id, note="reader started work")

    latest = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    delivery = latest[0] if latest else None
    if delivery and delivery.reader_id == reader.id:
        delivery.delivery_text = delivery_text.strip()
        delivery.is_draft = action != "deliver"
        if action == "deliver":
            delivery.delivered_at = datetime.utcnow()
    else:
        delivery = OrderDelivery(order_id=order.id, reader_id=reader.id, delivery_text=delivery_text.strip(), is_draft=action != "deliver", delivered_at=datetime.utcnow() if action == "deliver" else None)
        db.add(delivery)

    should_notify = action in {"deliver_notify", "deliver_auto"}
    notify_mode = "auto" if action == "deliver_auto" else "delivery"

    if action in {"deliver", "deliver_notify", "deliver_auto"}:
        update_order_status(db, order, to_status="delivered", actor_type="reader", actor_id=reader.id, note="delivery completed")
        upsert_yaml_log(db, order, created_by_type='reader', created_by_id=reader.id, delivery_text=delivery.delivery_text, summary={'menu': order.menu.name if order.menu else None, 'reader': reader.display_name})

    _sync_payout(db, order, reader)
    db.commit()

    if should_notify:
        _run_async_notification(notify_line_delivery(order, mode=notify_mode))

    return RedirectResponse(url=f"/reader/orders/{order_code}", status_code=303)


@router.get("/reader/profile", response_class=HTMLResponse)
def reader_profile(request: Request, reader: Astrologer = Depends(get_current_reader)):
    return templates.TemplateResponse(
        request=request,
        name="reader_profile.html",
        context={
            "request": request,
            "reader": reader,
            "error": request.query_params.get("error"),
            "success": request.query_params.get("success"),
            "force_password": request.query_params.get("force_password"),
            "line_status_label": line_status_label,
        },
    )


@router.post("/reader/profile")
def reader_profile_update(
    request: Request,
    display_name: str = Form(...),
    new_password: str = Form(''),
    new_password_confirm: str = Form(''),
    line_accepting_enabled: str | None = Form(None),
    line_accepting_status: str = Form('open'),
    line_accepting_message: str = Form(''),
    reader: Astrologer = Depends(get_current_reader),
    db: Session = Depends(get_db),
):
    display_name = display_name.strip()
    if display_name:
        reader.display_name = display_name

    new_password = (new_password or '').strip()
    new_password_confirm = (new_password_confirm or '').strip()
    if reader.is_temp_password and not new_password:
        return RedirectResponse(url='/reader/profile?error=temp_password_change_required&force_password=1', status_code=303)
    if new_password or new_password_confirm:
        if len(new_password) < 8:
            return RedirectResponse(url='/reader/profile?error=password_too_short', status_code=303)
        if new_password != new_password_confirm:
            return RedirectResponse(url='/reader/profile?error=password_mismatch', status_code=303)
        from auth import hash_password
        reader.password_hash = hash_password(new_password)
        reader.is_temp_password = False

    reader.line_accepting_enabled = str(line_accepting_enabled or '').lower() in {'1', 'true', 'on', 'yes'}
    reader.line_accepting_status = normalize_line_status(line_accepting_status)
    reader.line_accepting_message = (line_accepting_message or '').strip() or None
    db.commit()
    return RedirectResponse(url='/reader/profile?success=updated', status_code=303)


@router.get("/reader/payouts", response_class=HTMLResponse)
def reader_payouts(request: Request, reader: Astrologer = Depends(get_current_reader), db: Session = Depends(get_db)):
    payouts = db.scalars(select(Payout).options(selectinload(Payout.order)).where(Payout.reader_id == reader.id).order_by(Payout.created_at.desc())).all()
    total = sum(p.reader_amount for p in payouts)
    return templates.TemplateResponse(request=request, name="reader_payouts.html", context={"request": request, "reader": reader, "payouts": payouts, "total": total})
