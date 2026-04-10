from __future__ import annotations

from datetime import datetime
import asyncio
import json
from urllib.parse import urlencode

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session, selectinload

from auth import clear_all_logins, get_current_staff, set_admin_login, set_reader_login, verify_password
from db import get_db
from models import AdminUser, Astrologer, Order, OrderDelivery, OrderResultView, Payout, YamlLog
from routes_admin import STATUS_LABELS, _resolve_admin_login
from services.order_service import update_order_status
from services.location import PREFECTURE_OPTIONS, format_location_summary, resolve_birth_location
from services.yaml_log_service import create_yaml_log, upsert_yaml_log
from services.reader_availability import line_status_label
from services.result_builder import build_result_payload, render_report_html, render_result_html
from services.notification_service import notify_delivery_email, notify_line_delivery
from services.astrologer_summary import build_full_astrologer_summary
from services.auto_order_ai_service import process_order_auto_reading

router = APIRouter()
templates = Jinja2Templates(directory="templates")

STAFF_STATUSES = ["received", "paid", "in_progress", "completed", "cancelled"]


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _safe_json_loads(value: str | None) -> dict[str, object]:
    try:
        data = json.loads(value or "{}") if value else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _run_async_safely(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def _customer_line_id(order: Order) -> str | None:
    customer_line_id = (getattr(getattr(order, "customer", None), "line_user_id", None) or "").strip()
    if customer_line_id:
        return customer_line_id
    user_contact = (getattr(order, "user_contact", None) or "").strip()
    if user_contact.startswith("U"):
        return user_contact
    return None


def _customer_email(order: Order) -> str | None:
    user_contact = (getattr(order, "user_contact", None) or "").strip()
    if user_contact and "@" in user_contact and not user_contact.startswith("U"):
        return user_contact
    customer_email = (getattr(getattr(order, "customer", None), "email", None) or "").strip()
    return customer_email or None


def _source_label(order: Order) -> str:
    source = (getattr(order, "source", None) or "").strip().lower()
    if source == "line":
        return "LINE"
    if source in {"self", "web", "site", "homepage"}:
        return "ホームページ"
    return source or "不明"


def _queue_customer_delivery_notification(order: Order, *, mode: str | None, has_line_contact: bool) -> None:
    if not mode:
        print(f"Customer delivery notification skipped: mode missing for order {order.order_code}")
        return

    line_ok = False
    email_ok = False

    if has_line_contact:
        try:
            line_ok = bool(_run_async_safely(notify_line_delivery(order, mode=mode)))
        except Exception as exc:
            print(f"LINE delivery notification failed for {order.order_code}: {exc!r}")

    try:
        email_ok = bool(_run_async_safely(notify_delivery_email(order, mode=mode)))
    except Exception as exc:
        print(f"Email delivery notification failed for {order.order_code}: {exc!r}")

    print(
        f"Customer delivery notification finished for {order.order_code}: "
        f"line_contact={has_line_contact}, line_ok={line_ok}, email_ok={email_ok}, mode={mode}"
    )



def _meaningful_text(value: str | None) -> str:
    text = (value or '').strip()
    if not text or 'DEBUG' in text or '空文字' in text:
        return ''
    return text


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


def _resolve_reader_login(db: Session, login_input: str) -> Astrologer | None:
    normalized = (login_input or "").strip().lower()
    if not normalized:
        return None
    reader = db.scalar(select(Astrologer).where(Astrologer.login_email == normalized))
    if reader:
        return reader
    alias = normalized if "@" not in normalized else normalized.split("@", 1)[0]
    readers = db.scalars(select(Astrologer).where(Astrologer.status == "active")).all()
    for user in readers:
        email = (user.login_email or "").strip().lower()
        if email and email.split("@", 1)[0] == alias:
            return user
    return None


def _staff_actor(staff: dict[str, object]) -> tuple[str, int | None]:
    role = str(staff.get("role") or "reader")
    user = staff.get("user")
    return role, getattr(user, "id", None)


def _latest_delivery(order: Order) -> OrderDelivery | None:
    latest = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    return latest[0] if latest else None


def _has_report_html(order: Order) -> bool:
    try:
        views = list(getattr(order, "result_views", []) or [])
    except Exception:
        return False
    latest = next(iter(sorted(views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    return bool(latest and getattr(latest, "report_html", None))


def _active_yaml(order: Order) -> YamlLog | None:
    return next((x for x in sorted(order.yaml_logs, key=lambda x: (x.version_no or 0, x.updated_at or x.created_at), reverse=True) if getattr(x, "is_active", True)), None)


def _ensure_active_yaml_from_delivery(db: Session, order: Order, staff: dict[str, object]) -> YamlLog | None:
    active_yaml = _active_yaml(order)
    delivery = _latest_delivery(order)
    if active_yaml:
        return active_yaml
    if not delivery or not (delivery.delivery_text or '').strip():
        return None
    actor_type, actor_id = _staff_actor(staff)
    yaml_creator_id = actor_id if actor_type == 'reader' else None
    yaml_body = f"title: 鑑定結果\nreports:\n  web: |\n" + "\n".join([f"    {line}" for line in (delivery.delivery_text or '').splitlines()])
    return create_yaml_log(
        db, order, yaml_body=yaml_body, summary={"reports": {"web": delivery.delivery_text}, "saved_from": "delivery"},
        created_by_type=actor_type, created_by_id=yaml_creator_id, log_type='generated', set_active=True,
    )


def _build_publish_payload(order: Order, yaml_log: YamlLog):
    delivery = _latest_delivery(order)
    delivery_text = delivery.delivery_text if delivery else None
    payload = build_result_payload(order, yaml_log, delivery_text=delivery_text)
    return payload, delivery


def _preferred_default_reader(db: Session) -> Astrologer | None:
    readers = db.scalars(select(Astrologer).where(Astrologer.status == "active").order_by(Astrologer.id.asc())).all()
    if not readers:
        return None
    for reader in readers:
        display_name = (reader.display_name or "").strip()
        login_email = (reader.login_email or "").strip().lower()
        if "七海" in display_name or "nanami" in display_name.lower() or login_email.startswith("nanami"):
            return reader
    return readers[0]


def _ensure_reader_for_save(db: Session, order: Order, staff: dict[str, object]) -> Astrologer:
    if staff.get("role") == "reader":
        reader = staff.get("user")
        if isinstance(reader, Astrologer):
            if order.assigned_reader_id != reader.id:
                order.assigned_reader_id = reader.id
            return reader
    if order.assigned_reader_id:
        reader = db.get(Astrologer, order.assigned_reader_id)
        if reader and reader.status == "active":
            return reader
    reader = _preferred_default_reader(db)
    if not reader:
        raise HTTPException(status_code=400, detail="有効な占い師がいません。占い師アカウントを有効化してください。")
    order.assigned_reader_id = reader.id
    return reader


@router.get("/login", response_class=HTMLResponse)
def staff_login_page(request: Request):
    return templates.TemplateResponse(request=request, name="staff_login.html", context={"request": request, "error": None})


@router.post("/login")
def staff_login(request: Request, login_email: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    admin = _resolve_admin_login(db, login_email)
    if admin and verify_password(password, admin.password_hash):
        if not admin.is_active:
            return templates.TemplateResponse(request=request, name="staff_login.html", context={"request": request, "error": "このアカウントは現在利用できません。"}, status_code=400)
        redirect = RedirectResponse(url="/dashboard", status_code=303)
        set_admin_login(redirect, admin)
        return redirect

    reader = _resolve_reader_login(db, login_email)
    if reader and verify_password(password, reader.password_hash):
        if reader.status != "active":
            return templates.TemplateResponse(request=request, name="staff_login.html", context={"request": request, "error": "このアカウントは現在利用できません。"}, status_code=400)
        redirect = RedirectResponse(url="/dashboard", status_code=303)
        set_reader_login(redirect, reader)
        return redirect

    return templates.TemplateResponse(request=request, name="staff_login.html", context={"request": request, "error": "ログインID / メールアドレス またはパスワードが違います。"}, status_code=400)


@router.get("/logout")
def staff_logout():
    response = RedirectResponse(url="/login", status_code=303)
    clear_all_logins(response)
    return response


@router.get("/dashboard", response_class=HTMLResponse)
def staff_dashboard(request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    counts = {
        "free": db.scalar(select(func.count()).select_from(Order).where(Order.order_kind == "free")) or 0,
        "received": db.scalar(select(func.count()).select_from(Order).where(Order.order_kind != "free", Order.status == "received")) or 0,
        "paid": db.scalar(select(func.count()).select_from(Order).where(Order.order_kind != "free", Order.status == "paid")) or 0,
        "in_progress": db.scalar(select(func.count()).select_from(Order).where(Order.order_kind != "free", Order.status.in_(["assigned", "in_progress"]))) or 0,
        "completed": db.scalar(select(func.count()).select_from(Order).where(Order.order_kind != "free", Order.status.in_(["delivered", "completed"]))) or 0,
    }
    recent_orders = db.scalars(
        select(Order)
        .options(selectinload(Order.menu), selectinload(Order.assigned_reader), selectinload(Order.customer))
        .order_by(Order.created_at.desc())
        .limit(20)
    ).all()
    active_readers = db.scalars(select(Astrologer).where(Astrologer.status == "active").order_by(Astrologer.display_name.asc())).all()
    return templates.TemplateResponse(
        request=request,
        name="staff_dashboard.html",
        context={
            "request": request,
            "staff": staff,
            "counts": counts,
            "recent_orders": recent_orders,
            "status_labels": STATUS_LABELS,
            "active_readers": active_readers,
            "line_status_label": line_status_label,
        },
    )


@router.post("/staff/stores/mail-sync")
def staff_stores_mail_sync(staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    from routes_public_orders import sync_stores_order_emails
    try:
        result = sync_stores_order_emails(
            db,
            limit=int(os.getenv("STORES_MAIL_SYNC_MANUAL_LIMIT", "200")),
        )
        print(f"Manual mail sync by staff: {result}")
        msg = (
            f"メール取込完了: "
            f"取得{result.get('fetched',0)}件 / "
            f"解析{result.get('parsed',0)}件 / "
            f"登録{result.get('upserted',0)}件 / "
            f"スキップ{result.get('skipped',0)}件 / "
            f"エラー{result.get('errors',0)}件"
        )
        if result.get("message"):
            msg += f" / 詳細: {result.get('message')}"
    except Exception as e:
        msg = f"メール取込エラー: {e}"
        print(f"Manual mail sync error: {e}")
    from fastapi.responses import RedirectResponse
    from urllib.parse import quote
    return RedirectResponse(url=f"/dashboard?sync_msg={quote(msg)}", status_code=303)


@router.get("/staff/orders", response_class=HTMLResponse)
def staff_orders(request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    orders = db.scalars(
        select(Order)
        .options(selectinload(Order.menu), selectinload(Order.assigned_reader), selectinload(Order.customer))
        .order_by(Order.created_at.desc())
    ).all()
    return templates.TemplateResponse(
        request=request,
        name="staff_orders.html",
        context={"request": request, "staff": staff, "orders": orders, "status_labels": STATUS_LABELS},
    )


@router.get("/staff/orders/{order_code}", response_class=HTMLResponse)
def staff_order_detail(order_code: str, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    try:
        order = db.scalar(
            select(Order)
            .options(
                selectinload(Order.menu),
                selectinload(Order.deliveries).selectinload(OrderDelivery.reader),
                selectinload(Order.customer),
                selectinload(Order.yaml_logs),
                selectinload(Order.payment_transactions),
                selectinload(Order.status_logs),
                selectinload(Order.assigned_reader),
                selectinload(Order.payouts),
                selectinload(Order.result_views),
                selectinload(Order.source_free_order),
            )
            .where(Order.order_code == order_code)
        )
    except (OperationalError, ProgrammingError):
        db.rollback()
        order = db.scalar(
            select(Order)
            .options(
                selectinload(Order.menu),
                selectinload(Order.deliveries).selectinload(OrderDelivery.reader),
                selectinload(Order.customer),
                selectinload(Order.yaml_logs),
                selectinload(Order.payment_transactions),
                selectinload(Order.status_logs),
                selectinload(Order.assigned_reader),
                selectinload(Order.payouts),
            )
            .where(Order.order_code == order_code)
        )
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    readers = db.scalars(select(Astrologer).where(Astrologer.status == "active").order_by(Astrologer.display_name.asc())).all()
    default_reader = _preferred_default_reader(db)
    default_reader_id = default_reader.id if default_reader else None
    previous_logs = []
    if order.customer_id:
        previous_logs = db.scalars(
            select(YamlLog).where(YamlLog.customer_id == order.customer_id, YamlLog.order_id != order.id).order_by(YamlLog.created_at.desc()).limit(5)
        ).all()
    delivery = _latest_delivery(order)
    active_yaml = _active_yaml(order)
    try:
        result_candidates = list(getattr(order, 'result_views', []) or [])
    except (OperationalError, ProgrammingError):
        db.rollback()
        result_candidates = []
    result_view = next(iter(sorted(result_candidates, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if not active_yaml:
        result_status = 'no_yaml'
        result_status_label = 'YAML未保存'
    elif not result_view:
        result_status = 'not_reflected'
        result_status_label = '未完了（要対応）'
    elif not result_view.published_at:
        result_status = 'draft'
        result_status_label = '下書き・未反映'
    else:
        result_status = 'completed'
        result_status_label = '完了'
    editor_seed_text = ""
    editor_seed_source = "empty"
    linked_free_order = getattr(order, 'source_free_order', None)
    receipt_line = f"受付番号: {order.order_code}" if order.order_kind == "free" and order.order_code else ""

    if delivery and delivery.delivery_text and 'DEBUG' not in delivery.delivery_text and '空文字' not in delivery.delivery_text:
        editor_seed_text = delivery.delivery_text
        if receipt_line and receipt_line not in editor_seed_text:
            editor_seed_text = editor_seed_text.rstrip() + f"\n\n{receipt_line}"
        editor_seed_source = "delivery"
    elif order.order_kind == "free" and (order.free_result_text or '').strip():
        editor_seed_text = (order.free_result_text or '').strip()
        if receipt_line and receipt_line not in editor_seed_text:
            editor_seed_text = editor_seed_text.rstrip() + f"\n\n{receipt_line}"
        editor_seed_source = "free_result"
    elif linked_free_order and (linked_free_order.free_result_text or '').strip():
        editor_seed_text = (linked_free_order.free_result_text or '').strip()
        editor_seed_source = "source_free_result"

    return templates.TemplateResponse(
        request=request,
        name="staff_order_detail.html",
        context={
            "request": request,
            "staff": staff,
            "order": order,
            "delivery": delivery,
            "previous_logs": previous_logs,
            "readers": readers,
            "status_labels": STATUS_LABELS,
            "valid_statuses": STAFF_STATUSES,
            "prefecture_options": PREFECTURE_OPTIONS,
            "location_summary": format_location_summary(order.birth_prefecture, order.birth_place, order.birth_lat, order.birth_lon, order.location_source),
            "active_yaml": active_yaml,
            "result_view": result_view,
            "result_status": result_status,
            "result_status_label": result_status_label,
            "auto_ai_ready": bool((order.ai_status == "completed") and result_status != "completed"),
            "editor_seed_text": editor_seed_text,
            "editor_seed_source": editor_seed_source,
            "source_label": _source_label(order),
            "has_line_contact": bool(_customer_line_id(order)),
            "customer_line_id": _customer_line_id(order),
            "customer_email": _customer_email(order),
            "default_reader_id": default_reader_id,
        },
    )


@router.post("/staff/orders/{order_code}/update")
def staff_order_update(
    order_code: str,
    assigned_reader_id: str = Form(""),
    status: str = Form(...),
    staff_memo: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    actor_type, actor_id = _staff_actor(staff)
    reader_id = int(assigned_reader_id) if str(assigned_reader_id or "").strip() else None
    order.assigned_reader_id = reader_id
    order.staff_memo = (staff_memo or "").strip() or None

    target_status = status.strip()
    if target_status not in STAFF_STATUSES:
        raise HTTPException(status_code=400, detail="invalid status")

    if target_status == "in_progress" and not order.assigned_reader_id:
        chosen = _ensure_reader_for_save(db, order, staff)
        order.assigned_reader_id = chosen.id

    if target_status == "completed" and order.status not in {"delivered", "completed"} and order.deliveries:
        update_order_status(db, order, to_status="delivered", actor_type=actor_type, actor_id=actor_id, note="staff marked delivered before completed")
    update_order_status(db, order, to_status=target_status, actor_type=actor_type, actor_id=actor_id, note="staff updated order")
    db.commit()
    return _redirect(f"/staff/orders/{order_code}")


@router.post("/staff/orders/{order_code}/save")
def staff_save_delivery(
    order_code: str,
    delivery_text: str = Form(""),
    action: str = Form("draft"),
    assigned_reader_id: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).options(selectinload(Order.deliveries), selectinload(Order.menu), selectinload(Order.customer), selectinload(Order.result_views)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    if assigned_reader_id.strip():
        order.assigned_reader_id = int(assigned_reader_id)

    reader = _ensure_reader_for_save(db, order, staff)
    actor_type, actor_id = _staff_actor(staff)

    has_report_html = _has_report_html(order)

    latest = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    delivery = latest[0] if latest else None

    has_line_contact = bool(_customer_line_id(order))

    is_free_order = (order.order_kind == "free")

    if action == "resend_notify":
        if not is_free_order and not has_report_html:
            print(f"Customer delivery notification skipped: report_html missing for order {order.order_code}")
            return _redirect(f"/staff/orders/{order_code}")
        db.commit()
        notify_mode = "delivery_text_only" if is_free_order else "delivery_with_report"
        _queue_customer_delivery_notification(order, mode=notify_mode, has_line_contact=has_line_contact)
        return _redirect(f"/staff/orders/{order_code}")

    if action == "deliver" and not is_free_order and not has_report_html:
        print(f"Delivery blocked: report_html missing for order {order.order_code}")
        return _redirect(f"/staff/orders/{order_code}")

    cleaned_delivery_text = (delivery_text or '').strip()
    if order.status in {"received", "paid", "assigned"}:
        update_order_status(db, order, to_status="in_progress", actor_type=actor_type, actor_id=actor_id, note="staff started work")

    if delivery and delivery.reader_id == reader.id:
        delivery.delivery_text = cleaned_delivery_text
        delivery.is_draft = action != "deliver"
        if action == "deliver":
            delivery.delivered_at = datetime.utcnow()
    else:
        delivery = OrderDelivery(
            order_id=order.id,
            reader_id=reader.id,
            delivery_text=cleaned_delivery_text,
            is_draft=action != "deliver",
            delivered_at=datetime.utcnow() if action == "deliver" else None,
        )
        db.add(delivery)

    if action == "deliver":
        update_order_status(db, order, to_status="delivered", actor_type=actor_type, actor_id=actor_id, note="staff delivery completed")
        upsert_yaml_log(
            db,
            order,
            created_by_type="reader",
            created_by_id=reader.id,
            delivery_text=delivery.delivery_text,
            summary={"menu": order.menu.name if order.menu else None, "reader": reader.display_name},
        )

    _sync_payout(db, order, reader)
    db.commit()

    if action == "deliver":
        if is_free_order:
            _queue_customer_delivery_notification(order, mode="delivery_text_only", has_line_contact=has_line_contact)
        elif has_report_html:
            _queue_customer_delivery_notification(order, mode="delivery_with_report", has_line_contact=has_line_contact)
        else:
            print(f"Customer delivery notification skipped after deliver: report_html missing for order {order.order_code}")

    return _redirect(f"/staff/orders/{order_code}")


@router.post("/staff/orders/{order_code}/yaml-save")
def staff_save_yaml(
    order_code: str,
    yaml_body: str = Form(...),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    actor_type, actor_id = _staff_actor(staff)
    yaml_creator_id = actor_id if actor_type == "reader" else None
    create_yaml_log(
        db,
        order,
        yaml_body=(yaml_body or '').strip(),
        summary={"saved_from": "staff_detail"},
        created_by_type=actor_type,
        created_by_id=yaml_creator_id,
        log_type='edited',
        set_active=True,
    )
    db.commit()
    return _redirect(f"/staff/orders/{order_code}")


@router.post("/staff/orders/{order_code}/publish-result")
def staff_publish_result(
    order_code: str,
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).options(selectinload(Order.yaml_logs), selectinload(Order.result_views), selectinload(Order.deliveries)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    active_yaml = _ensure_active_yaml_from_delivery(db, order, staff)
    if not active_yaml:
        raise HTTPException(status_code=400, detail="YAMLまたは納品本文がありません")
    payload, delivery = _build_publish_payload(order, active_yaml)
    actor_type, actor_id = _staff_actor(staff)
    view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if not view:
        view = OrderResultView(order_id=order.id)
        db.add(view)
    view.source_yaml_log_id = active_yaml.id
    view.result_payload_json = json.dumps(payload, ensure_ascii=False)
    view.result_html = render_result_html(payload)
    view.horoscope_image_url = payload.get('horoscope_image_url') or None
    view.published_at = datetime.utcnow()
    view.updated_by_type = actor_type
    view.updated_by_id = actor_id
    if delivery and not delivery.is_draft and order.status not in {'delivered','completed'}:
        update_order_status(db, order, to_status='delivered', actor_type=actor_type, actor_id=actor_id, note='result published')
    db.commit()
    return _redirect(f"/staff/orders/{order_code}/astrologer-result")


@router.post("/staff/orders/{order_code}/generate-report")
def staff_generate_report(
    order_code: str,
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).options(selectinload(Order.result_views), selectinload(Order.yaml_logs), selectinload(Order.deliveries)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    active_yaml = _ensure_active_yaml_from_delivery(db, order, staff)
    if not active_yaml:
        raise HTTPException(status_code=400, detail="YAMLまたは納品本文がありません")
    view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if not view or not view.result_payload_json:
        payload, _delivery = _build_publish_payload(order, active_yaml)
        if not view:
            view = OrderResultView(order_id=order.id)
            db.add(view)
        view.source_yaml_log_id = active_yaml.id
        view.result_payload_json = json.dumps(payload, ensure_ascii=False)
        view.result_html = render_result_html(payload)
        view.horoscope_image_url = payload.get('horoscope_image_url') or None
    else:
        payload = json.loads(view.result_payload_json)
    view.report_html = render_report_html(order, payload)
    view.report_generated_at = datetime.utcnow()
    actor_type, actor_id = _staff_actor(staff)
    view.updated_by_type = actor_type
    view.updated_by_id = actor_id
    db.commit()
    return _redirect(f"/staff/orders/{order_code}")


@router.get("/staff/orders/{order_code}/report-download")
def staff_report_download(
    order_code: str,
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).options(selectinload(Order.result_views)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if not view or not view.report_html:
        raise HTTPException(status_code=404, detail="report not found")
    headers = {"Content-Disposition": f'attachment; filename="report-{order.order_code}.html"'}
    return Response(content=view.report_html, media_type='text/html; charset=utf-8', headers=headers)




@router.get("/staff/orders/{order_code}/analysis-result", response_class=HTMLResponse)
def staff_analysis_result(order_code: str, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.result_views), selectinload(Order.deliveries), selectinload(Order.yaml_logs), selectinload(Order.menu)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    result_view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if result_view and result_view.result_html:
        return HTMLResponse(content=result_view.result_html)
    active_yaml = _ensure_active_yaml_from_delivery(db, order, staff)
    if active_yaml:
        payload, _delivery = _build_publish_payload(order, active_yaml)
        return HTMLResponse(content=render_result_html(payload))
    return _redirect(f"/result/{order_code}")


@router.get("/staff/orders/{order_code}/astrologer-result", response_class=HTMLResponse)
def staff_astrologer_result(order_code: str, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.yaml_logs), selectinload(Order.result_views), selectinload(Order.deliveries)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    result_view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    payload = _safe_json_loads(getattr(result_view, 'result_payload_json', None))
    raw = payload.get('raw_json') if isinstance(payload.get('raw_json'), dict) else {}
    structure = {}
    reader_text = ''

    active_yaml = _active_yaml(order)
    if active_yaml:
        summary_data = _safe_json_loads(active_yaml.summary_json)
        if not raw:
            raw = summary_data.get('raw_json') if isinstance(summary_data.get('raw_json'), dict) else {}
        structure = summary_data.get('structure_summary') if isinstance(summary_data.get('structure_summary'), dict) else {}
        reports = summary_data.get('reports') if isinstance(summary_data.get('reports'), dict) else {}
        reader_text = _meaningful_text(reports.get('reader'))

    if not reader_text:
        for sec in (payload.get('sections') or []):
            if isinstance(sec, dict) and (sec.get('heading') == '占い師メモ'):
                reader_text = _meaningful_text(sec.get('body'))
                if reader_text:
                    break

    summary = build_full_astrologer_summary(raw if isinstance(raw, dict) else {}, structure if isinstance(structure, dict) else {})

    # AI本文・各種ログを取り出す
    reports = {}
    if active_yaml:
        summary_data = _safe_json_loads(active_yaml.summary_json)
        reports = summary_data.get('reports') if isinstance(summary_data.get('reports'), dict) else {}
    ai_text = _meaningful_text(reports.get('web')) or ''
    if not ai_text:
        sections = payload.get('sections') or []
        ai_text = '\n\n'.join(
            f"<h3>{s.get('heading','')}</h3>\n{s.get('body','')}"
            for s in sections if isinstance(s, dict) and s.get('body')
        )

    handoff_yaml_full = (active_yaml.yaml_body or '') if active_yaml else ''

    return templates.TemplateResponse(
        request=request,
        name="result.html",
        context={
            "request": request,
            # result.html が必要とする変数をすべて渡す
            "ai_text": ai_text,
            "reader_text": reader_text,
            "raw_reader_text": reader_text,
            "line_text": "",
            "include_reader": bool(reader_text),
            "raw_json": raw if isinstance(raw, dict) else {},
            "inputs_json": {},
            "payload_json": {},
            "structure_summary_json": structure if isinstance(structure, dict) else {},
            "transit_data": None,
            "unknowns": [],
            "bias_guard": None,
            "handoff_json":       "",
            "handoff_yaml":       handoff_yaml_full,
            "handoff_json_full":  "",
            "handoff_yaml_full":  handoff_yaml_full,
            "handoff_json_delta": "",
            "handoff_yaml_delta": "",
            "from_order_code": order_code,
            "order_code": order_code,  # 管理画面へ戻るリンク用
        },
    )

@router.get("/admin", include_in_schema=False)
def admin_root_redirect(staff: dict = Depends(get_current_staff)):
    return _redirect("/dashboard")


@router.get("/reader", include_in_schema=False)
def reader_root_redirect(staff: dict = Depends(get_current_staff)):
    return _redirect("/dashboard")




@router.post("/staff/orders/{order_code}/auto-generate")
def staff_auto_generate(
    order_code: str,
    background_tasks: BackgroundTasks,
    user_name: str = Form(...),
    birth_date: str = Form(...),
    birth_time: str = Form(""),
    birth_prefecture: str = Form(""),
    birth_place: str = Form(""),
    birth_lat: str = Form(""),
    birth_lon: str = Form(""),
    gender: str = Form("female"),
    consultation_text: str = Form(""),
    analysis_type: str = Form("single"),
    astrology_system: str = Form("western"),
    reading_style: str = Form("general"),
    theme: str = Form("overall"),
    generate_ai: str = Form("true"),
    yaml_only: str = Form(""),
    include_reader: str = Form(""),
    include_transit: str = Form(""),
    day_change_at_23: str = Form(""),
    house_system: str = Form("P"),
    node_mode: str = Form("true"),
    lilith_mode: str = Form("mean"),
    include_chiron: str = Form("true"),
    include_lilith: str = Form(""),
    include_vertex: str = Form(""),
    include_asteroids: str = Form(""),
    name_b: str = Form(""),
    birth_date_b: str = Form(""),
    birth_time_b: str = Form(""),
    gender_b: str = Form("female"),
    birth_place_b: str = Form(""),
    prefecture_b: str = Form(""),
    lat_b: str = Form(""),
    lon_b: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).options(selectinload(Order.deliveries)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    try:
        order.birth_date = datetime.strptime((birth_date or '').strip(), '%Y-%m-%d').date()
    except ValueError:
        raise HTTPException(status_code=400, detail='birth_date must be YYYY-MM-DD')

    order.user_name = (user_name or '').strip() or order.user_name
    order.birth_time = (birth_time or '').strip() or None
    order.gender = (gender or '').strip() or None
    order.consultation_text = (consultation_text or '').strip() or None

    pref = (birth_prefecture or '').strip() or None
    place = (birth_place or '').strip() or None
    lat_raw = (birth_lat or '').strip()
    lon_raw = (birth_lon or '').strip()
    if lat_raw or lon_raw:
        try:
            order.birth_lat = float(lat_raw) if lat_raw else None
            order.birth_lon = float(lon_raw) if lon_raw else None
        except ValueError:
            raise HTTPException(status_code=400, detail='lat/lon must be numeric')
        order.birth_prefecture = pref
        order.birth_place = place
        order.location_source = 'manual'
        order.location_note = '案件詳細から手動入力'
    else:
        resolved = resolve_birth_location(pref, place)
        order.birth_prefecture = resolved.get('birth_prefecture')
        order.birth_place = resolved.get('birth_place')
        order.birth_lat = resolved.get('birth_lat')
        order.birth_lon = resolved.get('birth_lon')
        order.location_source = resolved.get('location_source')
        order.location_note = resolved.get('location_note')

    reader = _ensure_reader_for_save(db, order, staff)
    latest = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    delivery = latest[0] if latest else None
    if delivery and delivery.reader_id == reader.id:
        delivery.delivery_text = ''
        delivery.is_draft = True
    elif not delivery:
        db.add(OrderDelivery(order_id=order.id, reader_id=reader.id, delivery_text='', is_draft=True))

    def _is_on(v: str) -> bool:
        return str(v or '').lower() in {'1', 'true', 'on', 'yes'}

    analysis_options = {
        'analysis_type': (analysis_type or 'single').strip(),
        'astrology_system': (astrology_system or 'western').strip(),
        'reading_style': (reading_style or 'general').strip(),
        'theme': (theme or 'overall').strip(),
        'ai_provider': 'claude',
        'generate_ai': _is_on(generate_ai) if generate_ai != '' else True,
        'yaml_only': _is_on(yaml_only),
        'include_reader': _is_on(include_reader),
        'include_transit': _is_on(include_transit),
        'day_change_at_23': _is_on(day_change_at_23),
        'house_system': (house_system or 'P').strip(),
        'node_mode': (node_mode or 'true').strip(),
        'lilith_mode': (lilith_mode or 'mean').strip(),
        'include_chiron': _is_on(include_chiron) if include_chiron != '' else True,
        'include_lilith': _is_on(include_lilith),
        'include_vertex': _is_on(include_vertex),
        'include_asteroids': _is_on(include_asteroids),
        'name_b': (name_b or '').strip() or None,
        'birth_date_b': (birth_date_b or '').strip() or None,
        'birth_time_b': (birth_time_b or '').strip() or None,
        'gender_b': (gender_b or 'female').strip(),
        'birth_place_b': (birth_place_b or '').strip() or None,
        'prefecture_b': (prefecture_b or '').strip() or None,
        'lat_b': (lat_b or '').strip() or None,
        'lon_b': (lon_b or '').strip() or None,
    }

    order.ai_status = 'queued'
    db.commit()
    background_tasks.add_task(process_order_auto_reading, order.id, analysis_options)
    return _redirect(f'/staff/orders/{order_code}')


@router.get("/staff/orders/{order_code}/auto-status")
def staff_order_auto_status(
    order_code: str,
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(
        select(Order)
        .options(selectinload(Order.result_views))
        .where(Order.order_code == order_code)
    )
    if not order:
        return JSONResponse({"status": "not_found", "has_result": False}, status_code=404)

    try:
        result_views = list(getattr(order, 'result_views', []) or [])
    except Exception:
        result_views = []
    latest_view = next(iter(sorted(result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    has_result = bool(latest_view and (getattr(latest_view, 'result_html', None) or getattr(latest_view, 'result_payload_json', None)))

    current_ai_status = order.ai_status or "idle"
    return JSONResponse({
        "status": current_ai_status,
        "ai_status": current_ai_status,
        "has_result": has_result,
        "auto_ai_ready": bool((current_ai_status == "completed") and not has_result),
        "order_code": order.order_code,
    })


@router.get("/staff/orders/{order_code}/analyze")
def staff_order_analyze_redirect(order_code: str, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    params = {
        "name": order.user_name or "",
        "birth_date": order.birth_date.isoformat() if order.birth_date else "",
        "birth_time": order.birth_time or "",
        "prefecture": order.birth_prefecture or "",
        "birth_place": order.birth_place or "",
        "lat": "" if order.birth_lat is None else str(order.birth_lat),
        "lon": "" if order.birth_lon is None else str(order.birth_lon),
        "gender": order.gender or "female",
        "consultation_text": order.consultation_text or "",
        "from_order_code": order.order_code,
    }
    return RedirectResponse(url='/?' + urlencode(params), status_code=303)


@router.post("/staff/orders/{order_code}/location")
def staff_update_location(
    order_code: str,
    birth_prefecture: str = Form(""),
    birth_place: str = Form(""),
    birth_lat: str = Form(""),
    birth_lon: str = Form(""),
    action: str = Form("auto"),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    order = db.scalar(select(Order).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail="order not found")

    pref = (birth_prefecture or "").strip() or None
    place = (birth_place or "").strip() or None

    if action == "manual":
        try:
            lat = float((birth_lat or "").strip()) if (birth_lat or "").strip() else None
            lon = float((birth_lon or "").strip()) if (birth_lon or "").strip() else None
        except ValueError:
            raise HTTPException(status_code=400, detail="lat/lon must be numeric")
        order.birth_prefecture = pref
        order.birth_place = place
        order.birth_lat = lat
        order.birth_lon = lon
        order.location_source = "manual"
        order.location_note = "スタッフ手動修正"
    else:
        resolved = resolve_birth_location(pref, place)
        order.birth_prefecture = resolved.get("birth_prefecture")
        order.birth_place = resolved.get("birth_place")
        order.birth_lat = resolved.get("birth_lat")
        order.birth_lon = resolved.get("birth_lon")
        order.location_source = resolved.get("location_source")
        order.location_note = resolved.get("location_note")

    actor_type, actor_id = _staff_actor(staff)
    yaml_creator_id = actor_id if actor_type == "reader" else None
    db.add(
        YamlLog(
            order_id=order.id,
            customer_id=order.customer_id,
            created_by_type=actor_type,
            created_by_id=yaml_creator_id,
            yaml_body=f"location_update: {order.birth_prefecture or '-'} / {order.birth_place or '-'} / {order.birth_lat or '-'} / {order.birth_lon or '-'}",
            summary_json=json.dumps({"location_source": order.location_source, "location_note": order.location_note}, ensure_ascii=False),
        )
    )
    db.commit()
    return _redirect(f"/staff/orders/{order_code}")
