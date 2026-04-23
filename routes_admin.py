from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from auth import clear_admin_login, get_current_admin, hash_password, set_admin_login, verify_password
from db import get_db
from models import AdminUser, Astrologer, AuditLog, Customer, Menu, Order, Payout, YamlLog
from services.app_settings import get_line_bot_settings, set_setting
from services.notification_service import notify_line_delivery
from services.order_service import update_order_status

router = APIRouter()
templates = Jinja2Templates(directory="templates")
VALID_STATUSES = ["draft", "received", "pending_payment", "paid", "assigned", "in_progress", "delivered", "completed", "cancelled", "refund_requested", "refunded", "payment_failed", "expired"]


def _parse_date_param(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _order_filters_from_request(request: Request) -> dict[str, str]:
    params = request.query_params
    return {
        "q": (params.get("q") or "").strip(),
        "status": (params.get("status") or "").strip(),
        "reader_id": (params.get("reader_id") or "").strip(),
        "menu_id": (params.get("menu_id") or "").strip(),
        "order_kind": (params.get("order_kind") or "").strip(),
        "source": (params.get("source") or "").strip(),
        "ai_status": (params.get("ai_status") or "").strip(),
        "created_from": (params.get("created_from") or "").strip(),
        "created_to": (params.get("created_to") or "").strip(),
    }


def _apply_order_filters(stmt, filters: dict[str, str]):
    q = filters.get("q") or ""
    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            Order.order_code.ilike(like)
            | Order.user_name.ilike(like)
            | Order.user_contact.ilike(like)
            | Order.free_reading_code.ilike(like)
            | Order.external_order_ref.ilike(like)
        )

    status = filters.get("status") or ""
    if status:
        stmt = stmt.where(Order.status == status)

    reader_id = filters.get("reader_id") or ""
    if reader_id.isdigit():
        stmt = stmt.where(Order.assigned_reader_id == int(reader_id))

    menu_id = filters.get("menu_id") or ""
    if menu_id.isdigit():
        stmt = stmt.where(Order.menu_id == int(menu_id))

    order_kind = filters.get("order_kind") or ""
    if order_kind in {"free", "paid"}:
        stmt = stmt.where(Order.order_kind == order_kind)

    source = (filters.get("source") or "").lower()
    if source:
        stmt = stmt.where(func.lower(Order.source) == source)

    ai_status = filters.get("ai_status") or ""
    if ai_status:
        if ai_status == "(blank)":
            stmt = stmt.where((Order.ai_status.is_(None)) | (Order.ai_status == ""))
        else:
            stmt = stmt.where(Order.ai_status == ai_status)

    created_from = _parse_date_param(filters.get("created_from"))
    if created_from:
        stmt = stmt.where(Order.created_at >= datetime.combine(created_from, time.min))

    created_to = _parse_date_param(filters.get("created_to"))
    if created_to:
        stmt = stmt.where(Order.created_at < datetime.combine(created_to + timedelta(days=1), time.min))

    return stmt
STATUS_LABELS = {
    "draft": "下書き",
    "received": "受付済み",
    "pending_payment": "未決済",
    "paid": "決済済み",
    "assigned": "担当割当済み",
    "in_progress": "鑑定中",
    "delivered": "納品済み",
    "completed": "完了",
    "cancelled": "キャンセル",
    "refund_requested": "返金申請中",
    "refunded": "返金済み",
    "payment_failed": "決済失敗",
    "expired": "期限切れ",
}


def _safe_scalar_count(db: Session, stmt, default: int = 0) -> int:
    try:
        return int(db.scalar(stmt) or 0)
    except Exception as exc:
        print("admin count query failed:", repr(exc))
        return default



def _clean_email(value: str) -> str:
    return (value or "").strip().lower()


def _clean_display_name(value: str) -> str:
    return (value or "").strip()


def _resolve_admin_login(db: Session, login_input: str) -> AdminUser | None:
    normalized = (login_input or '').strip().lower()
    if not normalized:
        return None

    admin = db.scalar(select(AdminUser).where(AdminUser.login_email == normalized))
    if admin:
        return admin

    alias = normalized if '@' not in normalized else normalized.split('@', 1)[0]
    users = db.scalars(select(AdminUser)).all()
    for user in users:
        email = (user.login_email or '').strip().lower()
        if email and email.split('@', 1)[0] == alias:
            return user
    return None


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)




def _run_async_notification(coro) -> None:
    try:
        asyncio.run(coro)
    except RuntimeError:
        print('Notification skipped: existing event loop')
    except Exception as exc:
        print('Notification error:', repr(exc))

def _audit(db: Session, *, actor_type: str, actor_id: int | None, action: str, target_type: str, target_id: int | None, before: dict | None = None, after: dict | None = None):
    db.add(AuditLog(actor_type=actor_type, actor_id=actor_id, action=action, target_type=target_type, target_id=target_id, before_json=json.dumps(before, ensure_ascii=False) if before is not None else None, after_json=json.dumps(after, ensure_ascii=False) if after is not None else None))


def _is_truthy(value: str | None) -> bool:
    return str(value or '').lower() in {'1', 'true', 'on', 'yes', 'active'}


def _has_open_orders_for_reader(db: Session, reader_id: int) -> bool:
    open_statuses = {'paid', 'assigned', 'in_progress'}
    count = db.scalar(select(func.count()).select_from(Order).where(Order.assigned_reader_id == reader_id, Order.status.in_(open_statuses))) or 0
    return int(count) > 0


@router.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    return templates.TemplateResponse(request=request, name="admin_login.html", context={"request": request, "error": None})


@router.post("/admin/login")
def admin_login(response: Response, request: Request, login_email: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    admin = _resolve_admin_login(db, login_email)
    if not admin or not verify_password(password, admin.password_hash):
        return templates.TemplateResponse(request=request, name="admin_login.html", context={"request": request, "error": "ログインID / メールアドレス またはパスワードが違います。"}, status_code=400)
    if not admin.is_active:
        return templates.TemplateResponse(request=request, name="admin_login.html", context={"request": request, "error": "このアカウントは現在利用できません。"}, status_code=400)
    target = '/admin/account?force_password=1' if getattr(admin, 'is_temp_password', False) else '/admin/dashboard'
    redirect = RedirectResponse(url=target, status_code=303)
    set_admin_login(redirect, admin)
    return redirect


@router.get("/admin/logout")
def admin_logout():
    response = RedirectResponse(url="/admin/login", status_code=303)
    clear_admin_login(response)
    return response


@router.get("/admin/dashboard", response_class=HTMLResponse)
def admin_dashboard(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    metrics = {
        'today_orders': _safe_scalar_count(db, select(func.count()).select_from(Order)),
        'received': _safe_scalar_count(db, select(func.count()).select_from(Order).where(Order.status == 'received')),
        'pending_payment': _safe_scalar_count(db, select(func.count()).select_from(Order).where(Order.status == 'pending_payment')),
        'paid': _safe_scalar_count(db, select(func.count()).select_from(Order).where(Order.status == 'paid')),
        'undelivered': _safe_scalar_count(db, select(func.count()).select_from(Order).where(Order.status.in_(['received','paid','assigned','in_progress']))),
        'customers': _safe_scalar_count(db, select(func.count()).select_from(Customer)),
        'yaml_logs': _safe_scalar_count(db, select(func.count()).select_from(YamlLog)),
    }
    recent_orders = db.scalars(select(Order).options(selectinload(Order.menu), selectinload(Order.assigned_reader)).order_by(Order.created_at.desc()).limit(10)).all()
    line_settings = get_line_bot_settings(db)
    return templates.TemplateResponse(request=request, name="admin_dashboard.html", context={"request": request, "admin": admin, "metrics": metrics, "recent_orders": recent_orders, "status_labels": STATUS_LABELS, "line_settings": line_settings, "success": request.query_params.get("success")})


@router.post('/admin/settings/line')
def admin_line_settings_update(
    request: Request,
    line_bot_enabled: str = Form('false'),
    line_order_accepting: str = Form('false'),
    line_bot_mode: str = Form('order'),
    admin: AdminUser = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    before = get_line_bot_settings(db)
    normalized_mode = (line_bot_mode or 'order').strip().lower()
    if normalized_mode not in {'order', 'fortune', 'off'}:
        normalized_mode = 'order'
    set_setting(db, 'line_bot_enabled', 'true' if str(line_bot_enabled).lower() in {'1','true','on','yes'} else 'false')
    set_setting(db, 'line_order_accepting', 'true' if str(line_order_accepting).lower() in {'1','true','on','yes'} else 'false')
    set_setting(db, 'line_bot_mode', normalized_mode)
    after = get_line_bot_settings(db)
    _audit(db, actor_type='admin', actor_id=admin.id, action='update_line_settings', target_type='app_setting', target_id=None, before=before, after=after)
    db.commit()
    return _redirect('/admin/dashboard?success=line_settings_saved')


@router.get("/admin/orders", response_class=HTMLResponse)
def admin_orders(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    filters = _order_filters_from_request(request)
    stmt = select(Order).options(selectinload(Order.menu), selectinload(Order.assigned_reader), selectinload(Order.customer), selectinload(Order.source_free_order)).order_by(Order.created_at.desc())
    stmt = _apply_order_filters(stmt, filters)
    orders = db.scalars(stmt).all()
    readers = db.scalars(select(Astrologer).order_by(Astrologer.display_name.asc())).all()
    menus = db.scalars(select(Menu).order_by(Menu.price.asc(), Menu.id.asc())).all()
    source_rows = db.scalars(select(Order.source).distinct().order_by(Order.source.asc())).all()
    ai_rows = db.scalars(select(Order.ai_status).distinct().order_by(Order.ai_status.asc())).all()
    return templates.TemplateResponse(request=request, name="admin_orders.html", context={
        "request": request,
        "admin": admin,
        "orders": orders,
        "status_labels": STATUS_LABELS,
        "filters": filters,
        "filter_readers": readers,
        "filter_menus": menus,
        "filter_sources": [src for src in source_rows if src],
        "filter_ai_statuses": [status for status in ai_rows if status],
        "order_count": len(orders),
    })


@router.get("/admin/orders/{order_code}", response_class=HTMLResponse)
def admin_order_detail(order_code: str, request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.menu), selectinload(Order.assigned_reader), selectinload(Order.deliveries), selectinload(Order.status_logs), selectinload(Order.customer), selectinload(Order.yaml_logs), selectinload(Order.payment_transactions), selectinload(Order.source_free_order)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    readers = db.scalars(select(Astrologer).order_by(Astrologer.display_name.asc())).all()
    payouts = db.scalars(select(Payout).where(Payout.order_id == order.id).order_by(Payout.created_at.desc())).all()
    return templates.TemplateResponse(request=request, name='admin_order_detail.html', context={'request': request, 'admin': admin, 'order': order, 'readers': readers, 'valid_statuses': VALID_STATUSES, 'status_labels': STATUS_LABELS, 'payouts': payouts})


@router.post("/admin/orders/{order_code}/update")
def admin_order_update(order_code: str, request: Request, assigned_reader_id: str | None = Form(None), status: str = Form(...), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.assigned_reader)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    before = {'assigned_reader_id': order.assigned_reader_id, 'status': order.status}
    if assigned_reader_id:
        reader = db.get(Astrologer, int(assigned_reader_id))
        if not reader:
            raise HTTPException(status_code=404, detail='reader not found')
        order.assigned_reader_id = reader.id
        if order.status == 'paid':
            update_order_status(db, order, to_status='assigned', actor_type='admin', actor_id=admin.id, note='reader assigned by admin')
    else:
        order.assigned_reader_id = None
    if status not in VALID_STATUSES:
        raise HTTPException(status_code=400, detail='invalid status')
    if order.status != status:
        update_order_status(db, order, to_status=status, actor_type='admin', actor_id=admin.id, note='status updated by admin')
    _audit(db, actor_type='admin', actor_id=admin.id, action='update_order', target_type='order', target_id=order.id, before=before, after={'assigned_reader_id': order.assigned_reader_id, 'status': order.status})
    db.commit()
    return RedirectResponse(url=f'/admin/orders/{order_code}', status_code=303)


@router.post('/admin/orders/{order_code}/notify-line')
def admin_order_notify_line(order_code: str, mode: str = Form('delivery'), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.deliveries), selectinload(Order.customer), selectinload(Order.result_views), selectinload(Order.menu)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    normalized_mode = 'auto' if (mode or '').strip().lower() == 'auto' else 'delivery'
    _run_async_notification(notify_line_delivery(order, mode=normalized_mode))
    return _redirect(f'/admin/orders/{order_code}?success=line_sent')


@router.get('/admin/payouts', response_class=HTMLResponse)
def admin_payouts(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    payouts = db.scalars(select(Payout).options(selectinload(Payout.reader), selectinload(Payout.order)).order_by(Payout.created_at.desc())).all()
    total_reader = sum(int(p.reader_amount) for p in payouts)
    total_platform = sum(int(p.platform_amount) for p in payouts)
    return templates.TemplateResponse(request=request, name='admin_payouts.html', context={'request': request, 'admin': admin, 'payouts': payouts, 'total_reader': total_reader, 'total_platform': total_platform})


@router.get('/admin/astrologers', response_class=HTMLResponse)
def admin_astrologers(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    readers = db.scalars(select(Astrologer).order_by(Astrologer.display_name.asc())).all()
    return templates.TemplateResponse(request=request, name='admin_astrologers.html', context={'request': request, 'admin': admin, 'readers': readers, 'error': request.query_params.get('error'), 'success': request.query_params.get('success')})


@router.post('/admin/astrologers')
def admin_astrologers_create(request: Request, display_name: str = Form(...), login_email: str = Form(...), password: str = Form(...), commission_rate: float = Form(60.0), status: str = Form('active'), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    display_name = _clean_display_name(display_name)
    login_email = _clean_email(login_email)
    if not display_name or not login_email or not password:
        readers = db.scalars(select(Astrologer).order_by(Astrologer.display_name.asc())).all()
        return templates.TemplateResponse(request=request, name='admin_astrologers.html', context={'request': request, 'admin': admin, 'readers': readers, 'error': '表示名・メールアドレス・パスワードは必須です。', 'success': None}, status_code=400)
    if db.scalar(select(Astrologer).where(Astrologer.login_email == login_email)):
        readers = db.scalars(select(Astrologer).order_by(Astrologer.display_name.asc())).all()
        return templates.TemplateResponse(request=request, name='admin_astrologers.html', context={'request': request, 'admin': admin, 'readers': readers, 'error': '同じメールアドレスの占い師が既にいます。', 'success': None}, status_code=400)
    reader = Astrologer(display_name=display_name, login_email=login_email, password_hash=hash_password(password), is_temp_password=True, commission_rate=commission_rate, status=status)
    db.add(reader)
    db.flush()
    _audit(db, actor_type='admin', actor_id=admin.id, action='create_reader', target_type='astrologer', target_id=reader.id, after={'display_name': reader.display_name, 'login_email': reader.login_email, 'commission_rate': float(reader.commission_rate), 'status': reader.status})
    db.commit()
    return _redirect('/admin/astrologers?success=created')


@router.post('/admin/astrologers/{reader_id}/update')
def admin_astrologers_update(reader_id: int, request: Request, display_name: str = Form(...), login_email: str = Form(...), commission_rate: float = Form(...), status: str = Form(...), password: str = Form(''), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    reader = db.get(Astrologer, reader_id)
    if not reader:
        raise HTTPException(status_code=404, detail='reader not found')
    login_email = _clean_email(login_email)
    display_name = _clean_display_name(display_name)
    if not display_name or not login_email:
        raise HTTPException(status_code=400, detail='display_name and login_email required')
    existing = db.scalar(select(Astrologer).where(Astrologer.login_email == login_email, Astrologer.id != reader_id))
    if existing:
        raise HTTPException(status_code=400, detail='email already used')
    before = {'display_name': reader.display_name, 'login_email': reader.login_email, 'commission_rate': float(reader.commission_rate), 'status': reader.status}
    reader.display_name = display_name
    reader.login_email = login_email
    reader.commission_rate = commission_rate
    reader.status = status
    if password.strip():
        reader.password_hash = hash_password(password.strip())
        reader.is_temp_password = True
    _audit(db, actor_type='admin', actor_id=admin.id, action='update_reader', target_type='astrologer', target_id=reader.id, before=before, after={'display_name': reader.display_name, 'login_email': reader.login_email, 'commission_rate': float(reader.commission_rate), 'status': reader.status})
    db.commit()
    return _redirect('/admin/astrologers?success=updated')


@router.get('/admin/users', response_class=HTMLResponse)
def admin_users(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    users = db.scalars(select(AdminUser).order_by(AdminUser.display_name.asc())).all()
    return templates.TemplateResponse(request=request, name='admin_users.html', context={'request': request, 'admin': admin, 'users': users, 'error': request.query_params.get('error'), 'success': request.query_params.get('success')})


@router.post('/admin/users')
def admin_users_create(request: Request, display_name: str = Form(...), login_email: str = Form(...), password: str = Form(...), is_active: str = Form('true'), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    display_name = _clean_display_name(display_name)
    login_email = _clean_email(login_email)
    if not display_name or not login_email or not password:
        users = db.scalars(select(AdminUser).order_by(AdminUser.display_name.asc())).all()
        return templates.TemplateResponse(request=request, name='admin_users.html', context={'request': request, 'admin': admin, 'users': users, 'error': '表示名・メールアドレス・パスワードは必須です。', 'success': None}, status_code=400)
    if db.scalar(select(AdminUser).where(AdminUser.login_email == login_email)):
        users = db.scalars(select(AdminUser).order_by(AdminUser.display_name.asc())).all()
        return templates.TemplateResponse(request=request, name='admin_users.html', context={'request': request, 'admin': admin, 'users': users, 'error': '同じメールアドレスの管理者が既にいます。', 'success': None}, status_code=400)
    active_flag = str(is_active).lower() in {'1','true','on','yes','active'}
    user = AdminUser(display_name=display_name, login_email=login_email, password_hash=hash_password(password), is_active=active_flag, is_temp_password=True)
    db.add(user)
    db.flush()
    _audit(db, actor_type='admin', actor_id=admin.id, action='create_admin_user', target_type='admin_user', target_id=user.id, after={'display_name': user.display_name, 'login_email': user.login_email, 'is_active': user.is_active})
    db.commit()
    return _redirect('/admin/users?success=created')


@router.post('/admin/users/{user_id}/update')
def admin_users_update(user_id: int, request: Request, display_name: str = Form(...), login_email: str = Form(...), password: str = Form(''), is_active: str = Form('false'), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    user = db.get(AdminUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail='admin user not found')
    login_email = _clean_email(login_email)
    display_name = _clean_display_name(display_name)
    existing = db.scalar(select(AdminUser).where(AdminUser.login_email == login_email, AdminUser.id != user_id))
    if existing:
        raise HTTPException(status_code=400, detail='email already used')
    before = {'display_name': user.display_name, 'login_email': user.login_email, 'is_active': user.is_active}
    user.display_name = display_name
    user.login_email = login_email
    user.is_active = str(is_active).lower() in {'1','true','on','yes','active'}
    if password.strip():
        user.password_hash = hash_password(password.strip())
        user.is_temp_password = True
    _audit(db, actor_type='admin', actor_id=admin.id, action='update_admin_user', target_type='admin_user', target_id=user.id, before=before, after={'display_name': user.display_name, 'login_email': user.login_email, 'is_active': user.is_active})
    db.commit()
    return _redirect('/admin/users?success=updated')


@router.post('/admin/astrologers/{reader_id}/delete')
def admin_astrologers_delete(reader_id: int, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    reader = db.get(Astrologer, reader_id)
    if not reader:
        raise HTTPException(status_code=404, detail='reader not found')
    if _has_open_orders_for_reader(db, reader.id):
        return _redirect('/admin/astrologers?error=reader_has_open_orders')
    before = {'display_name': reader.display_name, 'login_email': reader.login_email, 'status': reader.status}
    db.delete(reader)
    _audit(db, actor_type='admin', actor_id=admin.id, action='delete_reader', target_type='astrologer', target_id=reader_id, before=before, after=None)
    db.commit()
    return _redirect('/admin/astrologers?success=deleted')




@router.post('/admin/users/{user_id}/delete')
def admin_users_delete(user_id: int, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    user = db.get(AdminUser, user_id)
    if not user:
        raise HTTPException(status_code=404, detail='admin user not found')
    active_count = db.scalar(select(func.count()).select_from(AdminUser).where(AdminUser.is_active == True)) or 0
    if user.id == admin.id:
        return _redirect('/admin/users?error=cannot_delete_self')
    if user.is_active and int(active_count) <= 1:
        return _redirect('/admin/users?error=last_active_admin')
    before = {'display_name': user.display_name, 'login_email': user.login_email, 'is_active': user.is_active}
    db.delete(user)
    _audit(db, actor_type='admin', actor_id=admin.id, action='delete_admin_user', target_type='admin_user', target_id=user_id, before=before, after=None)
    db.commit()
    return _redirect('/admin/users?success=deleted')


@router.get('/admin/account', response_class=HTMLResponse)
def admin_account(request: Request, admin: AdminUser = Depends(get_current_admin)):
    return templates.TemplateResponse(request=request, name='admin_account.html', context={'request': request, 'admin': admin, 'error': request.query_params.get('error'), 'success': request.query_params.get('success'), 'force_password': request.query_params.get('force_password')})


@router.post('/admin/account/password')
def admin_account_password_update(current_password: str = Form(''), new_password: str = Form(...), new_password_confirm: str = Form(...), admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    current_password = (current_password or '').strip()
    new_password = (new_password or '').strip()
    new_password_confirm = (new_password_confirm or '').strip()
    if len(new_password) < 8:
        return _redirect('/admin/account?error=password_too_short')
    if new_password != new_password_confirm:
        return _redirect('/admin/account?error=password_mismatch')
    if not admin.is_temp_password and not verify_password(current_password, admin.password_hash):
        return _redirect('/admin/account?error=current_password_invalid')
    admin.password_hash = hash_password(new_password)
    admin.is_temp_password = False
    _audit(db, actor_type='admin', actor_id=admin.id, action='change_admin_password', target_type='admin_user', target_id=admin.id, before=None, after={'is_temp_password': False})
    db.commit()
    return _redirect('/admin/account?success=password_updated')


@router.get('/admin/customers', response_class=HTMLResponse)
def admin_customers(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    customers = db.scalars(select(Customer).order_by(Customer.updated_at.desc())).all()
    return templates.TemplateResponse(request=request, name='admin_customers.html', context={'request': request, 'admin': admin, 'customers': customers})


@router.get('/admin/customers/{customer_id}', response_class=HTMLResponse)
def admin_customer_detail(customer_id: int, request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    customer = db.scalar(select(Customer).options(selectinload(Customer.orders), selectinload(Customer.yaml_logs)).where(Customer.id == customer_id))
    if not customer:
        raise HTTPException(status_code=404, detail='customer not found')
    orders = db.scalars(select(Order).options(selectinload(Order.menu), selectinload(Order.assigned_reader)).where(Order.customer_id == customer.id).order_by(Order.created_at.desc())).all()
    logs = db.scalars(select(YamlLog).where(YamlLog.customer_id == customer.id).order_by(YamlLog.created_at.desc()).limit(50)).all()
    return templates.TemplateResponse(request=request, name='admin_customer_detail.html', context={'request': request, 'admin': admin, 'customer': customer, 'orders': orders, 'logs': logs})


@router.get('/admin/yaml-logs', response_class=HTMLResponse)
def admin_yaml_logs(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    logs = db.scalars(select(YamlLog).options(selectinload(YamlLog.order), selectinload(YamlLog.customer)).order_by(YamlLog.created_at.desc()).limit(200)).all()
    return templates.TemplateResponse(request=request, name='admin_yaml_logs.html', context={'request': request, 'admin': admin, 'logs': logs})


@router.get('/admin/audit-logs', response_class=HTMLResponse)
def admin_audit_logs(request: Request, admin: AdminUser = Depends(get_current_admin), db: Session = Depends(get_db)):
    logs = db.scalars(select(AuditLog).order_by(AuditLog.created_at.desc()).limit(200)).all()
    return templates.TemplateResponse(request=request, name='admin_audit_logs.html', context={'request': request, 'admin': admin, 'logs': logs})


@router.post('/admin/maintenance/cleanup-drafts')
def admin_cleanup_drafts(
    dry_run: str = Form('1'),
    admin: AdminUser = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    dry = str(dry_run or '1').strip() != '0'
    result = cleanup_expired_drafts(db, dry_run=dry)
    if not dry:
        db.commit()
    return result


@router.post('/admin/maintenance/cleanup-orphan-reports')
def admin_cleanup_orphan_reports(
    dry_run: str = Form('1'),
    admin: AdminUser = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    dry = str(dry_run or '1').strip() != '0'
    result = cleanup_orphan_reports(db, dry_run=dry)
    if not dry:
        db.commit()
    return result
