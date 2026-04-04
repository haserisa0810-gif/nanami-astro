from __future__ import annotations

from datetime import date, datetime
import json

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from db import get_db
from models import Menu, Order, PaymentTransaction
from services.free_reading_service import FREE_RESULT_FOOTER, ensure_unique_free_reading_code, process_free_reading
from services.order_service import create_order, get_or_create_customer
from services.location import PREFECTURE_OPTIONS, resolve_birth_location

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _normalize_free_link_code(text: str | None) -> str | None:
    value = (text or '').strip().upper()
    if not value:
        return None
    import re
    m = re.search(r'(F-[A-Z0-9-]+|A[A-Z0-9]{6,})', value)
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


COURSE_SLUG_PRICE_MAP = {
    "light": 3000,
    "standard": 5000,
    "premium": 10000,
}


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


@router.get("/menu", response_class=HTMLResponse)
def menu_page(
    request: Request,
    db: Session = Depends(get_db),
    free_reading_code: str | None = None,
    menu_id: int | None = None,
    course: str | None = None,
    payment_order_ref: str | None = None,
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
            "initial_course": (course or '').strip().lower(),
            "initial_payment_order_ref": (payment_order_ref or '').strip(),
            "initial_line_user_id": (line_user_id or '').strip(),
            "initial_line_name": (line_name or '').strip(),
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
    line_user_id: str | None = None,
    line_name: str | None = None,
):
    normalized_course = (course or '').strip().lower()
    if normalized_course not in COURSE_SLUG_PRICE_MAP:
        raise HTTPException(status_code=404, detail="course not found")
    return menu_page(
        request,
        db,
        free_reading_code=free_reading_code,
        course=normalized_course,
        payment_order_ref=payment_order_ref,
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
    line_user_id: str | None = Form(None),
    line_name: str | None = Form(None),
    course: str | None = Form(None),
    db: Session = Depends(get_db),
):
    menu = db.get(Menu, menu_id)
    if not menu or not menu.is_active:
        raise HTTPException(status_code=404, detail="menu not found")

    normalized_contact = (user_contact or '').strip()
    normalized_payment_order_ref = (payment_order_ref or '').strip().upper()

    def render_form_error(message: str, status_code: int = 400):
        menus = db.scalars(select(Menu).where(Menu.is_active == True).order_by(Menu.price.asc())).all()
        submitted_course = (course or '').strip().lower()
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
                "initial_payment_order_ref": normalized_payment_order_ref,
                "initial_line_user_id": (line_user_id or '').strip(),
                "initial_line_name": (line_name or '').strip(),
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
                    "payment_order_ref": normalized_payment_order_ref,
                    "free_reading_code": free_reading_code,
                    "line_user_id": (line_user_id or '').strip(),
                    "line_name": (line_name or '').strip(),
                    "course": submitted_course,
                },
            },
            status_code=status_code,
        )

    if not normalized_payment_order_ref:
        return render_form_error("注文番号が未入力です。ご購入完了メールまたは購入履歴の注文番号を入力してください。")

    duplicate = db.scalar(
        select(Order).where(
            Order.external_platform == 'stores',
            Order.external_order_ref == normalized_payment_order_ref,
        )
    )
    if duplicate:
        return render_form_error(f"この注文番号はすでに受付済みです。受付番号は {duplicate.order_code} です。", status_code=409)

    if not normalized_contact:
        return render_form_error("ホームページからのお申込みは連絡先メールアドレスが必須です。")
    if '@' not in normalized_contact:
        return render_form_error("正しいメールアドレスを入力してください。")
    try:
        birth_date_obj = date.fromisoformat(birth_date)
    except ValueError:
        return render_form_error("生年月日の形式が正しくありません。")

    location = resolve_birth_location((birth_prefecture or '').strip() or None, (birth_place or '').strip() or None)
    normalized_line_user_id = (line_user_id or '').strip() or None
    normalized_line_name = (line_name or '').strip() or None
    customer = get_or_create_customer(
        db,
        display_name=(normalized_line_name or user_name.strip()),
        line_user_id=normalized_line_user_id,
        email=normalized_contact,
    )
    free_reading_code = (free_reading_code or '').strip().upper() or None
    source_free_order = _find_source_free_order(db, free_reading_code)
    order = create_order(
        db,
        menu=menu,
        user_name=user_name.strip(),
        user_contact=normalized_contact or None,
        birth_date=birth_date_obj,
        birth_time=(birth_time or '').strip() or None,
        birth_prefecture=location.get('birth_prefecture'),
        birth_place=location.get('birth_place'),
        birth_lat=location.get('birth_lat'),
        birth_lon=location.get('birth_lon'),
        location_source=location.get('location_source'),
        location_note=location.get('location_note'),
        gender=(gender or '').strip() or None,
        consultation_text=(consultation_text or '').strip() or None,
        customer=customer,
        source='self',
        external_platform='stores',
        external_order_ref=normalized_payment_order_ref,
        status='paid',
        inputs_json=json.dumps({
            'user_name': user_name,
            'user_contact': normalized_contact,
            'birth_date': birth_date,
            'birth_time': birth_time,
            'birth_prefecture': birth_prefecture,
            'birth_place': birth_place,
            'birth_lat': location.get('birth_lat'),
            'birth_lon': location.get('birth_lon'),
            'location_source': location.get('location_source'),
            'gender': gender,
            'consultation_text': consultation_text,
            'menu_id': menu_id,
            'payment_order_ref': normalized_payment_order_ref,
            'line_user_id': normalized_line_user_id,
            'line_name': normalized_line_name,
        }, ensure_ascii=False),
    )
    if source_free_order:
        order.source_free_order_id = source_free_order.id
    db.add(
        PaymentTransaction(
            order_id=order.id,
            provider='base',
            provider_payment_id=normalized_payment_order_ref,
            provider_session_id=normalized_payment_order_ref,
            amount=order.price,
            currency='jpy',
            status='paid',
            paid_at=datetime.utcnow(),
            raw_event_json=json.dumps({'provider': 'stores', 'payment_order_ref': normalized_payment_order_ref}, ensure_ascii=False),
        )
    )
    db.commit()
    return RedirectResponse(url=f"/order/confirm?order_code={order.order_code}", status_code=303)


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
    normalized_contact = (user_contact or '').strip()

    try:
        birth_date_obj = date.fromisoformat(birth_date)
    except ValueError:
        return templates.TemplateResponse(request=request, name="free_start.html", context={"request": request, "error": "生年月日の形式が正しくありません。", "prefecture_options": PREFECTURE_OPTIONS}, status_code=400)

    menu = db.scalar(select(Menu).where(Menu.name == '無料鑑定'))
    if not menu:
        raise HTTPException(status_code=500, detail='無料鑑定メニューが見つかりません')

    customer = None
    location = resolve_birth_location((birth_prefecture or '').strip() or None, (birth_place or '').strip() or None)
    if user_contact:
        customer = get_or_create_customer(db, display_name=user_name.strip(), email=user_contact.strip() if "@" in user_contact else None)

    order = create_order(
        db,
        menu=menu,
        user_name=user_name.strip(),
        user_contact=normalized_contact or None,
        birth_date=birth_date_obj,
        birth_time=(birth_time or '').strip() or None,
        birth_prefecture=location.get('birth_prefecture'),
        birth_place=location.get('birth_place'),
        birth_lat=location.get('birth_lat'),
        birth_lon=location.get('birth_lon'),
        location_source=location.get('location_source'),
        location_note=location.get('location_note'),
        gender=(gender or '').strip() or None,
        consultation_text=(consultation_text or '').strip() or None,
        customer=customer,
        source='self',
        status='received',
        inputs_json=json.dumps({
            'user_name': user_name,
            'user_contact': normalized_contact,
            'birth_date': birth_date,
            'birth_time': birth_time,
            'birth_prefecture': birth_prefecture,
            'birth_place': birth_place,
            'gender': gender,
            'consultation_text': consultation_text,
            'menu_id': menu.id,
        }, ensure_ascii=False),
    )
    order.order_kind = 'free'
    order.price = 0
    order.free_reading_code = ensure_unique_free_reading_code(db)
    order.ai_status = 'queued'
    db.commit()
    background_tasks.add_task(process_free_reading, order.id)
    return RedirectResponse(url=f"/free-reading/{order.order_code}/wait", status_code=303)


@router.get("/free-reading/{order_code}/wait", response_class=HTMLResponse)
def free_reading_wait(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).where(Order.order_code == order_code, Order.order_kind == 'free'))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    return templates.TemplateResponse(request=request, name="free_wait.html", context={"request": request, "order": order})


@router.get("/free-reading/{order_code}/status")
def free_reading_status(order_code: str, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).where(Order.order_code == order_code, Order.order_kind == 'free'))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    return {
        'order_code': order.order_code,
        'free_reading_code': order.free_reading_code,
        'status': order.ai_status or 'queued',
        'result_url': f'/free-reading/{order.order_code}/result' if (order.ai_status == 'completed') else None,
    }


@router.get("/free-reading/{order_code}/result", response_class=HTMLResponse)
def free_reading_result(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.yaml_logs), selectinload(Order.result_views)).where(Order.order_code == order_code, Order.order_kind == 'free'))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    result_payload = None
    if order.result_payload_json:
        try:
            result_payload = json.loads(order.result_payload_json)
        except Exception:
            result_payload = None
    yaml_log = sorted(order.yaml_logs, key=lambda x: x.updated_at or x.created_at, reverse=True)
    latest_yaml = yaml_log[0] if yaml_log else None
    return templates.TemplateResponse(request=request, name="free_result.html", context={"request": request, "order": order, "result_payload": result_payload, "yaml_log": latest_yaml, "footer_message": FREE_RESULT_FOOTER})


@router.get("/order/confirm", response_class=HTMLResponse)
def order_confirm(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.menu)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    return templates.TemplateResponse(request=request, name="order_confirm.html", context={"request": request, "order": order})

@router.get("/result/{order_code}", response_class=HTMLResponse)
def order_result(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.deliveries), selectinload(Order.menu), selectinload(Order.yaml_logs), selectinload(Order.result_views)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    latest_delivery = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
    delivery = latest_delivery[0] if latest_delivery else None
    yaml_log = sorted(order.yaml_logs, key=lambda x: x.updated_at or x.created_at, reverse=True)
    latest_yaml = yaml_log[0] if yaml_log else None
    result_view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    result_payload = None
    raw_payload = (result_view.result_payload_json if result_view and result_view.result_payload_json else order.result_payload_json)
    if raw_payload:
        try:
            result_payload = json.loads(raw_payload)
        except Exception:
            result_payload = None
    return templates.TemplateResponse(request=request, name="order_result.html", context={"request": request, "order": order, "delivery": delivery, 'yaml_log': latest_yaml, 'result_view': result_view, 'result_payload': result_payload})


@router.get("/report/{order_code}", response_class=HTMLResponse)
def order_report(order_code: str, request: Request, db: Session = Depends(get_db)):
    order = db.scalar(select(Order).options(selectinload(Order.result_views), selectinload(Order.deliveries), selectinload(Order.yaml_logs), selectinload(Order.menu)).where(Order.order_code == order_code))
    if not order:
        raise HTTPException(status_code=404, detail='order not found')
    result_view = next(iter(sorted(order.result_views, key=lambda x: x.updated_at or x.created_at, reverse=True)), None)
    if result_view and result_view.report_html:
        return HTMLResponse(content=result_view.report_html)
    return order_result(order_code=order_code, request=request, db=db)
