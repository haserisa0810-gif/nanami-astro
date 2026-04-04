from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Customer, Menu, Order, OrderInputSnapshot, OrderStatusLog


def generate_order_code() -> str:
    return f"A{uuid4().hex[:10].upper()}"


def get_or_create_customer(db: Session, *, display_name: str | None = None, line_user_id: str | None = None, email: str | None = None, phone: str | None = None) -> Customer:
    customer = None
    if line_user_id:
        customer = db.scalar(select(Customer).where(Customer.line_user_id == line_user_id))
    if not customer and email:
        customer = db.scalar(select(Customer).where(Customer.email == email))
    if customer:
        if display_name and not customer.display_name:
            customer.display_name = display_name
        if phone and not customer.phone:
            customer.phone = phone
        return customer
    customer = Customer(display_name=display_name, line_user_id=line_user_id, email=email, phone=phone)
    db.add(customer)
    db.flush()
    return customer


def create_order(
    db: Session,
    *,
    menu: Menu,
    user_name: str,
    birth_date: date,
    user_contact: str | None = None,
    birth_time: str | None = None,
    birth_prefecture: str | None = None,
    birth_place: str | None = None,
    birth_lat: float | None = None,
    birth_lon: float | None = None,
    location_source: str | None = None,
    location_note: str | None = None,
    gender: str | None = None,
    consultation_text: str | None = None,
    customer: Customer | None = None,
    source: str = 'self',
    external_platform: str | None = None,
    external_order_ref: str | None = None,
    status: str = 'pending_payment',
    inputs_json: str | None = None,
    payload_json: str | None = None,
    unknowns_json: str | None = None,
) -> Order:
    paid_at = datetime.utcnow() if status == 'paid' else None
    order = Order(
        order_code=generate_order_code(),
        customer_id=customer.id if customer else None,
        source=source,
        external_platform=external_platform,
        external_order_ref=external_order_ref,
        user_name=user_name,
        user_contact=user_contact,
        birth_date=birth_date,
        birth_time=birth_time,
        birth_prefecture=birth_prefecture,
        birth_place=birth_place,
        birth_lat=birth_lat,
        birth_lon=birth_lon,
        location_source=location_source,
        location_note=location_note,
        gender=gender,
        consultation_text=consultation_text,
        menu_id=menu.id,
        price=menu.price,
        status=status,
        paid_at=paid_at,
    )
    db.add(order)
    db.flush()
    db.add(OrderStatusLog(order_id=order.id, from_status=None, to_status=status, actor_type='system', actor_id=None, note='order created'))
    if any([inputs_json, payload_json, unknowns_json]):
        db.add(OrderInputSnapshot(order_id=order.id, inputs_json=inputs_json, payload_json=payload_json, unknowns_json=unknowns_json))
    return order


def update_order_status(db: Session, order: Order, *, to_status: str, actor_type: str, actor_id: int | None = None, note: str | None = None) -> None:
    if order.status == to_status:
        return
    old = order.status
    order.status = to_status
    if to_status == 'paid' and not order.paid_at:
        order.paid_at = datetime.utcnow()
    if to_status == 'delivered' and not order.delivered_at:
        order.delivered_at = datetime.utcnow()
    if to_status == 'completed' and not order.completed_at:
        order.completed_at = datetime.utcnow()
    db.add(OrderStatusLog(order_id=order.id, from_status=old, to_status=to_status, actor_type=actor_type, actor_id=actor_id, note=note))



def auto_assign_reader(db: Session, order: Order, *, preferred_reader_id: int | None = None, preferred_reader_email: str | None = None, actor_type: str = 'system', note: str | None = None):
    """有効な占い師を1名自動割当する。既に割当済みならそのまま返す。"""
    from services.reader_availability import get_line_available_reader

    if order.assigned_reader_id:
        return order.assigned_reader

    reader = get_line_available_reader(db, preferred_reader_id=preferred_reader_id, preferred_reader_email=preferred_reader_email)
    if not reader:
        return None

    order.assigned_reader_id = reader.id
    if order.status == 'paid':
        update_order_status(db, order, to_status='assigned', actor_type=actor_type, actor_id=None, note=note or 'auto assigned after payment')
    return reader
