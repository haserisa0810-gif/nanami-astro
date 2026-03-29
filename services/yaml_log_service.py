from __future__ import annotations

import json
from datetime import datetime

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import Order, YamlLog


def build_yaml_payload(order: Order, delivery_text: str | None = None, summary: dict | None = None) -> dict:
    return {
        'order_code': order.order_code,
        'customer_id': order.customer_id,
        'source': order.source,
        'menu': order.menu.name if order.menu else None,
        'status': order.status,
        'created_at': order.created_at.isoformat() if order.created_at else None,
        'paid_at': order.paid_at.isoformat() if order.paid_at else None,
        'inputs': {
            'name': order.user_name,
            'birth_date': order.birth_date.isoformat() if order.birth_date else None,
            'birth_time': order.birth_time,
            'birth_place': order.birth_place,
            'gender': order.gender,
            'consultation_text': order.consultation_text,
        },
        'delivery': {
            'delivered_at': order.delivered_at.isoformat() if order.delivered_at else None,
            'result_excerpt': (delivery_text or '')[:1000],
        },
        'summary': summary or {},
        'generated_at': datetime.utcnow().isoformat() + 'Z',
    }


def create_yaml_log(
    db: Session,
    order: Order,
    *,
    yaml_body: str,
    summary: dict | None = None,
    created_by_type: str = 'system',
    created_by_id: int | None = None,
    log_type: str = 'generated',
    set_active: bool = True,
) -> YamlLog:
    latest_version = db.scalar(select(YamlLog.version_no).where(YamlLog.order_id == order.id).order_by(YamlLog.version_no.desc()).limit(1)) or 0
    if set_active:
        for old in db.scalars(select(YamlLog).where(YamlLog.order_id == order.id, YamlLog.is_active == True)).all():
            old.is_active = False
    log = YamlLog(
        order_id=order.id,
        customer_id=order.customer_id,
        yaml_body=yaml_body,
        summary_json=json.dumps(summary or {}, ensure_ascii=False),
        created_by_type=created_by_type,
        created_by_id=created_by_id,
        log_type=log_type,
        version_no=latest_version + 1,
        is_active=set_active,
    )
    db.add(log)
    return log


def upsert_yaml_log(db: Session, order: Order, *, created_by_type: str = 'system', created_by_id: int | None = None, delivery_text: str | None = None, summary: dict | None = None) -> YamlLog:
    payload = build_yaml_payload(order, delivery_text=delivery_text, summary=summary)
    yaml_body = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
    return create_yaml_log(
        db,
        order,
        yaml_body=yaml_body,
        summary=summary,
        created_by_type=created_by_type,
        created_by_id=created_by_id,
        log_type='published_to_result' if delivery_text else 'generated',
        set_active=True,
    )
