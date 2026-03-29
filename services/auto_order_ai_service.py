from __future__ import annotations

import json
from datetime import datetime

from db import db_session
from models import Order, OrderDelivery
from services.order_service import update_order_status
from services.yaml_log_service import create_yaml_log
from analyze_engine import build_base_meta, build_handoff_logs, build_payload_a, format_reports, run_single


def process_order_auto_reading(order_id: int) -> None:
    with db_session() as db:
        order = db.get(Order, order_id)
        if not order:
            return

        order.ai_status = "running"
        db.commit()

        try:
            unknowns: list[str] = []
            payload_a = build_payload_a(
                birth_date=order.birth_date.isoformat(),
                birth_time=order.birth_time,
                birth_place=order.birth_place,
                prefecture=order.birth_prefecture,
                lat=order.birth_lat,
                lon=order.birth_lon,
                gender=order.gender or 'female',
                house_system='P',
                node_mode='true',
                lilith_mode='mean',
                include_asteroids=False,
                include_chiron=True,
                include_lilith=False,
                include_vertex=False,
                unknowns=unknowns,
            )
            base_meta = build_base_meta(
                birth_date=order.birth_date.isoformat(),
                output_style='normal',
                detail_level='standard',
                house_system='P',
                node_mode='true',
                lilith_mode='mean',
                include_asteroids=False,
                include_chiron=True,
                include_lilith=False,
                include_vertex=False,
                include_reader=True,
                theme='overall',
                message=order.consultation_text,
                observations_text=None,
                analysis_type='single',
                astrology_system='western',
                ai_model=None,
                day_change_at_23=False,
                name=order.user_name,
                name_b=None,
                gender=order.gender or 'female',
                gender_b='female',
            )
            astro_result, payload_view, report_web, report_line, report_raw, report_reader, guard_meta = run_single(
                'western', payload_a, base_meta, order.consultation_text, False, False
            )
            report_web, report_raw, report_reader, report_line = format_reports(
                report_web, report_raw, report_reader, report_line, 'standard', 'normal', True
            )
            logs = build_handoff_logs(
                inputs_view={
                    'name': order.user_name,
                    'birth_date': order.birth_date.isoformat(),
                    'birth_time': order.birth_time,
                    'birth_prefecture': order.birth_prefecture,
                    'birth_place': order.birth_place,
                    'gender': order.gender,
                    'consultation_text': order.consultation_text,
                },
                payload_view=payload_view,
                unknowns=unknowns,
                astro_result=astro_result,
                report_web=report_web,
                report_raw=report_raw,
                report_reader=report_reader,
                report_line=report_line,
                observations_text=None,
                bias_guard_obj=guard_meta,
            )
            summary_obj = logs.get('structure_summary_json') if isinstance(logs.get('structure_summary_json'), dict) else None
            summary = {
                'reports': {
                    'web': report_web,
                    'raw': report_raw,
                    'reader': report_reader,
                    'line': report_line,
                },
                'structure_summary': summary_obj,
                'raw_json': astro_result,
                'order': {'order_code': order.order_code},
                'saved_from': 'staff_auto_ai',
            }
            yaml_body = logs.get('handoff_yaml_full') or logs.get('handoff_yaml') or (
                "title: 自動鑑定\nreports:\n  web: |\n" + "\n".join([f"    {line}" for line in (report_web or '').splitlines()])
            )
            create_yaml_log(
                db,
                order,
                yaml_body=yaml_body,
                summary=summary,
                created_by_type='system',
                created_by_id=None,
                log_type='generated',
                set_active=True,
            )

            latest = sorted(order.deliveries, key=lambda d: d.updated_at or d.created_at, reverse=True)
            delivery = latest[0] if latest else None
            draft_text = (report_reader or report_web or '').strip()
            if delivery:
                delivery.delivery_text = draft_text
                delivery.is_draft = True
            elif order.assigned_reader_id:
                db.add(OrderDelivery(order_id=order.id, reader_id=order.assigned_reader_id, delivery_text=draft_text, is_draft=True))

            if order.status in {'received', 'paid', 'assigned'}:
                update_order_status(db, order, to_status='in_progress', actor_type='system', note='auto ai started')
            order.ai_status = 'completed'
            db.commit()
        except Exception as exc:
            order.ai_status = 'failed'
            order.location_note = f'自動鑑定エラー: {exc}'[:255]
            db.commit()
