from __future__ import annotations

import json
import random
import string
from datetime import datetime

from sqlalchemy import select

from analyze_engine import build_base_meta, build_handoff_logs, build_payload_a, format_reports, run_single
from db import db_session
from models import Menu, Order, OrderResultView, YamlLog
from services.order_service import update_order_status
from services.result_builder import render_result_html


FREE_RESULT_FOOTER = "有料鑑定をご希望の際は、このIDをご記入いただくとご案内がスムーズです。"


def generate_free_reading_code() -> str:
    date_part = datetime.now().strftime('%y%m%d')
    rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f'F-{date_part}-{rand}'


def ensure_unique_free_reading_code(db) -> str:
    for _ in range(20):
        code = generate_free_reading_code()
        if not db.scalar(select(Order).where(Order.free_reading_code == code)):
            return code
    raise RuntimeError('free_reading_code could not be generated')


def _menu_name_from_order(order: Order) -> str:
    return '無料鑑定'


def build_free_result_payload(order: Order, report_web: str, astro_result: dict, structure_summary: dict | None = None) -> dict:
    summary = structure_summary or {}
    sections = [
        {'heading': '無料鑑定結果', 'body': report_web.strip()},
    ]
    advice_list = []
    for idx, item in enumerate((summary.get('advice') or [])[:3], start=1):
        if isinstance(item, dict):
            title = item.get('title') or item.get('heading') or f'意識したいこと {idx}'
            body = item.get('body') or item.get('text') or ''
        else:
            title = f'意識したいこと {idx}'
            body = str(item)
        advice_list.append({'title': title, 'body': body})
    raw = astro_result if isinstance(astro_result, dict) else {}
    planets = raw.get('planets') or raw.get('western', {}).get('planets') or []
    planet_list = []
    for p in planets[:7]:
        if isinstance(p, dict):
            house = p.get('house') or p.get('house_num') or '-'
            if isinstance(house, (int, float)):
                house = f'{int(house)}ハウス'
            planet_list.append({'name': p.get('name') or '-', 'sign': p.get('sign') or '-', 'house': house, 'note': p.get('note') or ''})
    return {
        'title': _menu_name_from_order(order),
        'order_code': order.order_code,
        'summary': {
            'essence': summary.get('core_message') or summary.get('essence') or '',
            'strength': summary.get('strengths') or summary.get('strength') or '',
            'caution': summary.get('cautions') or summary.get('caution') or summary.get('theme') or '',
        },
        'sections': sections,
        'planet_list': planet_list,
        'advice_list': advice_list,
        'horoscope_image_url': '',
        'free_reading_code': order.free_reading_code,
    }


def process_free_reading(order_id: int) -> None:
    with db_session() as db:
        order = db.get(Order, order_id)
        if not order:
            return
        order.ai_status = 'running'
        try:
            payload_a = build_payload_a(
                birth_date=order.birth_date.isoformat(),
                birth_time=order.birth_time,
                birth_place=order.birth_place,
                prefecture=order.birth_prefecture,
                lat=order.birth_lat,
                lon=order.birth_lon,
                gender=order.gender or '4',
                house_system='placidus',
                node_mode='true',
                lilith_mode='mean',
                include_asteroids=False,
                include_chiron=True,
                include_lilith=False,
                include_vertex=False,
                unknowns=[],
            )
            base_meta = build_base_meta(
                birth_date=order.birth_date.isoformat(),
                output_style='web',
                detail_level='short',
                house_system='placidus',
                node_mode='true',
                lilith_mode='mean',
                include_asteroids=False,
                include_chiron=True,
                include_lilith=False,
                include_vertex=False,
                include_reader=False,
                theme='free_reading',
                message=order.consultation_text,
                observations_text=None,
                analysis_type='single',
                astrology_system='western',
                ai_model='gemini-2.5-flash-lite',
                day_change_at_23=False,
                name=order.user_name,
                name_b=None,
                gender=order.gender or '4',
                gender_b='4',
            )
            astro_result, payload_view, report_web, report_line, report_raw, report_reader, guard_meta = run_single(
                'western', payload_a, base_meta, order.consultation_text, False, False
            )
            report_web, report_raw, report_reader, report_line = format_reports(
                report_web, report_raw, report_reader, report_line, 'short', 'web', False
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
                unknowns=[],
                astro_result=astro_result,
                report_web=report_web,
                report_raw=report_raw,
                report_reader=report_reader,
                report_line=report_line,
                observations_text=None,
                bias_guard_obj=guard_meta,
            )
            summary_obj = logs.get('structure_summary_json') if isinstance(logs.get('structure_summary_json'), dict) else None
            payload = build_free_result_payload(order, report_web, astro_result, summary_obj)
            order.result_payload_json = json.dumps(payload, ensure_ascii=False)
            order.result_html = render_result_html(payload)
            order.free_result_text = report_web.strip()
            order.ai_status = 'completed'
            update_order_status(db, order, to_status='completed', actor_type='system', note='free reading completed')
            db.add(YamlLog(
                order_id=order.id,
                customer_id=order.customer_id,
                yaml_body=logs.get('handoff_yaml') or '',
                summary_json=json.dumps({
                    'reports': {'web': report_web},
                    'structure_summary': summary_obj,
                    'raw_json': astro_result,
                    'order': {'free_reading_code': order.free_reading_code},
                }, ensure_ascii=False),
                created_by_type='system',
                log_type='generated',
                version_no=1,
                is_active=True,
            ))
            db.add(OrderResultView(
                order_id=order.id,
                result_payload_json=order.result_payload_json,
                result_html=order.result_html,
                published_at=datetime.utcnow(),
                updated_by_type='system',
                report_html=None,
            ))
        except Exception as exc:
            order.ai_status = 'failed'
            order.free_result_text = f'無料鑑定の生成に失敗しました。時間をおいて再度お試しください。\n\n詳細: {exc}'
