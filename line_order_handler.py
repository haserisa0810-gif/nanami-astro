from __future__ import annotations

import json
import os
from datetime import date
import re

from sqlalchemy import or_, select
from sqlalchemy.orm import joinedload

from db import SessionLocal
from models import Menu, Order
from services.order_service import create_order, get_or_create_customer
from services.location import infer_prefecture_name, resolve_birth_location

_START_WORDS = {
    '予約',
    '予約したい',
    '予約希望',
    '申し込み',
    '申込み',
    '申し込みたい',
    '鑑定したい',
    '鑑定希望',
    '鑑定申し込み',
    '有料鑑定',
    '有料鑑定したい',
}

_COURSE_OPTIONS = {
    '1': {'key': 'light', 'name': 'ライト鑑定', 'price': 3000, 'description': '西洋占星術'},
    '2': {'key': 'standard', 'name': 'スタンダード鑑定', 'price': 5000, 'description': '相性鑑定 または 西洋+インド'},
    '3': {'key': 'premium', 'name': 'じっくり鑑定', 'price': 10000, 'description': '西洋+インド+四柱推命'},
}

_COURSE_INPUT_MAP = {
    '1': '1', 'ライト': '1', 'ライト鑑定': '1', '3000': '1', '3,000': '1', '３０００': '1',
    '2': '2', 'スタンダード': '2', 'スタンダード鑑定': '2', '5000': '2', '5,000': '2', '５０００': '2',
    '3': '3', 'じっくり': '3', 'じっくり鑑定': '3', '10000': '3', '10,000': '3', '１００００': '3',
}


def should_start_order(text: str | None) -> bool:
    value = (text or '').strip().lower()
    if not value:
        return False

    triggers = (
        '予約',
        '申し込み',
        '申込み',
        '鑑定したい',
        '鑑定希望',
        '有料鑑定',
    )
    return value in _START_WORDS or any(t in value for t in triggers)


def get_order_by_code(order_code: str | None) -> Order | None:
    if not order_code:
        return None
    with SessionLocal() as db:
        order = (
            db.query(Order)
            .options(joinedload(Order.menu), joinedload(Order.customer))
            .filter(Order.order_code == order_code)
            .first()
        )
        if order is not None:
            db.expunge(order)
        return order


def correction_select_prompt() -> str:
    return (
        '修正したい項目の番号を送ってください。\n\n'
        '1. コース\n'
        '2. お名前\n'
        '3. 生年月日\n'
        '4. 出生時間\n'
        '5. 出生地\n'
        '6. 性別\n'
        '7. ご相談内容\n\n'
        'キャンセルする場合は「キャンセル」と送ってください。'
    )


def apply_correction_to_order(order_code: str | None, field_code: str, new_value: str) -> tuple[bool, str]:
    if not order_code or field_code not in _EDITABLE_FIELDS:
        return False, ''
    _, label = _EDITABLE_FIELDS[field_code]
    with SessionLocal() as db:
        order = db.query(Order).filter(Order.order_code == order_code).first()
        if not order:
            return False, label
        value = (new_value or '').strip()
        if field_code == '1':
            menu = _resolve_menu_for_course(db, _normalize_course(value))
            if not menu:
                return False, label
            order.menu = menu
            order.price = menu.price
        elif field_code == '2':
            if not value:
                return False, label
            order.user_name = value
        elif field_code == '3':
            normalized = _normalize_birth_date(value)
            if not normalized:
                return False, label
            from datetime import date as _date
            order.birth_date = _date.fromisoformat(normalized)
        elif field_code == '4':
            order.birth_time = None if value in {'', '不明', 'わからない', '不詳'} else value
        elif field_code == '5':
            order.birth_place = None if value in {'', '不明', 'わからない', '不詳'} else value
        elif field_code == '6':
            normalized = _normalize_gender(value)
            if not normalized:
                return False, label
            order.gender = normalized
        elif field_code == '7':
            order.consultation_text = None if value in {'', 'なし', '特になし', '不要', '未記入'} else value
        db.commit()
        return True, label


def append_correction_note(order_code: str | None, message_text: str) -> bool:
    if not order_code or not (message_text or '').strip():
        return False
    with SessionLocal() as db:
        order = db.query(Order).filter(Order.order_code == order_code).first()
        if not order:
            return False
        current = (order.consultation_text or '').strip()
        note = f"[LINE修正]\n{message_text.strip()}"
        order.consultation_text = f"{current}\n\n{note}".strip() if current else note
        db.commit()
        return True


def _start_prompt() -> str:
    return (
        'ご予約ありがとうございます。\n'
        '以下を1通でまとめて送ってください（改行あり・なしどちらでも大丈夫です）\n\n'
        '【コース】1 / 2 / 3\n'
        '1. ライト鑑定（3,000円）\n'
        '   西洋占星術\n'
        '2. スタンダード鑑定（5,000円）\n'
        '   相性鑑定 または 総合鑑定（西洋占星術＋インド占星術）\n'
        '3. じっくり鑑定（10,000円）\n'
        '   西洋占星術＋インド占星術＋四柱推命\n\n'
        '【お名前】\n'
        '【生年月日】1980-05-05 または 19800505\n'
        '【出生時間】不明可\n'
        '【出生地】不明可\n'
        '【性別】女性 / 男性 / その他 / 回答しない\n'
        '【ご相談内容】任意\n\n'
        '▼スタンダード鑑定で「相性鑑定」をご希望の方のみ\n'
        '以下もあわせてご記入ください。\n'
        '【お相手の生年月日】\n'
        '【お相手の出生時間】不明可\n'
        '【お相手の出生地】不明可\n'
        '【関係性】\n\n'
        '【無料鑑定ID または 受付番号】お持ちの場合のみ任意'
    )


_COURSE_LABELS = ('コース', 'プラン', 'メニュー', '鑑定コース')
_DATE_LABELS = ('生年月日', '誕生日', '生年月')
_NAME_LABELS = ('お名前', '名前', '氏名')
_TIME_LABELS = ('出生時間', '生まれた時間', '生誕時間', '時間')
_PLACE_LABELS = ('出生地', '生まれた場所', '生誕地', '場所')
_GENDER_LABELS = ('性別',)
_CONSULT_LABELS = ('ご相談内容', '相談内容', '相談', 'ご質問', '質問')
_FREE_ID_LABELS = ('無料鑑定ID', '無料鑑定id', '無料ID', 'フリーID', '受付番号')


def _normalize_free_link_code(text: str | None) -> str | None:
    value = (text or '').strip().upper()
    if not value:
        return None
    m = re.search(r'(F-[A-Z0-9-]+|A[A-Z0-9]{6,})', value)
    return m.group(1) if m else None


def _find_source_free_order(db, code: str | None) -> Order | None:
    normalized = _normalize_free_link_code(code)
    if not normalized:
        return None
    return db.scalar(
        select(Order).where(
            Order.order_kind == 'free',
            or_(
                Order.free_reading_code == normalized,
                Order.order_code == normalized,
            ),
        )
    )


def _normalize_birth_date(text: str | None) -> str | None:
    value = (text or '').strip()
    if not value:
        return None
    if re.fullmatch(r'\d{8}', value):
        value = f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    try:
        date.fromisoformat(value)
    except ValueError:
        return None
    return value


def _normalize_gender(text: str | None) -> str | None:
    value = (text or '').strip()
    mapping = {
        '1': '1', '女性': '1', '女': '1', 'female': '1',
        '2': '2', '男性': '2', '男': '2', 'male': '2',
        '3': '3', 'その他': '3', 'other': '3',
        '4': '4', '回答しない': '4', '無回答': '4', 'なし': '4', 'prefer not to say': '4',
    }
    return mapping.get(value.lower(), mapping.get(value))


def _normalize_course(text: str | None) -> str | None:
    value = (text or '').strip()
    if not value:
        return None
    return _COURSE_INPUT_MAP.get(value.lower(), _COURSE_INPUT_MAP.get(value))


def _course_label(course_code: str | None) -> str:
    item = _COURSE_OPTIONS.get(str(course_code or ''))
    if not item:
        return '未選択'
    return f"{item['name']}（¥{item['price']:,}）"


def _extract_labeled_value(text: str, labels: tuple[str, ...]) -> str | None:
    raw = (text or '').replace('：', ':')
    for label in labels:
        patterns = [
            rf'【{re.escape(label)}】\s*(.*?)(?=(?:\s*【[^】]+】)|$)',
            rf'{re.escape(label)}\s*:\s*(.*?)(?=(?:\s*【[^】]+】)|$)',
        ]
        for pattern in patterns:
            match = re.search(pattern, raw, flags=re.DOTALL)
            if match:
                value = match.group(1).strip()
                if value:
                    return value

    lines = [ln.strip() for ln in raw.splitlines()]
    normalized = [ln for ln in lines if ln]
    for i, line in enumerate(normalized):
        for label in labels:
            if line.startswith(f'【{label}】'):
                rest = line[len(f'【{label}】'):].strip(' :')
                if rest:
                    return rest
                for j in range(i + 1, len(normalized)):
                    nxt = normalized[j]
                    if re.match(r'^【.+】', nxt):
                        break
                    if nxt:
                        return nxt
            if line.lower().startswith(f'{label.lower()}:'):
                rest = line.split(':', 1)[1].strip()
                if rest:
                    return rest
    return None


def _extract_free_reading_code(text: str) -> str | None:
    labeled = _extract_labeled_value(text, _FREE_ID_LABELS)
    normalized = _normalize_free_link_code(labeled)
    if normalized:
        return normalized
    return _normalize_free_link_code(text)


def _parse_bundle_input(text: str) -> tuple[dict, list[str], list[str]]:
    draft: dict[str, object] = {}
    missing: list[str] = []
    errors: list[str] = []

    course = _extract_labeled_value(text, _COURSE_LABELS)
    name = _extract_labeled_value(text, _NAME_LABELS)
    birth_date = _extract_labeled_value(text, _DATE_LABELS)
    birth_time = _extract_labeled_value(text, _TIME_LABELS)
    birth_place = _extract_labeled_value(text, _PLACE_LABELS)
    gender = _extract_labeled_value(text, _GENDER_LABELS)
    consultation = _extract_labeled_value(text, _CONSULT_LABELS)
    free_reading_code = _extract_free_reading_code(text)

    normalized_course = _normalize_course(course)
    if not course:
        missing.append('コース')
    elif not normalized_course:
        errors.append('コースは 1 / 2 / 3 のいずれかで入力してください。')
    else:
        draft['course_code'] = normalized_course

    if not name:
        missing.append('お名前')
    else:
        draft['user_name'] = name

    normalized_birth_date = _normalize_birth_date(birth_date)
    if not birth_date:
        missing.append('生年月日')
    elif not normalized_birth_date:
        errors.append('生年月日は 1976-08-10 または 19760810 の形で入力してください。')
    else:
        draft['birth_date'] = normalized_birth_date

    if not birth_time:
        missing.append('出生時間')
    else:
        draft['birth_time'] = None if birth_time in {'不明', 'わからない', '不詳'} else birth_time

    if not birth_place:
        missing.append('出生地')
    else:
        draft['birth_place'] = None if birth_place in {'不明', 'わからない', '不詳'} else birth_place

    normalized_gender = _normalize_gender(gender)
    if not gender:
        missing.append('性別')
    elif not normalized_gender:
        errors.append('性別は 女性 / 男性 / その他 / 回答しない のいずれかで入力してください。')
    else:
        draft['gender'] = normalized_gender

    draft['consultation_text'] = None if (consultation or '').strip() in {'', 'なし', '特になし', '不要', '未記入'} else consultation
    if free_reading_code:
        draft['free_reading_code'] = free_reading_code
    return draft, missing, errors


def _resume_prompt(state: str) -> str:
    labels = {
        'input_bundle': '予約情報まとめ入力',
        'confirm': '確認',
        'edit_field': '修正入力',
    }
    return f"現在は【{labels.get(state, '受付開始')}】の途中です。続きを入力してください。"


_EDITABLE_FIELDS = {
    '1': ('course_code', 'コース'),
    '2': ('user_name', 'お名前'),
    '3': ('birth_date', '生年月日'),
    '4': ('birth_time', '出生時間'),
    '5': ('birth_place', '出生地'),
    '6': ('gender', '性別'),
    '7': ('consultation_text', 'ご相談内容'),
}


def _confirm_message(draft: dict) -> str:
    gender_map = {'1': '女性', '2': '男性', '3': 'その他', '4': '回答しない'}
    consultation = (draft.get('consultation_text') or '').strip() or '未記入'
    free_part = ''
    if (draft.get('free_reading_code') or '').strip():
        free_part = f"【無料鑑定ID / 受付番号】\n{draft.get('free_reading_code')}\n\n"
    return (
        'ありがとうございます。\n以下の内容で予約受付します。\n\n'
        f"【コース】\n{_course_label(draft.get('course_code'))}\n\n"
        f"【お名前】\n{draft.get('user_name','-')}\n\n"
        f"【生年月日】\n{draft.get('birth_date','-')}\n\n"
        f"【出生時間】\n{draft.get('birth_time') or '不明'}\n\n"
        f"【出生地】\n{draft.get('birth_place') or '不明'}\n\n"
        f"【性別】\n{gender_map.get(draft.get('gender',''), '未指定')}\n\n"
        + free_part +
        f"【ご相談内容】\n{consultation}\n\n"
        'よろしければ「確定」と送ってください。\n'
        '修正したい場合は次から選んでください。\n\n'
        '1. コース\n'
        '2. 名前\n'
        '3. 生年月日\n'
        '4. 出生時間\n'
        '5. 出生地\n'
        '6. 性別\n'
        '7. ご相談内容\n'
        '8. 最初からやり直す'
    )


def _not_started_message() -> str:
    return 'ご予約をご希望の方は「予約」または「予約したい」と送ってください。'


def _edit_prompt(field_code: str, draft: dict) -> str:
    field_key, label = _EDITABLE_FIELDS[field_code]
    current_value = draft.get(field_key)
    gender_map = {'1': '女性', '2': '男性', '3': 'その他', '4': '回答しない'}
    display_value = current_value or '未入力'
    if field_key == 'course_code':
        display_value = _course_label(current_value)
    elif field_key == 'gender':
        display_value = gender_map.get(str(current_value or ''), '未入力')
    elif field_key == 'consultation_text':
        display_value = (current_value or '').strip() or '未記入'
    instructions = {
        '1': 'コースを送ってください。1 / 2 / 3',
        '2': '新しいお名前をそのまま送ってください。',
        '3': '新しい生年月日を送ってください。例: 1976-08-10 または 19760810',
        '4': '新しい出生時間を送ってください。不明の場合は「不明」で大丈夫です。',
        '5': '新しい出生地を送ってください。不明の場合は「不明」で大丈夫です。',
        '6': '性別を送ってください。女性 / 男性 / その他 / 回答しない',
        '7': '新しいご相談内容を送ってください。不要なら「なし」で大丈夫です。',
    }
    return (
        f'【{label}】を修正します。\n'
        f'現在の内容: {display_value}\n\n'
        f'{instructions[field_code]}\n'
        '修正をやめる場合は「確認に戻る」と送ってください。'
    )


def _apply_single_field_edit(field_code: str, raw_value: str, draft: dict) -> tuple[dict | None, str | None]:
    value = (raw_value or '').strip()
    updated = dict(draft)
    if field_code == '1':
        normalized = _normalize_course(value)
        if not normalized:
            return None, 'コースは 1 / 2 / 3 のいずれかで入力してください。'
        updated['course_code'] = normalized
        return updated, None
    if field_code == '2':
        if not value:
            return None, 'お名前が空です。もう一度送ってください。'
        updated['user_name'] = value
        return updated, None
    if field_code == '3':
        normalized = _normalize_birth_date(value)
        if not normalized:
            return None, '生年月日は 1976-08-10 または 19760810 の形で入力してください。'
        updated['birth_date'] = normalized
        return updated, None
    if field_code == '4':
        updated['birth_time'] = None if value in {'', '不明', 'わからない', '不詳'} else value
        return updated, None
    if field_code == '5':
        updated['birth_place'] = None if value in {'', '不明', 'わからない', '不詳'} else value
        return updated, None
    if field_code == '6':
        normalized = _normalize_gender(value)
        if not normalized:
            return None, '性別は 女性 / 男性 / その他 / 回答しない のいずれかで入力してください。'
        updated['gender'] = normalized
        return updated, None
    if field_code == '7':
        updated['consultation_text'] = None if value in {'', 'なし', '特になし', '不要', '未記入'} else value
        return updated, None
    return None, '修正項目を認識できませんでした。'


def _resolve_menu_for_course(db, course_code: str | None) -> Menu | None:
    course = _COURSE_OPTIONS.get(str(course_code or ''))
    if not course:
        return None

    preferred_names = {
        '1': ['ライト鑑定', '西洋占星術鑑定'],
        '2': ['スタンダード鑑定', '相性・西洋インド鑑定'],
        '3': ['じっくり鑑定', '統合鑑定'],
    }.get(str(course_code), [])

    for name in preferred_names:
        menu = db.scalar(select(Menu).where(Menu.name == name, Menu.is_active == True))
        if menu:
            return menu

    neutral_name = course['name']
    existing = db.scalar(select(Menu).where(Menu.name == neutral_name))
    if existing:
        existing.price = int(course['price'])
        existing.description = str(course['description'])
        existing.lead_time_hours = 48 if str(course_code) in {'1', '2'} else 72
        existing.is_active = True
        db.flush()
        return existing

    new_menu = Menu(
        name=neutral_name,
        description=str(course['description']),
        price=int(course['price']),
        lead_time_hours=48 if str(course_code) in {'1', '2'} else 72,
        is_active=True,
    )
    db.add(new_menu)
    db.flush()
    return new_menu


def handle_order_message(
    user_id: str | None,
    text: str,
    session: dict,
    line_display_name: str | None = None,
) -> tuple[str, dict, bool, str | None]:
    text = (text or '').strip()
    state = session.get('state') or 'idle'
    draft = dict(session.get('draft_order') or {})

    if text == 'キャンセル':
        return '今回の受付はキャンセルしました。\nまた必要になったら「予約」と送ってください。', {'state': 'idle', 'draft_order': {}}, True, None
    if should_start_order(text):
        return _start_prompt(), {'state': 'input_bundle', 'draft_order': {}}, False, None
    if text == 'やり直し':
        return _start_prompt(), {'state': 'input_bundle', 'draft_order': {}}, False, None
    if text == '続き':
        if state in {'idle', 'completed'}:
            return _start_prompt(), {'state': 'input_bundle', 'draft_order': {}}, False, None
        return _resume_prompt(state), {'state': state, 'draft_order': draft, 'order_code': session.get('order_code')}, False, None

    if state in {'idle', 'completed'}:
        parsed_draft, missing, errors = _parse_bundle_input(text)
        if not missing and not errors:
            draft.update(parsed_draft)
            return _confirm_message(draft), {'state': 'confirm', 'draft_order': draft}, False, None
        return _not_started_message(), {'state': state, 'draft_order': draft, 'order_code': session.get('order_code')}, False, None

    if state == 'input_bundle':
        parsed_draft, missing, errors = _parse_bundle_input(text)
        if missing or errors:
            parts: list[str] = []
            if missing:
                parts.append('未入力: ' + ' / '.join(missing))
            if errors:
                parts.extend(errors)
            parts.append('\n次の形式でまとめて送ってください。改行なしでも大丈夫です。')
            parts.append('【コース】1 / 2 / 3')
            parts.append('【お名前】')
            parts.append('【生年月日】1976-08-10 または 19760810')
            parts.append('【出生時間】不明可')
            parts.append('【出生地】不明可')
            parts.append('【性別】女性 / 男性 / その他 / 回答しない')
            parts.append('【ご相談内容】任意')
            parts.append('【無料鑑定ID または 受付番号】任意')
            return '\n'.join(parts), {'state': 'input_bundle', 'draft_order': draft}, False, None
        draft.update(parsed_draft)
        return _confirm_message(draft), {'state': 'confirm', 'draft_order': draft}, False, None

    if state == 'edit_field':
        field_code = str(session.get('edit_field') or '')
        if text == '確認に戻る':
            return _confirm_message(draft), {'state': 'confirm', 'draft_order': draft}, False, None
        if field_code not in _EDITABLE_FIELDS:
            return _confirm_message(draft), {'state': 'confirm', 'draft_order': draft}, False, None
        updated_draft, error = _apply_single_field_edit(field_code, text, draft)
        if error:
            return error, {'state': 'edit_field', 'draft_order': draft, 'edit_field': field_code}, False, None
        return _confirm_message(updated_draft), {'state': 'confirm', 'draft_order': updated_draft}, False, None

    if state == 'confirm':
        if text == '8':
            return _start_prompt(), {'state': 'input_bundle', 'draft_order': {}}, False, None
        if text in _EDITABLE_FIELDS:
            return _edit_prompt(text, draft), {'state': 'edit_field', 'draft_order': draft, 'edit_field': text}, False, None
        if text != '確定':
            return '「確定」と送るか、修正番号を送ってください。', {'state': state, 'draft_order': draft}, False, None

        with SessionLocal() as db:
            menu = _resolve_menu_for_course(db, draft.get('course_code'))
            if not menu:
                return 'コース情報の取得に失敗しました。最初からやり直してください。', {'state': 'input_bundle', 'draft_order': {}}, False, None
            customer = get_or_create_customer(db, display_name=draft.get('user_name') or line_display_name, line_user_id=user_id)
            location = resolve_birth_location(infer_prefecture_name(draft.get('birth_place')), draft.get('birth_place'))
            free_reading_code = (draft.get('free_reading_code') or '').strip().upper()
            source_free_order = _find_source_free_order(db, free_reading_code)
            order = create_order(
                db,
                menu=menu,
                user_name=draft.get('user_name') or (line_display_name or 'LINEユーザー'),
                birth_date=date.fromisoformat(str(draft['birth_date'])),
                user_contact=user_id,
                birth_time=draft.get('birth_time'),
                birth_prefecture=location.get('birth_prefecture'),
                birth_place=location.get('birth_place'),
                birth_lat=location.get('birth_lat'),
                birth_lon=location.get('birth_lon'),
                location_source=location.get('location_source'),
                location_note=location.get('location_note'),
                gender=draft.get('gender'),
                consultation_text=draft.get('consultation_text'),
                customer=customer,
                source='line',
                external_platform='line',
                external_order_ref=user_id,
                status='pending_payment',
                inputs_json=json.dumps(draft, ensure_ascii=False),
            )
            if source_free_order:
                order.source_free_order_id = source_free_order.id
            db.commit()
            base_url = (os.getenv('BASE_URL') or '').rstrip('/')
            payment_link = f"{base_url}/payment/{order.order_code}" if base_url else f"/payment/{order.order_code}"
            reply = (
                f'ご予約ありがとうございます。\n予約番号は【{order.order_code}】です。\n\n'
                f'コース: {_course_label(draft.get("course_code"))}\n\n'
                'ご入力内容は受付済みです。\n'
                'こちらからお申し込みをお願いいたします。\n\n'
                f'{payment_link}\n\n'
                '決済後は自動で反映されます。予約番号を送り直す必要はありません。\n'
                '内容を間違えた場合のみ、そのままこのLINEに返信してください。'
            )
            next_session = {'state': 'completed', 'draft_order': {}, 'order_code': order.order_code}
            return reply, next_session, False, order.order_code

    return _start_prompt(), {'state': 'input_bundle', 'draft_order': {}}, False, None
