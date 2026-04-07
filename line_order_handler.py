from __future__ import annotations

import json
import os
import re
from datetime import date
from urllib.parse import urlencode

from sqlalchemy import or_, select, text
from sqlalchemy.orm import joinedload

from db import SessionLocal
from models import Menu, Order
from services.location import infer_prefecture_name, resolve_birth_location
from services.order_service import create_order, get_or_create_customer

_START_WORDS = {
    "予約",
    "予約したい",
    "予約希望",
    "申し込み",
    "申込み",
    "申し込みたい",
    "鑑定したい",
    "鑑定希望",
    "鑑定申し込み",
    "有料鑑定",
    "有料鑑定したい",
    "購入完了",
    "購入済み",
    "直接購入",
    "フォーム",
    "フォームだけ",
}

_COURSE_OPTIONS = {
    "1": {"key": "light", "name": "ライト鑑定", "price": 3000, "description": "西洋占星術"},
    "2": {"key": "standard", "name": "スタンダード鑑定", "price": 5000, "description": "相性鑑定 または 総合鑑定（西洋占星術＋インド占星術）"},
    "3": {"key": "premium", "name": "じっくり鑑定", "price": 10000, "description": "西洋占星術＋インド占星術＋四柱推命"},
}

_COURSE_INPUT_MAP = {
    "1": "1", "ライト": "1", "ライト鑑定": "1", "3000": "1", "3,000": "1", "３０００": "1",
    "2": "2", "スタンダード": "2", "スタンダード鑑定": "2", "5000": "2", "5,000": "2", "５０００": "2",
    "3": "3", "じっくり": "3", "じっくり鑑定": "3", "10000": "3", "10,000": "3", "１００００": "3",
}

_ORDER_NO_PATTERN = re.compile(r"^\d{10}$")


# =========================
# 基本判定 / 既存補助関数
# =========================
def should_start_order(text: str | None) -> bool:
    value = (text or "").strip().lower()
    if not value:
        return False

    triggers = (
        "予約",
        "申し込み",
        "申込み",
        "鑑定したい",
        "鑑定希望",
        "有料鑑定",
        "購入完了",
        "購入済み",
        "直接購入",
        "フォーム",
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
        "修正したい項目の番号を送ってください。\n\n"
        "1. コース\n"
        "2. お名前\n"
        "3. 生年月日\n"
        "4. 出生時間\n"
        "5. 出生地\n"
        "6. 性別\n"
        "7. ご相談内容\n\n"
        "キャンセルする場合は「キャンセル」と送ってください。"
    )


_EDITABLE_FIELDS = {
    "1": ("course_code", "コース"),
    "2": ("user_name", "お名前"),
    "3": ("birth_date", "生年月日"),
    "4": ("birth_time", "出生時間"),
    "5": ("birth_place", "出生地"),
    "6": ("gender", "性別"),
    "7": ("consultation_text", "ご相談内容"),
}


def apply_correction_to_order(order_code: str | None, field_code: str, new_value: str) -> tuple[bool, str]:
    if not order_code or field_code not in _EDITABLE_FIELDS:
        return False, ""
    _, label = _EDITABLE_FIELDS[field_code]
    with SessionLocal() as db:
        order = db.query(Order).filter(Order.order_code == order_code).first()
        if not order:
            return False, label
        value = (new_value or "").strip()
        if field_code == "1":
            menu = _resolve_menu_for_course(db, _normalize_course(value))
            if not menu:
                return False, label
            order.menu = menu
            order.price = menu.price
        elif field_code == "2":
            if not value:
                return False, label
            order.user_name = value
        elif field_code == "3":
            normalized = _normalize_birth_date(value)
            if not normalized:
                return False, label
            order.birth_date = date.fromisoformat(normalized)
        elif field_code == "4":
            order.birth_time = None if value in {"", "不明", "わからない", "不詳"} else value
        elif field_code == "5":
            order.birth_place = None if value in {"", "不明", "わからない", "不詳"} else value
        elif field_code == "6":
            normalized = _normalize_gender(value)
            if not normalized:
                return False, label
            order.gender = normalized
        elif field_code == "7":
            order.consultation_text = None if value in {"", "なし", "特になし", "不要", "未記入"} else value
        db.commit()
        return True, label


def append_correction_note(order_code: str | None, message_text: str) -> bool:
    if not order_code or not (message_text or "").strip():
        return False
    with SessionLocal() as db:
        order = db.query(Order).filter(Order.order_code == order_code).first()
        if not order:
            return False
        current = (order.consultation_text or "").strip()
        note = f"[LINE修正]\n{message_text.strip()}"
        order.consultation_text = f"{current}\n\n{note}".strip() if current else note
        db.commit()
        return True


_COURSE_LABELS = ("コース", "プラン", "メニュー", "鑑定コース")
_DATE_LABELS = ("生年月日", "誕生日", "生年月")
_NAME_LABELS = ("お名前", "名前", "氏名")
_TIME_LABELS = ("出生時間", "生まれた時間", "生誕時間", "時間")
_PLACE_LABELS = ("出生地", "生まれた場所", "生誕地", "場所")
_GENDER_LABELS = ("性別",)
_CONSULT_LABELS = ("ご相談内容", "相談内容", "相談", "ご質問", "質問")
_FREE_ID_LABELS = ("無料鑑定ID", "無料鑑定id", "無料ID", "フリーID", "受付番号")


def _normalize_free_link_code(text: str | None) -> str | None:
    value = (text or "").strip().upper()
    if not value:
        return None
    m = re.search(r"(F-[A-Z0-9-]+|A[A-Z0-9]{6,})", value)
    return m.group(1) if m else None


def _find_source_free_order(db, code: str | None) -> Order | None:
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


def _normalize_birth_date(text: str | None) -> str | None:
    value = (text or "").strip()
    if not value:
        return None
    if re.fullmatch(r"\d{8}", value):
        value = f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    try:
        date.fromisoformat(value)
    except ValueError:
        return None
    return value


def _normalize_gender(text: str | None) -> str | None:
    value = (text or "").strip()
    mapping = {
        "1": "1", "女性": "1", "女": "1", "female": "1",
        "2": "2", "男性": "2", "男": "2", "male": "2",
        "3": "3", "その他": "3", "other": "3",
        "4": "4", "回答しない": "4", "無回答": "4", "なし": "4", "prefer not to say": "4",
    }
    return mapping.get(value.lower(), mapping.get(value))


def _normalize_course(text: str | None) -> str | None:
    value = (text or "").strip()
    if not value:
        return None
    return _COURSE_INPUT_MAP.get(value.lower(), _COURSE_INPUT_MAP.get(value))


def _course_label(course_code: str | None) -> str:
    item = _COURSE_OPTIONS.get(str(course_code or ""))
    if not item:
        return "未選択"
    return f"{item['name']}（¥{item['price']:,}）"


def _shop_url() -> str:
    return (
        os.getenv("STORES_SHOP_URL")
        or os.getenv("BASE_SHOP_URL")
        or "https://nanami-astro.stores.jp"
    ).strip().rstrip("/")


def _payment_item_url(course_code: str | None) -> str:
    normalized = str(course_code or "").strip()
    default_map = {
        "1": "https://nanami-astro.stores.jp/items/69d31bca9300a952c7e8068a",
        "2": "https://nanami-astro.stores.jp/items/69d31c5d5840995346071f28",
        "3": "https://nanami-astro.stores.jp/items/69d31c8ca0c07f56e936317a",
    }
    env_map = {
        "1": (os.getenv("STORES_LIGHT_URL") or os.getenv("BASE_LIGHT_URL") or "").strip(),
        "2": (os.getenv("STORES_STANDARD_URL") or os.getenv("BASE_STANDARD_URL") or "").strip(),
        "3": (os.getenv("STORES_PREMIUM_URL") or os.getenv("BASE_PREMIUM_URL") or "").strip(),
    }
    return env_map.get(normalized) or default_map.get(normalized) or _shop_url()


def _intake_url(
    course_code: str | None = None,
    *,
    line_user_id: str | None = None,
    line_display_name: str | None = None,
    payment_order_ref: str | None = None,
) -> str:
    base = (os.getenv("PUBLIC_ORDER_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or os.getenv("BASE_URL") or "").rstrip("/")
    slug_map = {"1": "light", "2": "standard", "3": "premium"}
    slug = slug_map.get(str(course_code or "").strip())
    path = f"/menu/{slug}" if slug else "/menu"
    params: dict[str, str] = {}
    if (payment_order_ref or "").strip():
        params["payment_order_ref"] = (payment_order_ref or "").strip()
    if (line_user_id or "").strip():
        params["line_user_id"] = (line_user_id or "").strip()
    if (line_display_name or "").strip():
        params["line_name"] = (line_display_name or "").strip()
    query = ("?" + urlencode(params)) if params else ""
    if not base:
        return f"{path}{query}"
    return f"{base}{path}{query}"


def _extract_labeled_value(text: str, labels: tuple[str, ...]) -> str | None:
    raw = (text or "").replace("：", ":")
    for label in labels:
        patterns = [
            rf"【{re.escape(label)}】\s*(.*?)(?=(?:\s*【[^】]+】)|$)",
            rf"{re.escape(label)}\s*:\s*(.*?)(?=(?:\s*【[^】]+】)|$)",
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
            if line.startswith(f"【{label}】"):
                rest = line[len(f"【{label}】"):].strip(" :")
                if rest:
                    return rest
                for j in range(i + 1, len(normalized)):
                    nxt = normalized[j]
                    if re.match(r"^【.+】", nxt):
                        break
                    if nxt:
                        return nxt
            if line.lower().startswith(f"{label.lower()}:"):
                rest = line.split(":", 1)[1].strip()
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
        missing.append("コース")
    elif not normalized_course:
        errors.append("コースは 1 / 2 / 3 のいずれかで入力してください。")
    else:
        draft["course_code"] = normalized_course

    if not name:
        missing.append("お名前")
    else:
        draft["user_name"] = name

    normalized_birth_date = _normalize_birth_date(birth_date)
    if not birth_date:
        missing.append("生年月日")
    elif not normalized_birth_date:
        errors.append("生年月日は 1976-08-10 または 19760810 の形で入力してください。")
    else:
        draft["birth_date"] = normalized_birth_date

    if not birth_time:
        missing.append("出生時間")
    else:
        draft["birth_time"] = None if birth_time in {"不明", "わからない", "不詳"} else birth_time

    if not birth_place:
        missing.append("出生地")
    else:
        draft["birth_place"] = None if birth_place in {"不明", "わからない", "不詳"} else birth_place

    normalized_gender = _normalize_gender(gender)
    if not gender:
        missing.append("性別")
    elif not normalized_gender:
        errors.append("性別は 女性 / 男性 / その他 / 回答しない のいずれかで入力してください。")
    else:
        draft["gender"] = normalized_gender

    draft["consultation_text"] = None if (consultation or "").strip() in {"", "なし", "特になし", "不要", "未記入"} else consultation
    if free_reading_code:
        draft["free_reading_code"] = free_reading_code
    return draft, missing, errors


def _resume_prompt(state: str) -> str:
    labels = {
        "input_bundle": "予約情報まとめ入力",
        "confirm": "確認",
        "edit_field": "修正入力",
        "awaiting_order_code": "注文番号入力",
    }
    return f"現在は【{labels.get(state, '受付開始')}】の途中です。続きを入力してください。"


def _confirm_message(draft: dict) -> str:
    gender_map = {"1": "女性", "2": "男性", "3": "その他", "4": "回答しない"}
    consultation = (draft.get("consultation_text") or "").strip() or "未記入"
    free_part = ""
    if (draft.get("free_reading_code") or "").strip():
        free_part = f"【無料鑑定ID / 受付番号】\n{draft.get('free_reading_code')}\n\n"
    return (
        "ありがとうございます。\n以下の内容で予約受付します。\n\n"
        f"【コース】\n{_course_label(draft.get('course_code'))}\n\n"
        f"【お名前】\n{draft.get('user_name', '-')}\n\n"
        f"【生年月日】\n{draft.get('birth_date', '-')}\n\n"
        f"【出生時間】\n{draft.get('birth_time') or '不明'}\n\n"
        f"【出生地】\n{draft.get('birth_place') or '不明'}\n\n"
        f"【性別】\n{gender_map.get(draft.get('gender', ''), '未指定')}\n\n"
        + free_part
        + f"【ご相談内容】\n{consultation}\n\n"
        "よろしければ「確定」と送ってください。\n"
        "修正したい場合は次から選んでください。\n\n"
        "1. コース\n"
        "2. 名前\n"
        "3. 生年月日\n"
        "4. 出生時間\n"
        "5. 出生地\n"
        "6. 性別\n"
        "7. ご相談内容\n"
        "8. 最初からやり直す"
    )


def _not_started_message() -> str:
    return "ご予約をご希望の方は「予約」または「予約したい」と送ってください。"


def _edit_prompt(field_code: str, draft: dict) -> str:
    field_key, label = _EDITABLE_FIELDS[field_code]
    current_value = draft.get(field_key)
    gender_map = {"1": "女性", "2": "男性", "3": "その他", "4": "回答しない"}
    display_value = current_value or "未入力"
    if field_key == "course_code":
        display_value = _course_label(current_value)
    elif field_key == "gender":
        display_value = gender_map.get(str(current_value or ""), "未入力")
    elif field_key == "consultation_text":
        display_value = (current_value or "").strip() or "未記入"
    instructions = {
        "1": "コースを送ってください。1 / 2 / 3",
        "2": "新しいお名前をそのまま送ってください。",
        "3": "新しい生年月日を送ってください。例: 1976-08-10 または 19760810",
        "4": "新しい出生時間を送ってください。不明の場合は『不明』で大丈夫です。",
        "5": "新しい出生地を送ってください。不明の場合は『不明』で大丈夫です。",
        "6": "性別を送ってください。女性 / 男性 / その他 / 回答しない",
        "7": "新しいご相談内容を送ってください。不要なら『なし』で大丈夫です。",
    }
    return (
        f"【{label}】を修正します。\n"
        f"現在の内容: {display_value}\n\n"
        f"{instructions[field_code]}\n"
        "修正をやめる場合は「確認に戻る」と送ってください。"
    )


def _apply_single_field_edit(field_code: str, raw_value: str, draft: dict) -> tuple[dict | None, str | None]:
    value = (raw_value or "").strip()
    updated = dict(draft)
    if field_code == "1":
        normalized = _normalize_course(value)
        if not normalized:
            return None, "コースは 1 / 2 / 3 のいずれかで入力してください。"
        updated["course_code"] = normalized
        return updated, None
    if field_code == "2":
        if not value:
            return None, "お名前が空です。もう一度送ってください。"
        updated["user_name"] = value
        return updated, None
    if field_code == "3":
        normalized = _normalize_birth_date(value)
        if not normalized:
            return None, "生年月日は 1976-08-10 または 19760810 の形で入力してください。"
        updated["birth_date"] = normalized
        return updated, None
    if field_code == "4":
        updated["birth_time"] = None if value in {"", "不明", "わからない", "不詳"} else value
        return updated, None
    if field_code == "5":
        updated["birth_place"] = None if value in {"", "不明", "わからない", "不詳"} else value
        return updated, None
    if field_code == "6":
        normalized = _normalize_gender(value)
        if not normalized:
            return None, "性別は 女性 / 男性 / その他 / 回答しない のいずれかで入力してください。"
        updated["gender"] = normalized
        return updated, None
    if field_code == "7":
        updated["consultation_text"] = None if value in {"", "なし", "特になし", "不要", "未記入"} else value
        return updated, None
    return None, "修正項目を認識できませんでした。"


def _resolve_menu_for_course(db, course_code: str | None) -> Menu | None:
    course = _COURSE_OPTIONS.get(str(course_code or ""))
    if not course:
        return None

    preferred_names = {
        "1": ["ライト鑑定", "西洋占星術鑑定"],
        "2": ["スタンダード鑑定", "相性・西洋インド鑑定"],
        "3": ["じっくり鑑定", "統合鑑定"],
    }.get(str(course_code), [])

    for name in preferred_names:
        menu = db.scalar(select(Menu).where(Menu.name == name, Menu.is_active == True))
        if menu:
            return menu

    neutral_name = course["name"]
    existing = db.scalar(select(Menu).where(Menu.name == neutral_name))
    if existing:
        existing.price = int(course["price"])
        existing.description = str(course["description"])
        existing.lead_time_hours = 48 if str(course_code) in {"1", "2"} else 72
        existing.is_active = True
        db.flush()
        return existing

    new_menu = Menu(
        name=neutral_name,
        description=str(course["description"]),
        price=int(course["price"]),
        lead_time_hours=48 if str(course_code) in {"1", "2"} else 72,
        is_active=True,
    )
    db.add(new_menu)
    db.flush()
    return new_menu


def _course_select_prompt() -> str:
    return (
        "ご予約ありがとうございます。\n"
        "ご希望のコース番号を送ってください。\n\n"
        "1. ライト鑑定（3,000円）\n"
        "   西洋占星術\n"
        "2. スタンダード鑑定（5,000円）\n"
        "   相性鑑定 または 総合鑑定（西洋占星術＋インド占星術）\n"
        "3. じっくり鑑定（10,000円）\n"
        "   西洋占星術＋インド占星術＋四柱推命\n\n"
        "例：1 または 2 または 3\n"
        "やめる場合は『キャンセル』と送ってください。"
    )


def _course_select_prompt_after_purchase() -> str:
    return (
        "ご購入ありがとうございます。\n\n"
        "フォームURLをご案内しますので、ご購入済みのコース番号を送ってください。\n\n"
        "1. ライト鑑定（3,000円）\n"
        "2. スタンダード鑑定（5,000円）\n"
        "3. じっくり鑑定（10,000円）\n\n"
        "例：1\n"
        "やめる場合は『キャンセル』と送ってください。"
    )


def _ask_order_code_prompt(course_code: str | None) -> str:
    course_text = f"コース: {_course_label(course_code)}\n\n" if course_code else ""
    return (
        "ありがとうございます。\n\n"
        f"{course_text}"
        "STORESの注文番号を送ってください。\n"
        "10桁の数字です。\n"
        "例：0086944639"
    )


def _verify_stores_order_no(order_no: str) -> tuple[str, str | None]:
    with SessionLocal() as db:
        row = db.execute(
            text(
                """
                SELECT stores_order_no, payment_status
                FROM stores_payments
                WHERE stores_order_no = :order_no
                """
            ),
            {"order_no": order_no},
        ).mappings().first()

    if not row:
        return "not_found", None

    status = str(row.get("payment_status") or "unknown").lower()
    if status == "paid":
        return "paid", order_no
    if status in {"ordered", "pending", "unknown"}:
        return "unpaid", order_no
    if status == "cancelled":
        return "cancelled", order_no
    return "needs_review", order_no


def _build_form_reply(
    *,
    user_id: str | None,
    line_display_name: str | None,
    course_code: str | None,
    order_no: str,
) -> str:
    intake_link = _intake_url(
        course_code,
        line_user_id=user_id,
        line_display_name=line_display_name,
        payment_order_ref=order_no,
    )
    return (
        "確認できました。\n\n"
        f"コース: {_course_label(course_code)}\n\n"
        "以下のフォームにご入力ください。\n"
        f"{intake_link}\n\n"
        "※ STORES注文番号はフォームに引き継がれています。\n"
        "※ フォームでは購入時メールアドレスの入力が必要です。\n"
        "フォーム入力後に受付完了となります。"
    )


# =========================
# メインハンドラ
# =========================
def handle_order_message(
    user_id: str | None,
    text: str,
    session: dict,
    line_display_name: str | None = None,
) -> tuple[str, dict, bool, str | None]:
    text = (text or "").strip()
    state = session.get("state") or "idle"
    normalized = text.lower()

    form_trigger_values = {"購入完了", "購入済み", "直接購入", "フォーム", "フォームだけ"}
    form_triggers = {v.lower() for v in form_trigger_values}

    if text == "キャンセル":
        return (
            "今回の受付はキャンセルしました。\nまた必要になったら『予約』と送ってください。",
            {"state": "idle", "draft_order": {}, "order_code": None, "selected_course": None, "payment_order_ref": None},
            True,
            None,
        )

    if text == "やり直し":
        return (
            _course_select_prompt(),
            {"state": "course_select", "draft_order": {}, "order_code": None, "selected_course": None, "payment_order_ref": None},
            False,
            None,
        )

    if text == "続き":
        selected_course = session.get("selected_course")
        payment_order_ref = session.get("payment_order_ref")
        if state == "awaiting_order_code" and selected_course:
            return (
                _ask_order_code_prompt(selected_course),
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": payment_order_ref, "draft_order": {}, "order_code": None},
                False,
                None,
            )
        if payment_order_ref and selected_course:
            return (
                _build_form_reply(
                    user_id=user_id,
                    line_display_name=line_display_name,
                    course_code=selected_course,
                    order_no=payment_order_ref,
                ),
                {"state": "completed", "selected_course": selected_course, "payment_order_ref": payment_order_ref, "draft_order": {}, "order_code": None},
                False,
                None,
            )
        return (
            _course_select_prompt(),
            {"state": "course_select", "draft_order": {}, "order_code": None, "selected_course": None, "payment_order_ref": None},
            False,
            None,
        )

    if should_start_order(text) and normalized not in form_triggers:
        return (
            _course_select_prompt(),
            {"state": "course_select", "draft_order": {}, "order_code": None, "selected_course": None, "payment_order_ref": None},
            False,
            None,
        )

    if state == "idle" and normalized in form_triggers:
        return (
            _course_select_prompt_after_purchase(),
            {"state": "course_select_after_purchase", "draft_order": {}, "order_code": None, "selected_course": None, "payment_order_ref": None},
            False,
            None,
        )

    if state in {"idle", "completed"}:
        return (
            _not_started_message(),
            {"state": state, "draft_order": {}, "order_code": session.get("order_code"), "selected_course": session.get("selected_course"), "payment_order_ref": session.get("payment_order_ref")},
            False,
            None,
        )

    if state in {"course_select", "course_select_after_purchase"}:
        course_code = _normalize_course(text)
        if not course_code:
            prompt = _course_select_prompt() if state == "course_select" else _course_select_prompt_after_purchase()
            return (
                "コース番号は 1 / 2 / 3 のいずれかで送ってください。\n\n" + prompt,
                {"state": state, "draft_order": {}, "order_code": None, "selected_course": session.get("selected_course"), "payment_order_ref": None},
                False,
                None,
            )

        if state == "course_select_after_purchase":
            return (
                _ask_order_code_prompt(course_code),
                {"state": "awaiting_order_code", "selected_course": course_code, "payment_order_ref": None, "draft_order": {}, "order_code": None},
                False,
                None,
            )

        payment_link = _payment_item_url(course_code)
        reply = (
            "ありがとうございます。\n\n"
            f"コース: {_course_label(course_code)}\n\n"
            "▼ご購入はこちら\n"
            f"{payment_link}\n\n"
            "ご購入後、STORESの注文番号をそのまま送ってください。\n"
            "10桁の数字です。\n"
            "例：0086944639"
        )
        return (
            reply,
            {"state": "awaiting_order_code", "selected_course": course_code, "payment_order_ref": None, "draft_order": {}, "order_code": None},
            False,
            None,
        )

    if state == "awaiting_order_code":
        selected_course = session.get("selected_course")

        if normalized in form_triggers:
            return (
                _ask_order_code_prompt(selected_course),
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": session.get("payment_order_ref"), "draft_order": {}, "order_code": None},
                False,
                None,
            )

        if not _ORDER_NO_PATTERN.fullmatch(text):
            return (
                "注文番号は10桁の数字で送ってください。\n例：0086944639",
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": None, "draft_order": {}, "order_code": None},
                False,
                None,
            )

        verify_status, verified_order_no = _verify_stores_order_no(text)
        if verify_status == "not_found":
            return (
                "この注文番号はまだ確認できません。購入完了メールの注文番号をご確認のうえ、少し時間をおいて再度お試しください。",
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": None, "draft_order": {}, "order_code": None},
                False,
                None,
            )
        if verify_status == "unpaid":
            return (
                "ご注文は確認できましたが、まだ入金確認前です。入金完了後にもう一度注文番号を送ってください。",
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": None, "draft_order": {}, "order_code": None},
                False,
                None,
            )
        if verify_status == "cancelled":
            return (
                "この注文番号はキャンセル扱いになっています。別の注文番号をご確認ください。",
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": None, "draft_order": {}, "order_code": None},
                False,
                None,
            )
        if verify_status == "needs_review" or not verified_order_no:
            return (
                "注文番号は見つかりましたが確認に失敗しました。時間をおいて再度お試しください。",
                {"state": "awaiting_order_code", "selected_course": selected_course, "payment_order_ref": None, "draft_order": {}, "order_code": None},
                False,
                None,
            )

        return (
            _build_form_reply(
                user_id=user_id,
                line_display_name=line_display_name,
                course_code=selected_course,
                order_no=verified_order_no,
            ),
            {"state": "completed", "selected_course": selected_course, "payment_order_ref": verified_order_no, "draft_order": {}, "order_code": None},
            False,
            None,
        )

    return (
        _course_select_prompt(),
        {"state": "course_select", "draft_order": {}, "order_code": None, "selected_course": None, "payment_order_ref": None},
        False,
        None,
    )
