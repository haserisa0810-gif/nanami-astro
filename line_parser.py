"""
line_parser.py
LINEユーザーのテキスト入力を解析・正規化するモジュール。
webhook本体やセッション管理には関与しない。
"""
from __future__ import annotations

import re
from typing import Any

try:
    from src.web.shared import normalize_prefecture_name  # type: ignore
except Exception:
    from shared import normalize_prefecture_name  # type: ignore


# ── 定数 ────────────────────────────────────────────────────────────────────

LINE_FIELD_ALIASES: dict[str, str] = {
    "生年月日": "birth_date",
    "誕生日": "birth_date",
    "birthdate": "birth_date",
    "birth_date": "birth_date",
    "date": "birth_date",
    "出生時刻": "birth_time",
    "生まれた時間": "birth_time",
    "時間": "birth_time",
    "時刻": "birth_time",
    "birth_time": "birth_time",
    "time": "birth_time",
    "都道府県": "prefecture",
    "都道府県名": "prefecture",
    "県": "prefecture",
    "prefecture": "prefecture",
    "出生地": "birth_place",
    "birth_place": "birth_place",
    "場所": "birth_place",
    "地名": "birth_place",
    "city": "birth_place",
    "文章量": "detail_level",
    "detail_level": "detail_level",
}

DETAIL_LEVEL_ALIASES: dict[str, str] = {
    "short": "short",
    "standard": "standard",
    "detailed": "detailed",
    "短め": "short",
    "短い": "short",
    "標準": "standard",
    "ふつう": "standard",
    "普通": "standard",
    "詳細": "detailed",
    "詳しく": "detailed",
    "長め": "detailed",
}

RESET_PATTERNS = [
    r"^やり直し$",
    r"^リセット$",
    r"^最初から$",
    r"^reset$",
    r"^clear$",
]

HELP_PATTERNS = [
    r"^help$",
    r"^ヘルプ$",
    r"^使い方$",
    r"^入力例$",
]


# ── コマンド判定 ─────────────────────────────────────────────────────────────

def should_reset(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(re.fullmatch(p, t) for p in RESET_PATTERNS)


def should_show_help(text: str) -> bool:
    t = (text or "").strip().lower()
    return any(re.fullmatch(p, t) for p in HELP_PATTERNS)


# ── テキスト正規化 ───────────────────────────────────────────────────────────

def normalize_line_key(key: str) -> str:
    key = (key or "").strip().lower()
    return LINE_FIELD_ALIASES.get(key, key)


def normalize_date_text(value: str) -> str:
    v = (value or "").strip()
    v = re.sub(r"[./年]", "-", v)
    v = re.sub(r"月", "-", v)
    v = re.sub(r"日", "", v)
    v = re.sub(r"\s+", "", v)
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", v)
    if not m:
        return (value or "").strip()
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def normalize_time_text(value: str) -> str:
    v = (value or "").strip()
    v = v.replace("：", ":").replace("時", ":").replace("分", "")
    v = re.sub(r"\s+", "", v)
    m = re.match(r"^(\d{1,2})(?::(\d{1,2}))?$", v)
    if not m:
        return (value or "").strip()
    hh = int(m.group(1))
    mm = int(m.group(2) or "0")
    if 0 <= hh <= 23 and 0 <= mm <= 59:
        return f"{hh:02d}:{mm:02d}"
    return (value or "").strip()


def normalize_detail_level(value: str) -> str:
    return DETAIL_LEVEL_ALIASES.get((value or "").strip().lower(), "standard")


def normalize_prefecture(value: str | None) -> str | None:
    if not value:
        return None
    raw = (value or "").strip()
    if not raw:
        return None
    normalized = normalize_prefecture_name(raw)
    return normalized if normalized else None


# ── 抽出ヘルパー ─────────────────────────────────────────────────────────────

def extract_any_date(text: str) -> str | None:
    m = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日)", text)
    return normalize_date_text(m.group(1)) if m else None


def extract_any_time(text: str) -> str | None:
    m = re.search(r"(\d{1,2}:\d{1,2}|\d{1,2}：\d{1,2}|\d{1,2}時\d{1,2}分|\d{1,2}時)", text)
    return normalize_time_text(m.group(1)) if m else None


def extract_detail_level(text: str) -> str | None:
    t = (text or "").strip()
    if not t:
        return None
    for key in DETAIL_LEVEL_ALIASES:
        if key in t.lower() or key in t:
            return normalize_detail_level(key)
    return None


def maybe_capture_prefecture(text: str) -> str | None:
    t = (text or "").strip()
    if not t:
        return None

    candidates = sorted(
        {
            "北海道", "東京都", "大阪府", "京都府",
            "東京", "大阪", "京都",
            "青森", "岩手", "宮城", "秋田", "山形", "福島",
            "茨城", "栃木", "群馬", "埼玉", "千葉", "神奈川",
            "新潟", "富山", "石川", "福井", "山梨", "長野",
            "岐阜", "静岡", "愛知", "三重", "滋賀", "兵庫", "奈良", "和歌山",
            "鳥取", "島根", "岡山", "広島", "山口",
            "徳島", "香川", "愛媛", "高知",
            "福岡", "佐賀", "長崎", "熊本", "大分", "宮崎", "鹿児島", "沖縄",
            "hokkaido", "tokyo", "osaka", "kyoto", "kanagawa",
            "saitama", "chiba", "fukuoka", "okinawa",
        },
        key=len,
        reverse=True,
    )
    low = t.lower()
    for cand in candidates:
        if cand.lower() in low:
            normalized = normalize_prefecture(cand)
            if normalized:
                return normalized

    m = re.search(r"([一-龯]{2,4}[都道府県])", t)
    if m:
        normalized = normalize_prefecture(m.group(1))
        if normalized:
            return normalized

    return None


def extract_birth_place(text: str) -> str | None:
    t = (text or "").strip()
    if not t:
        return None

    t = re.sub(r"^(出生地|場所|地名|birth_place|city)\s*[:：]?\s*", "", t, flags=re.I).strip()
    if not t:
        return None

    if any(token in t for token in ["生年月日", "出生時刻", "都道府県", "birth_date", "birth_time", "prefecture"]):
        return None

    if extract_any_date(t) or extract_any_time(t):
        return None

    if re.fullmatch(r"[\d\s:/年月日.-]+", t):
        return None

    pref = maybe_capture_prefecture(t)
    if pref:
        short = {pref, pref.removesuffix("都"), pref.removesuffix("道"), pref.removesuffix("府"), pref.removesuffix("県")}
        if t in short:
            return None

    return t if len(t) <= 40 else None


def sanitize_birth_place(birth_place: str | None) -> str | None:
    if not birth_place:
        return None
    if extract_any_date(birth_place) or extract_any_time(birth_place):
        return None
    if "都道府県" in birth_place or "生年月日" in birth_place:
        return None
    return birth_place


# ── パース本体 ───────────────────────────────────────────────────────────────

def _extract_labeled_value(text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    normalized = (text or "").replace("：", ":").strip()
    if ":" in normalized:
        key, value = normalized.split(":", 1)
        nk = normalize_line_key(key.strip())
        value = value.strip()
    else:
        pieces = normalized.split(None, 1)
        if len(pieces) != 2:
            return data
        nk = normalize_line_key(pieces[0].strip())
        value = pieces[1].strip()

    if nk == "birth_date":
        data["birth_date"] = normalize_date_text(value)
    elif nk == "birth_time":
        data["birth_time"] = normalize_time_text(value)
    elif nk == "prefecture":
        normalized_pref = normalize_prefecture(value)
        data["prefecture"] = normalized_pref or value
    elif nk == "birth_place":
        data["birth_place"] = value
    elif nk == "detail_level":
        data["detail_level"] = normalize_detail_level(value)
    return data


def _extract_fallbacks(text: str) -> dict[str, str]:
    found: dict[str, str] = {}
    compact = (text or "").replace("　", " ").strip()

    birth_date = extract_any_date(compact)
    if birth_date:
        found["birth_date"] = birth_date

    birth_time = extract_any_time(compact)
    if birth_time:
        found["birth_time"] = birth_time

    pref = maybe_capture_prefecture(compact)
    if pref:
        found["prefecture"] = pref

    detail = extract_detail_level(compact)
    if detail:
        found["detail_level"] = detail

    place = extract_birth_place(compact)
    if place:
        found["birth_place"] = place

    return found


def parse_line_text(text: str) -> dict[str, str]:
    """LINEユーザーのテキストを解析してフィールド辞書を返す。"""
    data: dict[str, str] = {}
    whole = (text or "").replace("　", " ").strip()

    for k, v in _extract_fallbacks(whole).items():
        data.setdefault(k, v)

    for raw_line in whole.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        labeled = _extract_labeled_value(line)
        if labeled:
            data.update(labeled)
            continue

        for k, v in _extract_fallbacks(line).items():
            data.setdefault(k, v)

    if "detail_level" in data:
        data["detail_level"] = normalize_detail_level(data["detail_level"])
    if "prefecture" in data:
        data["prefecture"] = normalize_prefecture(data["prefecture"]) or data["prefecture"]

    return data


def merge_user_state(session: dict[str, Any], parsed: dict[str, str]) -> dict[str, str]:
    """セッションデータと今回のパース結果をマージする。"""
    merged: dict[str, str] = {}
    for key in ["birth_date", "birth_time", "prefecture", "birth_place", "detail_level"]:
        value = parsed.get(key) or session.get(key)
        if isinstance(value, str) and value.strip():
            merged[key] = value.strip()
    return merged


# ── メッセージテンプレート ───────────────────────────────────────────────────

def help_text() -> str:
    return (
        "生年月日・時間・都道府県を送ってください。\n"
        "1通でまとめても、分けて送っても大丈夫です。\n\n"
        "例1）1986/2/23 7:25 宮城\n"
        "例2）生年月日: 1986-02-23\n出生時間: 7:25\n都道府県: 宮城\n"
        "例3）1986年2月23日 7時25分 宮城\n\n"
        "やり直すときは「リセット」と送ってください。"
    )


def missing_fields_message(state: dict[str, str]) -> str | None:
    """不足フィールドがあればガイドメッセージを返す。なければ None。"""
    has_date = bool(state.get("birth_date"))
    has_pref = bool(state.get("prefecture") or state.get("birth_place"))
    if not has_date and not has_pref:
        return help_text()
    if not has_date:
        return (
            "生年月日がまだ分かっていません。\n"
            "例: 1986/2/23\n"
            "時間や都道府県も一緒に送れます。"
        )
    if not has_pref:
        return (
            f"生年月日 {state['birth_date']} は受け取りました。\n"
            "次に都道府県を送ってください。\n"
            "例: 宮城 / 東京都 / 福岡"
        )
    return None
