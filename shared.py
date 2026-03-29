from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException

from prefs import PREF_COORDS, PREF_LABELS

_EN_TO_JP = {en: jp for en, jp in PREF_LABELS}
_JP_TO_EN = {jp: en for en, jp in PREF_LABELS}

# よくある省略入力も受ける
for _label, _key in list(_JP_TO_EN.items()):
    _short = _label.removesuffix("都").removesuffix("道").removesuffix("府").removesuffix("県")
    _JP_TO_EN.setdefault(_short, _key)

# 英字のゆれにも対応
for _en in list(_EN_TO_JP.keys()):
    _JP_TO_EN.setdefault(_en, _en)
    _JP_TO_EN.setdefault(_en.lower(), _en)


def prefecture_to_coord_key(prefecture: str | None) -> str | None:
    if not prefecture:
        return None
    raw = str(prefecture).strip()
    if not raw:
        return None
    key = _JP_TO_EN.get(raw) or _JP_TO_EN.get(raw.lower())
    if key:
        return key
    short = raw.removesuffix("都").removesuffix("道").removesuffix("府").removesuffix("県")
    return _JP_TO_EN.get(short) or _JP_TO_EN.get(short.lower())


def normalize_prefecture_name(prefecture: str | None) -> str | None:
    key = prefecture_to_coord_key(prefecture)
    if not key:
        return str(prefecture).strip() if prefecture else None
    return _EN_TO_JP.get(key, key)


def get_prefecture_coords(prefecture: str | None):
    key = prefecture_to_coord_key(prefecture)
    if not key:
        return None
    return PREF_COORDS.get(key)

def _age_years(birth_date_iso: str, today_iso: str) -> int | None:
    try:
        y, m, d = [int(x) for x in birth_date_iso.split("-")]
        ty, tm, td = [int(x) for x in today_iso.split("-")]
        age = ty - y
        if (tm, td) < (m, d):
            age -= 1
        return age
    except Exception:
        return None


def _attach_meta(data: Any, meta: dict[str, Any]) -> dict[str, Any]:
    if isinstance(data, dict):
        out = dict(data)
        existing = out.get("_meta")
        if isinstance(existing, dict):
            merged = dict(existing)
            merged.update(meta or {})
            out["_meta"] = merged
        else:
            out["_meta"] = dict(meta or {})
        return out
    return {"data": data, "_meta": dict(meta or {})}


# =========================
# Output formatting
# =========================
def _compress_by_sections(text: str, keep_sections: int) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    parts = re.split(r"\n(?=###\s+)", t)
    if len(parts) <= keep_sections:
        return t
    return ("\n".join(parts[:keep_sections]).strip() + "\n\n（以下省略）")


def apply_detail_level(text: str, level: str) -> str:
    if level == "short":
        return _compress_by_sections(text, keep_sections=4)
    if level == "detailed":
        return text
    return text


def format_for_line(text: str) -> str:
    t = (text or "").strip()
    return re.sub(r"\n{3,}", "\n\n", t)


def format_for_instagram(text: str) -> str:
    t = (text or "").strip()
    t = t.replace("### ", "【")
    t = t.replace("\n【", "】\n\n【")
    return re.sub(r"\n{3,}", "\n\n", t)


def format_by_style(text: str, style: str) -> str:
    if style == "line":
        return format_for_line(text)
    if style == "instagram":
        return format_for_instagram(text)
    if style == "points":
        t = (text or "").strip()
        paras = [p.strip() for p in re.split(r"\n{2,}", t) if p.strip()]
        take = paras[:6]
        cleaned = [p.replace("\n", " ") for p in take]
        bullets = "\n".join("・" + p for p in cleaned)
        return f"【要点まとめ】\n{bullets}"
    return text


# =========================
# Payload builder
# =========================
def _calc_payload_from_inputs(
    birth_date: str,
    birth_time: str | None,
    birth_place: str | None,
    prefecture: str | None,
    lat: float | None,
    lon: float | None,
    unknowns: list[str],
) -> dict[str, Any]:
    try:
        y, m, d = birth_date.split("-")
        year, month, day = int(y), int(m), int(d)
    except Exception:
        raise HTTPException(status_code=400, detail="生年月日の形式が不正です（YYYY-MM-DD）")

    bt = (birth_time or "").strip()
    if bt:
        try:
            hh, mm = bt.split(":")
            hour, minute = int(hh), int(mm)
        except Exception:
            raise HTTPException(status_code=400, detail="出生時刻の形式が不正です（HH:MM）")
    else:
        hour, minute = 12, 0
        unknowns.append("出生時刻が未入力のため12:00として計算しています。")

    normalized_prefecture = None
    if prefecture:
        normalized_prefecture = normalize_prefecture_name(prefecture)
        coords = get_prefecture_coords(prefecture) or PREF_COORDS.get(prefecture)
        if coords:
            lat, lon = coords
        else:
            unknowns.append(
                f"都道府県({prefecture})の座標が未登録です。例: 東京 / 大阪 / 福岡 のように入力してください。"
            )

    # 地図選択 or 都道府県がない場合は計算に必要な座標が足りない
    if lat is None or lon is None:
        raise HTTPException(
            status_code=400,
            detail="出生地は都道府県を選ぶか、地図で地点を選択してください。"
        )

    city = (birth_place or "").strip() or (normalized_prefecture or prefecture or "").strip()
    if not city:
        city = f"{float(lat):.6f},{float(lon):.6f}"

    return {
        "year": year,
        "month": month,
        "day": day,
        "hour": hour,
        "minute": minute,
        "lat": float(lat),
        "lng": float(lon),
        "city": city,

        # FreeAstro寄せの既定値
        "house_system": "P",       # Placidus
        "zodiac_type": "tropical",
        "node_mode": "true",
        "lilith_mode": "mean",
        "include_asteroids": True,
        "include_chiron": True,
        "include_lilith": True,
        "include_vertex": True,
    }
