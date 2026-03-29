from __future__ import annotations

from typing import Optional, Tuple
import re

import requests

# 都道府県庁所在地ベースの代表座標
PREFECTURE_COORDS: dict[str, tuple[float, float]] = {
    "北海道": (43.06417, 141.34694),
    "青森": (40.82444, 140.74000),
    "岩手": (39.70361, 141.15250),
    "宮城": (38.26889, 140.87194),
    "秋田": (39.71861, 140.10250),
    "山形": (38.24056, 140.36333),
    "福島": (37.75000, 140.46778),
    "茨城": (36.34139, 140.44667),
    "栃木": (36.56583, 139.88361),
    "群馬": (36.39111, 139.06083),
    "埼玉": (35.85694, 139.64889),
    "千葉": (35.60472, 140.12333),
    "東京": (35.68950, 139.69170),
    "神奈川": (35.44778, 139.64250),
    "新潟": (37.90222, 139.02361),
    "富山": (36.69528, 137.21139),
    "石川": (36.59444, 136.62556),
    "福井": (36.06528, 136.22194),
    "山梨": (35.66389, 138.56833),
    "長野": (36.65139, 138.18111),
    "岐阜": (35.39111, 136.72222),
    "静岡": (34.97694, 138.38306),
    "愛知": (35.18028, 136.90667),
    "三重": (34.73028, 136.50861),
    "滋賀": (35.00444, 135.86833),
    "京都": (35.02139, 135.75556),
    "大阪": (34.68639, 135.52000),
    "兵庫": (34.69139, 135.18306),
    "奈良": (34.68528, 135.83278),
    "和歌山": (34.22611, 135.16750),
    "鳥取": (35.50361, 134.23833),
    "島根": (35.47222, 133.05056),
    "岡山": (34.66167, 133.93500),
    "広島": (34.39639, 132.45944),
    "山口": (34.18583, 131.47139),
    "徳島": (34.06583, 134.55944),
    "香川": (34.34028, 134.04333),
    "愛媛": (33.84167, 132.76611),
    "高知": (33.55972, 133.53111),
    "福岡": (33.60639, 130.41806),
    "佐賀": (33.24944, 130.29889),
    "長崎": (32.74472, 129.87361),
    "熊本": (32.78972, 130.74167),
    "大分": (33.23806, 131.61250),
    "宮崎": (31.91111, 131.42389),
    "鹿児島": (31.56028, 130.55806),
    "沖縄": (26.21250, 127.68111),
}

PREFECTURE_OPTIONS: list[str] = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

PREF_ALIASES: dict[str, str] = {
    "東京都": "東京",
    "東京": "東京",
    "大阪府": "大阪",
    "大阪": "大阪",
    "京都府": "京都",
    "京都": "京都",
    "北海道": "北海道",
    "神奈川県": "神奈川",
    "埼玉県": "埼玉",
    "千葉県": "千葉",
    "福岡県": "福岡",
    "沖縄県": "沖縄",
    "tokyo": "東京",
    "tokyo-to": "東京",
    "osaka": "大阪",
    "osaka-fu": "大阪",
    "kyoto": "京都",
    "kyoto-fu": "京都",
    "hokkaido": "北海道",
    "kanagawa": "神奈川",
    "saitama": "埼玉",
    "chiba": "千葉",
    "fukuoka": "福岡",
    "okinawa": "沖縄",
}

FULL_NAME_TO_BASE: dict[str, str] = {
    "青森県": "青森", "岩手県": "岩手", "宮城県": "宮城", "秋田県": "秋田", "山形県": "山形", "福島県": "福島",
    "茨城県": "茨城", "栃木県": "栃木", "群馬県": "群馬", "埼玉県": "埼玉", "千葉県": "千葉", "東京都": "東京", "神奈川県": "神奈川",
    "新潟県": "新潟", "富山県": "富山", "石川県": "石川", "福井県": "福井", "山梨県": "山梨", "長野県": "長野",
    "岐阜県": "岐阜", "静岡県": "静岡", "愛知県": "愛知", "三重県": "三重",
    "滋賀県": "滋賀", "京都府": "京都", "大阪府": "大阪", "兵庫県": "兵庫", "奈良県": "奈良", "和歌山県": "和歌山",
    "鳥取県": "鳥取", "島根県": "島根", "岡山県": "岡山", "広島県": "広島", "山口県": "山口",
    "徳島県": "徳島", "香川県": "香川", "愛媛県": "愛媛", "高知県": "高知",
    "福岡県": "福岡", "佐賀県": "佐賀", "長崎県": "長崎", "熊本県": "熊本", "大分県": "大分", "宮崎県": "宮崎", "鹿児島県": "鹿児島", "沖縄県": "沖縄",
}


def normalize_prefecture_name(value: str) -> str:
    if not value:
        return ""
    v = value.strip()
    v_lower = v.lower()
    if v in PREF_ALIASES:
        return PREF_ALIASES[v]
    if v_lower in PREF_ALIASES:
        return PREF_ALIASES[v_lower]
    if v in FULL_NAME_TO_BASE:
        return FULL_NAME_TO_BASE[v]
    if v.endswith(("都", "府", "県")) and len(v) >= 2:
        v = v[:-1]
    return v


def infer_prefecture_name(place_text: str | None) -> str | None:
    text = (place_text or "").strip()
    if not text:
        return None
    for full_name in PREFECTURE_OPTIONS:
        if text.startswith(full_name):
            return full_name
    for full_name, base in FULL_NAME_TO_BASE.items():
        if text.startswith(base):
            return full_name
    return None


def get_prefecture_coords(prefecture: str) -> Optional[Tuple[float, float]]:
    normalized = normalize_prefecture_name(prefecture)
    return PREFECTURE_COORDS.get(normalized)


def resolve_prefecture_or_raise(prefecture: str) -> Tuple[str, float, float]:
    normalized = normalize_prefecture_name(prefecture)
    coords = PREFECTURE_COORDS.get(normalized)
    if not coords:
        raise ValueError(
            f"都道府県({prefecture})の座標が未登録です。例: 東京 / 大阪 / 福岡 のように入力してください。"
        )
    lat, lng = coords
    return normalized, lat, lng


def _looks_like_prefecture_only(prefecture: str | None, place_text: str | None) -> bool:
    place = (place_text or "").strip()
    if not place:
        return True
    normalized_place = normalize_prefecture_name(place)
    normalized_pref = normalize_prefecture_name(prefecture or "")
    if normalized_place and normalized_place == normalized_pref:
        return True
    return place in PREFECTURE_OPTIONS or place in FULL_NAME_TO_BASE or normalized_place in PREFECTURE_COORDS


def geocode_place(query: str) -> tuple[float | None, float | None, str | None]:
    q = (query or "").strip()
    if not q:
        return None, None, None
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "jsonv2", "limit": 1, "accept-language": "ja"},
            headers={"User-Agent": "nanami-astro/1.0"},
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            return None, None, None
        item = payload[0]
        return float(item["lat"]), float(item["lon"]), item.get("display_name")
    except Exception:
        return None, None, None


def resolve_birth_location(prefecture: str | None, place_text: str | None) -> dict[str, object | None]:
    pref_value = (prefecture or "").strip() or infer_prefecture_name(place_text)
    place_value = (place_text or "").strip() or None
    pref_value = pref_value or None

    if not pref_value and not place_value:
        return {
            "birth_prefecture": None,
            "birth_place": None,
            "birth_lat": None,
            "birth_lon": None,
            "location_source": None,
            "location_note": None,
        }

    if pref_value and _looks_like_prefecture_only(pref_value, place_value):
        coords = get_prefecture_coords(pref_value)
        if coords:
            lat, lon = coords
            return {
                "birth_prefecture": pref_value,
                "birth_place": place_value or pref_value,
                "birth_lat": lat,
                "birth_lon": lon,
                "location_source": "prefecture_center",
                "location_note": "都道府県代表座標を使用",
            }

    query_parts = [part for part in [pref_value, place_value] if part]
    query = " ".join(query_parts)
    lat, lon, display_name = geocode_place(query)
    if lat is not None and lon is not None:
        return {
            "birth_prefecture": pref_value,
            "birth_place": place_value or display_name or query,
            "birth_lat": lat,
            "birth_lon": lon,
            "location_source": "geocoded",
            "location_note": display_name or query,
        }

    if pref_value:
        coords = get_prefecture_coords(pref_value)
        if coords:
            lat, lon = coords
            return {
                "birth_prefecture": pref_value,
                "birth_place": place_value or pref_value,
                "birth_lat": lat,
                "birth_lon": lon,
                "location_source": "prefecture_center_fallback",
                "location_note": "住所解決に失敗したため都道府県代表座標を使用",
            }

    return {
        "birth_prefecture": pref_value,
        "birth_place": place_value,
        "birth_lat": None,
        "birth_lon": None,
        "location_source": "unresolved",
        "location_note": "座標を特定できませんでした",
    }


def format_location_summary(prefecture: str | None, place_text: str | None, lat: float | None, lon: float | None, source: str | None) -> str:
    parts = [part for part in [prefecture, place_text] if part]
    label = " / ".join(parts) if parts else "未設定"
    if lat is not None and lon is not None:
        return f"{label} ({lat:.6f}, {lon:.6f}) [{source or '-'}]"
    return f"{label} [座標未設定]"
