from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NormalizedLocation:
    """入力元ごとの揺れを吸収した出生地情報。

    通常フォーム・外部受注・将来のSTORES/CSV連携で、場所データが
    dict / オブジェクト / 文字列 / None のどれで来ても、計算側には
    lat/lon/prefecture/place/source/note の同じ形で渡すための共通DTOです。
    """

    lat: float | None = None
    lon: float | None = None
    prefecture: str | None = None
    place: str | None = None
    source: str | None = None
    note: str | None = None
    raw: Any = None

    @property
    def has_coords(self) -> bool:
        return self.lat is not None and self.lon is not None


def _first_present_mapping(value: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in value and value.get(key) not in (None, ""):
            return value.get(key)
    return None


def _first_present_attr(value: Any, keys: tuple[str, ...]) -> Any:
    for key in keys:
        if hasattr(value, key):
            v = getattr(value, key)
            if v not in (None, ""):
                return v
    return None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_location(
    value: Any,
    *,
    fallback_prefecture: str | None = None,
    fallback_place: str | None = None,
) -> NormalizedLocation:
    """場所情報を共通形式に正規化する。

    対応する入力例:
    - {"birth_lat": 35.68, "birth_lon": 139.69, ...}
    - {"lat": 35.68, "lon": 139.69}
    - SimpleNamespace(lat=..., lon=...)
    - SQLAlchemy/独自オブジェクトで birth_lat/birth_lon を持つもの
    - 文字列やNone（座標なしとして返す）
    """

    if isinstance(value, NormalizedLocation):
        return value

    lat = lon = None
    prefecture = fallback_prefecture
    place = fallback_place
    source = None
    note = None

    if isinstance(value, dict):
        lat = _to_float(_first_present_mapping(value, ("lat", "latitude", "birth_lat")))
        lon = _to_float(_first_present_mapping(value, ("lon", "lng", "longitude", "birth_lon")))
        prefecture = _first_present_mapping(value, ("prefecture", "birth_prefecture")) or prefecture
        place = _first_present_mapping(value, ("place", "birth_place", "display_name", "location_label")) or place
        source = _first_present_mapping(value, ("source", "location_source"))
        note = _first_present_mapping(value, ("note", "location_note"))
    elif value is not None and not isinstance(value, (str, bytes)):
        lat = _to_float(_first_present_attr(value, ("lat", "latitude", "birth_lat")))
        lon = _to_float(_first_present_attr(value, ("lon", "lng", "longitude", "birth_lon")))
        prefecture = _first_present_attr(value, ("prefecture", "birth_prefecture")) or prefecture
        place = _first_present_attr(value, ("place", "birth_place", "display_name", "location_label")) or place
        source = _first_present_attr(value, ("source", "location_source"))
        note = _first_present_attr(value, ("note", "location_note"))
    elif isinstance(value, str) and value.strip():
        place = value.strip()

    return NormalizedLocation(
        lat=lat,
        lon=lon,
        prefecture=(str(prefecture).strip() if prefecture else None),
        place=(str(place).strip() if place else None),
        source=(str(source).strip() if source else None),
        note=(str(note).strip() if note else None),
        raw=value,
    )
