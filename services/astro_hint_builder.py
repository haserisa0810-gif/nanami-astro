from __future__ import annotations

from typing import Any


def _get_planets(astro_data: dict[str, Any]) -> list[dict[str, Any]]:
    planets = astro_data.get("planets")
    if isinstance(planets, list):
        return planets

    western = astro_data.get("western")
    if isinstance(western, dict) and isinstance(western.get("planets"), list):
        return western["planets"]

    return []


def _count_houses(planets: list[dict[str, Any]]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for p in planets:
        house = p.get("house")
        if isinstance(house, (int, float)):
            h = int(house)
            counts[h] = counts.get(h, 0) + 1
    return counts


def _find_planet(planets: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    for p in planets:
        if str(p.get("name") or "").strip() == name:
            return p
    return None


def build_astro_hint_line(astro_data: dict[str, Any]) -> str:
    """
    無料鑑定文に1行だけ差し込むための
    占いっぽい“根拠チラ見せ”文を返す
    """

    if not isinstance(astro_data, dict):
        return ""

    planets = _get_planets(astro_data)
    if not planets:
        return ""

    house_counts = _count_houses(planets)

    # ─────────────────────────
    # ① ハウス集中（最優先）
    # ─────────────────────────
    if house_counts.get(6, 0) >= 2:
        return "6ハウスに天体が集まる配置からも、日々の役割に強く意識が向きやすい流れが出ています。"

    if house_counts.get(5, 0) >= 2:
        return "5ハウスの強さからも、自分らしさや創造性を表現したい気持ちがはっきり表れています。"

    # ─────────────────────────
    # ② 太陽（最優先個体）
    # ─────────────────────────
    sun = _find_planet(planets, "Sun")
    if sun:
        sign = str(sun.get("sign") or "").strip()
        house = sun.get("house")

        if sign and isinstance(house, (int, float)):
            return f"太陽が{sign}の{int(house)}ハウスにある配置からも、自分らしい輝き方が人生の軸になりやすいタイプです。"

        if isinstance(house, (int, float)):
            return f"太陽が{int(house)}ハウスにある配置からも、自分らしさを表現する力が強く出ています。"

    # ─────────────────────────
    # ③ 月（内面）
    # ─────────────────────────
    moon = _find_planet(planets, "Moon")
    if moon:
        sign = str(moon.get("sign") or "").strip()
        house = moon.get("house")

        if sign and isinstance(house, (int, float)):
            return f"月が{sign}の{int(house)}ハウスにあることからも、内側ではとても繊細に物事を受け取る傾向がうかがえます。"

        return "月の配置を見ると、安心できる環境や関係性が大きな支えになりやすいタイプです。"

    # ─────────────────────────
    # ④ 水星（思考）
    # ─────────────────────────
    mercury = _find_planet(planets, "Mercury")
    if mercury:
        return "水星の配置からも、物事を細かく捉え、考えを深めていく力が強く出ています。"

    # ─────────────────────────
    # ⑤ 金星（感情・好み）
    # ─────────────────────────
    venus = _find_planet(planets, "Venus")
    if venus:
        return "金星の配置からも、心地よさや人との関係性に対する感受性の高さが見えてきます。"

    # ─────────────────────────
    # ⑥ 火星（行動力）
    # ─────────────────────────
    mars = _find_planet(planets, "Mars")
    if mars:
        return "火星の位置からも、一度スイッチが入ると一気に動ける行動力の強さが表れています。"

    # fallback
    return ""