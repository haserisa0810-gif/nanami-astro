
from __future__ import annotations

import html
import json
import math
import re
from pathlib import Path
from typing import Any

from models import Order, YamlLog
from services.shichu_formatter import extract_shichu_data, render_shichu_summary_html, render_shichu_table_html

SIGN_JA = ["牡羊", "牡牛", "双子", "蟹", "獅子", "乙女", "天秤", "蠍", "射手", "山羊", "水瓶", "魚"]
VEDIC_PLANET_LABELS = {
    "Sun": "SURYA", "Moon": "CHANDRA", "Mars": "MANGALA", "Mercury": "BUDHA", "Jupiter": "GURU",
    "Venus": "SHUKRA", "Saturn": "SHANI", "Rahu": "RAHU", "Ketu": "KETU", "Ascendant": "LAGNA",
}
PLANET_SYMBOLS = {
    "Sun": "☉", "Moon": "☽", "Mercury": "☿", "Venus": "♀", "Mars": "♂", "Jupiter": "♃", "Saturn": "♄",
    "Uranus": "♅", "Neptune": "♆", "Pluto": "♇", "ASC": "ASC", "MC": "MC", "North Node": "☊", "South Node": "☋",
    "Chiron": "⚷", "Lilith": "⚸", "Vertex": "Vx"
}
HOUSE_MEANINGS = {
    1: "1H：外見・第一印象・自己の出方の領域", 2: "2H：お金・所有・価値観・資産の領域", 3: "3H：学び・発信・近距離移動の領域",
    4: "4H：家庭・居場所・基盤の領域", 5: "5H：創造・自己表現・楽しみの領域", 6: "6H：日常・仕事・健康・習慣の領域",
    7: "7H：対人関係・パートナーシップの領域", 8: "8H：共有・深い結びつき・変容の領域", 9: "9H：思想・探求・遠方の領域",
    10: "10H：社会的地位・職業・使命の領域", 11: "11H：社会・仲間・理想・未来の領域", 12: "12H：無意識・癒し・見えない領域"
}
VEDIC_ROLE_DEFAULTS = {
    "Sun": "魂・父性・権威", "Moon": "心・母性・感情", "Mars": "行動力・闘志・突破力", "Mercury": "知性・言語・商才",
    "Jupiter": "知恵・拡大・保護", "Venus": "愛・美意識・享受", "Saturn": "責任・忍耐・課題", "Rahu": "執着・拡張・欲望", "Ketu": "手放し・霊性・切離"
}
PLANET_ROLE_DEFAULTS = {
    "Sun": "人生のテーマ・自己表現", "Moon": "感情・安心感のパターン", "Mercury": "思考・コミュニケーション",
    "Venus": "愛情表現・価値観", "Mars": "行動力・エネルギーの使い方", "Jupiter": "拡大・幸運・成長の方向",
    "Saturn": "責任・課題・成熟のテーマ", "ASC": "第一印象・外への出方", "MC": "社会的な方向性・キャリア"
}
DEBUG_MARKERS = ("DEBUG", "空文字を返しました")


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _nl2br(value: Any) -> str:
    return html.escape(_safe_text(value)).replace("\n", "<br>")


def _sign_to_ja(sign: Any) -> str:
    s = _safe_text(sign).strip()
    mapping = {
        "Aries": "牡羊", "Taurus": "牡牛", "Gemini": "双子", "Cancer": "蟹", "Leo": "獅子", "Virgo": "乙女",
        "Libra": "天秤", "Scorpio": "蠍", "Sagittarius": "射手", "Capricorn": "山羊", "Aquarius": "水瓶", "Pisces": "魚",
        "Ari": "牡羊", "Tau": "牡牛", "Gem": "双子", "Can": "蟹", "Vir": "乙女", "Lib": "天秤",
        "Sco": "蠍", "Sag": "射手", "Cap": "山羊", "Aqu": "水瓶", "Pis": "魚",
    }
    return mapping.get(s, s or "-")


def _is_meaningful_text(value: Any) -> bool:
    text = _safe_text(value).strip()
    return bool(text) and not any(marker in text for marker in DEBUG_MARKERS)


def _first_meaningful(*values: Any) -> str:
    for value in values:
        text = _safe_text(value).strip()
        if _is_meaningful_text(text):
            return text
    return ""


def _deep_get(obj: Any, *paths: str) -> Any:
    for path in paths:
        cur = obj
        ok = True
        for part in path.split('.'):
            if isinstance(cur, dict) and part in cur:
                cur = cur.get(part)
            else:
                ok = False
                break
        if ok:
            return cur
    return None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _raw_western(raw: dict[str, Any]) -> dict[str, Any]:
    western = raw.get('western')
    return western if isinstance(western, dict) else raw


def _raw_vedic(raw: dict[str, Any]) -> dict[str, Any]:
    vedic = raw.get('vedic')
    return vedic if isinstance(vedic, dict) else raw if raw.get('system') == 'vedic' else {}


def _is_compatibility_raw(raw: dict[str, Any]) -> bool:
    return isinstance(raw.get('personA'), dict) and isinstance(raw.get('personB'), dict)


def _compat_people(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if _is_compatibility_raw(raw):
        return _dict(raw.get('personA')), _dict(raw.get('personB'))
    return {}, {}


def _format_birth_label(value: Any, birth_time: Any = None) -> str:
    text = _safe_text(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        y, m, d = text.split('-')
        text = f"{y}年{m}月{d}日"
    if birth_time:
        bt = _safe_text(birth_time).strip()
        if bt:
            text = (text + f" {bt}").strip()
    return text or '-'


def _planet_items(raw: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    western = _raw_western(raw)
    planets = western.get("planets") or raw.get("planets") or []
    if not isinstance(planets, list):
        return out
    for p in planets:
        if not isinstance(p, dict):
            continue
        name = p.get("name") or p.get("planet") or "-"
        sign = _sign_to_ja(p.get("sign") or "-")
        deg = p.get("degree")
        deg_text = f" {int(round(float(deg)))}°" if isinstance(deg, (int, float)) else ""
        house = p.get("house") or p.get("house_num") or "-"
        house_num = None
        if isinstance(house, (int, float)):
            house_num = int(house)
            house_text = f"{house_num}ハウス"
        else:
            hs = _safe_text(house)
            m = re.search(r"(\d+)", hs)
            if m:
                house_num = int(m.group(1))
            house_text = hs if hs else "-"
        out.append({
            "name": name,
            "sign": f"{sign}{deg_text}",
            "house": house_text,
            "note": p.get("note") or PLANET_ROLE_DEFAULTS.get(name, "-"),
            "house_desc": HOUSE_MEANINGS.get(house_num, ""),
            "lon": p.get("lon") if isinstance(p.get("lon"), (int, float)) else p.get("longitude") if isinstance(p.get("longitude"), (int, float)) else None,
        })
    return out


def _vedic_items(raw: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    vedic = _raw_vedic(raw)
    planets = vedic.get('planets') or []
    if not isinstance(planets, list):
        return out
    for p in planets:
        if not isinstance(p, dict):
            continue
        name = _safe_text(p.get('name') or p.get('planet'))
        sign = _safe_text(p.get('rashi_name') or p.get('sign') or '-')
        deg = p.get('deg_in_sign')
        deg_text = f" {int(round(float(deg)))}°" if isinstance(deg, (int, float)) else ""
        house_no = p.get('house_no')
        house_text = f"{int(house_no)}ハウス" if isinstance(house_no, (int, float)) else '-'
        house_desc = HOUSE_MEANINGS.get(int(house_no), '') if isinstance(house_no, (int, float)) else ''
        out.append({
            'name': VEDIC_PLANET_LABELS.get(name, name.upper()),
            'sign': f"{sign}{deg_text}",
            'house': house_text,
            'note': VEDIC_ROLE_DEFAULTS.get(name, '-'),
            'house_desc': house_desc,
            'rashi_name': sign,
            'lon': p.get('sidereal_lon_deg') if isinstance(p.get('sidereal_lon_deg'), (int, float)) else None,
        })
    return out


def _chart_svg(raw: dict[str, Any], size: int = 520) -> str:
    planets = _planet_items(raw)
    if not planets:
        return ""
    cx = cy = size / 2
    outer = size * 0.42
    mid = size * 0.34
    inner = size * 0.16
    parts = [f"<svg viewBox='0 0 {size} {size}' width='{size}' height='{size}' style='display:block;max-width:100%;height:auto' xmlns='http://www.w3.org/2000/svg'>"]
    parts.append("<defs><filter id='glow'><feGaussianBlur stdDeviation='2.2' result='b'/><feMerge><feMergeNode in='b'/><feMergeNode in='SourceGraphic'/></feMerge></filter></defs>")
    parts.append(f"<circle cx='{cx}' cy='{cy}' r='{outer}' fill='none' stroke='rgba(201,169,110,0.72)' stroke-width='1.5'/>")
    parts.append(f"<circle cx='{cx}' cy='{cy}' r='{mid}' fill='none' stroke='rgba(201,169,110,0.34)' stroke-width='1'/>")
    parts.append(f"<circle cx='{cx}' cy='{cy}' r='{inner}' fill='none' stroke='rgba(201,169,110,0.22)' stroke-width='1'/>")
    for i in range(12):
        ang = math.radians(-90 + i * 30)
        x2 = cx + outer * math.cos(ang)
        y2 = cy + outer * math.sin(ang)
        parts.append(f"<line x1='{cx}' y1='{cy}' x2='{x2}' y2='{y2}' stroke='rgba(201,169,110,0.18)' stroke-width='1'/>")
        label_ang = math.radians(-75 + i * 30)
        lx = cx + (outer + 24) * math.cos(label_ang)
        ly = cy + (outer + 24) * math.sin(label_ang)
        parts.append(f"<text x='{lx:.1f}' y='{ly:.1f}' text-anchor='middle' dominant-baseline='middle' fill='rgba(232,213,176,0.94)' font-size='18' font-family='Noto Serif JP, serif'>{SIGN_JA[i]}</text>")
        hx = cx + (mid - 18) * math.cos(label_ang)
        hy = cy + (mid - 18) * math.sin(label_ang)
        parts.append(f"<text x='{hx:.1f}' y='{hy:.1f}' text-anchor='middle' dominant-baseline='middle' fill='rgba(232,224,212,0.68)' font-size='15'>{i+1}</text>")
    sign_index = {name: i for i, name in enumerate(SIGN_JA)}
    placed: list[tuple[float, float, dict[str, Any]]] = []
    longs: list[float] = []
    for idx, p in enumerate(planets[:14]):
        lon = p.get('lon')
        if lon is None:
            sign = _safe_text(p.get('sign')).split()[0]
            sidx = sign_index.get(sign, idx % 12)
            lon = sidx * 30 + (idx % 3) * 6 + 5
        longs.append(float(lon))
        angle = math.radians(float(lon) - 90)
        r = mid + 28
        x = cx + r * math.cos(angle)
        y = cy + r * math.sin(angle)
        placed.append((x, y, p))
    for i in range(len(placed)):
        for j in range(i + 1, len(placed)):
            diff = abs((longs[i] - longs[j] + 180) % 360 - 180)
            color = None
            if abs(diff - 180) <= 6 or abs(diff - 90) <= 5:
                color = 'rgba(196,120,138,0.48)'
            elif abs(diff - 120) <= 5 or abs(diff - 60) <= 4:
                color = 'rgba(122,154,184,0.44)'
            if color:
                x1, y1, _ = placed[i]
                x2, y2, _ = placed[j]
                parts.append(f"<line x1='{x1:.1f}' y1='{y1:.1f}' x2='{x2:.1f}' y2='{y2:.1f}' stroke='{color}' stroke-width='1.18'/>")
    for x, y, p in placed:
        sym = PLANET_SYMBOLS.get(_safe_text(p.get('name')), _safe_text(p.get('name'))[:2])
        parts.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='16' fill='rgba(41,33,58,0.96)' stroke='rgba(201,169,110,0.66)' stroke-width='1.7' filter='url(#glow)'/>")
        parts.append(f"<text x='{x:.1f}' y='{y+1:.1f}' text-anchor='middle' dominant-baseline='middle' fill='rgba(232,224,212,0.98)' font-size='16'>{html.escape(sym)}</text>")
    parts.append('</svg>')
    return ''.join(parts)


def _synastry_aspects(person_a: dict[str, Any], person_b: dict[str, Any]) -> list[dict[str, Any]]:
    a_map = { _safe_text(p.get('name')): p for p in _planet_items(person_a) if _safe_text(p.get('name')) }
    b_map = { _safe_text(p.get('name')): p for p in _planet_items(person_b) if _safe_text(p.get('name')) }
    preferred = [
        ('Sun', 'Moon'), ('Moon', 'Sun'), ('Venus', 'Mars'), ('Mars', 'Venus'),
        ('Sun', 'ASC'), ('ASC', 'Sun'), ('Moon', 'Moon'), ('MC', 'MC'),
    ]
    aspect_defs = [
        ('Conjunction', 0, 7), ('Sextile', 60, 5), ('Square', 90, 6),
        ('Trine', 120, 6), ('Opposition', 180, 7),
    ]
    found: list[dict[str, Any]] = []
    for left, right in preferred:
        pa = a_map.get(left)
        pb = b_map.get(right)
        if not pa or not pb:
            continue
        la = pa.get('lon')
        lb = pb.get('lon')
        if la is None or lb is None:
            continue
        diff = abs((float(la) - float(lb) + 180.0) % 360.0 - 180.0)
        hit = None
        for label, target, orb in aspect_defs:
            if abs(diff - target) <= orb:
                hit = (label, target)
                break
        if not hit:
            continue
        found.append({
            'planet1': left, 'planet2': right, 'aspect': hit[0], 'angle': hit[1], 'diff': round(diff, 1),
        })
        if len(found) >= 4:
            break
    return found


def _synastry_chart_svg(person_a: dict[str, Any], person_b: dict[str, Any], size: int = 520) -> str:
    planets_a = _planet_items(person_a)
    planets_b = _planet_items(person_b)
    if not planets_a or not planets_b:
        return ''
    cx = cy = size / 2
    outer = size * 0.42
    sign_r = size * 0.34
    house_r = size * 0.26
    outer_planet_r = size * 0.29
    inner_planet_r = size * 0.19
    aspect_r = size * 0.15
    parts = [f"<svg viewBox='0 0 {size} {size}' width='100%' height='100%' xmlns='http://www.w3.org/2000/svg'>"]
    parts.append("<defs><filter id='glow'><feGaussianBlur stdDeviation='1.8' result='b'/><feMerge><feMergeNode in='b'/><feMergeNode in='SourceGraphic'/></feMerge></filter></defs>")
    parts.append(f"<rect x='0' y='0' width='{size}' height='{size}' fill='#1e1a2b'/>")
    sign_jp = SIGN_JA
    # sign ring
    for i in range(12):
        s = -90 + i * 30
        e = s + 30
        p1 = (cx + outer * math.cos(math.radians(s)), cy + outer * math.sin(math.radians(s)))
        p2 = (cx + outer * math.cos(math.radians(e)), cy + outer * math.sin(math.radians(e)))
        p3 = (cx + sign_r * math.cos(math.radians(e)), cy + sign_r * math.sin(math.radians(e)))
        p4 = (cx + sign_r * math.cos(math.radians(s)), cy + sign_r * math.sin(math.radians(s)))
        parts.append(f"<path d='M {p4[0]:.1f} {p4[1]:.1f} L {p1[0]:.1f} {p1[1]:.1f} A {outer:.1f} {outer:.1f} 0 0 1 {p2[0]:.1f} {p2[1]:.1f} L {p3[0]:.1f} {p3[1]:.1f} A {sign_r:.1f} {sign_r:.1f} 0 0 0 {p4[0]:.1f} {p4[1]:.1f} Z' fill='rgba(30,24,40,0.18)' stroke='rgba(201,169,110,0.18)' stroke-width='1'/>")
        mid = math.radians(-75 + i * 30)
        tx = cx + ((outer + sign_r) / 2) * math.cos(mid)
        ty = cy + ((outer + sign_r) / 2) * math.sin(mid)
        parts.append(f"<text x='{tx:.1f}' y='{ty:.1f}' text-anchor='middle' dominant-baseline='middle' font-size='15' font-weight='700' fill='rgba(232,224,212,0.88)'>{sign_jp[i]}</text>")
    for r, stroke, sw in [
        (outer, 'rgba(201,169,110,0.58)', 1.5),
        (sign_r, 'rgba(201,169,110,0.42)', 1.2),
        (house_r, 'rgba(201,169,110,0.22)', 1),
        (outer_planet_r, 'rgba(201,169,110,0.16)', 1),
        (inner_planet_r, 'rgba(201,169,110,0.16)', 1),
        (aspect_r, 'rgba(201,169,110,0.20)', 1),
    ]:
        parts.append(f"<circle cx='{cx}' cy='{cy}' r='{r:.1f}' fill='none' stroke='{stroke}' stroke-width='{sw}'/>")
    houses = (_raw_western(person_a).get('houses') or person_a.get('houses') or [])
    if isinstance(houses, list):
        for i, h in enumerate(houses[:12]):
            lon = h.get('lon') if isinstance(h, dict) and isinstance(h.get('lon'), (int, float)) else None
            if lon is None and isinstance(h, dict):
                lon = h.get('abs_pos') if isinstance(h.get('abs_pos'), (int, float)) else None
            if lon is None:
                lon = i * 30
            ang = math.radians(float(lon) - 90)
            x2 = cx + outer * math.cos(ang)
            y2 = cy + outer * math.sin(ang)
            x1 = cx + aspect_r * math.cos(ang)
            y1 = cy + aspect_r * math.sin(ang)
            parts.append(f"<line x1='{x1:.1f}' y1='{y1:.1f}' x2='{x2:.1f}' y2='{y2:.1f}' stroke='rgba(201,169,110,0.22)' stroke-width='1'/>")
    glyph = PLANET_SYMBOLS
    def place(points, radius, fill, stroke, text_fill):
        occupied: list[float] = []
        placed = []
        for p in points:
            lon = p.get('lon')
            if lon is None:
                continue
            deg = float(lon)
            r = radius
            for other in occupied:
                diff = abs((deg - other + 180.0) % 360.0 - 180.0)
                if diff < 6:
                    r -= 12
            occupied.append(deg)
            ang = math.radians(deg - 90)
            x = cx + r * math.cos(ang)
            y = cy + r * math.sin(ang)
            sym = glyph.get(_safe_text(p.get('name')), _safe_text(p.get('name'))[:2] or '•')
            parts.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='12' fill='{fill}' stroke='{stroke}' stroke-width='1.3' filter='url(#glow)'/>")
            parts.append(f"<text x='{x:.1f}' y='{y+0.5:.1f}' text-anchor='middle' dominant-baseline='middle' font-size='12.5' font-weight='700' fill='{text_fill}'>{html.escape(sym)}</text>")
            placed.append((x, y, p))
        return placed
    major = {'Sun','Moon','Mercury','Venus','Mars','Jupiter','Saturn','ASC','MC'}
    pa = [p for p in planets_a if _safe_text(p.get('name')) in major]
    pb = [p for p in planets_b if _safe_text(p.get('name')) in major]
    placed_a = place(pa, inner_planet_r, 'rgba(37,30,49,0.96)', '#c9a96e', '#efe6d6')
    placed_b = place(pb, outer_planet_r, 'rgba(27,22,38,0.96)', '#c9a96e', '#efe6d6')

    map_a = {_safe_text(p.get('name')): (x, y) for x, y, p in placed_a}
    map_b = {_safe_text(p.get('name')): (x, y) for x, y, p in placed_b}
    aspect_colors = {
        'Conjunction': 'rgba(201,169,110,0.62)',
        'Sextile': 'rgba(201,169,110,0.34)',
        'Square': 'rgba(201,169,110,0.30)',
        'Trine': 'rgba(201,169,110,0.46)',
        'Opposition': 'rgba(201,169,110,0.46)',
    }
    for asp in _synastry_aspects(person_a, person_b):
        a_pt = map_a.get(asp['planet1'])
        b_pt = map_b.get(asp['planet2'])
        if not a_pt or not b_pt:
            continue
        parts.append(f"<line x1='{a_pt[0]:.1f}' y1='{a_pt[1]:.1f}' x2='{b_pt[0]:.1f}' y2='{b_pt[1]:.1f}' stroke='{aspect_colors.get(asp['aspect'], '#a0a0a0')}' stroke-width='1.2' opacity='0.82'/>")
    parts.append(f"<text x='{cx - 78:.1f}' y='{size - 16:.1f}' font-size='11' fill='rgba(201,169,110,0.92)'>内円：あなた</text>")
    parts.append(f"<text x='{cx + 14:.1f}' y='{size - 16:.1f}' font-size='11' fill='rgba(201,169,110,0.78)'>外円：お相手</text>")
    parts.append('</svg>')
    return ''.join(parts)


def _vedic_chart_svg(raw: dict[str, Any], size: int = 520) -> str:
    vedic = _raw_vedic(raw)
    planets = _vedic_items(raw)
    if not vedic or not planets:
        return ""
    margin = 16
    s = size - margin * 2
    x0 = y0 = margin
    x1 = y1 = x0 + s
    xm = x0 + s / 2
    ym = y0 + s / 2
    # South Indian style fixed sign layout
    boxes = {
        1:  (x0, y0 + s*0.75, x0+s*0.25, y1),
        2:  (x0, y0 + s*0.50, x0+s*0.25, y0+s*0.75),
        3:  (x0, y0 + s*0.25, x0+s*0.25, y0+s*0.50),
        4:  (x0, y0, x0+s*0.25, y0+s*0.25),
        5:  (x0+s*0.25, y0, xm, y0+s*0.25),
        6:  (xm, y0, x0+s*0.75, y0+s*0.25),
        7:  (x0+s*0.75, y0, x1, y0+s*0.25),
        8:  (x0+s*0.75, y0+s*0.25, x1, y0+s*0.50),
        9:  (x0+s*0.75, y0+s*0.50, x1, y0+s*0.75),
        10: (x0+s*0.75, y0+s*0.75, x1, y1),
        11: (xm, y0+s*0.75, x0+s*0.75, y1),
        12: (x0+s*0.25, y0+s*0.75, xm, y1),
    }
    # house mapping by ascendant sign number
    asc = _dict(vedic.get('ascendant'))
    asc_rashi_no = int(asc.get('rashi_no') or 1)
    house_by_sign = {((asc_rashi_no - 1 + h - 1) % 12) + 1: h for h in range(1, 13)}
    occupants = {i: [] for i in range(1,13)}
    for p in vedic.get('planets') or []:
        if not isinstance(p, dict):
            continue
        sign_no = p.get('rashi_no')
        if not isinstance(sign_no, int):
            continue
        label = VEDIC_PLANET_LABELS.get(_safe_text(p.get('name')), _safe_text(p.get('name')).upper())
        deg = p.get('deg_in_sign')
        deg_txt = f" {int(round(float(deg)))}°" if isinstance(deg, (int, float)) else ""
        occupants[sign_no].append(label + deg_txt)
    parts = [f"<svg viewBox='0 0 {size} {size}' width='100%' height='100%' xmlns='http://www.w3.org/2000/svg'>"]
    parts.append(f"<rect x='{x0}' y='{y0}' width='{s}' height='{s}' fill='none' stroke='rgba(122,154,184,0.62)' stroke-width='2'/>")
    # internal lines
    for xx in [x0+s*0.25, xm, x0+s*0.75]:
        parts.append(f"<line x1='{xx}' y1='{y0}' x2='{xx}' y2='{y1}' stroke='rgba(122,154,184,0.20)' stroke-width='1'/>")
    for yy in [y0+s*0.25, ym, y0+s*0.75]:
        parts.append(f"<line x1='{x0}' y1='{yy}' x2='{x1}' y2='{yy}' stroke='rgba(122,154,184,0.20)' stroke-width='1'/>")
    parts.append(f"<rect x='{x0+s*0.25}' y='{y0+s*0.25}' width='{s*0.5}' height='{s*0.5}' fill='rgba(35,29,46,0.92)' stroke='rgba(122,154,184,0.28)' stroke-width='1.2'/>")
    for sign_no, (bx0, by0, bx1, by1) in boxes.items():
        cx = (bx0 + bx1) / 2
        cy = (by0 + by1) / 2
        house_no = house_by_sign.get(sign_no, sign_no)
        sign_label = _safe_text(_dict(next((p for p in vedic.get('planets', []) if isinstance(p, dict) and p.get('rashi_no') == sign_no), {})).get('rashi_name')) or SIGN_JA[sign_no-1]
        occ = occupants.get(sign_no) or []
        text = '<br/>'.join(html.escape(t) for t in occ[:4])
        # sign / house small labels
        parts.append(f"<text x='{bx0+8:.1f}' y='{by0+16:.1f}' fill='rgba(122,154,184,0.92)' font-size='11'>{sign_no}</text>")
        parts.append(f"<text x='{bx1-8:.1f}' y='{by0+16:.1f}' text-anchor='end' fill='rgba(232,224,212,0.74)' font-size='10'>{house_no}H</text>")
        parts.append(f"<text x='{cx:.1f}' y='{cy-12:.1f}' text-anchor='middle' fill='rgba(232,224,212,0.92)' font-size='13' font-family='Noto Serif JP, serif'>{html.escape(sign_label)}</text>")
        if occ:
            dy = 4
            for idx, line in enumerate(occ[:4]):
                parts.append(f"<text x='{cx:.1f}' y='{cy+dy+idx*14:.1f}' text-anchor='middle' fill='rgba(232,224,212,0.88)' font-size='11'>{html.escape(line)}</text>")
        if sign_no == asc_rashi_no:
            parts.append(f"<text x='{cx:.1f}' y='{by1-8:.1f}' text-anchor='middle' fill='rgba(122,154,184,0.95)' font-size='11'>Lagna</text>")
    parts.append(f"<text x='{xm:.1f}' y='{ym-8:.1f}' text-anchor='middle' fill='rgba(122,154,184,0.95)' font-size='14' font-family='Cinzel, serif'>RASHI CHART</text>")
    parts.append(f"<text x='{xm:.1f}' y='{ym+14:.1f}' text-anchor='middle' fill='rgba(232,224,212,0.70)' font-size='11'>South Indian style / Lahiri</text>")
    parts.append('</svg>')
    return ''.join(parts)


def _replace_nth_tbody(tpl: str, body_html: str, nth: int) -> str:
    pattern = re.compile(r"<tbody>.*?</tbody>", flags=re.S)
    matches = list(pattern.finditer(tpl))
    if len(matches) < nth:
        return tpl
    m = matches[nth - 1]
    return tpl[:m.start()] + "<tbody>" + body_html + "</tbody>" + tpl[m.end():]


def _replace_table_after_heading(tpl: str, heading_label: str, body_html: str, table_class: str = 'planet-table') -> str:
    pattern = re.compile(
        rf'(\<span class="system-heading-label[^"]*">{re.escape(heading_label)}\</span>.*?\<table class="{re.escape(table_class)}">\s*\<tbody\>)(.*?)(\</tbody\>)',
        flags=re.S,
    )
    return pattern.sub(rf'\1{body_html}\3', tpl, count=1)


def _replace_first_reading_text(tpl: str, body_html: str) -> str:
    return re.sub(r'<div class="reading-text">.*?</div>', f'<div class="reading-text">{body_html}</div>', tpl, count=1, flags=re.S)


def _render_planet_rows(planets: list[dict[str, Any]]) -> str:
    rows = []
    if not planets:
        return "<tr><td colspan='4' class='pt-role'>主要天体データはありません。</td></tr>"
    for p in planets[:10]:
        name = html.escape(_safe_text(p.get('name')).upper())
        sign = html.escape(_safe_text(p.get('sign')))
        house = html.escape(_safe_text(p.get('house')))
        note = html.escape(_safe_text(p.get('note')))
        house_desc = html.escape(_safe_text(p.get('house_desc')))
        rows.append(f"<tr><td class='pt-name'>{name}</td><td class='pt-sign'>{sign}</td><td class='pt-house'>{house}</td><td class='pt-role'>{note}<span class='pt-house-desc'>{house_desc}</span></td></tr>")
    return ''.join(rows)



def _person_subtitle(person: dict[str, Any]) -> str:
    birth_label = _safe_text(person.get('birth_label')).strip()
    place_label = _safe_text(person.get('place_label')).strip()
    sun_sign = _safe_text(person.get('sun_sign')).strip()
    parts = [p for p in [birth_label, place_label] if p and p != '-']
    tail = ' ／ '.join(parts)
    if sun_sign and sun_sign != '-':
        tail = (tail + f" ／ {sun_sign}座") if tail else f"{sun_sign}座"
    return tail or '-'


def _person_birth_info(person: dict[str, Any]) -> str:
    birth_label = _safe_text(person.get('birth_label')).strip()
    place_label = _safe_text(person.get('place_label')).strip()
    parts = [p for p in [birth_label, place_label] if p and p != '-']
    return ' ／ '.join(parts) or '-'


def _compatibility_aspect_cards(aspects: list[dict[str, Any]]) -> str:
    if not aspects:
        return (
            "<div class='aspect-item'>"
            "<div class='aspect-head'>主要アスペクトを表示できませんでした</div>"
            "<div class='aspect-body'>出生データから十分な相性アスペクトを抽出できなかったため、本文中心でご確認ください。</div>"
            "</div>"
        )
    labels = {
        'Conjunction': 'コンジャンクション',
        'Sextile': 'セクスタイル',
        'Square': 'スクエア',
        'Trine': 'トライン',
        'Opposition': 'オポジション',
    }
    meanings = {
        ('Sun', 'Moon'): '意識と感情が結びつきやすく、相手の存在が日常の温度感に影響しやすい配置です。',
        ('Moon', 'Sun'): '感情の受け取り方と自己表現が噛み合いやすく、安心感と印象が強く残りやすい組み合わせです。',
        ('Venus', 'Mars'): '魅力の感じ方と行動の熱量が刺激し合い、惹かれやすさと温度差の両方が出やすい配置です。',
        ('Mars', 'Venus'): '行動の勢いが愛情表現を動かしやすく、関係が進みやすい一方でペース差も生まれやすいです。',
        ('Moon', 'Moon'): '心の揺れ方や安心ポイントが似るかぶつかるかが、関係の居心地を大きく左右します。',
        ('Sun', 'ASC'): '第一印象や存在感に強く影響しやすく、会った瞬間の引力が出やすい配置です。',
        ('ASC', 'Sun'): '相手の自己表現が外側の印象に直接届きやすく、自然と目を引きやすい関係です。',
        ('MC', 'MC'): '社会的な方向性や将来像の重なりを見やすく、長期の現実感に関わる組み合わせです。',
    }
    rows = []
    for asp in aspects[:6]:
        p1 = _safe_text(asp.get('planet1'))
        p2 = _safe_text(asp.get('planet2'))
        label = labels.get(_safe_text(asp.get('aspect')), _safe_text(asp.get('aspect')) or '主要ポイント')
        angle = _safe_text(asp.get('angle'))
        diff = _safe_text(asp.get('diff'))
        body = meanings.get((p1, p2)) or f"{p1}と{p2}の間に{label}傾向があり、関係のリズムや反応の仕方に特徴が出やすい組み合わせです。"
        meta = f"{p1} × {p2}"
        if angle or diff:
            meta += f" ／ {label}"
            if diff:
                meta += f"（差 {diff}°）"
        rows.append(
            "<div class='aspect-item'>"
            f"<div class='aspect-head'>{meta}</div>"
            f"<div class='aspect-body'>{body}</div>"
            "</div>"
        )
    return ''.join(rows)


def _template_name(payload: dict[str, Any]) -> str:
    raw = _dict(payload.get('raw_json'))
    if _is_compatibility_raw(raw):
        return 'report_compatibility.html'
    systems = payload.get('systems') or {}
    chart_count = sum([
        bool(systems.get('western')),
        bool(systems.get('vedic')),
        bool(systems.get('shichu')),
    ])
    if chart_count >= 3:
        return 'report-triple.html'
    if chart_count == 2:
        return 'report-double.html'
    return 'report-single.html'


def _replace_chart_block(tpl: str, placeholder_note: str, inner_html: str) -> str:
    pattern = re.compile(
        rf'<div class="chart-img-wrap[^>]*">(?:(?!<div class=\"chart-img-wrap).)*?{re.escape(placeholder_note)}(?:(?!<div class=\"chart-img-wrap).)*?</div>',
        flags=re.S,
    )
    return pattern.sub(inner_html, tpl, count=1)


def build_result_payload(order: Order, yaml_log: YamlLog, delivery_text: str | None = None) -> dict[str, Any]:
    try:
        data = json.loads(yaml_log.summary_json or "{}") if yaml_log.summary_json else {}
    except Exception:
        data = {}
    reports = data.get("reports") if isinstance(data.get("reports"), dict) else {}
    structure = data.get("structure_summary") if isinstance(data.get("structure_summary"), dict) else {}
    raw = data.get("raw_json") if isinstance(data.get("raw_json"), dict) else {}
    payload_json = data.get("payload_json") if isinstance(data.get("payload_json"), dict) else {}
    inputs_json = data.get("inputs") if isinstance(data.get("inputs"), dict) else {}
    order_data = data.get("order") if isinstance(data.get("order"), dict) else {}
    title = f"{order.menu.name if order.menu else '鑑定結果'}"
    summary = {
        "essence": structure.get("core_message") or structure.get("essence") or order.consultation_text or "",
        "strength": structure.get("strengths") or structure.get("strength") or "",
        "caution": structure.get("cautions") or structure.get("caution") or structure.get("theme") or "",
    }
    sections: list[dict[str, Any]] = []
    body_text = _first_meaningful(
        delivery_text,
        reports.get('web'),
        payload_json.get('web_text'),
        payload_json.get('report_text'),
        _deep_get(raw, 'reports.web'),
        _deep_get(raw, 'web_text'),
        _deep_get(raw, 'report_text'),
        _deep_get(raw, 'western.report_text'),
        _deep_get(raw, 'western.web_text'),
    )
    if body_text:
        sections.append({"heading": "鑑定本文", "body": body_text})
    reader_text = _first_meaningful(
        reports.get("reader"),
        payload_json.get('reader_text'),
        _deep_get(raw, 'reports.reader'),
        _deep_get(raw, 'reader_text'),
    )
    if reader_text:
        sections.append({"heading": "占い師メモ", "body": reader_text})
    planet_list = _planet_items(raw)
    vedic_list = _vedic_items(raw)
    shichu_data = extract_shichu_data(raw)
    horoscope_image_url = _safe_text(
        _first_meaningful(
            _deep_get(raw, 'western.chart_image_url'),
            _deep_get(raw, 'western.wheel_image_url'),
            raw.get('chart_image_url'),
            raw.get('wheel_image_url'),
            order_data.get('horoscope_image_url'),
        )
    )
    compat_a, compat_b = _compat_people(raw)
    compat_planet_list_a = _planet_items(compat_a) if compat_a else []
    compat_planet_list_b = _planet_items(compat_b) if compat_b else []
    compat_chart_svg = _synastry_chart_svg(compat_a, compat_b) if compat_a and compat_b else ''
    compat_aspects = _synastry_aspects(compat_a, compat_b) if compat_a and compat_b else []
    chart_svg = compat_chart_svg or _chart_svg(raw)
    vedic_chart_svg = _vedic_chart_svg(raw)
    vedic_raw = _raw_vedic(raw)
    person_a = {
        'name': _safe_text(inputs_json.get('user_name') or order_data.get('user_name') or order.user_name),
        'birth_label': _format_birth_label(inputs_json.get('birth_date') or order_data.get('birth_date') or getattr(order, 'birth_date', ''), inputs_json.get('birth_time') or order_data.get('birth_time') or getattr(order, 'birth_time', None)),
        'place_label': ' '.join([v for v in [_safe_text(inputs_json.get('prefecture') or order_data.get('birth_prefecture') or getattr(order, 'birth_prefecture', '')), _safe_text(inputs_json.get('birth_place') or order_data.get('birth_place') or getattr(order, 'birth_place', ''))] if v]).strip() or '-',
        'sun_sign': next((_safe_text(p.get('sign')).split()[0] for p in (compat_planet_list_a or planet_list) if _safe_text(p.get('name')).upper() == 'SUN'), '-'),
    }
    person_b = {
        'name': _safe_text(inputs_json.get('name_b') or inputs_json.get('partner_name') or order_data.get('partner_name') or order_data.get('person_b_name') or order_data.get('name_b') or 'お相手'),
        'birth_label': _format_birth_label(inputs_json.get('birth_date_b') or order_data.get('birth_date_b') or order_data.get('partner_birth_date'), inputs_json.get('birth_time_b') or order_data.get('birth_time_b') or order_data.get('partner_birth_time')), 
        'place_label': ' '.join([v for v in [_safe_text(inputs_json.get('prefecture_b') or order_data.get('prefecture_b') or order_data.get('partner_prefecture')), _safe_text(inputs_json.get('birth_place_b') or order_data.get('birth_place_b') or order_data.get('partner_birth_place'))] if v]).strip() or '-',
        'sun_sign': next((_safe_text(p.get('sign')).split()[0] for p in compat_planet_list_b if _safe_text(p.get('name')).upper() == 'SUN'), '-'),
    }
    systems = {
        'western': bool(planet_list or compat_planet_list_a or compat_planet_list_b or chart_svg or horoscope_image_url),
        'vedic': bool(vedic_list or vedic_chart_svg or vedic_raw),
        'shichu': bool(shichu_data.get('exists')),
    }
    return {
        "title": title,
        "order_code": order.order_code,
        "summary": summary,
        "sections": sections,
        "planet_list": compat_planet_list_a or planet_list,
        "compat_planet_list_b": compat_planet_list_b,
        "compatibility_aspects": compat_aspects,
        "person_a": person_a,
        "person_b": person_b,
        "vedic_planet_list": vedic_list,
        "shichu": shichu_data,
        "advice_list": [],
        "horoscope_image_url": horoscope_image_url,
        "chart_svg": chart_svg,
        "vedic_chart_svg": vedic_chart_svg,
        "systems": systems,
        "raw_json": raw,
    }


def render_result_html(payload: dict[str, Any]) -> str:
    parts = [f"<h1>{html.escape(payload.get('title') or '鑑定結果')}</h1>"]
    summary = payload.get("summary") or {}
    if any(summary.values()):
        parts.append("<section><h2>要約</h2>")
        for label, key in [("本質", "essence"), ("強み", "strength"), ("テーマ", "caution")]:
            val = summary.get(key)
            if val:
                parts.append(f"<p><strong>{label}</strong><br>{_nl2br(val)}</p>")
        parts.append("</section>")
    chart_svg = payload.get('chart_svg') or ''
    vedic_chart_svg = payload.get('vedic_chart_svg') or ''
    if chart_svg:
        parts.append(f"<section><h2>西洋ホロスコープ</h2><div style='max-width:620px;margin:0 auto'>{chart_svg}</div></section>")
    elif payload.get('horoscope_image_url'):
        parts.append(f"<section><h2>西洋ホロスコープ</h2><img src='{html.escape(payload['horoscope_image_url'])}' alt='horoscope' style='max-width:100%;height:auto'></section>")
    if vedic_chart_svg:
        parts.append(f"<section><h2>インド占星術チャート</h2><div style='max-width:620px;margin:0 auto'>{vedic_chart_svg}</div></section>")
    shichu = payload.get('shichu') or {}
    if shichu.get('exists'):
        parts.append("<section><h2>四柱推命 命式</h2>" + render_shichu_table_html(shichu) + render_shichu_summary_html(shichu) + "</section>")
    for sec in payload.get("sections") or []:
        parts.append(f"<section><h2>{html.escape(sec.get('heading') or '本文')}</h2><div style='white-space:pre-wrap'>{_nl2br(sec.get('body') or '')}</div></section>")
    return "\n".join(parts)


def render_report_html(order: Order, payload: dict[str, Any]) -> str:
    try:
        template_name = _template_name(payload)
        template_path = Path(__file__).resolve().parent.parent / 'templates' / template_name
        tpl = template_path.read_text(encoding='utf-8')

        sections = payload.get('sections') or []
        planets = payload.get('planet_list') or []
        vedic_planets = payload.get('vedic_planet_list') or []
        shichu = payload.get('shichu') or {}

        birth_label = order.birth_date.strftime('%Y年%m月%d日') if getattr(order, 'birth_date', None) else '-'
        if getattr(order, 'birth_time', None):
            birth_label += f" {order.birth_time}"

        place_label = ' '.join([
            v for v in [
                getattr(order, 'birth_prefecture', ''),
                getattr(order, 'birth_place', '')
            ] if v
        ]) or '-'

        report_date = '-'
        if getattr(order, 'updated_at', None):
            try:
                report_date = order.updated_at.strftime('%Y.%m.%d')
            except Exception:
                report_date = _safe_text(order.updated_at) or '-'

        asc_sign = '-'
        sun_sign = '-'
        moon_sign = '-'
        for p in planets:
            n = _safe_text(p.get('name')).upper()
            sign = _safe_text(p.get('sign')).split()[0] or '-'
            if n == 'ASC':
                asc_sign = sign
            elif n == 'SUN':
                sun_sign = sign
            elif n == 'MOON':
                moon_sign = sign

        vedic_raw = _raw_vedic(payload.get('raw_json') or {})
        lagna = _safe_text(_dict(vedic_raw.get('ascendant')).get('rashi_name')) or '-'
        vedic_moon = '-'
        for p in vedic_raw.get('planets') or []:
            if isinstance(p, dict) and _safe_text(p.get('name')).lower() == 'moon':
                vedic_moon = _safe_text(p.get('rashi_name')) or '-'
                break

        western_chart = ''
        if payload.get('chart_svg'):
            western_chart = f'<div class="chart-img-wrap round">{payload["chart_svg"]}</div>'
        elif payload.get('horoscope_image_url'):
            western_chart = (
                f'<div class="chart-img-wrap round">'
                f'<img src="{html.escape(payload["horoscope_image_url"])}" alt="ホロスコープ">'
                f'</div>'
            )
        else:
            western_chart = (
                '<div class="chart-img-wrap round">'
                '<div class="chart-placeholder-icon">◯</div>'
                '<div class="chart-placeholder-label">HOROSCOPE</div>'
                '<div class="chart-placeholder-note">西洋チャート画像をここに</div>'
                '</div>'
            )

        vedic_chart = ''
        if payload.get('vedic_chart_svg'):
            vedic_chart = f'<div class="chart-img-wrap square">{payload["vedic_chart_svg"]}</div>'
        else:
            vedic_chart = (
                '<div class="chart-img-wrap square">'
                '<div class="chart-placeholder-icon">⊞</div>'
                '<div class="chart-placeholder-label">RASHI CHART</div>'
                '<div class="chart-placeholder-note">インドチャート画像をここに</div>'
                '</div>'
            )

        reading_text = _nl2br(sections[0].get('body') if sections else '')

        # --- 新テンプレート（single/double/triple）共通処理 ---
        # 旧テンプレートが来ても同じ処理でカバー


        # --- 相性鑑定テンプレ専用 ---
        if template_name == 'report_compatibility.html':
            compat_a = payload.get('person_a') or {}
            compat_b = payload.get('person_b') or {}
            compat_rows_a = _render_planet_rows(payload.get('planet_list') or [])
            compat_rows_b = _render_planet_rows(payload.get('compat_planet_list_b') or [])
            compat_reading = reading_text or _nl2br('相性鑑定本文を取得できませんでした。')
            compat_chart_svg = payload.get('chart_svg') or ''
            compat_chart_block = (
                f'<div class="chart-img-wrap">{compat_chart_svg}</div>' if compat_chart_svg else
                '<div class="chart-img-wrap"><div class="chart-placeholder-note">シナストリーチャートを表示できませんでした。</div></div>'
            )
            compat_aspect_cards = _compatibility_aspect_cards(payload.get('compatibility_aspects') or [])

            replacements = {
                '{{REPORT_DATE}}': html.escape(report_date),
                '{{PERSON_A_NAME}}': html.escape(_safe_text(compat_a.get('name')) or _safe_text(order.user_name) or 'あなた'),
                '{{PERSON_B_NAME}}': html.escape(_safe_text(compat_b.get('name')) or 'お相手'),
                '{{PERSON_A_SUB}}': html.escape(_person_subtitle(compat_a)),
                '{{PERSON_B_SUB}}': html.escape(_person_subtitle(compat_b)),
                '{{PERSON_A_BIRTH_INFO}}': html.escape(_person_birth_info(compat_a)),
                '{{PERSON_B_BIRTH_INFO}}': html.escape(_person_birth_info(compat_b)),
                '{{PERSON_A_ROWS}}': compat_rows_a,
                '{{PERSON_B_ROWS}}': compat_rows_b,
                '{{SYNASTRY_CHART_BLOCK}}': compat_chart_block,
                '{{SYNASTRY_CAPTION}}': '内円：あなた ／ 外円：お相手',
                '{{COMPATIBILITY_ASPECTS}}': compat_aspect_cards,
                '{{COMPATIBILITY_READING}}': compat_reading,
            }
            for key, val in replacements.items():
                tpl = tpl.replace(key, val)
            return tpl

        # --- それ以外のテンプレ ---
        tpl = tpl.replace('鑑定日：20XX年XX月XX日', f'鑑定日：{html.escape(report_date)}')
        tpl = tpl.replace('19XX年XX月XX日 XX:XX', html.escape(birth_label), 1)
        tpl = tpl.replace('〇〇県〇〇市', html.escape(place_label), 1)
        tpl = tpl.replace('〇〇 さま', html.escape(_safe_text(order.user_name)) + ' さま', 1)

        shichu_table_html = render_shichu_table_html(shichu) if shichu.get('exists') else ''
        shichu_summary_html = render_shichu_summary_html(shichu) if shichu.get('exists') else ''
        shichu_chart_block = (
            '<div class="shichu-chart-wrap">' + shichu_table_html + '</div>' +
            (f'<div class="chart-caption shichu-summary">{shichu_summary_html}</div>' if shichu_summary_html else '')
        ) if shichu.get('exists') else '<div class="chart-img-wrap wide"><div class="chart-placeholder-label">SHICHU DATA</div><div class="chart-placeholder-note">四柱推命データをここに</div></div>'

        # 図の割り当て: western→I, vedic→II(or I if no western), shichu→II or III
        systems = payload.get('systems') or {}
        charts = []
        chart_captions = []
        data_sections = []
        if systems.get('western'):
            charts.append(western_chart)
            chart_captions.append(f'ASC {html.escape(asc_sign)} ／ 太陽 {html.escape(sun_sign)} ／ 月 {html.escape(moon_sign)}')
            data_sections.append(_render_planet_rows(planets))
        if systems.get('vedic'):
            charts.append(vedic_chart)
            chart_captions.append(f'ラグナ {html.escape(lagna)} ／ 月 {html.escape(vedic_moon)}')
            data_sections.append(_render_planet_rows(vedic_planets))
        if systems.get('shichu'):
            charts.append(shichu_chart_block)
            chart_captions.append('')
            data_sections.append(shichu_table_html + (shichu_summary_html if shichu_summary_html else ''))
        # 不足分は空文字で埋める
        while len(charts) < 3:
            charts.append('')
            chart_captions.append('')
            data_sections.append('')

        # データセクションII・IIIをHTML化（ラベル付き）
        vedic_data_section = (
            '<div class="system-heading"><span class="system-heading-label">II</span>'
            '<div class="system-heading-line"></div></div>'
            f'<table class="planet-table"><tbody>{data_sections[1]}</tbody></table>'
        ) if data_sections[1] else ''

        placeholders = {
            '{{REPORT_DATE}}': html.escape(report_date),
            '{{USER_NAME}}': html.escape(_safe_text(order.user_name)),
            '{{PERSON_A_NAME}}': html.escape(_safe_text(order.user_name)),
            '{{BIRTH_LABEL}}': html.escape(birth_label),
            '{{BIRTH_INFO}}': html.escape(birth_label),
            '{{PLACE_LABEL}}': html.escape(place_label),
            '{{BIRTH_PLACE}}': html.escape(place_label),
            '{{ASC_SIGN}}': html.escape(asc_sign),
            '{{ASC_LABEL}}': html.escape(asc_sign),
            '{{REPORT_SUBTITLE}}': '',
            '{{CHART_BLOCK_1}}': charts[0],
            '{{CHART_BLOCK_2}}': charts[1],
            '{{CHART_BLOCK_3}}': charts[2],
            '{{CHART_CAPTION_1}}': html.escape(chart_captions[0]) if chart_captions[0] else '',
            '{{CHART_CAPTION_2}}': html.escape(chart_captions[1]) if chart_captions[1] else '',
            '{{CHART_CAPTION_3}}': '',
            '{{PLANET_ROWS}}': data_sections[0],
            '{{WESTERN_ROWS}}': data_sections[0],
            '{{VEDIC_DATA_SECTION}}': vedic_data_section,
            '{{VEDIC_ROWS}}': data_sections[1],
            '{{SHICHU_DATA_TABLE}}': data_sections[2] if len(data_sections) > 2 else '',
            '{{READING_TEXT}}': reading_text,
            '{{INTEGRATED_READING}}': reading_text,
            # 旧テンプレート互換
            '{{WESTERN_CHART_BLOCK}}': charts[0],
            '{{VEDIC_CHART_BLOCK}}': charts[1] if len(charts) > 1 else '',
            '{{WESTERN_CAPTION}}': html.escape(chart_captions[0]) if chart_captions else '',
            '{{VEDIC_CAPTION}}': html.escape(chart_captions[1]) if len(chart_captions) > 1 else '',
            '{{SHICHU_CHART_BLOCK}}': charts[2] if len(charts) > 2 else '',
        }
        for key, val in placeholders.items():
            if key in tpl:
                tpl = tpl.replace(key, val)

        return tpl

    except Exception as e:
        body = _nl2br((payload.get('sections') or [{}])[0].get('body', ''))
        planets = payload.get('planet_list') or []
        birth_label = order.birth_date.strftime('%Y年%m月%d日') if getattr(order, 'birth_date', None) else '-'
        if getattr(order, 'birth_time', None):
            birth_label += f" {order.birth_time}"
        place_label = ' '.join([v for v in [getattr(order, 'birth_prefecture', ''), getattr(order, 'birth_place', '')] if v]) or '-'
        chart = payload.get('chart_svg') or ''
        rows = _render_planet_rows(planets)

        return f"""
        <html>
        <head><meta charset="utf-8"><title>鑑定書</title></head>
        <body style="font-family:'Noto Serif JP',serif;background:#0e0c10;color:#e8e0d4;padding:24px;">
          <h1>{html.escape(_safe_text(order.user_name))} さまの鑑定書</h1>
          <p>生年月日：{html.escape(birth_label)}</p>
          <p>出生地：{html.escape(place_label)}</p>
          <div style="max-width:560px;margin:24px auto;">{chart}</div>
          <table style="width:100%;border-collapse:collapse;"><tbody>{rows}</tbody></table>
          <div style="margin-top:24px;white-space:pre-line;">{body}</div>
          <p style="margin-top:24px;font-size:12px;opacity:0.7;">fallback rendered: {html.escape(str(e))}</p>
        </body>
        </html>
        """

def build_yaml_from_analysis(order: Order, inputs_json: dict[str, Any] | None = None, payload_json: dict[str, Any] | None = None, raw_json: dict[str, Any] | None = None, structure_summary_json: dict[str, Any] | None = None, ai_text: str = "", reader_text: str = "", line_text: str = "", handoff_yaml_full: str = "") -> str:
    inputs_json = inputs_json or {}
    payload_json = payload_json or {}
    raw_json = raw_json or {}
    structure_summary_json = structure_summary_json or {}
    data = {
        "order": {
            "order_code": order.order_code,
            "user_name": order.user_name,
            "birth_date": order.birth_date.isoformat() if getattr(order, "birth_date", None) else None,
            "birth_time": order.birth_time,
            "birth_prefecture": getattr(order, "birth_prefecture", None),
            "birth_place": getattr(order, "birth_place", None),
            "consultation_text": order.consultation_text,
            "horoscope_image_url": raw_json.get('chart_image_url') or raw_json.get('wheel_image_url') or '',
        },
        "inputs": inputs_json,
        "payload": payload_json,
        "raw_json": raw_json,
        "structure_summary": structure_summary_json,
        "reports": {
            "web": ai_text or payload_json.get("web_text") or payload_json.get("report_text") or "",
            "reader": reader_text or "",
            "line": line_text or "",
        },
    }
    return json.dumps(data, ensure_ascii=False, indent=2)
