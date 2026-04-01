
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
    parts = [f"<svg viewBox='0 0 {size} {size}' width='100%' height='100%' xmlns='http://www.w3.org/2000/svg'>"]
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


def _replace_first_reading_text(tpl: str, body_html: str) -> str:
    if '{{INTEGRATED_READING}}' in tpl:
        return tpl.replace('{{INTEGRATED_READING}}', body_html, 1)
    tpl = re.sub(r'<div class="reading-text">.*?</div>', f'<div class="reading-text">{body_html}</div>', tpl, count=1, flags=re.S)
    tpl = re.sub(r'<p class="reading-text">.*?</p>', f'<p class="reading-text">{body_html}</p>', tpl, count=1, flags=re.S)
    return tpl


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


def _template_name(payload: dict[str, Any]) -> str:
    systems = payload.get('systems') or {}
    if systems.get('western') and systems.get('vedic') and systems.get('shichu'):
        return 'report-integrated.html'
    if systems.get('western') and systems.get('vedic'):
        return 'report-western-vedic.html'
    if systems.get('western') and systems.get('shichu'):
        return 'report-western-shicyu.html'
    if systems.get('vedic') and systems.get('shichu'):
        return 'report-vedic-shicyu.html'
    return 'report_template_source.html'


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
    chart_svg = _chart_svg(raw)
    vedic_chart_svg = _vedic_chart_svg(raw)
    vedic_raw = _raw_vedic(raw)
    asc = _dict(_deep_get(raw, 'western.angles.Asc') if False else {})
    systems = {
        'western': bool(planet_list or chart_svg or horoscope_image_url),
        'vedic': bool(vedic_list or vedic_chart_svg or vedic_raw),
        'shichu': bool(shichu_data.get('exists')),
    }
    return {
        "title": title,
        "order_code": order.order_code,
        "summary": summary,
        "sections": sections,
        "planet_list": planet_list,
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
    template_path = Path(__file__).resolve().parent.parent / 'templates' / _template_name(payload)
    tpl = template_path.read_text(encoding='utf-8')
    sections = payload.get('sections') or []
    planets = payload.get('planet_list') or []
    vedic_planets = payload.get('vedic_planet_list') or []
    shichu = payload.get('shichu') or {}
    birth_label = order.birth_date.strftime('%Y年%m月%d日') if getattr(order, 'birth_date', None) else '-'
    if order.birth_time:
        birth_label += f" {order.birth_time}"
    place_label = ' '.join([v for v in [order.birth_prefecture, order.birth_place] if v]) or '-'

    # title and header system labels
    tpl = tpl.replace('鑑定日：20XX年XX月XX日', f"鑑定日：{html.escape(_safe_text(getattr(order, 'updated_at', '') or ''))}") if getattr(order, 'updated_at', None) else tpl
    tpl = tpl.replace('19XX年XX月XX日 XX:XX', html.escape(birth_label), 1)
    tpl = tpl.replace('〇〇県〇〇市', html.escape(place_label), 1)
    tpl = tpl.replace('〇〇 さま', html.escape(order.user_name) + ' さま', 1)

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
        if isinstance(p, dict) and p.get('name') == 'Moon':
            vedic_moon = _safe_text(p.get('rashi_name')) or '-'
            break

    tpl = tpl.replace('ASCENDANT</span>\n      〇〇座', f'ASCENDANT</span>\n      {html.escape(asc_sign)}', 1)
    tpl = tpl.replace('ASC 〇〇座 ／ 太陽 〇〇座 ／ 月 〇〇座', f'ASC {html.escape(asc_sign)} ／ 太陽 {html.escape(sun_sign)} ／ 月 {html.escape(moon_sign)}')
    tpl = tpl.replace('ラグナ 〇〇座 ／ 月 〇〇座', f'ラグナ {html.escape(lagna)} ／ 月 {html.escape(vedic_moon)}')

    # Chart replacements
    if payload.get('chart_svg'):
        western_chart = f"<div class=\"chart-img-wrap round\">{payload['chart_svg']}</div>"
    elif payload.get('horoscope_image_url'):
        western_chart = f"<div class=\"chart-img-wrap round\"><img src=\"{html.escape(payload['horoscope_image_url'])}\" alt=\"ホロスコープ\"></div>"
    else:
        western_chart = '<div class="chart-img-wrap round"><div class="chart-placeholder-label">NO DATA</div></div>'

    vedic_chart_svg = payload.get('vedic_chart_svg') or ''
    vedic_chart = f"<div class=\"chart-img-wrap square\">{vedic_chart_svg}</div>" if vedic_chart_svg else '<div class="chart-img-wrap square"><div class="chart-placeholder-label">NO DATA</div></div>'

    if shichu.get('exists'):
        shichu_data_table = render_shichu_table_html(shichu)
        shichu_chart = f'<div class="shichu-chart-wrap">{shichu_data_table}</div>'
    else:
        shichu_data_table = '<table class="shicyu-table"><tbody><tr><td>データはありません。</td></tr></tbody></table>'
        shichu_chart = '<div class="chart-img-wrap wide"><div class="chart-placeholder-label">NO DATA</div></div>'

    tpl = tpl.replace('{{WESTERN_CHART_BLOCK}}', western_chart, 1)
    tpl = tpl.replace('{{VEDIC_CHART_BLOCK}}', vedic_chart, 1)
    tpl = tpl.replace('{{SHICHU_CHART_BLOCK}}', shichu_chart, 1)

    tpl = tpl.replace('{{WESTERN_ROWS}}', _render_planet_rows(planets), 1)
    tpl = tpl.replace('{{VEDIC_ROWS}}', _render_planet_rows(vedic_planets), 1)
    tpl = tpl.replace('{{SHICHU_DATA_TABLE}}', shichu_data_table, 1)

    body = _nl2br(sections[0].get('body') if sections else '')
    tpl = _replace_first_reading_text(tpl, body)
    return tpl


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
