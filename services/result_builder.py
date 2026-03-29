from __future__ import annotations

import html
import json
import math
import re
from pathlib import Path
from typing import Any

from models import Order, YamlLog

SIGN_JA = ["牡羊", "牡牛", "双子", "蟹", "獅子", "乙女", "天秤", "蠍", "射手", "山羊", "水瓶", "魚"]
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


def _raw_western(raw: dict[str, Any]) -> dict[str, Any]:
    western = raw.get('western')
    return western if isinstance(western, dict) else raw


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
    return {
        "title": title,
        "order_code": order.order_code,
        "summary": summary,
        "sections": sections,
        "planet_list": planet_list,
        "advice_list": [],
        "horoscope_image_url": horoscope_image_url,
        "chart_svg": chart_svg,
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
    if chart_svg:
        parts.append(f"<section><h2>ホロスコープ</h2><div style='max-width:620px;margin:0 auto'>{chart_svg}</div></section>")
    elif payload.get('horoscope_image_url'):
        parts.append(f"<section><h2>ホロスコープ</h2><img src='{html.escape(payload['horoscope_image_url'])}' alt='horoscope' style='max-width:100%;height:auto'></section>")
    for sec in payload.get("sections") or []:
        parts.append(f"<section><h2>{html.escape(sec.get('heading') or '本文')}</h2><div style='white-space:pre-wrap'>{_nl2br(sec.get('body') or '')}</div></section>")
    return "\n".join(parts)


def render_report_html(order: Order, payload: dict[str, Any]) -> str:
    template_path = Path(__file__).resolve().parent.parent / 'templates' / 'report_template_source.html'
    tpl = template_path.read_text(encoding='utf-8')
    sections = payload.get('sections') or []
    planets = payload.get('planet_list') or []
    birth_label = order.birth_date.strftime('%Y年%m月%d日') if getattr(order, 'birth_date', None) else '-'
    if order.birth_time:
        birth_label += f" {order.birth_time}"
    place_label = ' '.join([v for v in [order.birth_prefecture, order.birth_place] if v]) or '-'
    asc_sign = '-'
    for p in planets:
        if _safe_text(p.get('name')).upper() == 'ASC':
            asc_sign = _safe_text(p.get('sign')).split()[0] or '-'
            break
    tpl = tpl.replace('〇〇 さまの<br><em>星の物語</em>', f"{html.escape(order.user_name)} さまの<br><em>星の物語</em>")
    tpl = tpl.replace('19XX年XX月XX日 XX:XX', html.escape(birth_label), 1)
    tpl = tpl.replace('〇〇県〇〇市', html.escape(place_label), 1)
    tpl = tpl.replace('〇〇座', html.escape(asc_sign), 1)

    chart_svg = payload.get('chart_svg') or ''
    chart_img = payload.get('horoscope_image_url') or ''
    chart_block = ''
    if chart_svg:
        chart_block = (
            "<div class='section'><span class='section-label'>CHART</span><h2 class='section-title'>ホロスコープ</h2>"
            "<div class='divider'></div>"
            f"<div style='max-width:620px;margin:0 auto'>{chart_svg}</div>"
            + (f"<div style='margin-top:12px;text-align:center'><img src='{html.escape(chart_img)}' alt='horoscope' style='max-width:100%;height:auto;display:none' onerror=\"this.remove()\" onload=\"this.style.display='inline-block'\"></div>" if chart_img else "")
            + "</div>"
        )
    elif chart_img:
        chart_block = (
            "<div class='section'><span class='section-label'>CHART</span><h2 class='section-title'>ホロスコープ</h2>"
            "<div class='divider'></div>"
            f"<div style='max-width:620px;margin:0 auto'><img src='{html.escape(chart_img)}' alt='horoscope' style='width:100%;height:auto;border:1px solid var(--border);border-radius:18px'></div></div>"
        )
    marker = '<!-- 惑星配置テーブル -->'
    if chart_block and marker in tpl:
        tpl = tpl.replace(marker, chart_block + "\n\n" + marker, 1)

    rows = []
    if not planets:
        rows.append("<tr><td colspan='4' class='pt-role'>主要天体データはありません。</td></tr>")
    else:
        for p in planets[:8]:
            name = html.escape(_safe_text(p.get('name')).upper())
            sign = html.escape(_safe_text(p.get('sign')))
            house = html.escape(_safe_text(p.get('house')))
            note = html.escape(_safe_text(p.get('note')))
            house_desc = html.escape(_safe_text(p.get('house_desc')))
            rows.append(f"<tr><td class='pt-name'>{name}</td><td class='pt-sign'>{sign}</td><td class='pt-house'>{house}</td><td class='pt-role'>{note}<span class='pt-house-desc'>{house_desc}</span></td></tr>")
    tpl = re.sub(r"<tbody>.*?</tbody>", "<tbody>" + ''.join(rows) + "</tbody>", tpl, flags=re.S)

    body = _nl2br(sections[0].get('body') if sections else '')
    tpl = tpl.replace('ここに鑑定文をそのまま貼り付けてください。\n\n改行はそのまま反映されます。段落ごとに空行を入れると読みやすくなります。\n\nさらに長い文章も、このエリアにすべて収まります。', body)
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
