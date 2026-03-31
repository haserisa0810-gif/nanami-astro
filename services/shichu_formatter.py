
from __future__ import annotations

import html
from typing import Any

PILLAR_KEYS = ["year", "month", "day", "hour"]
PILLAR_LABELS = {"year": "年柱", "month": "月柱", "day": "日柱", "hour": "時柱"}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def extract_shichu_data(raw: dict[str, Any]) -> dict[str, Any]:
    candidate = {}
    if isinstance(raw.get("shichusuimei"), dict):
        candidate = raw["shichusuimei"]
    elif raw.get("system") == "shichusuimei":
        candidate = raw
    normalized = _dict(candidate.get("normalized_data"))
    pillars = _dict(normalized.get("pillars"))
    if not pillars and isinstance(candidate.get("pillars"), dict):
        raw_pillars = candidate.get("pillars") or {}
        pillars = {
            key: {"stem": _safe_text(raw_pillars.get(key, [None, None])[0]), "branch": _safe_text(raw_pillars.get(key, [None, None])[1])}
            for key in PILLAR_KEYS
        }
    hidden = _dict(normalized.get("hidden_stems")) or _dict(_dict(candidate.get("features")).get("hidden_stems"))
    twelve = _dict(normalized.get("twelve_fortune")) or _dict(_dict(candidate.get("features")).get("twelve_fortune"))
    five = _dict(_dict(normalized.get("five_elements"))) or _dict(_dict(candidate.get("features")).get("five_elements"))
    strength = _dict(_dict(candidate.get("structure_report")).get("strength_index")) or _dict(_dict(candidate.get("features")).get("strength"))
    kubo = candidate.get("kubo") or _dict(_dict(candidate.get("features")).get("kubo")) or _dict(normalized).get("kubo")
    if isinstance(kubo, list):
        kubo_text = "・".join(_safe_text(x) for x in kubo if _safe_text(x))
    elif isinstance(kubo, str):
        kubo_text = kubo
    else:
        kubo_text = ""
    day_master = _safe_text(candidate.get("day_master"))
    day_master_element = _safe_text(_dict(candidate.get("structure_report")).get("day_master_element"))
    daiun = _dict(_dict(candidate.get("features")).get("daiun")) or _dict(normalized.get("daiun"))
    periods = daiun.get("periods") if isinstance(daiun.get("periods"), list) else []
    first_period = periods[0] if periods else {}
    return {
        "exists": bool(candidate),
        "raw": candidate,
        "pillars": pillars,
        "hidden_stems": hidden,
        "twelve_fortune": twelve,
        "five_elements": five,
        "strength": strength,
        "kubo_text": kubo_text,
        "day_master": day_master,
        "day_master_element": day_master_element,
        "daiun_first": {
            "age_start": _safe_text(_dict(first_period).get("start_age")),
            "pillar": _safe_text(_dict(first_period).get("kanshi")),
        },
    }


def render_shichu_table_html(data: dict[str, Any]) -> str:
    pillars = _dict(data.get("pillars"))
    hidden = _dict(data.get("hidden_stems"))
    twelve = _dict(data.get("twelve_fortune"))
    values = []
    for key in PILLAR_KEYS:
        p = _dict(pillars.get(key))
        values.append({
            "stem": _safe_text(p.get("stem")) or "-",
            "branch": _safe_text(p.get("branch")) or "-",
            "hidden": "・".join(_safe_text(x) for x in (hidden.get(key) or []) if _safe_text(x)) or "-",
            "twelve": _safe_text(twelve.get(key)) or "-",
        })
    return (
        '<table class="shicyu-table">'
        '<thead><tr><th></th><th>年柱</th><th>月柱</th><th>日柱</th><th>時柱</th></tr></thead>'
        '<tbody>'
        f'<tr><th>天干</th><td>{html.escape(values[0]["stem"])}</td><td>{html.escape(values[1]["stem"])}</td><td>{html.escape(values[2]["stem"])}</td><td>{html.escape(values[3]["stem"])}</td></tr>'
        f'<tr><th>地支</th><td>{html.escape(values[0]["branch"])}</td><td>{html.escape(values[1]["branch"])}</td><td>{html.escape(values[2]["branch"])}</td><td>{html.escape(values[3]["branch"])}</td></tr>'
        f'<tr><th>蔵干</th><td class="muted">{html.escape(values[0]["hidden"])}</td><td class="muted">{html.escape(values[1]["hidden"])}</td><td class="muted">{html.escape(values[2]["hidden"])}</td><td class="muted">{html.escape(values[3]["hidden"])}</td></tr>'
        f'<tr><th>十二運</th><td class="muted">{html.escape(values[0]["twelve"])}</td><td class="muted">{html.escape(values[1]["twelve"])}</td><td class="muted">{html.escape(values[2]["twelve"])}</td><td class="muted">{html.escape(values[3]["twelve"])}</td></tr>'
        '</tbody></table>'
    )


def render_shichu_summary_html(data: dict[str, Any]) -> str:
    five = _dict(data.get("five_elements"))
    strength = _dict(data.get("strength"))
    day_master = (_safe_text(data.get("day_master")) + _safe_text(data.get("day_master_element"))).strip()
    gokyo = " / ".join(f"{k}{_safe_text(five.get(k) or 0)}" for k in ["木", "火", "土", "金", "水"])
    parts = []
    if day_master:
        parts.append(f"日主：{html.escape(day_master)}")
    if gokyo:
        parts.append(f"五行バランス：{html.escape(gokyo)}")
    label = _safe_text(strength.get("label"))
    score = _safe_text(strength.get("score"))
    if label:
        parts.append(f"身強弱：{html.escape(label)}" + (f"（{html.escape(score)}）" if score else ""))
    if _safe_text(data.get("kubo_text")):
        parts.append(f"空亡：{html.escape(_safe_text(data.get('kubo_text')))}")
    first = _dict(data.get("daiun_first"))
    if _safe_text(first.get("pillar")):
        age = _safe_text(first.get("age_start"))
        parts.append(f"大運初回：{html.escape(age)}歳頃から {html.escape(_safe_text(first.get('pillar')))}")
    if not parts:
        return ""
    return "<div class=\"reading-text\">" + "<br>".join(parts) + "</div>"
