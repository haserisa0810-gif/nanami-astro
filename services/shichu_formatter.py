
from __future__ import annotations

import html
from typing import Any

PILLAR_KEYS = ["year", "month", "day", "hour"]
PILLAR_LABELS = {"year": "年柱", "month": "月柱", "day": "日柱", "hour": "時柱"}
BRANCH_POSITION_LABELS = {"year": "年支", "month": "月支", "day": "日支", "hour": "時支"}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _number_text(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return _safe_text(value)


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
    interactions = _dict(normalized.get("interactions")) or _dict(candidate.get("interactions"))
    shinsatsu = _dict(normalized.get("shinsatsu")) or _dict(candidate.get("shinsatsu"))
    timing = _dict(normalized.get("timing")) or _dict(candidate.get("timing"))
    structure = _dict(candidate.get("structure_report"))
    hidden_stem_command = _dict(normalized.get("hidden_stem_command"))
    pattern_candidates = _dict(structure.get("pattern_candidates"))
    yojin_candidates = _dict(structure.get("yojin_candidates"))
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
        "interactions": interactions,
        "shinsatsu": shinsatsu,
        "timing": timing,
        "hidden_stem_command": hidden_stem_command,
        "pattern_candidates": pattern_candidates,
        "yojin_candidates": yojin_candidates,
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
    timing = _dict(data.get("timing"))
    current_daiun = _dict(timing.get("current_daiun"))
    if current_daiun:
        kanshi = _safe_text(current_daiun.get("kanshi"))
        god = _safe_text(current_daiun.get("stem_ten_god"))
        age = _number_text(current_daiun.get("age_from"))
        detail = "・".join(value for value in (god, f"{age}歳頃〜" if age else "") if value)
        parts.append("現大運：" + html.escape(kanshi + (f"（{detail}）" if detail else "")))
    ryunen_items = _dict(timing.get("ryunen")).get("items")
    if isinstance(ryunen_items, list):
        current_ryunen = next((_dict(item) for item in ryunen_items if _dict(item).get("is_current")), {})
        if current_ryunen:
            kanshi = _safe_text(current_ryunen.get("kanshi"))
            god = _safe_text(current_ryunen.get("stem_ten_god"))
            parts.append("今年：" + html.escape(kanshi + (f"（{god}）" if god else "")))
    command = _dict(data.get("hidden_stem_command"))
    if command:
        stem = _safe_text(command.get("commanding_stem"))
        phase = _safe_text(command.get("phase"))
        if stem:
            parts.append("月令：" + html.escape(stem + "司令" + (f"（{phase}）" if phase else "")))
    pattern_items = _dict(data.get("pattern_candidates")).get("candidates")
    if isinstance(pattern_items, list) and pattern_items:
        confidence_labels = {"high": "高", "medium": "中", "low": "低"}
        labels = []
        for raw_item in pattern_items:
            item = _dict(raw_item)
            name = _safe_text(item.get("name"))
            confidence = confidence_labels.get(_safe_text(item.get("confidence")), "")
            if name:
                labels.append(name + (f"（{confidence}）" if confidence else ""))
        if labels:
            parts.append("格局候補：" + html.escape("・".join(labels)))
    yojin = _dict(data.get("yojin_candidates"))
    fu_yi = _dict(yojin.get("fu_yi"))
    climate = _dict(yojin.get("climate"))
    if fu_yi:
        status = _safe_text(fu_yi.get("status"))
        if status in ("hold", "withheld"):
            reason = "境界域" if status == "hold" else "従格・専旺疑い"
            parts.append(f"推奨五行：保留（{reason}）")
        else:
            fu_items = fu_yi.get("candidates") if isinstance(fu_yi.get("candidates"), list) else []
            climate_items = climate.get("candidates") if isinstance(climate.get("candidates"), list) else []
            fu_elements = "・".join(_safe_text(_dict(item).get("element")) for item in fu_items if _safe_text(_dict(item).get("element")))
            climate_elements = "・".join(_safe_text(_dict(item).get("element")) for item in climate_items if _safe_text(_dict(item).get("element")))
            labels = []
            if fu_elements:
                labels.append(f"{fu_elements}（扶抑）")
            if climate_elements:
                labels.append(f"{climate_elements}（調候）")
            if labels:
                parts.append("推奨五行：" + html.escape("／".join(labels)))
    interaction_items = _dict(data.get("interactions")).get("items")
    if isinstance(interaction_items, list):
        labels = []
        for raw_item in interaction_items:
            item = _dict(raw_item)
            members = item.get("members") if isinstance(item.get("members"), list) else []
            label = "".join(_safe_text(member) for member in members) + " " + _safe_text(item.get("type"))
            if item.get("provisional"):
                label += " ※時刻未確定"
            if label.strip():
                labels.append(label.strip())
        if labels:
            parts.append("命式内の作用：" + html.escape("・".join(labels)))
    shinsatsu_items = _dict(data.get("shinsatsu")).get("items")
    if isinstance(shinsatsu_items, list):
        labels = []
        for raw_item in shinsatsu_items:
            item = _dict(raw_item)
            found_in = item.get("found_in") if isinstance(item.get("found_in"), list) else []
            position_labels = PILLAR_LABELS if item.get("basis") == "day_pillar" else BRANCH_POSITION_LABELS
            found_labels = "・".join(position_labels.get(_safe_text(pillar), _safe_text(pillar)) for pillar in found_in)
            label = _safe_text(item.get("name"))
            if found_labels:
                label += f"（{found_labels}）"
            if item.get("provisional"):
                label += " ※時刻未確定"
            if label:
                labels.append(label)
        if labels:
            parts.append("神殺：" + html.escape("・".join(labels)))
    if not parts:
        return ""
    from services.shichu_annual_reading import build_annual_themes
    annual_blocks = []
    for theme in build_annual_themes(_dict(data.get("raw"))):
        lines = [*theme["evidence"], *theme["reasons"]]
        annual_blocks.append(
            '<details class="shichu-annual"><summary>' + html.escape(theme["label"] + "：" + theme["summary"]) +
            '</summary><ul>' + ''.join('<li>' + html.escape(line) + '</li>' for line in lines) + '</ul></details>'
        )
    return "<div class=\"reading-text\">" + "<br>".join(parts) + "</div>" + ''.join(annual_blocks)
