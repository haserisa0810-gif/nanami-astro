from __future__ import annotations

"""四柱推命の月律・五行力量・格局候補・推奨五行候補。"""

from typing import Any

from services.shichu_interactions import _BRANCH_CLASHES
from services.shichusuimei_calc import (
    HIDDEN_STEMS,
    STEM_ELEMENT,
    _element_control,
    _element_generate,
    ten_god,
)

ELEMENTS = ["木", "火", "土", "金", "水"]

_MONTH_COMMAND_TABLE = {
    "子": (("壬", "余気", 10), ("癸", "本気", 20)),
    "丑": (("癸", "余気", 9), ("辛", "中気", 3), ("己", "本気", 18)),
    "寅": (("戊", "余気", 7), ("丙", "中気", 7), ("甲", "本気", 16)),
    "卯": (("甲", "余気", 10), ("乙", "本気", 20)),
    "辰": (("乙", "余気", 9), ("癸", "中気", 3), ("戊", "本気", 18)),
    "巳": (("戊", "余気", 7), ("庚", "中気", 7), ("丙", "本気", 16)),
    "午": (("丙", "余気", 10), ("己", "中気", 9), ("丁", "本気", 11)),
    "未": (("丁", "余気", 9), ("乙", "中気", 3), ("己", "本気", 18)),
    "申": (("戊", "余気", 7), ("壬", "中気", 7), ("庚", "本気", 16)),
    "酉": (("庚", "余気", 10), ("辛", "本気", 20)),
    "戌": (("辛", "余気", 9), ("丁", "中気", 3), ("戊", "本気", 18)),
    "亥": (("戊", "余気", 7), ("甲", "中気", 7), ("壬", "本気", 16)),
}

_JIANLU = {
    "甲": "寅", "乙": "卯", "丙": "巳", "丁": "午", "戊": "巳",
    "己": "午", "庚": "申", "辛": "酉", "壬": "亥", "癸": "子",
}
_MONTH_BLADE = {"甲": "卯", "丙": "午", "戊": "午", "庚": "酉", "壬": "子"}
_PILLAR_LABELS = {"year": "年", "month": "月", "day": "日", "hour": "時"}


def _kanshi(p4: Any, pillar: str) -> str:
    value = p4.get(pillar, "") if isinstance(p4, dict) else getattr(p4, pillar, "")
    return str(value or "")


def _generating_element(element: str) -> str:
    return next(candidate for candidate in ELEMENTS if _element_generate(candidate) == element)


def _controlling_element(element: str) -> str:
    return next(candidate for candidate in ELEMENTS if _element_control(candidate) == element)


def month_command(month_branch: str, elapsed_days: float) -> dict[str, Any]:
    """節入りからの経過日数に基づいて月律分野蔵干を返す。"""
    if month_branch not in _MONTH_COMMAND_TABLE:
        raise ValueError(f"unsupported month branch: {month_branch}")
    composition = [
        {"stem": stem, "phase": phase, "days": days}
        for stem, phase, days in _MONTH_COMMAND_TABLE[month_branch]
    ]
    commanding = composition[-1]
    cumulative = 0
    for item in composition:
        cumulative += item["days"]
        if elapsed_days < cumulative:
            commanding = item
            break
    return {
        "month_branch": month_branch,
        "elapsed_days_since_jieqi": elapsed_days,
        "commanding_stem": commanding["stem"],
        "phase": commanding["phase"],
        "composition": composition,
        "rule_id": "month_command_v1",
    }


def element_power(p4: Any, hidden_stem_command: dict[str, Any]) -> dict[str, Any]:
    """固定重みによる命式全体の五行力量を算出する。"""
    raw = {element: 0.0 for element in ELEMENTS}
    for pillar in ("year", "month", "day", "hour"):
        kanshi = _kanshi(p4, pillar)
        if kanshi and kanshi[0] in STEM_ELEMENT:
            raw[STEM_ELEMENT[kanshi[0]]] += 1.0

    hidden_weights = (1.0, 0.5, 0.3)
    position_weights = {"year": 0.8, "month": 2.0, "day": 1.2, "hour": 1.0}
    for pillar in ("year", "month", "day", "hour"):
        kanshi = _kanshi(p4, pillar)
        if len(kanshi) < 2:
            continue
        for index, stem in enumerate(HIDDEN_STEMS.get(kanshi[1], [])):
            raw[STEM_ELEMENT[stem]] += hidden_weights[index] * position_weights[pillar]

    command_stem = str(hidden_stem_command.get("commanding_stem") or "")
    if command_stem in STEM_ELEMENT:
        raw[STEM_ELEMENT[command_stem]] += 0.5

    raw = {element: round(value, 3) for element, value in raw.items()}
    total = sum(raw.values())
    percent = {
        element: round(value / total * 100, 1) if total else 0.0
        for element, value in raw.items()
    }
    return {
        "rule_id": "element_power_v1",
        "raw": raw,
        "percent": percent,
        "weights_note": "天干1.0/蔵干(本気1.0,中気0.5,余気0.3)/位置係数(月2.0,日1.2,時1.0,年0.8)/司令+0.5",
    }


def strength_v2(day_stem: str, power: dict[str, Any]) -> dict[str, Any]:
    """五行力量から日主のサポート比を算出する。"""
    day_element = STEM_ELEMENT[day_stem]
    resource_element = _generating_element(day_element)
    supporter_elements = [day_element, resource_element]
    drainer_elements = [
        _element_generate(day_element),
        _element_control(day_element),
        _controlling_element(day_element),
    ]
    raw = power.get("raw", {})
    supporter_power = max(0.0, float(raw.get(day_element, 0.0)) - 1.0) + float(raw.get(resource_element, 0.0))
    drainer_power = sum(float(raw.get(element, 0.0)) for element in drainer_elements)
    denominator = supporter_power + drainer_power
    ratio = supporter_power / denominator if denominator else 0.0
    score = round(ratio * 100)
    band = "強め域" if score >= 65 else "弱め域" if score <= 35 else "中庸域"

    reasons: list[str] = []
    if score >= 85 or score <= 15:
        reasons.append("従格・専旺の可能性")
    dominant = [
        element for element, value in power.get("percent", {}).items()
        if float(value) >= 55.0
    ]
    for element in dominant:
        reasons.append(f"{element}が五行力量の55%以上")
    return {
        "rule_id": "strength_v2",
        "score": score,
        "support_ratio": round(ratio, 4),
        "supporters": {"elements": supporter_elements, "power": round(supporter_power, 3)},
        "drainers": {"elements": drainer_elements, "power": round(drainer_power, 3)},
        "band": band,
        "special_pattern_suspicion": bool(reasons),
        "suspicion_reasons": reasons,
    }


def _counter_evidence(p4: Any, strength: dict[str, Any]) -> list[str]:
    month_kanshi = _kanshi(p4, "month")
    month_branch = month_kanshi[1] if len(month_kanshi) >= 2 else ""
    evidence: list[str] = []
    for pillar in ("year", "day", "hour"):
        kanshi = _kanshi(p4, pillar)
        if len(kanshi) >= 2 and frozenset((month_branch, kanshi[1])) in _BRANCH_CLASHES:
            evidence.append("月支に冲があり格の成立が揺らぐ")
            break
    if strength.get("special_pattern_suspicion"):
        evidence.append("従格・専旺の可能性があり普通格局の前提が崩れる場合がある")
    return evidence


def pattern_candidates(
    p4: Any,
    hidden_stem_command: dict[str, Any],
    strength: dict[str, Any],
) -> dict[str, Any]:
    """普通格局だけを確度と反証付きの候補として返す。"""
    day_stem = _kanshi(p4, "day")[0]
    month_branch = _kanshi(p4, "month")[1]
    counters = _counter_evidence(p4, strength)
    candidates: list[dict[str, Any]] = []

    if _JIANLU.get(day_stem) == month_branch:
        candidates.append({
            "name": "建禄格",
            "confidence": "high",
            "basis": {"month_branch": month_branch, "hidden_stem": None, "phase": None, "transparent_in": []},
            "rationale": [f"月支{month_branch}が日干{day_stem}の建禄支"],
            "counter_evidence": list(counters),
        })
    elif _MONTH_BLADE.get(day_stem) == month_branch:
        candidates.append({
            "name": "月刃格",
            "confidence": "high",
            "basis": {"month_branch": month_branch, "hidden_stem": None, "phase": None, "transparent_in": []},
            "rationale": [f"月支{month_branch}が日干{day_stem}の月刃支"],
            "counter_evidence": list(counters),
        })
    else:
        visible = {
            pillar: _kanshi(p4, pillar)[0]
            for pillar in ("year", "month", "hour")
            if _kanshi(p4, pillar)
        }
        transparent_found = False
        for item in hidden_stem_command.get("composition", []):
            stem = str(item.get("stem") or "")
            transparent_in = [pillar for pillar, visible_stem in visible.items() if visible_stem == stem]
            if not transparent_in:
                continue
            god = ten_god(day_stem, stem)
            if god in ("比肩", "劫財"):
                continue
            transparent_found = True
            confidence = "high" if item.get("phase") == "本気" else "medium"
            pillar_text = "・".join(f"{_PILLAR_LABELS[pillar]}干" for pillar in transparent_in)
            candidates.append({
                "name": god + "格",
                "confidence": confidence,
                "basis": {
                    "month_branch": month_branch,
                    "hidden_stem": stem,
                    "phase": item.get("phase"),
                    "transparent_in": transparent_in,
                },
                "rationale": [f"月支{item.get('phase')}の{stem}({god})が{pillar_text}に透干"],
                "counter_evidence": list(counters),
            })
        if not transparent_found:
            main = next(
                (item for item in hidden_stem_command.get("composition", []) if item.get("phase") == "本気"),
                None,
            )
            if main:
                stem = str(main["stem"])
                god = ten_god(day_stem, stem)
                if god not in ("比肩", "劫財"):
                    candidates.append({
                        "name": god + "格",
                        "confidence": "medium",
                        "basis": {
                            "month_branch": month_branch,
                            "hidden_stem": stem,
                            "phase": "本気",
                            "transparent_in": [],
                        },
                        "rationale": [f"透干がないため月支本気の{stem}({god})を候補とする"],
                        "counter_evidence": list(counters),
                    })

    candidates.sort(key=lambda item: 0 if item["confidence"] == "high" else 1)
    return {
        "rule_id": "pattern_v1",
        "candidates": candidates,
        "policy": "普通格局のみ。確定ラベルではなく候補。外格は判定しない",
    }


def fu_yi_candidates(day_stem: str, strength: dict[str, Any]) -> dict[str, Any]:
    """strength_v2に基づく扶抑上の推奨五行候補。"""
    score = int(strength.get("score", 0))
    if strength.get("special_pattern_suspicion"):
        return {
            "rule_id": "fu_yi_v1", "status": "withheld",
            "hold_reason": "従格・専旺の可能性があるため通常の扶抑を適用しない",
            "candidates": [], "policy": "扶抑上の推奨五行候補。用神の確定ではない",
        }
    if 36 <= score <= 64:
        return {
            "rule_id": "fu_yi_v1", "status": "hold",
            "hold_reason": "身強弱が境界域のため自動判定を保留",
            "candidates": [], "policy": "扶抑上の推奨五行候補。用神の確定ではない",
        }

    day_element = STEM_ELEMENT[day_stem]
    if score >= 65:
        specs = [
            (_element_generate(day_element), "食傷(泄)"),
            (_element_control(day_element), "財(耗)"),
            (_controlling_element(day_element), "官殺(制)"),
        ]
        rationale = f"身強寄り(score {score})のため日干の力を流す・耗らす・制する五行を候補とする"
    else:
        specs = [(_generating_element(day_element), "印"), (day_element, "比劫")]
        rationale = f"身弱寄り(score {score})のため日干を生じる・支える五行を候補とする"
    candidates = [{
        "element": element,
        "role": role,
        "confidence": "medium",
        "rationale": [rationale],
        "counter_evidence": ["調候候補と競合する場合は調候を優先検討"],
    } for element, role in specs]
    return {
        "rule_id": "fu_yi_v1", "status": "proposed", "hold_reason": None,
        "candidates": candidates, "policy": "扶抑上の推奨五行候補。用神の確定ではない",
    }


def climate_candidates(month_branch: str) -> dict[str, Any]:
    specs = {
        "亥": ("火", "medium", "寒月のため暖が必要"),
        "子": ("火", "medium", "寒月のため暖が必要"),
        "丑": ("火", "medium", "寒月のため暖が必要"),
        "巳": ("水", "medium", "暑月のため潤が必要"),
        "午": ("水", "medium", "暑月のため潤が必要"),
        "未": ("水", "medium", "暑月のため潤が必要"),
        "寅": ("火", "low", "余寒が残る"),
        "申": ("水", "low", "残暑が残る"),
    }
    candidates = []
    if month_branch in specs:
        element, confidence, reason = specs[month_branch]
        candidates.append({
            "element": element,
            "confidence": confidence,
            "rationale": [reason],
            "counter_evidence": [],
        })
    return {
        "rule_id": "climate_simplified_v1",
        "candidates": candidates,
        "policy": "季節軸の簡易調候。窮通宝鑑の完全表は未実装",
    }


def yojin_candidates(day_stem: str, month_branch: str, strength: dict[str, Any]) -> dict[str, Any]:
    fu_yi = fu_yi_candidates(day_stem, strength)
    climate = climate_candidates(month_branch)
    fu_elements = {item["element"] for item in fu_yi["candidates"]}
    climate_elements = {item["element"] for item in climate["candidates"]}
    matched = [element for element in ELEMENTS if element in fu_elements & climate_elements]
    return {
        "fu_yi": fu_yi,
        "climate": climate,
        "agreement": {
            "matched_elements": matched,
            "conflicted": bool(fu_elements and climate_elements and not matched),
        },
    }


def build_structure_analysis(p4: Any, elapsed_days: float) -> dict[str, Any]:
    command = month_command(_kanshi(p4, "month")[1], elapsed_days)
    power = element_power(p4, command)
    strength = strength_v2(_kanshi(p4, "day")[0], power)
    patterns = pattern_candidates(p4, command, strength)
    yojin = yojin_candidates(_kanshi(p4, "day")[0], _kanshi(p4, "month")[1], strength)
    return {
        "hidden_stem_command": command,
        "element_power": power,
        "strength_v2": strength,
        "pattern_candidates": patterns,
        "yojin_candidates": yojin,
    }


calculate_element_power = element_power
calculate_strength_v2 = strength_v2
calculate_pattern_candidates = pattern_candidates
calculate_fu_yi_candidates = fu_yi_candidates
calculate_climate_candidates = climate_candidates
calculate_yojin_candidates = yojin_candidates
