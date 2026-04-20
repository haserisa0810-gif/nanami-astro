from __future__ import annotations

from datetime import date, datetime
from typing import Any


def _to_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def calculate_age(birth_date: Any, today: date | None = None) -> int | None:
    born = _to_date(birth_date)
    if born is None:
        return None
    today = today or date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


# -------------------------
# 年齢モード
# -------------------------
def detect_age_mode(birth_date: Any) -> str:
    age = calculate_age(birth_date)
    if age is None:
        return "adult"
    if age <= 12:
        return "child"
    if 13 <= age <= 18:
        return "teen"
    return "adult"


# -------------------------
# structured判定
# -------------------------
def detect_structured_mode(structure: dict[str, Any] | None) -> dict[str, Any]:
    structure = structure or {}
    score = 0
    reasons: list[str] = []

    connection_density = int(structure.get("connection_density", 0) or 0)
    if connection_density >= 12:
        score += 5
        reasons.append(f"connection_density:{connection_density}")

    if bool(structure.get("house_concentration", False)):
        score += 4
        reasons.append("house_concentration")

    hard_aspects = int(structure.get("hard_aspects", 0) or 0)
    if hard_aspects >= 6:
        score += 4
        reasons.append(f"hard_aspects:{hard_aspects}")

    if bool(structure.get("contradictions", False)):
        score += 4
        reasons.append("contradictions")

    mode = "structured" if score >= 8 else "general"
    return {
        "score": score,
        "mode": mode,
        "reasons": reasons,
        "inputs": {
            "connection_density": connection_density,
            "house_concentration": bool(structure.get("house_concentration", False)),
            "hard_aspects": hard_aspects,
            "contradictions": bool(structure.get("contradictions", False)),
        },
    }


# -------------------------
# インド発火
# -------------------------
def detect_vedic_trigger(message: str = "", observations: str = "", chart_flags: dict[str, Any] | None = None, structured: bool = False) -> dict[str, Any]:
    chart_flags = chart_flags or {}

    score = 0
    reasons: list[str] = []

    message = str(message or "")
    observations = str(observations or "")
    text = f"{message} {observations}".strip()

    strong = ["運命", "宿命", "繰り返す", "なぜいつも", "意味", "転機"]
    medium = ["流れ", "タイミング", "なぜか", "不思議"]

    for w in strong:
        if w in text:
            score += 5
            reasons.append(f"text:{w}")

    for w in medium:
        if w in text:
            score += 3
            reasons.append(f"text:{w}")

    if "繰り返し" in observations:
        score += 6
        reasons.append("obs:repeat")

    if "説明できない" in observations:
        score += 5
        reasons.append("obs:unknown")

    if chart_flags.get("node"):
        score += 4
        reasons.append("chart:node")

    if chart_flags.get("deep_house"):
        score += 3
        reasons.append("chart:deep_house")

    if chart_flags.get("pluto"):
        score += 2
        reasons.append("chart:pluto")

    if chart_flags.get("lilith_chiron"):
        score += 2
        reasons.append("chart:lilith")

    if structured:
        score += 2
        reasons.append("mode:structured")

    text_trigger = any(w in text for w in strong + medium) or bool(observations.strip())
    chart_flag_count = sum(1 for k in ("node", "deep_house", "pluto", "lilith_chiron") if chart_flags.get(k))

    if not text_trigger:
        if chart_flag_count >= 3 and (chart_flags.get("deep_house") or chart_flags.get("pluto") or chart_flags.get("lilith_chiron")):
            score = max(score, 12)
            score = min(score, 16)
            reasons.append("chart_only:qualified")
        else:
            score = min(score, 11)
            reasons.append("chart_only:threshold_not_met")

    if score >= 20:
        level = "strong"
    elif score >= 12:
        level = "light"
    else:
        level = "off"

    return {
        "score": score,
        "level": level,
        "reasons": reasons,
        "text_required": False,
    }


# -------------------------
# 配合決定
# -------------------------
def decide_distribution(base_mode: str, vedic_level: str) -> dict[str, int]:
    base_mode = str(base_mode or "balanced").strip().lower()
    if base_mode == "western":
        base = {"western": 50, "shichu": 40, "vedic": 10}
    elif base_mode == "shichu":
        base = {"western": 30, "shichu": 60, "vedic": 10}
    else:
        base = {"western": 45, "shichu": 45, "vedic": 10}

    if vedic_level == "strong":
        base["vedic"] += 10
    elif vedic_level == "light":
        base["vedic"] += 5

    return base
