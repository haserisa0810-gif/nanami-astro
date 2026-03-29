from typing import Any, Dict, List, Tuple


def summarize_chart(raw: Dict[str, Any]) -> List[Tuple[str, List[str]]]:
    planets = raw.get("planets", []) or []
    by_sign: Dict[str, List[str]] = {}

    for p in planets:
        sign = p.get("sign")
        name = p.get("name") or p.get("id")
        if sign and name:
            by_sign.setdefault(sign, []).append(name)

    focus = sorted(by_sign.items(), key=lambda x: len(x[1]), reverse=True)
    return focus[:3]


def build_human_summary(focus: List[Tuple[str, List[str]]]) -> str:
    lines = ["🌌 惑星集中（上位3）"]
    for sign, names in focus:
        lines.append(f"・{sign}: {len(names)} → {', '.join(names)}")
    return "\n".join(lines)
