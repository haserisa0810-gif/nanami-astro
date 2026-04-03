from __future__ import annotations

from typing import Any

SOFT_ACTION_LINE = "少し整える時間を意識すると、流れが落ち着きやすくなります。"


def append_soft_action_line(text: str) -> str:
    base = (text or "").strip()
    if not base:
        return SOFT_ACTION_LINE
    if SOFT_ACTION_LINE in base:
        return base
    return f"{base}\n\n{SOFT_ACTION_LINE}"


def enrich_daily_theme_result(result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict):
        return result

    social_post = (result.get("social_post") or "").strip()
    if social_post:
        result["social_post"] = append_soft_action_line(social_post)

    actions = result.get("recommended_actions")
    if isinstance(actions, list):
        if SOFT_ACTION_LINE not in actions:
            actions.append(SOFT_ACTION_LINE)
    elif not actions:
        result["recommended_actions"] = [SOFT_ACTION_LINE]

    return result
