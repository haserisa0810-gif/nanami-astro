from __future__ import annotations

import json
import os
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import yaml

try:
    from anthropic import Anthropic
except Exception:  # pragma: no cover - optional dependency failure is reported at runtime
    Anthropic = None  # type: ignore

from services.note_article_service import (
    _event_line,
    _extract_response_text,
    _prune_empty,
    _response_warnings,
    extract_monthly_transit_context,
)
from services.text_formatter import fix_punctuation, normalize_layout
from services.transit_calc import calc_global_transit_snapshot


JST = ZoneInfo("Asia/Tokyo")

PERIODS = {
    "month": "月",
    "quarter": "四半期",
    "year": "年",
}

THEMES = {
    "overall": "全体",
    "ai_technology": "AI・技術",
    "economy": "経済",
    "politics_systems": "政治・制度",
    "disaster_prevention": "防災・災害",
    "health_life": "医療・暮らし",
}

OUTPUT_TYPES = {
    "note_draft": "note記事下書き",
    "threads": "Threads投稿",
    "x": "X投稿",
    "instagram": "Instagram構成",
    "yaml": "YAML",
    "all": "まとめて生成",
}

CLAUDE_MODELS = {
    "sonnet": {
        "label": "Claude Sonnet 5 相当（標準）",
        "model": os.getenv("CLAUDE_SONNET_5_MODEL", "claude-sonnet-4-6"),
    },
    "opus": {
        "label": "Claude Opus 4.8（高品質）",
        "model": os.getenv("CLAUDE_OPUS_4_8_MODEL", "claude-opus-4-8"),
    },
}


class SocialFlowError(RuntimeError):
    pass


def default_base_date(today: date | None = None) -> str:
    return (today or datetime.now(JST).date()).strftime("%Y-%m-%d")


def _validate_base_date(value: str) -> date:
    try:
        parsed = datetime.strptime((value or "").strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise SocialFlowError("基準日は YYYY-MM-DD 形式で指定してください。") from exc
    if not 2000 <= parsed.year <= 2100:
        raise SocialFlowError("基準日は2000年から2100年の範囲で指定してください。")
    return parsed


def _month_keys_for_period(base: date, period: str) -> list[str]:
    if period == "month":
        months = [(base.year, base.month)]
    elif period == "quarter":
        quarter_start = ((base.month - 1) // 3) * 3 + 1
        months = [(base.year, quarter_start + offset) for offset in range(3)]
    elif period == "year":
        months = [(base.year, month) for month in range(1, 13)]
    else:
        raise SocialFlowError("期間が不正です。")
    return [f"{year:04d}-{month:02d}" for year, month in months]


def _period_label(base: date, period: str) -> str:
    if period == "month":
        return f"{base.year}年{base.month}月"
    if period == "quarter":
        quarter = ((base.month - 1) // 3) + 1
        start_month = ((base.month - 1) // 3) * 3 + 1
        end_month = start_month + 2
        return f"{base.year}年Q{quarter}（{start_month}月〜{end_month}月）"
    if period == "year":
        return f"{base.year}年"
    return PERIODS.get(period, period)


def _target_range(base: date, period: str) -> dict[str, str]:
    months = _month_keys_for_period(base, period)
    first = datetime.strptime(months[0], "%Y-%m").date().replace(day=1)
    last_year, last_month = [int(part) for part in months[-1].split("-")]
    last = date(last_year, last_month, monthrange(last_year, last_month)[1])
    return {"start_date": first.isoformat(), "end_date": last.isoformat()}


def _merge_contexts(contexts: list[dict[str, Any]], *, base: date, period: str) -> dict[str, Any]:
    def merged(key: str, limit: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for context in contexts:
            for item in context.get(key, []) or []:
                marker = (str(item.get("date") or ""), str(item.get("label") or ""))
                if marker in seen:
                    continue
                seen.add(marker)
                rows.append(item)
        rows.sort(key=lambda item: (float(item.get("orb", 99)), str(item.get("date") or "")))
        return rows[:limit]

    flow = []
    for context in contexts:
        flow.append(
            {
                "month": context.get("month_label"),
                "theme_candidates": context.get("theme_candidates", [])[:3],
                "major_aspects": context.get("major_aspects", [])[:5],
                "month_flow": context.get("month_flow", []),
            }
        )

    major = merged("major_aspects", 18 if period == "year" else 12)
    return _prune_empty(
        {
            "base_date": base.isoformat(),
            "period": period,
            "period_label": _period_label(base, period),
            "target_range": _target_range(base, period),
            "month_keys": [context.get("target_month") for context in contexts],
            "snapshot_count": sum(int(context.get("snapshot_count") or 0) for context in contexts),
            "major_aspects": major,
            "tight_aspects": merged("tight_aspects", 12),
            "change_aspects": merged("change_aspects", 10),
            "inner_review_aspects": merged("inner_review_aspects", 10),
            "work_aspects": merged("work_aspects", 10),
            "caution_dates": merged("caution_dates", 8),
            "movement_dates": merged("movement_dates", 8),
            "theme_candidates": [
                item
                for context in contexts
                for item in (context.get("theme_candidates", []) or [])[:3]
            ][:12],
            "period_flow": flow,
        }
    )


def build_social_flow_context(
    *,
    base_date: str,
    period: str,
    snapshot_loader: Callable[..., dict[str, Any]] = calc_global_transit_snapshot,
) -> dict[str, Any]:
    base = _validate_base_date(base_date)
    if period not in PERIODS:
        raise SocialFlowError("期間が不正です。")
    contexts = [
        extract_monthly_transit_context(month_key, snapshot_loader=snapshot_loader)
        for month_key in _month_keys_for_period(base, period)
    ]
    return _merge_contexts(contexts, base=base, period=period)


def _context_for_prompt(context: dict[str, Any]) -> str:
    def lines(key: str, empty: str = "該当なし") -> str:
        values = context.get(key, []) or []
        return "\n".join(f"- {_event_line(item)}" for item in values) or f"- {empty}"

    month_blocks = []
    for row in context.get("period_flow", []) or []:
        aspects = " / ".join(_event_line(item) for item in row.get("major_aspects", [])[:3]) or "主要アスペクトなし"
        themes = " / ".join(row.get("theme_candidates", [])[:2]) or "候補なし"
        month_blocks.append(f"- {row.get('month')}: {themes} | {aspects}")

    return f"""対象期間: {context.get('period_label')}
範囲: {(context.get('target_range') or {}).get('start_date')} 〜 {(context.get('target_range') or {}).get('end_date')}
確認日数: {context.get('snapshot_count')}

主要アスペクト:
{lines('major_aspects')}

タイトなアスペクト:
{lines('tight_aspects')}

変化・見直し候補:
{lines('change_aspects')}
{lines('inner_review_aspects')}

社会活動・制度面に使いやすい候補:
{lines('work_aspects')}

注意して見たい日:
{lines('caution_dates')}

動きやすい日:
{lines('movement_dates')}

月別メモ:
{chr(10).join(month_blocks) or '- 該当なし'}"""


def _load_prompt() -> str:
    return (Path(__file__).resolve().parent.parent / "prompts" / "social_flow.txt").read_text(encoding="utf-8")


def _render_prompt(template: str, values: dict[str, Any]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))
    return rendered


def _strip_fence(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```yaml"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def _parse_json(raw: str) -> dict[str, Any]:
    text = _strip_fence(raw)
    candidates = [text]
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidates.append(text[start : end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise SocialFlowError("Claudeの応答をJSONとして読み取れませんでした。もう一度生成してください。")


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [normalize_layout(fix_punctuation(str(item or ""))) for item in value if str(item or "").strip()]
    if str(value or "").strip():
        return [normalize_layout(fix_punctuation(str(value)))]
    return []


def _normalize_generated(parsed: dict[str, Any]) -> dict[str, Any]:
    note = parsed.get("note_draft") if isinstance(parsed.get("note_draft"), dict) else {}
    sns = parsed.get("sns") if isinstance(parsed.get("sns"), dict) else {}
    yaml_data = parsed.get("yaml") if isinstance(parsed.get("yaml"), dict) else {}
    verification = parsed.get("future_verification") if isinstance(parsed.get("future_verification"), dict) else {}

    return _prune_empty(
        {
            "title_options": _coerce_list(note.get("title_options") or parsed.get("title_options"))[:5],
            "note_draft": {
                "opening": normalize_layout(fix_punctuation(str(note.get("opening") or ""))),
                "social_theme": normalize_layout(fix_punctuation(str(note.get("social_theme") or ""))),
                "notable_fields": _coerce_list(note.get("notable_fields"))[:8],
                "news_viewpoints": _coerce_list(note.get("news_viewpoints"))[:8],
                "ai_yaml_lead": normalize_layout(fix_punctuation(str(note.get("ai_yaml_lead") or ""))),
                "soft_service_lead": normalize_layout(fix_punctuation(str(note.get("soft_service_lead") or ""))),
            },
            "sns": {
                "threads": normalize_layout(fix_punctuation(str(sns.get("threads") or ""))),
                "x": normalize_layout(fix_punctuation(str(sns.get("x") or ""))),
                "instagram": _coerce_list(sns.get("instagram"))[:10],
            },
            "yaml": yaml_data,
            "future_verification": {
                "previous_forecast_theme": verification.get("previous_forecast_theme") or "",
                "actual_news": verification.get("actual_news") or [],
                "reflection_notes": verification.get("reflection_notes") or "",
            },
        }
    )


def build_social_flow_yaml(result: dict[str, Any]) -> str:
    payload = _prune_empty(
        {
            "version": "social-flow-v1",
            "purpose": "note記事・SNS素材へ展開するための社会の流れ 星読み下書き",
            "base_date": result.get("base_date"),
            "target_period": result.get("period_label"),
            "period": result.get("period"),
            "theme": result.get("theme_label"),
            "major_social_themes": ((result.get("generated") or {}).get("yaml") or {}).get("major_social_themes"),
            "notable_fields": ((result.get("generated") or {}).get("yaml") or {}).get("notable_fields"),
            "sns_short_text": ((result.get("generated") or {}).get("yaml") or {}).get("sns_short_text"),
            "note_heading_ideas": ((result.get("generated") or {}).get("yaml") or {}).get("note_heading_ideas"),
            "future_verification": (result.get("generated") or {}).get("future_verification"),
            "astrology_data": result.get("transit_context"),
        }
    )
    payload["future_verification"] = {
        "previous_forecast_theme": "",
        "actual_news": [],
        "reflection_notes": "",
    }
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=120)


def generate_social_flow(
    *,
    base_date: str,
    period: str,
    theme: str,
    output_type: str,
    model_key: str = "sonnet",
    snapshot_loader: Callable[..., dict[str, Any]] = calc_global_transit_snapshot,
    client_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    if period not in PERIODS:
        raise SocialFlowError("期間が不正です。")
    if theme not in THEMES:
        raise SocialFlowError("テーマが不正です。")
    if output_type not in OUTPUT_TYPES:
        raise SocialFlowError("出力タイプが不正です。")
    if model_key not in CLAUDE_MODELS:
        raise SocialFlowError("Claudeモデルが不正です。")

    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise SocialFlowError("ANTHROPIC_API_KEY が未設定です。社会の流れ生成を使う場合だけ環境変数を設定してください。")
    if Anthropic is None and client_factory is None:
        raise SocialFlowError("Anthropic SDKを読み込めません。社会の流れ生成の依存関係を確認してください。")

    context = build_social_flow_context(base_date=base_date, period=period, snapshot_loader=snapshot_loader)
    prompt = _render_prompt(
        _load_prompt(),
        {
            "base_date": context.get("base_date"),
            "period_label": context.get("period_label"),
            "period_type": PERIODS[period],
            "theme_label": THEMES[theme],
            "output_type_label": OUTPUT_TYPES[output_type],
            "transit_context": _context_for_prompt(context),
        },
    )

    factory = client_factory or Anthropic
    client = factory(api_key=api_key)
    model = CLAUDE_MODELS[model_key]["model"]
    response = client.messages.create(
        model=model,
        max_tokens=4500,
        temperature=0.55,
        system="あなたは七海先生の運用チーム向けに、マンデン占星術ベースの下書きを作る編集アシスタントです。",
        messages=[{"role": "user", "content": prompt}],
    )
    warnings = _response_warnings(response)
    generated = _normalize_generated(_parse_json(_extract_response_text(response)))
    result = {
        "base_date": context.get("base_date"),
        "period": period,
        "period_label": context.get("period_label"),
        "theme": theme,
        "theme_label": THEMES[theme],
        "output_type": output_type,
        "output_type_label": OUTPUT_TYPES[output_type],
        "model_key": model_key,
        "model_label": CLAUDE_MODELS[model_key]["label"],
        "transit_context": context,
        "generated": generated,
        "warnings": warnings,
    }
    result["yaml_preview"] = build_social_flow_yaml(result)
    return result
