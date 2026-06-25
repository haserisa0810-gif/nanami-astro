from __future__ import annotations

import json
import os
from calendar import monthrange
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

try:
    from anthropic import Anthropic
except Exception:  # pragma: no cover - optional dependency failure is reported at runtime
    Anthropic = None  # type: ignore

from services.ai_dispatcher import CLAUDE_HAIKU_MODEL, CLAUDE_SONNET_MODEL
from services.text_formatter import fix_punctuation, normalize_layout
from services.transit_calc import calc_global_transit_snapshot


JST = ZoneInfo("Asia/Tokyo")

ARTICLE_TYPES = {
    "monthly_themes": "今月のテーマ候補",
    "monthly_reading": "今月の星読み記事",
    "zodiac_fortunes": "12星座別運勢",
    "love_column": "恋愛コラム",
    "work_column": "仕事コラム",
    "sns_announcement": "SNS告知文",
}

CLAUDE_MODELS = {
    "haiku": {"label": "Claude Haiku（高速）", "model": CLAUDE_HAIKU_MODEL},
    "sonnet": {"label": "Claude Sonnet（高品質）", "model": CLAUDE_SONNET_MODEL},
}

PLANET_JA = {
    "Sun": "太陽",
    "Moon": "月",
    "Mercury": "水星",
    "Venus": "金星",
    "Mars": "火星",
    "Jupiter": "木星",
    "Saturn": "土星",
    "Uranus": "天王星",
    "Neptune": "海王星",
    "Pluto": "冥王星",
}

ASPECT_JA = {
    "conjunction": "コンジャンクション",
    "opposition": "オポジション",
    "square": "スクエア",
    "trine": "トライン",
    "sextile": "セクスタイル",
}

LOVE_PLANETS = {"Moon", "Venus", "Mars"}
WORK_PLANETS = {"Sun", "Mercury", "Saturn"}
HARD_ASPECTS = {"square", "opposition"}
FLOW_ASPECTS = {"trine", "sextile"}


class NoteArticleError(RuntimeError):
    pass


def default_target_month(today: date | None = None) -> str:
    value = today or datetime.now(JST).date()
    return value.strftime("%Y-%m")


def validate_target_month(value: str) -> tuple[int, int]:
    try:
        parsed = datetime.strptime((value or "").strip(), "%Y-%m")
    except ValueError as exc:
        raise NoteArticleError("対象月は YYYY-MM 形式で指定してください。") from exc
    if not 2000 <= parsed.year <= 2100:
        raise NoteArticleError("対象月は2000年から2100年の範囲で指定してください。")
    return parsed.year, parsed.month


def _aspect_key(item: dict[str, Any]) -> tuple[str, str, str]:
    planets = sorted([str(item.get("planet_a") or ""), str(item.get("planet_b") or "")])
    return planets[0], planets[1], str(item.get("aspect") or "")


def _aspect_label(item: dict[str, Any]) -> str:
    planet_a = PLANET_JA.get(str(item.get("planet_a") or ""), str(item.get("planet_a") or ""))
    planet_b = PLANET_JA.get(str(item.get("planet_b") or ""), str(item.get("planet_b") or ""))
    aspect = ASPECT_JA.get(str(item.get("aspect") or ""), str(item.get("aspect") or ""))
    return f"{planet_a} × {planet_b} {aspect}"


def _daily_snapshots(
    year: int,
    month: int,
    snapshot_loader: Callable[..., dict[str, Any]],
) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for day in range(1, monthrange(year, month)[1] + 1):
        target = datetime(year, month, day, 12, 0, tzinfo=JST)
        snapshot = snapshot_loader(target_date=target)
        snapshots.append(snapshot)
    return snapshots


def _collect_aspect_events(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for snapshot in snapshots:
        target_date = str(snapshot.get("transit_date") or "")
        for raw in snapshot.get("aspects", []) or []:
            if not isinstance(raw, dict):
                continue
            try:
                orb = float(raw.get("orb", 99))
            except (TypeError, ValueError):
                continue
            item = {
                "date": target_date,
                "planet_a": str(raw.get("planet_a") or ""),
                "planet_b": str(raw.get("planet_b") or ""),
                "aspect": str(raw.get("aspect") or ""),
                "orb": round(orb, 2),
            }
            if not all([item["date"], item["planet_a"], item["planet_b"], item["aspect"]]):
                continue
            grouped.setdefault(_aspect_key(item), []).append(item)

    events: list[dict[str, Any]] = []
    for occurrences in grouped.values():
        occurrences.sort(key=lambda item: item["date"])
        peak = min(occurrences, key=lambda item: item["orb"])
        events.append(
            {
                **peak,
                "label": _aspect_label(peak),
                "start_date": occurrences[0]["date"],
                "end_date": occurrences[-1]["date"],
                "days_active": len(occurrences),
            }
        )
    return sorted(events, key=lambda item: (item["orb"], item["date"], item["label"]))


def _event_line(event: dict[str, Any]) -> str:
    return f"{event['date']}: {event['label']}（orb {event['orb']:.2f}°）"


def _dedupe_dates(events: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        if event["date"] in seen:
            continue
        seen.add(event["date"])
        rows.append(event)
        if len(rows) >= limit:
            break
    return rows


def _period_summary(events: list[dict[str, Any]], year: int, month: int) -> list[dict[str, Any]]:
    last_day = monthrange(year, month)[1]
    periods = [
        ("上旬", 1, 10),
        ("中旬", 11, 20),
        ("下旬", 21, last_day),
    ]
    result: list[dict[str, Any]] = []
    for label, start_day, end_day in periods:
        matches = [
            event
            for event in events
            if start_day <= int(str(event["date"])[-2:]) <= end_day
        ]
        result.append(
            {
                "period": label,
                "aspects": [_event_line(event) for event in matches[:3]],
            }
        )
    return result


def _theme_candidates(
    major: list[dict[str, Any]],
    love: list[dict[str, Any]],
    work: list[dict[str, Any]],
) -> list[str]:
    candidates: list[str] = []
    if major:
        candidates.append(f"{major[0]['label']}から読む、今月いちばん大きな流れ")
    if love:
        candidates.append(f"{love[0]['label']}から読む、心の距離と関係の整え方")
    if work:
        candidates.append(f"{work[0]['label']}から読む、働き方と判断のタイミング")
    if len(major) > 1:
        candidates.append(f"{major[1]['label']}を味方につけるための小さな選択")
    candidates.append("今月を前半・後半に分けて読む、動く時と整える時")
    return candidates[:5]


def extract_monthly_transit_context(
    target_month: str,
    *,
    snapshot_loader: Callable[..., dict[str, Any]] = calc_global_transit_snapshot,
) -> dict[str, Any]:
    year, month = validate_target_month(target_month)
    snapshots = _daily_snapshots(year, month, snapshot_loader)
    events = _collect_aspect_events(snapshots)
    major = events[:12]
    tight = [event for event in events if event["orb"] <= 1.0][:10]
    love = [
        event
        for event in events
        if {event["planet_a"], event["planet_b"]} & LOVE_PLANETS
    ][:8]
    work = [
        event
        for event in events
        if {event["planet_a"], event["planet_b"]} & WORK_PLANETS
    ][:8]
    caution = _dedupe_dates(
        [event for event in events if event["aspect"] in HARD_ASPECTS and event["orb"] <= 1.25]
    )
    movement = _dedupe_dates(
        [event for event in events if event["aspect"] in FLOW_ASPECTS and event["orb"] <= 1.25]
    )

    return {
        "target_month": target_month,
        "month_label": f"{year}年{month}月",
        "major_aspects": major,
        "tight_aspects": tight,
        "love_aspects": love,
        "work_aspects": work,
        "month_flow": _period_summary(events, year, month),
        "caution_dates": caution,
        "movement_dates": movement,
        "theme_candidates": _theme_candidates(major, love, work),
        "snapshot_count": len(snapshots),
    }


def _context_for_prompt(context: dict[str, Any]) -> str:
    def lines(key: str, empty: str = "該当なし") -> str:
        values = context.get(key, []) or []
        return "\n".join(f"- {_event_line(item)}" for item in values) or f"- {empty}"

    flow_lines = []
    for row in context.get("month_flow", []) or []:
        aspects = row.get("aspects") or ["主要アスペクトなし"]
        flow_lines.append(f"- {row.get('period')}: {' / '.join(aspects)}")

    return f"""対象月: {context['month_label']}

主要アスペクト:
{lines('major_aspects')}

タイトなアスペクト:
{lines('tight_aspects')}

恋愛テーマに使える配置:
{lines('love_aspects')}

仕事テーマに使える配置:
{lines('work_aspects')}

月全体の流れ:
{chr(10).join(flow_lines)}

注意日候補:
{lines('caution_dates')}

動きやすい日候補:
{lines('movement_dates')}

Python抽出のテーマ候補:
{chr(10).join(f"- {item}" for item in context['theme_candidates'])}
"""


def _load_system_prompt() -> str:
    path = Path(__file__).resolve().parents[1] / "prompts" / "note_article_system.txt"
    return path.read_text(encoding="utf-8").strip()


def _build_user_prompt(
    *,
    context: dict[str, Any],
    article_type: str,
    custom_theme: str,
) -> str:
    type_label = ARTICLE_TYPES[article_type]
    custom_line = custom_theme.strip() or "指定なし。配置データから自然なテーマを選ぶ。"
    zodiac_instruction = (
        "zodiac_fortunes には牡羊座から魚座まで12星座を順番に、各80〜140字で必ず含める。"
        if article_type == "zodiac_fortunes"
        else "zodiac_fortunes は空文字にする。"
    )
    return f"""以下の計算済みトランジット情報だけを根拠に、note記事の下書きを作成してください。

記事タイプ: {type_label}
任意テーマ: {custom_line}

{_context_for_prompt(context)}

出力は次のJSONオブジェクトだけにしてください。Markdownコードフェンスは付けません。
{{
  "title": "note記事タイトル",
  "article_body": "note本文下書き。見出しを含めてよい",
  "zodiac_fortunes": "12星座別運勢、または空文字",
  "sns_copy": "SNS告知文。120〜220字程度"
}}

{zodiac_instruction}
article_body は記事タイプに合う十分な長さで、一文を短くし、断定を避けてください。
配置に触れる場合は「金星 × 土星 トライン（orb 0.55°）」の順序で書き、
直後に生活レベルの感覚へ翻訳してください。
入力にない配置、日付、orb、度数は追加しないでください。
"""


def _extract_response_text(response: Any) -> str:
    parts: list[str] = []
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", "") == "text":
            text = getattr(block, "text", "")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "\n".join(parts).strip()


def _parse_json_response(raw: str) -> dict[str, str]:
    cleaned = (raw or "").strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    candidates = [cleaned]
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        candidates.append(cleaned[start : end + 1])

    parsed: dict[str, Any] | None = None
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            parsed = value
            break
    if parsed is None:
        raise NoteArticleError("Claudeの応答を記事データとして読み取れませんでした。もう一度生成してください。")

    result = {}
    for key in ("title", "article_body", "zodiac_fortunes", "sns_copy"):
        value = parsed.get(key, "")
        result[key] = normalize_layout(fix_punctuation(str(value or "")))
    if not result["article_body"]:
        raise NoteArticleError("Claudeからnote本文を取得できませんでした。")
    return result


def generate_note_article(
    *,
    target_month: str,
    article_type: str,
    custom_theme: str = "",
    model_key: str = "haiku",
    snapshot_loader: Callable[..., dict[str, Any]] = calc_global_transit_snapshot,
    client_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    if article_type not in ARTICLE_TYPES:
        raise NoteArticleError("記事タイプが不正です。")
    if model_key not in CLAUDE_MODELS:
        raise NoteArticleError("Claudeモデルが不正です。")

    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise NoteArticleError(
            "ANTHROPIC_API_KEY が未設定です。note記事生成を使う場合だけ環境変数を設定してください。"
        )
    if Anthropic is None and client_factory is None:
        raise NoteArticleError("Anthropic SDKを読み込めません。note記事生成の依存関係を確認してください。")

    context = extract_monthly_transit_context(target_month, snapshot_loader=snapshot_loader)
    factory = client_factory or Anthropic
    client = factory(api_key=api_key)
    model = CLAUDE_MODELS[model_key]["model"]
    response = client.messages.create(
        model=model,
        max_tokens=7000 if article_type == "zodiac_fortunes" else 5000,
        temperature=0.65,
        system=_load_system_prompt(),
        messages=[
            {
                "role": "user",
                "content": _build_user_prompt(
                    context=context,
                    article_type=article_type,
                    custom_theme=custom_theme,
                ),
            }
        ],
    )
    generated = _parse_json_response(_extract_response_text(response))
    return {
        "target_month": target_month,
        "article_type": article_type,
        "article_type_label": ARTICLE_TYPES[article_type],
        "model_key": model_key,
        "model_label": CLAUDE_MODELS[model_key]["label"],
        "custom_theme": custom_theme.strip(),
        "transit_context": context,
        **generated,
    }
