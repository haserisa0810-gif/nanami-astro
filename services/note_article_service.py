from __future__ import annotations

import json
import logging
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

from services.ai_dispatcher import CLAUDE_HAIKU_MODEL, CLAUDE_SONNET_MODEL
from services.text_formatter import fix_punctuation, normalize_layout
from services.transit_calc import calc_global_transit_snapshot
from services.type_catalog import (
    get_type_definitions_for_prompt,
    get_type_subtype_combinations_for_prompt,
    get_type_subtype_groups_for_prompt,
)
from services.vedic_calc import build_vedic_gochara_points


JST = ZoneInfo("Asia/Tokyo")
logger = logging.getLogger(__name__)

ARTICLE_TYPES = {
    "monthly_themes": "今月のテーマ候補",
    "monthly_reading": "今月の星読み記事",
    "zodiac_fortunes": "12星座別運勢",
    "love_column": "恋愛コラム",
    "work_column": "仕事コラム",
    "sns_announcement": "SNS告知文",
    "type_monthly_fortunes": "タイプ別運勢",
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
CHANGE_PLANETS = {"Uranus", "Neptune", "Pluto"}
INNER_REVIEW_PLANETS = {"Moon", "Mercury", "Saturn"}
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


def _monthly_gochara_dates(target_month: str) -> dict[str, str]:
    year, month = validate_target_month(target_month)
    last_day = monthrange(year, month)[1]
    return {
        "month_start": f"{year:04d}-{month:02d}-01",
        "month_mid": f"{year:04d}-{month:02d}-{min(15, last_day):02d}",
        "month_end": f"{year:04d}-{month:02d}-{last_day:02d}",
    }


def extract_note_vedic_context(
    target_month: str,
) -> dict[str, Any] | None:
    gochara_dates = _monthly_gochara_dates(target_month)
    gochara = build_vedic_gochara_points(transit_dates=gochara_dates)
    gochara["target_month"] = target_month
    return _prune_empty({"gochara": gochara})


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


def _prune_empty(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned = {
            key: _prune_empty(item)
            for key, item in value.items()
            if item not in (None, "", [], {})
        }
        return {
            key: item
            for key, item in cleaned.items()
            if item not in (None, "", [], {})
        }
    if isinstance(value, list):
        return [
            item
            for item in (_prune_empty(item) for item in value)
            if item not in (None, "", [], {})
        ]
    return value


def _snapshot_for_yaml(snapshot: dict[str, Any]) -> dict[str, Any]:
    return _prune_empty(
        {
            "date": snapshot.get("transit_date"),
            "planet_positions": snapshot.get("today_planets"),
            "aspects": snapshot.get("aspects"),
            "aspect_count": snapshot.get("aspect_count"),
        }
    )


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
    change: list[dict[str, Any]] | None = None,
    inner_review: list[dict[str, Any]] | None = None,
) -> list[str]:
    candidates: list[str] = []
    if major:
        candidates.append(f"{major[0]['label']}から読む、今月いちばん大きな流れ")
    if love:
        candidates.append(f"{love[0]['label']}から読む、心の距離と関係の整え方")
    if work:
        candidates.append(f"{work[0]['label']}から読む、働き方と判断のタイミング")
    if change:
        candidates.append(f"{change[0]['label']}から読む、変化や切り替えの受け止め方")
    if inner_review:
        candidates.append(f"{inner_review[0]['label']}から読む、内面整理と見直しの進め方")
    if len(major) > 1:
        candidates.append(f"{major[1]['label']}を味方につけるための小さな選択")
    candidates.append("今月を前半・後半に分けて読む、動く時と整える時")
    return candidates[:5]


def _theme_score(events: list[dict[str, Any]], planets: set[str], *, tight_orb: float = 1.0) -> int:
    score = 0
    for event in events:
        if not ({event["planet_a"], event["planet_b"]} & planets):
            continue
        score += 2 if event["orb"] <= tight_orb else 1
        if event["days_active"] >= 5:
            score += 1
    return score


def _theme_profile(
    *,
    events: list[dict[str, Any]],
    love: list[dict[str, Any]],
    work: list[dict[str, Any]],
    change: list[dict[str, Any]],
    inner_review: list[dict[str, Any]],
    custom_theme: str = "",
) -> dict[str, Any]:
    scores = {
        "love_relationships": _theme_score(events, LOVE_PLANETS),
        "work_activity": _theme_score(events, WORK_PLANETS),
        "change_turning_point": _theme_score(events, CHANGE_PLANETS),
        "inner_review": _theme_score(events, INNER_REVIEW_PLANETS),
    }
    if custom_theme.strip():
        selected = "custom"
    else:
        selected = max(scores, key=scores.get) if any(scores.values()) else "monthly_flow"
    labels = {
        "love_relationships": "恋愛・対人",
        "work_activity": "仕事・活動",
        "change_turning_point": "変化・転機",
        "inner_review": "内面整理・見直し",
        "monthly_flow": "月全体の流れ",
        "custom": custom_theme.strip(),
    }
    evidence_map = {
        "love_relationships": love[:5],
        "work_activity": work[:5],
        "change_turning_point": change[:5],
        "inner_review": inner_review[:5],
        "monthly_flow": events[:5],
        "custom": events[:8],
    }
    return {
        "selected": selected,
        "label": labels.get(selected, selected),
        "scores": scores,
        "evidence": evidence_map.get(selected, []) or events[:5],
        "custom_theme": custom_theme.strip(),
    }


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
    change = [
        event
        for event in events
        if {event["planet_a"], event["planet_b"]} & CHANGE_PLANETS
    ][:8]
    inner_review = [
        event
        for event in events
        if {event["planet_a"], event["planet_b"]} & INNER_REVIEW_PLANETS
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
        "daily_snapshots": [_snapshot_for_yaml(snapshot) for snapshot in snapshots],
        "major_aspects": major,
        "tight_aspects": tight,
        "love_aspects": love,
        "work_aspects": work,
        "change_aspects": change,
        "inner_review_aspects": inner_review,
        "month_flow": _period_summary(events, year, month),
        "caution_dates": caution,
        "movement_dates": movement,
        "theme_candidates": _theme_candidates(major, love, work, change, inner_review),
        "theme_profile": _theme_profile(
            events=events,
            love=love,
            work=work,
            change=change,
            inner_review=inner_review,
        ),
        "snapshot_count": len(snapshots),
    }


def build_note_article_yaml(result: dict[str, Any]) -> str:
    context = result.get("transit_context") or {}
    article = _prune_empty(
        {
            "title": result.get("title"),
            "article_body": result.get("article_body"),
            "zodiac_fortunes": result.get("zodiac_fortunes"),
            "sns_copy": result.get("sns_copy"),
        }
    )
    western = _prune_empty(
        {
            "transits": {
                "target_month": context.get("target_month"),
                "month_label": context.get("month_label"),
                "snapshot_count": context.get("snapshot_count"),
                "theme_candidates": context.get("theme_candidates"),
                "theme_profile": context.get("theme_profile"),
                "major_aspects": context.get("major_aspects"),
                "tight_aspects": context.get("tight_aspects"),
                "love_aspects": context.get("love_aspects"),
                "work_aspects": context.get("work_aspects"),
                "change_aspects": context.get("change_aspects"),
                "inner_review_aspects": context.get("inner_review_aspects"),
                "month_flow": context.get("month_flow"),
                "caution_dates": context.get("caution_dates"),
                "movement_dates": context.get("movement_dates"),
                "daily_snapshots": context.get("daily_snapshots"),
            }
        }
    )
    payload = _prune_empty(
        {
            "version": "note-article-yaml-v1",
            "purpose": "Claudeでnote記事を作成・加筆するための計算済み占術データ",
            "note_article": {
                "target_month": result.get("target_month"),
                "article_type": result.get("article_type"),
                "article_type_label": result.get("article_type_label"),
                "custom_theme": result.get("custom_theme"),
                "model_label": result.get("model_label"),
                "generated_article": article,
                "warnings": result.get("warnings"),
            },
            "astrology_data": {
                "western_astrology": western,
                "vedic": result.get("vedic_context"),
            },
            "writing_instruction": {
                "use_only_provided_data": True,
                "do_not_recalculate": True,
                "missing_sections_mean_not_available": True,
            },
        }
    )
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False, width=120)


def _context_for_prompt(context: dict[str, Any]) -> str:
    def lines(key: str, empty: str = "該当なし") -> str:
        values = context.get(key, []) or []
        return "\n".join(f"- {_event_line(item)}" for item in values) or f"- {empty}"

    flow_lines = []
    for row in context.get("month_flow", []) or []:
        aspects = row.get("aspects") or ["主要アスペクトなし"]
        flow_lines.append(f"- {row.get('period')}: {' / '.join(aspects)}")
    theme_profile = context.get("theme_profile") or {}
    theme_evidence = "\n".join(f"- {_event_line(item)}" for item in theme_profile.get("evidence", []) or []) or "- 該当なし"

    return f"""対象月: {context['month_label']}

主要アスペクト:
{lines('major_aspects')}

タイトなアスペクト:
{lines('tight_aspects')}

恋愛テーマに使える配置:
{lines('love_aspects')}

仕事テーマに使える配置:
{lines('work_aspects')}

変化・転機テーマに使える配置:
{lines('change_aspects')}

内面整理・見直しテーマに使える配置:
{lines('inner_review_aspects')}

月全体の流れ:
{chr(10).join(flow_lines)}

注意日候補:
{lines('caution_dates')}

動きやすい日候補:
{lines('movement_dates')}

Python抽出のテーマ候補:
{chr(10).join(f"- {item}" for item in context['theme_candidates'])}

Python判定の中心テーマ:
- {theme_profile.get('label') or '月全体の流れ'}

中心テーマの根拠:
{theme_evidence}
"""


def _type_definitions_for_prompt() -> str:
    return json.dumps(get_type_definitions_for_prompt(), ensure_ascii=False, indent=2)


def _type_subtype_combinations_for_prompt() -> str:
    return json.dumps(get_type_subtype_combinations_for_prompt(), ensure_ascii=False, indent=2)


def _type_group_for_prompt(type_group: dict[str, Any] | None) -> str:
    if not type_group:
        return _type_subtype_combinations_for_prompt()
    return json.dumps(type_group, ensure_ascii=False, indent=2)


def _load_system_prompt() -> str:
    path = Path(__file__).resolve().parents[1] / "prompts" / "note_article_system.txt"
    return path.read_text(encoding="utf-8").strip()


def _build_user_prompt(
    *,
    context: dict[str, Any],
    article_type: str,
    custom_theme: str,
    type_group: dict[str, Any] | None = None,
) -> str:
    type_label = ARTICLE_TYPES[article_type]
    custom_line = custom_theme.strip() or "指定なし。配置データから自然なテーマを選ぶ。"
    type_fortune_instruction = ""
    if article_type == "type_monthly_fortunes":
        type_fortune_instruction = f"""

# タイプ別運勢の追加条件
これは完成済みの長文記事ではなく、有料noteや別のClaude AIで肉付けするための「鑑定素材」です。
無料の /type 診断本文ではなく、「親タイプ × サブタイプ × 今月の星」の読みの素材として作成してください。
article_body には、指定された親タイプ × サブタイプの短いMarkdown素材だけを書いてください。

必ず指定された全組み合わせを、次の見出し形式で出力してください。

## 親タイプ名 × サブタイプ名

根拠：
- 入力トランジットから最大2個

今月の読み：
2〜3文。長文にしない。1文は短くする。

出力してよい内容:
- そのタイプ × サブタイプに、今月の星の流れがどう出やすいか
- 今月だけの使い方、ズレにくい動き方
- 肉付け前の素材として使いやすい短い読み

出力してはいけない内容:
- 親タイプだけの長文運勢
- 無料 /type 診断の本質説明、基本性格、強み、苦手傾向の焼き直し
- 「仕事・恋愛・注意点」などの大見出しを全タイプに展開する長文記事
- 仕事・恋愛などの詳細見出し
- 全体冒頭の長い星の流れ
- 入力にない天体配置、日付、orb、度数

親タイプ一覧（参照用。本文ではこの説明を焼き直さない）:
{_type_definitions_for_prompt()}

今回出力する親タイプ × サブタイプ（必ずこの順番で全件出力）:
{_type_group_for_prompt(type_group)}

禁止:
- タイプ定義にない性質を断定しない。
- 西洋占星術・インド占星術・四柱推命の診断計算を再計算しない。
- 無料診断の summary を言い換えるだけの記事にしない。
- 入力にない天体配置、日付、orb、度数を追加しない。
- article_body を長文記事にしない。各組み合わせは「根拠2個まで」と「今月の読み2〜3文」まで。
"""
    zodiac_instruction = (
        "zodiac_fortunes には牡羊座から魚座まで12星座を順番に、各80〜140字で必ず含める。"
        if article_type == "zodiac_fortunes"
        else "zodiac_fortunes は空文字にする。"
    )
    return f"""以下の計算済みトランジット情報だけを根拠に、note記事の下書きを作成してください。

記事タイプ: {type_label}
任意テーマ: {custom_line}

{_context_for_prompt(context)}
{type_fortune_instruction}

# 出力形式（厳守）
必ず次のJSONオブジェクトだけを返してください。
JSONの前後に説明文、挨拶、注釈、Markdownコードフェンス、```json、``` を付けないでください。
キー名は必ず title, article_body, zodiac_fortunes, sns_copy の4つにしてください。
値はすべて文字列にしてください。改行は文字列内に含めて構いません。

{{
  "title": "note記事タイトル",
  "article_body": "note本文下書き。見出しを含めてよい",
  "zodiac_fortunes": "12星座別運勢、または空文字",
  "sns_copy": "SNS告知文。120〜220字程度"
}}

{zodiac_instruction}
{"article_body は短い鑑定素材にしてください。全体冒頭は入れないか、入れる場合も1文までにしてください。" if article_type == "type_monthly_fortunes" else "article_body は記事タイプに合う十分な長さで、一文を短くし、断定を避けてください。"}
配置に触れる場合は「金星 × 土星 トライン（orb 0.55°）」の順序で書き、
直後に生活レベルの感覚へ翻訳してください。
入力にない配置、日付、orb、度数は追加しないでください。
もう一度確認します。返答はJSONのみです。JSON以外の文章を前後に付けないでください。
"""


def _extract_response_text(response: Any) -> str:
    parts: list[str] = []
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", "") == "text":
            text = getattr(block, "text", "")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "\n".join(parts).strip()


def _strip_markdown_fence(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    elif cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()


def _json_candidates(raw: str) -> list[str]:
    cleaned = (raw or "").strip()
    unfenced = _strip_markdown_fence(cleaned)
    candidates = [unfenced]
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        candidates.append(cleaned[start : end + 1])
    start = unfenced.find("{")
    end = unfenced.rfind("}")
    if start >= 0 and end > start:
        candidates.append(unfenced[start : end + 1])
    return [candidate.strip() for candidate in candidates if candidate and candidate.strip()]


def _decode_first_json_object(text: str) -> dict[str, Any] | None:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _normalize_article_payload(parsed: dict[str, Any]) -> dict[str, str]:
    aliases = {
        "title": ("title", "タイトル"),
        "article_body": ("article_body", "body", "本文", "content", "article"),
        "zodiac_fortunes": ("zodiac_fortunes", "zodiac", "12星座別運勢"),
        "sns_copy": ("sns_copy", "sns", "SNS告知文", "announcement"),
    }
    result = {}
    for key, names in aliases.items():
        value = ""
        for name in names:
            if name in parsed:
                value = parsed.get(name, "")
                break
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, indent=2)
        result[key] = normalize_layout(fix_punctuation(str(value or "")))
    return result


def _fallback_article_payload(raw: str) -> dict[str, str] | None:
    body = normalize_layout(fix_punctuation(_strip_markdown_fence(raw)))
    if not body:
        return None
    return {
        "title": "",
        "article_body": body,
        "zodiac_fortunes": "",
        "sns_copy": "",
    }


def _parse_json_response(raw: str) -> dict[str, str]:
    logger.info("Claude note article raw response length=%s", len(raw or ""))
    logger.debug("Claude note article raw response:\n%s", raw)
    if not (raw or "").strip():
        raise NoteArticleError("Claudeから空の応答が返りました。もう一度生成してください。")

    parsed: dict[str, Any] | None = None
    for candidate in _json_candidates(raw):
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            value = _decode_first_json_object(candidate)
        if isinstance(value, dict):
            parsed = value
            break
    if parsed is None:
        logger.warning("Claude note article JSON parse failed. Falling back to raw text. raw=%r", raw[:2000])
        fallback = _fallback_article_payload(raw)
        if fallback:
            return fallback
        raise NoteArticleError("Claudeの応答を記事データとして読み取れませんでした。もう一度生成してください。")

    result = _normalize_article_payload(parsed)
    if not result["article_body"]:
        logger.warning("Claude note article JSON had no article_body. Falling back to raw text. keys=%s", sorted(parsed.keys()))
        fallback = _fallback_article_payload(raw)
        if fallback:
            fallback["title"] = result.get("title", "")
            fallback["zodiac_fortunes"] = result.get("zodiac_fortunes", "")
            fallback["sns_copy"] = result.get("sns_copy", "")
            return fallback
        raise NoteArticleError("Claudeからnote本文を取得できませんでした。")
    return result


def _response_warnings(response: Any) -> list[str]:
    stop_reason = str(getattr(response, "stop_reason", "") or "").strip()
    if stop_reason == "max_tokens":
        return ["出力が途中で切れた可能性があります。Claudeのmax_tokens上限に到達しました。"]
    return []


def _type_material_intro(context: dict[str, Any]) -> str:
    theme = ((context.get("theme_profile") or {}).get("label") or "月全体の流れ").strip()
    evidence = ((context.get("theme_profile") or {}).get("evidence") or [])[:2]
    lines = [f"# {context.get('month_label', '')} タイプ別運勢素材".strip(), "", f"全体メモ：{theme}を短く反映した鑑定素材です。"]
    if evidence:
        lines.extend(["", "主な根拠："])
        lines.extend(f"- {_event_line(item)}" for item in evidence)
    return "\n".join(lines).strip()


def _generate_type_monthly_fortunes(
    *,
    client: Any,
    model: str,
    context: dict[str, Any],
    custom_theme: str,
) -> dict[str, Any]:
    bodies: list[str] = []
    warnings: list[str] = []
    for group in get_type_subtype_groups_for_prompt():
        response = client.messages.create(
            model=model,
            max_tokens=1800,
            temperature=0.55,
            system=_load_system_prompt(),
            messages=[
                {
                    "role": "user",
                    "content": _build_user_prompt(
                        context=context,
                        article_type="type_monthly_fortunes",
                        custom_theme=custom_theme,
                        type_group=group,
                    ),
                }
            ],
        )
        warnings.extend(_response_warnings(response))
        generated = _parse_json_response(_extract_response_text(response))
        if generated.get("article_body"):
            bodies.append(generated["article_body"])

    if not bodies:
        raise NoteArticleError("Claudeからタイプ別運勢素材を取得できませんでした。")
    return {
        "title": f"{context.get('month_label', '')} タイプ別運勢素材".strip(),
        "article_body": normalize_layout("\n\n".join([_type_material_intro(context), *bodies])),
        "zodiac_fortunes": "",
        "sns_copy": "",
        "warnings": list(dict.fromkeys(warnings)),
    }


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
    context["theme_profile"] = _theme_profile(
        events=context.get("major_aspects", []) or [],
        love=context.get("love_aspects", []) or [],
        work=context.get("work_aspects", []) or [],
        change=context.get("change_aspects", []) or [],
        inner_review=context.get("inner_review_aspects", []) or [],
        custom_theme=custom_theme,
    )
    vedic_context = None
    vedic_warnings: list[str] = []
    try:
        vedic_context = extract_note_vedic_context(target_month)
    except Exception as exc:
        logger.warning("note article vedic context skipped: %r", exc)
        vedic_warnings.append(f"インド占星術ゴーチャラをYAMLに追加できませんでした: {exc}")
    factory = client_factory or Anthropic
    client = factory(api_key=api_key)
    model = CLAUDE_MODELS[model_key]["model"]
    if article_type == "type_monthly_fortunes":
        generated = _generate_type_monthly_fortunes(
            client=client,
            model=model,
            context=context,
            custom_theme=custom_theme,
        )
        result = {
            "target_month": target_month,
            "article_type": article_type,
            "article_type_label": ARTICLE_TYPES[article_type],
            "model_key": model_key,
            "model_label": CLAUDE_MODELS[model_key]["label"],
            "custom_theme": custom_theme.strip(),
            "transit_context": context,
            "vedic_context": vedic_context,
            **generated,
        }
        result["warnings"] = list(dict.fromkeys((result.get("warnings") or []) + vedic_warnings))
        result["yaml_preview"] = build_note_article_yaml(result)
        return result

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
    warnings = _response_warnings(response) + vedic_warnings
    generated = _parse_json_response(_extract_response_text(response))
    result = {
        "target_month": target_month,
        "article_type": article_type,
        "article_type_label": ARTICLE_TYPES[article_type],
        "model_key": model_key,
        "model_label": CLAUDE_MODELS[model_key]["label"],
        "custom_theme": custom_theme.strip(),
        "transit_context": context,
        "vedic_context": vedic_context,
        "warnings": warnings,
        **generated,
    }
    result["yaml_preview"] = build_note_article_yaml(result)
    return result
