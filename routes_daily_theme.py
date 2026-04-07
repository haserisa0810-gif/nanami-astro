from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")

SOFT_ACTION_LINE = "少し整える時間を先に確保すると、流れを立て直しやすくなります。"

DEFAULT_CAUTION = [
    "勢いだけで決めると、後から認識のズレや摩擦が表に出やすい。",
    "感情で反応した直後に結論を出すと、必要以上に話をこじらせやすい。",
]

DEFAULT_AVOID = [
    "思いつきだけで一気に話や作業を進めること。",
    "細部に詰まりすぎて、本来の流れや優先順位を見失うこと。",
]


def _calc_helpers():
    from services.transit_calc import calc_global_transit_snapshot
    return calc_global_transit_snapshot


def _safe_prompt_render(template: str, values: dict[str, Any]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace("{" + key + "}", str(value))
    return rendered


def _truncate_text(text: str, max_len: int) -> str:
    value = str(text or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "…"


def _sentences(text: str) -> list[str]:
    parts: list[str] = []
    normalized = str(text or "").replace("\n", "")
    for chunk in normalized.split("。"):
        chunk = chunk.strip()
        if chunk:
            parts.append(chunk + "。")
    return parts


def _clean_item(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("summary", "text", "label", "title", "name"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""

    text = str(value or "").strip()
    if not text:
        return ""

    if text.startswith("{") and text.endswith("}"):
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                for key in ("summary", "type_translation_axis", "social_post"):
                    val = str(obj.get(key) or "").strip()
                    if val:
                        return val
        except Exception:
            pass

    return text


def _coerce_list(value: Any, fallback_items: list[str], *, min_items: int = 2, max_items: int = 3) -> list[str]:
    items: list[str] = []

    if isinstance(value, list):
        seq = value
    elif isinstance(value, str) and value.strip():
        seq = [value]
    else:
        seq = []

    for item in seq:
        cleaned = _clean_item(item)
        if cleaned and cleaned not in items:
            items.append(cleaned)

    for fb in fallback_items:
        cleaned_fb = _clean_item(fb)
        if cleaned_fb and cleaned_fb not in items:
            items.append(cleaned_fb)
        if len(items) >= max(min_items, len(items)):
            # keep collecting until later trim
            pass

    if len(items) < min_items:
        for fb in fallback_items:
            cleaned_fb = _clean_item(fb)
            if cleaned_fb and cleaned_fb not in items:
                items.append(cleaned_fb)
            if len(items) >= min_items:
                break

    return items[:max_items]


def _parse_jsonish_response(raw: str, fallback: dict[str, object]) -> dict[str, object]:
    import ast
    import re

    cleaned = (raw or "").strip()

    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    def _normalize(text: str) -> str:
        t = (text or "").strip()
        start_obj = t.find("{")
        end_obj = t.rfind("}")
        if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
            t = t[start_obj : end_obj + 1].strip()
        t = t.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
        return t

    def _load_once(text: str):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, str):
                try:
                    parsed2 = json.loads(_normalize(parsed))
                    if isinstance(parsed2, dict):
                        return parsed2
                except Exception:
                    return None
        except Exception:
            return None
        return None

    norm = _normalize(cleaned)
    parsed = _load_once(norm)
    if parsed:
        return parsed

    try:
        lit = ast.literal_eval(norm)
        if isinstance(lit, dict):
            return lit
    except Exception:
        pass

    m = re.search(r'"summary"\s*:\s*"(.+?)"', norm, flags=re.DOTALL)
    if m:
        merged = dict(fallback)
        merged["summary"] = m.group(1).replace('\\"', '"').strip()
        merged["raw_text"] = raw or ""
        return merged

    merged = dict(fallback)
    merged["raw_text"] = raw or ""
    return merged


def _build_planet_summary(snapshot: dict[str, Any]) -> str:
    lines = []
    for p in snapshot.get("today_planets", [])[:10]:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name") or "").strip()
        sign = str(p.get("sign") or "").strip()
        degree = p.get("degree", 0)
        try:
            degree_text = f"{float(degree):.2f}°"
        except Exception:
            degree_text = str(degree)
        if name:
            lines.append(f"- {name}: {sign} {degree_text}".strip())
    return "\n".join(lines) if lines else "（データなし）"


def _build_aspect_summary(snapshot: dict[str, Any]) -> str:
    asp_map = {
        "conjunction": "コンジャンクション",
        "opposition": "オポジション",
        "square": "スクエア",
        "trine": "トライン",
        "sextile": "セクスタイル",
    }
    lines = []
    for a in snapshot.get("aspects", [])[:10]:
        if not isinstance(a, dict):
            continue
        pa = str(a.get("planet_a") or "").strip()
        pb = str(a.get("planet_b") or "").strip()
        aspect = asp_map.get(str(a.get("aspect") or "").strip(), str(a.get("aspect") or "").strip())
        orb = a.get("orb", 0)
        try:
            orb_text = f"{float(orb):.2f}°"
        except Exception:
            orb_text = str(orb)
        if pa and pb and aspect:
            lines.append(f"- {pa} {aspect} {pb} / orb {orb_text}")
    return "\n".join(lines) if lines else "（主要アスペクトなし）"


def _deterministic_theme_from_snapshot(
    *,
    snapshot: dict[str, Any],
    target_date: str,
    period: str,
    axis: str,
) -> dict[str, Any]:
    today_planets = snapshot.get("today_planets") or []
    aspects = snapshot.get("aspects") or []

    planet_names = [str(p.get("name") or "") for p in today_planets if isinstance(p, dict) and p.get("name")]
    aspect_names = [str(a.get("aspect") or "") for a in aspects if isinstance(a, dict)]

    has_moon = "Moon" in planet_names
    has_mercury = "Mercury" in planet_names
    has_venus = "Venus" in planet_names
    has_mars = "Mars" in planet_names
    has_jupiter = "Jupiter" in planet_names
    has_saturn = "Saturn" in planet_names

    has_square = "square" in aspect_names
    has_opposition = "opposition" in aspect_names
    has_trine = "trine" in aspect_names
    has_sextile = "sextile" in aspect_names
    has_conjunction = "conjunction" in aspect_names

    summary_parts: list[str] = []

    if has_square or has_opposition:
        summary_parts.append(
            "動きたい判断と慎重に整えたい感覚がぶつかりやすく、会話や段取りの場面で小さなズレが表に出やすい流れです。"
        )
    else:
        summary_parts.append(
            "全体としては流れを整えながら進めやすく、話し合いや作業の順番を少し調整するだけで噛み合いやすくなる流れです。"
        )

    if has_mercury and has_moon:
        summary_parts.append(
            "感情の反応が言葉に乗りやすいため、言い方や受け取り方ひとつで空気が大きく変わりやすい点が特徴です。"
        )
    elif has_mercury:
        summary_parts.append(
            "判断や連絡のテンポが早まりやすいぶん、説明不足のまま進めると認識差が残りやすくなります。"
        )
    elif has_moon:
        summary_parts.append(
            "気分や反応が先に立ちやすいため、結論より先に落ち着きを作ることが流れを整える鍵になります。"
        )

    if has_saturn and has_mars:
        summary_parts.append(
            "進めたい気持ちに対して現実的な制約や責任感がかかりやすく、勢いだけでは前に出にくい一方で、丁寧な再設計には向く時です。"
        )
    elif has_jupiter and (has_trine or has_sextile):
        summary_parts.append(
            "広げたい意欲を前向きに使いやすく、見通しを立てて共有すると周囲の協力も得やすくなります。"
        )

    summary = _truncate_text("".join(summary_parts), 220)

    core: list[str] = []
    push: list[str] = []
    caution: list[str] = []
    recommended: list[str] = []
    avoid: list[str] = []

    if has_square or has_opposition:
        core.append("勢いと慎重さのズレを整える")
        caution.append("正しさを急いでぶつけると、認識差が摩擦として残りやすい。")
        avoid.append("結論を急いで相手の反応を置き去りにすること。")

    if has_moon:
        core.append("感情の反応を先に落ち着かせる")
        caution.append("気分の波をそのまま判断に乗せると、必要以上に話が揺れやすい。")
        recommended.append("反応する前に、言葉と気持ちを一度切り分けて整理する。")

    if has_mercury:
        core.append("言葉の精度が空気を左右する")
        push.append("短く整理した説明ほど通りやすく、無駄な行き違いを減らしやすい。")
        recommended.append("連絡や説明は一度要点を絞ってから出し、確認の一文を添える。")

    if has_venus:
        push.append("柔らかい言い回しや配慮が関係調整にそのまま効きやすい。")
        recommended.append("意見を通す前に、相手が受け取りやすい形へ一段やわらげる。")

    if has_mars:
        push.append("手を動かしながら整える作業は進めやすく、停滞を崩すきっかけになりやすい。")
        caution.append("焦りが先に立つと、必要な確認を飛ばして後戻りが増えやすい。")
        avoid.append("思いつきの勢いで一気に着地まで持っていくこと。")

    if has_saturn:
        core.append("現実的な優先順位を引き直す")
        push.append("責任範囲や順番を整理すると、停滞していたことに筋道をつけやすい。")
        recommended.append("今やることと保留にすることを分け、負荷の置き場を明確にする。")

    if has_trine or has_sextile:
        push.append("小さな調整が全体の流れを整えやすく、協力や理解を得やすい。")

    if has_conjunction:
        core.append("テーマを一点に集めて扱う")
        caution.append("一点集中が強すぎると、周辺への目配りが抜けやすい。")

    if len(core) < 2:
        core.extend([
            "ズレを早めに微調整する",
            "感情と判断の間に余白を作る",
        ])

    if len(push) < 2:
        push.extend([
            "段取りを少し直すだけでも、結果と対話の噛み合いが良くなりやすい。",
            "急がず整える姿勢が、そのまま安定感として伝わりやすい。",
        ])

    if len(caution) < 2:
        caution.extend(DEFAULT_CAUTION)

    if len(recommended) < 2:
        recommended.extend([
            "一気に進める前に、目的・順番・伝え方の三点を整え直す。",
            SOFT_ACTION_LINE,
        ])

    if len(avoid) < 1:
        avoid.extend(DEFAULT_AVOID)

    social = (
        "今日は、勢いで押し切るより“整えてから動く”ほうが流れに乗りやすい日。"
        "言葉・段取り・感情の扱いを少し丁寧にするだけで、ズレや摩擦が落ち着きやすくなります。"
    )

    axis_text = (
        "勢いの強さを見るより、反応の出し方・言葉の精度・調整力をどう使うかを見ると、"
        "タイプ差を出しやすい日です。"
    )

    return {
        "date": target_date,
        "period": period,
        "axis": axis,
        "summary": summary or "今日は、流れを急がず整えるほど全体が噛み合いやすくなる日です。",
        "core_themes": core[:3],
        "push": push[:3],
        "caution": caution[:3],
        "recommended_actions": recommended[:3],
        "avoid_actions": avoid[:2],
        "social_post": _truncate_text(social, 160),
        "type_translation_axis": _truncate_text(axis_text, 85),
        "source": "deterministic_fallback",
    }


def _normalize_daily_theme_result(result: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    merged = dict(fallback)
    if isinstance(result, dict):
        merged.update(result)

    merged["date"] = str(merged.get("date") or fallback["date"]).strip()
    merged["period"] = str(merged.get("period") or fallback["period"]).strip()
    merged["axis"] = str(merged.get("axis") or fallback["axis"]).strip()

    summary = _clean_item(merged.get("summary")) or fallback["summary"]
    type_axis = _clean_item(merged.get("type_translation_axis")) or fallback["type_translation_axis"]
    social = _clean_item(merged.get("social_post")) or fallback["social_post"]

    merged["summary"] = _truncate_text(summary, 220)
    merged["type_translation_axis"] = _truncate_text(type_axis, 85)
    merged["social_post"] = _truncate_text(social, 160)

    merged["core_themes"] = _coerce_list(
        merged.get("core_themes"),
        fallback.get("core_themes", []),
        min_items=2,
        max_items=3,
    )
    merged["push"] = _coerce_list(
        merged.get("push"),
        fallback.get("push", []),
        min_items=2,
        max_items=3,
    )
    merged["caution"] = _coerce_list(
        merged.get("caution"),
        fallback.get("caution", DEFAULT_CAUTION),
        min_items=2,
        max_items=3,
    )
    merged["recommended_actions"] = _coerce_list(
        merged.get("recommended_actions"),
        fallback.get("recommended_actions", [SOFT_ACTION_LINE]),
        min_items=2,
        max_items=3,
    )
    merged["avoid_actions"] = _coerce_list(
        merged.get("avoid_actions"),
        fallback.get("avoid_actions", DEFAULT_AVOID),
        min_items=1,
        max_items=2,
    )

    if SOFT_ACTION_LINE not in merged["recommended_actions"]:
        merged["recommended_actions"].append(SOFT_ACTION_LINE)
        merged["recommended_actions"] = merged["recommended_actions"][:3]

    return merged


def _theme_quality_score(result: dict[str, Any]) -> int:
    score = 0

    summary = str(result.get("summary") or "").strip()
    social = str(result.get("social_post") or "").strip()
    axis = str(result.get("type_translation_axis") or "").strip()

    core = result.get("core_themes") if isinstance(result.get("core_themes"), list) else []
    push = result.get("push") if isinstance(result.get("push"), list) else []
    caution = result.get("caution") if isinstance(result.get("caution"), list) else []
    recommended = result.get("recommended_actions") if isinstance(result.get("recommended_actions"), list) else []
    avoid = result.get("avoid_actions") if isinstance(result.get("avoid_actions"), list) else []

    if len(summary) >= 110:
        score += 2
    elif len(summary) >= 80:
        score += 1

    if "。" in summary:
        score += 1
    if any(word in summary for word in ["場面", "会話", "連絡", "判断", "段取り", "感情", "ズレ", "摩擦", "流れ"]):
        score += 2
    if any(word in summary for word in ["なぜ", "ため", "ぶつか", "影響", "背景", "反応", "制約", "慎重"]):
        score += 1

    if len(core) >= 2:
        score += 1
    if len(push) >= 2:
        score += 1
    if len(caution) >= 2:
        score += 1
    if len(recommended) >= 2:
        score += 1
    if len(avoid) >= 1:
        score += 1

    if len(social) >= 90:
        score += 1
    if "。" in social:
        score += 1

    if len(axis) >= 35:
        score += 1

    return score


def _is_strong_theme(result: dict[str, Any]) -> bool:
    summary = str(result.get("summary") or "").strip()
    social = str(result.get("social_post") or "").strip()
    axis = str(result.get("type_translation_axis") or "").strip()

    core = result.get("core_themes") if isinstance(result.get("core_themes"), list) else []
    push = result.get("push") if isinstance(result.get("push"), list) else []
    caution = result.get("caution") if isinstance(result.get("caution"), list) else []
    recommended = result.get("recommended_actions") if isinstance(result.get("recommended_actions"), list) else []
    avoid = result.get("avoid_actions") if isinstance(result.get("avoid_actions"), list) else []

    if len(summary) < 100:
        return False
    if len(core) < 2 or len(push) < 2 or len(caution) < 2 or len(recommended) < 2 or len(avoid) < 1:
        return False
    if len(social) < 90 or len(axis) < 35:
        return False
    if _theme_quality_score(result) < 10:
        return False
    return True


def _call_llm_json(prompt: str, *, max_output_tokens: int = 1100) -> str:
    try:
        from google import genai
        from google.genai import types
    except Exception:
        return ""

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return ""

    client = genai.Client(api_key=api_key)
    model = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()

    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.35,
            top_p=0.9,
            max_output_tokens=max_output_tokens,
            response_mime_type="application/json",
        ),
    )
    text = getattr(resp, "text", None)
    return (text or "").strip()


def _build_retry_prompt(base_prompt: str, weak_result: dict[str, Any]) -> str:
    weak_json = json.dumps(weak_result, ensure_ascii=False, indent=2)
    return (
        base_prompt
        + "\n\n# 再生成指示（品質不足のため修正）\n"
        + "前回の出力は浅く、具体性が不足しています。\n"
        + "以下の弱い出力をそのまま繰り返さず、次の点を改善して再生成してください。\n"
        + "・summary に場面、ズレ、原因を明確に入れる\n"
        + "・push / caution / recommended_actions を具体化する\n"
        + "・social_post を投稿完成形にする\n"
        + "・曖昧な一般論を避ける\n"
        + "・JSON schema は厳守する\n\n"
        + "# 弱かった前回出力\n"
        + weak_json
    )


@router.get("/daily-theme", response_class=HTMLResponse)
def daily_theme_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="daily_theme.html",
        context={
            "request": request,
            "initial_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        },
    )


@router.post("/daily-theme/generate", response_class=JSONResponse)
async def daily_theme_generate(request: Request):
    try:
        calc_global_transit_snapshot = _calc_helpers()

        body = await request.json()
        date_str = str(body.get("date") or "").strip()
        period = str(body.get("period") or "daily").strip().lower()
        axis = str(body.get("axis") or "overall").strip().lower()
        lat = float(body.get("lat", 35.6895))
        lng = float(body.get("lng", 139.6917))

        target_date = None
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                raise HTTPException(status_code=400, detail="date は YYYY-MM-DD 形式で指定してください")

        snapshot = calc_global_transit_snapshot(target_date=target_date, lat=lat, lng=lng)

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl = (prompts_dir / "daily_theme.txt").read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")

        planet_summary = _build_planet_summary(snapshot)
        aspect_summary = _build_aspect_summary(snapshot)

        transit_date = str(snapshot.get("transit_date") or date_str or "").strip()

        prompt = _safe_prompt_render(
            tpl,
            {
                "common_rules": common_rules,
                "target_date": transit_date,
                "period": period,
                "axis": axis,
                "planet_summary": planet_summary,
                "aspect_summary": aspect_summary,
            },
        )

        fallback = _deterministic_theme_from_snapshot(
            snapshot=snapshot,
            target_date=transit_date,
            period=period,
            axis=axis,
        )

        raw = _call_llm_json(prompt, max_output_tokens=1100)
        parsed = _parse_jsonish_response(raw, fallback) if raw else fallback
        normalized = _normalize_daily_theme_result(parsed, fallback)

        if not _is_strong_theme(normalized):
            retry_prompt = _build_retry_prompt(prompt, normalized)
            raw_retry = _call_llm_json(retry_prompt, max_output_tokens=1200)
            retry_parsed = _parse_jsonish_response(raw_retry, fallback) if raw_retry else fallback
            retry_normalized = _normalize_daily_theme_result(retry_parsed, fallback)

            if _theme_quality_score(retry_normalized) >= _theme_quality_score(normalized):
                normalized = retry_normalized

        if not _is_strong_theme(normalized):
            normalized = _normalize_daily_theme_result(fallback, fallback)

        return JSONResponse(content=normalized)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@router.post("/daily-theme/generate-types", response_class=JSONResponse)
async def daily_theme_generate_types(request: Request):
    try:
        body = await request.json()
        base_theme = body.get("theme") or {}
        date_str = str(body.get("date") or base_theme.get("date") or "").strip()
        period = str(body.get("period") or base_theme.get("period") or "daily").strip().lower()
        axis = str(body.get("axis") or base_theme.get("axis") or "overall").strip().lower()
        raw_type_lines = body.get("type_lines") or []

        if isinstance(raw_type_lines, str):
            raw_type_lines = [raw_type_lines]
        if not isinstance(raw_type_lines, list):
            raise HTTPException(status_code=400, detail="type_lines は配列で指定してください")

        parsed_types: list[dict[str, str]] = []
        for line in raw_type_lines:
            if not isinstance(line, str):
                continue
            cleaned = line.strip()
            if not cleaned:
                continue
            if ":" in cleaned:
                name, hint = cleaned.split(":", 1)
            elif "：" in cleaned:
                name, hint = cleaned.split("：", 1)
            else:
                name, hint = cleaned, ""
            name = name.strip()
            hint = hint.strip()
            if name:
                parsed_types.append({"type_name": name, "type_hint": hint})

        if not parsed_types:
            raise HTTPException(status_code=400, detail="タイプを1件以上入力してください")

        prompts_dir = Path(__file__).resolve().parent / "prompts"
        tpl_path = prompts_dir / "daily_type_forecast.txt"

        if not tpl_path.exists():
            items = []
            base_summary = str(base_theme.get("summary") or "").strip() or "今日は流れを整えるほど動きやすい日です。"
            base_social = str(base_theme.get("social_post") or "").strip()
            for t in parsed_types:
                items.append({
                    "type_name": t["type_name"],
                    "type_hint": t["type_hint"],
                    "summary": base_summary,
                    "flow": "",
                    "likely_things": [],
                    "caution": [],
                    "advice": [SOFT_ACTION_LINE],
                    "social_post": base_social,
                })
            return JSONResponse(content={
                "date": date_str,
                "period": period,
                "axis": axis,
                "items": items,
                "source": "simple_fallback",
            })

        tpl = tpl_path.read_text(encoding="utf-8")
        common_rules = (prompts_dir / "common_rules.txt").read_text(encoding="utf-8")

        prompt = _safe_prompt_render(
            tpl,
            {
                "common_rules": common_rules,
                "target_date": date_str,
                "period": period,
                "axis": axis,
                "base_theme_json": json.dumps(base_theme, ensure_ascii=False, indent=2),
                "type_lines": json.dumps(parsed_types, ensure_ascii=False, indent=2),
            },
        )

        raw = _call_llm_json(prompt, max_output_tokens=1400)
        result: dict[str, Any] = {}

        if raw:
            try:
                result = json.loads(raw)
                if not isinstance(result, dict):
                    result = {}
            except Exception:
                result = {}

        if not result:
            items = []
            base_summary = str(base_theme.get("summary") or "").strip() or "今日は流れを整えるほど動きやすい日です。"
            base_social = str(base_theme.get("social_post") or "").strip()
            for t in parsed_types:
                items.append({
                    "type_name": t["type_name"],
                    "type_hint": t["type_hint"],
                    "summary": base_summary,
                    "flow": "",
                    "likely_things": [],
                    "caution": [],
                    "advice": [SOFT_ACTION_LINE],
                    "social_post": base_social,
                })
            result = {
                "date": date_str,
                "period": period,
                "axis": axis,
                "items": items,
                "source": "simple_fallback",
            }

        result.setdefault("date", date_str)
        result.setdefault("period", period)
        result.setdefault("axis", axis)
        if not isinstance(result.get("items"), list):
            result["items"] = []

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})
