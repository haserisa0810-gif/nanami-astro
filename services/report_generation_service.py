from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
from typing import Any
import json
import re

from google.cloud import storage

try:
    from anthropic import Anthropic
except Exception:  # pragma: no cover
    Anthropic = None  # type: ignore

from models import ExternalOrder
from services.location import resolve_birth_location
from services.location_normalizer import normalize_location, NormalizedLocation
from services.analyze_engine import build_base_meta, build_handoff_logs, build_payload_a, run_astro_calc
from services.transit_calc import calc_transits_long_term, calc_transits_single
from services.western_calc import calc_western_from_payload
from services.external_report_template_renderer import build_chapter_json_prompt, chapter_specs, parse_chapter_json, render_external_report_html

PROMPTS_DIR = Path(__file__).resolve().parents[1] / "prompts"

REPORT_PLAN_LABELS = {
    "light": "ライトプラン（西洋＋四柱・1〜4章）",
    "standard": "スタンダードプラン（西洋＋四柱・全7章）",
    "premium": "プレミアムプラン（三術統合・全13章）",
}
REPORT_PLAN_OPTIONS = ["light", "standard", "premium"]

REPORT_OPTION_LABELS = {
    "option_asteroids": "小惑星（Ceres / Pallas / Juno / Vesta）",
    "option_transit": "トランジット時期読み",
    "option_special_points": "特殊星・隠れた才能",
    "option_year_forecast": "今年・来年の運勢",
}

REPORT_OPTION_DEFAULTS = {
    "light": {"option_asteroids": False, "option_transit": False, "option_special_points": False, "option_year_forecast": False},
    "standard": {"option_asteroids": False, "option_transit": True, "option_special_points": False, "option_year_forecast": True},
    # premium は商品仕様として全オプション込み。画面入力やSTORES連携値よりこちらを優先します。
    "premium": {"option_asteroids": True, "option_transit": True, "option_special_points": True, "option_year_forecast": True},
}

PREMIUM_ALL_INCLUDED_OPTIONS = dict(REPORT_OPTION_DEFAULTS["premium"])

DEFAULT_CLAUDE_REPORT_MODEL = "claude-sonnet-4-6"


def resolve_report_claude_model() -> str:
    """外部受注鑑定書で使うClaudeモデル。

    ANTHROPIC_API_KEYは既存Haikuと同じキーを使います。
    モデルだけ環境変数で差し替え可能にしています。
    """
    return (
        os.getenv("EXTERNAL_REPORT_CLAUDE_MODEL")
        or os.getenv("ANTHROPIC_MODEL")
        or os.getenv("CLAUDE_SONNET_MODEL")
        or DEFAULT_CLAUDE_REPORT_MODEL
    ).strip() or DEFAULT_CLAUDE_REPORT_MODEL


def _read_prompt(name: str) -> str:
    path = (PROMPTS_DIR / name).resolve()
    prompts_root = PROMPTS_DIR.resolve()
    if prompts_root not in path.parents and path != prompts_root:
        raise ValueError("invalid prompt path")
    if not path.exists():
        raise FileNotFoundError(f"Prompt template not found: {path}")
    return path.read_text(encoding="utf-8")


def _bucket_name() -> str:
    return (os.getenv("EXTERNAL_REPORTS_BUCKET") or os.getenv("STAFF_REPORTS_BUCKET") or "").strip()


def _storage_object_name(order_code: str) -> str:
    return f"external_reports/{order_code}/report.html"


def _strip_code_fence(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        lines = t.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        t = "\n".join(lines).strip()
    return t


def _gender_for_calc(value: str | None) -> str:
    raw = (value or "").strip().lower()
    if raw in {"male", "m", "男性", "男"}:
        return "male"
    if raw in {"female", "f", "女性", "女", ""}:
        return "female"
    return raw or "female"


def _plan_config(plan: str) -> dict[str, Any]:
    p = (plan or "standard").strip().lower()
    if p == "light":
        return {
            "plan": "light",
            "astrology_system": "integrated_w_shichu",
            "prompt_file": "external_prompt_w_shichu_v3.txt",
            "prompt_key": "external_w_shichu_v3_light",
            "chapter_instruction": "ライトプランです。図セクションと第1章〜第4章のみ生成してください。第5章以降、目次の第5章以降、未生成章の見出しは出力しないでください。",
            "include_vedic": False,
            "include_premium_points": False,
        }
    if p == "premium":
        return {
            "plan": "premium",
            "astrology_system": "integrated3",
            "prompt_file": "external_prompt_integrated_v4.txt",
            "prompt_key": "external_integrated_v4_premium",
            "chapter_instruction": "プレミアムプランです。図セクションと第1章〜第13章をすべて生成してください。",
            "include_vedic": True,
            "include_premium_points": True,
        }
    return {
        "plan": "standard",
        "astrology_system": "integrated_w_shichu",
        "prompt_file": "external_prompt_w_shichu_v3.txt",
        "prompt_key": "external_w_shichu_v3_standard",
        "chapter_instruction": "スタンダードプランです。図セクションと第1章〜第7章をすべて生成してください。",
        "include_vedic": False,
        "include_premium_points": False,
    }


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    raw = str(value).strip().lower()
    if raw in {"1", "true", "on", "yes", "y", "checked"}:
        return True
    if raw in {"0", "false", "off", "no", "n", ""}:
        return False
    return default


def default_report_options(plan: str) -> dict[str, bool]:
    cfg = _plan_config(plan)
    return dict(REPORT_OPTION_DEFAULTS.get(cfg["plan"], REPORT_OPTION_DEFAULTS["standard"]))


def normalize_report_options(plan: str, options: dict[str, Any] | None = None) -> dict[str, bool]:
    """プラン初期値と手動オプションを統合します。

    light / standard は詳細オプションで上書き可能。
    premium は「全部入り」商品のため、手動値に関係なく全オプションONに固定します。
    """
    cfg = _plan_config(plan)
    if cfg["plan"] == "premium":
        return dict(PREMIUM_ALL_INCLUDED_OPTIONS)

    merged = default_report_options(cfg["plan"])
    for key in REPORT_OPTION_LABELS:
        if options and key in options:
            merged[key] = bool(options.get(key))
    return merged


def order_report_options(order: ExternalOrder, *, plan: str | None = None) -> dict[str, bool]:
    p = plan or getattr(order, "report_generation_plan", None) or "standard"
    raw: dict[str, Any] = {}
    for key in REPORT_OPTION_LABELS:
        val = getattr(order, key, None)
        if val is not None:
            raw[key] = bool(val)
    return normalize_report_options(p, raw)


def build_report_calc_options(plan: str, report_options: dict[str, bool] | None = None) -> dict[str, bool]:
    report_options = report_options or default_report_options(plan)
    return {
        "include_chiron": True,
        "include_lilith": True,
        "include_vertex": True,
        "include_asteroids": bool(report_options.get("option_asteroids")),
    }


def build_single_transit_data_for_report(payload_a: dict[str, Any]) -> dict[str, Any]:
    tmp = calc_western_from_payload(payload_a)
    natal_planets = tmp.get("planets", [])
    today_transit = calc_transits_single(natal_planets)
    long_term = calc_transits_long_term(natal_planets)
    return {**today_transit, "long_term": long_term}


def build_external_order_handoff(order: ExternalOrder, *, plan: str = "standard", report_options: dict[str, bool] | None = None) -> tuple[str, dict[str, Any]]:
    if not order.birth_date:
        raise ValueError("生年月日が未入力です")

    cfg = _plan_config(plan)
    gender = _gender_for_calc(order.gender)
    resolved_loc = resolve_birth_location(order.prefecture, order.birth_place)
    loc: NormalizedLocation = normalize_location(
        resolved_loc,
        fallback_prefecture=order.prefecture,
        fallback_place=order.birth_place,
    )
    unknowns: list[str] = []
    if not order.birth_time:
        unknowns.append("birth_time")
    if not loc.has_coords:
        unknowns.append("birth_location")

    report_options = normalize_report_options(cfg["plan"], report_options or order_report_options(order, plan=cfg["plan"]))
    calc_options = build_report_calc_options(cfg["plan"], report_options)
    payload_a = build_payload_a(
        birth_date=order.birth_date.isoformat(),
        birth_time=order.birth_time,
        birth_place=loc.place or order.birth_place,
        prefecture=loc.prefecture or order.prefecture,
        lat=loc.lat,
        lon=loc.lon,
        gender=gender,
        house_system="P",
        node_mode="true",
        lilith_mode="mean",
        include_asteroids=calc_options["include_asteroids"],
        include_chiron=calc_options["include_chiron"],
        include_lilith=calc_options["include_lilith"],
        include_vertex=calc_options["include_vertex"],
        unknowns=unknowns,
    )
    include_transit = bool(report_options.get("option_transit") or report_options.get("option_year_forecast"))
    transit_data: dict[str, Any] | None = None
    if include_transit:
        try:
            transit_data = build_single_transit_data_for_report(payload_a)
        except Exception:
            transit_data = {"error": "トランジット計算に失敗しました"}

    base_meta = build_base_meta(
        birth_date=order.birth_date.isoformat(),
        output_style="normal",
        detail_level="standard",
        house_system="P",
        node_mode="true",
        lilith_mode="mean",
        include_asteroids=calc_options["include_asteroids"],
        include_chiron=calc_options["include_chiron"],
        include_lilith=calc_options["include_lilith"],
        include_vertex=calc_options["include_vertex"],
        include_reader=False,
        theme="overall",
        message=order.consultation_text,
        observations_text=None,
        analysis_type="single",
        astrology_system=cfg["astrology_system"],
        ai_provider="claude",
        ai_model="sonnet",
        day_change_at_23=False,
        name=order.customer_name,
        name_b=None,
        gender=gender,
        gender_b="female",
    )
    # 外部鑑定書生成では、通常鑑定用AI本文は生成しない。
    # ここで run_single() を呼ぶと通常鑑定AIが先に走り、
    # Claude鑑定書HTML生成まで到達しない／タイムアウトする原因になる。
    # 必要なのは計算結果とhandoff YAMLだけなので run_astro_calc() に限定する。
    astro_result = run_astro_calc(cfg["astrology_system"], payload_a, day_change_at_23=False)
    if isinstance(astro_result, dict):
        astro_result.setdefault("_meta", {}).update(base_meta)
        if isinstance(transit_data, dict):
            astro_result["transit"] = transit_data
    payload_view = {**payload_a, "day_change_at_23": False}
    report_web = ""
    report_line = ""
    report_raw = ""
    report_reader = ""
    guard_meta: dict[str, Any] = {"status": "skipped", "ok": True, "issues": [], "reason": "external_report_handoff_only"}
    logs = build_handoff_logs(
        inputs_view={
            "source": "external",
            "order_code": order.order_code,
            "plan": cfg["plan"],
            "name": order.customer_name,
            "birth_date": order.birth_date.isoformat(),
            "birth_time": order.birth_time,
            "birth_prefecture": loc.prefecture or order.prefecture,
            "birth_place": loc.place or order.birth_place,
            "birth_lat": loc.lat,
            "birth_lon": loc.lon,
            "location_source": loc.source,
            "gender": gender,
            "consultation_text": order.consultation_text,
            "include_chiron": calc_options["include_chiron"],
            "include_lilith": calc_options["include_lilith"],
            "include_vertex": calc_options["include_vertex"],
            "include_asteroids": calc_options["include_asteroids"],
            "option_transit": bool(report_options.get("option_transit")),
            "option_special_points": bool(report_options.get("option_special_points")),
            "option_year_forecast": bool(report_options.get("option_year_forecast")),
        },
        payload_view=payload_view,
        unknowns=unknowns,
        astro_result=astro_result,
        report_web=report_web,
        report_raw=report_raw,
        report_reader=report_reader,
        report_line=report_line,
        observations_text=None,
        bias_guard_obj=guard_meta if isinstance(guard_meta, dict) else {},
        transit=transit_data,
    )
    yaml_body = logs.get("handoff_yaml_full") or logs.get("handoff_yaml") or ""
    if not yaml_body.strip():
        raise RuntimeError("handoff YAMLを生成できませんでした")
    return yaml_body, {"config": cfg, "logs": logs, "location": loc, "report_options": report_options, "astro_result": astro_result}


def build_option_instruction(plan: str, options: dict[str, bool]) -> str:
    cfg = _plan_config(plan)
    p = cfg["plan"]
    asteroids = bool(options.get("option_asteroids"))
    transit = bool(options.get("option_transit"))
    special = bool(options.get("option_special_points"))
    year = bool(options.get("option_year_forecast"))

    lines: list[str] = []
    if p == "light":
        lines.append("  - 基本範囲は図セクションと第1章〜第4章です。")
        if asteroids:
            lines.append("  - 『小惑星リーディング』をEX章として追加してください。章番号はEX01にしてください。")
        if transit:
            lines.append("  - 『トランジット時期読み』をEX章として追加してください。章番号はEX02にしてください。")
        if special:
            lines.append("  - 『特殊星・隠れた才能』をEX章として追加してください。章番号はEX03にしてください。")
        if year:
            lines.append("  - 『今年・来年の運勢』をEX章として追加してください。章番号はEX04にしてください。")
        if not (asteroids or transit or special or year):
            lines.append("  - 第5章以降とEX章は出力しないでください。")
    elif p == "standard":
        lines.append("  - 基本範囲は図セクションと第1章〜第7章です。")
        lines.append("  - 第7章『今年・来年の運勢』はスタンダード基本章です。option_year_forecastがOFFの場合だけ出力しないでください。")
        if not year:
            lines.append("  - 第7章『今年・来年の運勢』は出力しないでください。目次にも載せないでください。")
        if asteroids:
            lines.append("  - 『小惑星リーディング』をEX章として第6章の後または末尾に追加してください。章番号はEX01にしてください。")
        if transit:
            lines.append("  - 第7章を出す場合はトランジット時期読みを第7章内に統合してください。第7章を出さない場合は『トランジット時期読み』をEX02として追加してください。")
        if special:
            lines.append("  - 『特殊星・隠れた才能』をEX章として追加してください。章番号はEX03にしてください。")
    else:
        lines.append("  - プレミアムは全オプション込みです。図セクションと第1章〜第13章をすべて生成してください。")
        lines.append("  - 小惑星・特殊点・トランジット・今年来年運勢は追加章ではなく、13章構成の中に標準搭載として自然に統合してください。")
        lines.append("  - 第10章『特殊星・隠れた才能』と第13章『今年・来年の運勢』は必ず出力してください。")

    if asteroids:
        lines.append("  - 小惑星（Ceres / Pallas / Juno / Vesta）を図セクションと本文の根拠に含めてください。")
    else:
        lines.append("  - 小惑星（Ceres / Pallas / Juno / Vesta）は本文で扱わないでください。Chiron / Lilith / Vertex は使用可です。")

    if transit or year:
        lines.append("  - transitデータがYAMLにある場合は時期読みの根拠として使ってください。")
    else:
        lines.append("  - トランジット時期読みは入れないでください。現在時期の断定的予測も避けてください。")
    return "\n".join(lines)


def build_report_prompt(order: ExternalOrder, *, plan: str, handoff_yaml: str, report_options: dict[str, bool] | None = None) -> str:
    cfg = _plan_config(plan)
    report_options = normalize_report_options(cfg["plan"], report_options or order_report_options(order, plan=cfg["plan"]))
    base_prompt = _read_prompt(cfg["prompt_file"])
    enabled_options = [label for key, label in REPORT_OPTION_LABELS.items() if report_options.get(key)] or ["なし"]
    disabled_options = [label for key, label in REPORT_OPTION_LABELS.items() if not report_options.get(key)] or ["なし"]
    option_instruction = build_option_instruction(cfg["plan"], report_options)
    return f"""{base_prompt}

━━━━━━━━━━━━━━━━━━
今回の生成条件
━━━━━━━━━━━━━━━━━━
- 受付番号: {order.order_code}
- クライアント名: {order.customer_name}さん
- プラン: {REPORT_PLAN_LABELS.get(cfg['plan'], cfg['plan'])}
- 生成範囲: {cfg['chapter_instruction']}
- 有効な詳細オプション: {' / '.join(enabled_options)}
- 無効な詳細オプション: {' / '.join(disabled_options)}
- プレミアムの場合: 詳細オプションはすべて標準搭載。追加感を出さず、13章構成に自然統合する。
- 西洋オプション: Chiron / Lilith / Vertex は全プランで使用。小惑星（Ceres / Pallas / Juno / Vesta）は「小惑星を追加する」がONの場合のみ本文で扱う。プレミアムは常にON。
- オプション反映ルール:
{option_instruction}
- 使用モデル: Claude Sonnet 4.6
- 出力は完成HTMLのみ。説明文、Markdownコードフェンス、補足コメントは出力しない。
- 添付サンプルHTMLのCSS変数・クラス・縦積み構造を維持する。
- <script>タグは使わない。
- sample_kanteisho.html / lisa_kanteisho.html と同じ黒×金の鑑定書として成立させる。

━━━━━━━━━━━━━━━━━━
相談内容
━━━━━━━━━━━━━━━━━━
{order.consultation_text or '特記事項なし'}

━━━━━━━━━━━━━━━━━━
入力YAML（handoff-v1.9）
━━━━━━━━━━━━━━━━━━
{handoff_yaml}
""".strip()



def _model_output_max_tokens(model: str) -> int:
    """Return the safe maximum output token limit for the selected Claude model.

    Sonnet 4.6 supports 64k output tokens. Opus 4.7 supports 128k.
    Keep this local so EXTERNAL_REPORT_MAX_TOKENS=MAX never becomes an
    undefined placeholder such as `max_allowed`.
    """
    m = (model or "").lower()
    if "opus-4-7" in m:
        return 128000
    if "sonnet-4-6" in m:
        return 64000
    if "haiku-4-5" in m:
        return 64000
    # Conservative default for current long-output Claude 4 models.
    return 64000


def _resolve_external_report_max_tokens(model: str) -> int:
    raw = (os.getenv("EXTERNAL_REPORT_MAX_TOKENS") or "MAX").strip()
    max_allowed = _model_output_max_tokens(model)

    if not raw or raw.upper() in {"MAX", "MODEL_MAX", "CLAUDE_MAX"}:
        return max_allowed

    try:
        requested = int(raw)
    except ValueError:
        return max_allowed

    if requested <= 0:
        return max_allowed

    # Avoid API errors when an environment value is higher than the model allows.
    return min(requested, max_allowed)


def generate_text_with_claude(prompt: str, *, max_tokens: int | None = None) -> str:
    api_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY が未設定です。既存のHaiku用と同じAnthropic APIキーを設定してください")
    if Anthropic is None:
        raise RuntimeError("anthropic が読み込めません。requirements.txt を確認してください")

    model = resolve_report_claude_model()
    resolved_max = max_tokens or min(_resolve_external_report_max_tokens(model), 32000)
    client = Anthropic(api_key=api_key)

    print(f"[external_report][claude] start model={model} max_tokens={resolved_max} prompt_chars={len(prompt)}", flush=True)
    chunks: list[str] = []
    with client.messages.stream(
        model=model,
        max_tokens=resolved_max,
        system="あなたは星月七海の鑑定書制作担当です。必ず指定形式のJSONだけを返してください。",
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        text_stream = getattr(stream, "text_stream", None)
        if text_stream is not None:
            for text in text_stream:
                if isinstance(text, str):
                    chunks.append(text)
        else:
            for event in stream:
                if getattr(event, "type", "") == "content_block_delta":
                    delta = getattr(event, "delta", None)
                    if getattr(delta, "type", "") == "text_delta":
                        text = getattr(delta, "text", "")
                        if isinstance(text, str):
                            chunks.append(text)

    out = _strip_code_fence("".join(chunks))
    print(f"[external_report][claude] end model={model} output_chars={len(out)}", flush=True)
    return out


def generate_chapter_content_with_claude(order: ExternalOrder, *, plan: str, handoff_yaml: str, report_options: dict[str, bool]) -> dict[str, Any]:
    prompt = build_chapter_json_prompt(order, plan=plan, handoff_yaml=handoff_yaml, report_options=report_options)
    max_tokens = 14000 if plan == "light" else (22000 if plan == "standard" else 32000)
    text = generate_text_with_claude(prompt, max_tokens=max_tokens)
    return parse_chapter_json(text, chapter_specs(plan, report_options))


def save_external_report_html(order: ExternalOrder, html: str) -> str:
    bucket_name = _bucket_name()
    if not bucket_name:
        raise RuntimeError("EXTERNAL_REPORTS_BUCKET または STAFF_REPORTS_BUCKET が未設定です")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    object_name = _storage_object_name(order.order_code)
    blob = bucket.blob(object_name)
    blob.cache_control = "private, max-age=0, no-store"
    blob.upload_from_string(html, content_type="text/html; charset=utf-8")
    return object_name




def _set_generation_step(db, order: ExternalOrder, step: str, message: str | None = None) -> None:
    """Record lightweight progress without requiring a DB migration.

    Existing screens already read report_generation_status and last_error.
    While status is generating, last_error is used as a progress carrier:
    STEP:<step>|<message>
    On real failure it is replaced with the actual error message.
    """
    msg = (message or step).strip()
    order.report_generation_status = "generating"
    order.last_error = f"STEP:{step}|{msg}"[:2000]
    print(f"[external_report] order={getattr(order, 'id', None)} code={getattr(order, 'order_code', '')} step={step} message={msg}", flush=True)
    db.commit()

def generate_external_order_report(db, order: ExternalOrder, *, plan: str = "standard", report_options: dict[str, bool] | None = None) -> ExternalOrder:
    cfg = _plan_config(plan)
    report_options = normalize_report_options(cfg["plan"], report_options or order_report_options(order, plan=cfg["plan"]))
    order.report_generation_plan = cfg["plan"]
    order.report_generation_system = cfg["astrology_system"]
    order.report_generation_prompt_key = cfg["prompt_key"]
    for key, value in report_options.items():
        if key in REPORT_OPTION_LABELS:
            setattr(order, key, bool(value))
    order.report_generation_model = resolve_report_claude_model()
    _set_generation_step(db, order, "starting", "生成準備中")

    try:
        _set_generation_step(db, order, "calculating", "出生データと占術計算からhandoff YAMLを作成中")
        handoff_yaml, _meta = build_external_order_handoff(order, plan=cfg["plan"], report_options=report_options)
        order.yaml_log_text = handoff_yaml
        _set_generation_step(db, order, "handoff_ready", "handoff YAML作成完了")

        _set_generation_step(db, order, "prompt_building", "本文生成用プロンプトを組み立て中")
        _set_generation_step(db, order, "calling_claude", "Claude Sonnet 4.6 APIへ本文JSONを送信中")
        chapter_content = generate_chapter_content_with_claude(order, plan=cfg["plan"], handoff_yaml=handoff_yaml, report_options=report_options)
        _set_generation_step(db, order, "template_rendering", "固定テンプレートへ本文・図表を差し込み中")
        html = render_external_report_html(order, plan=cfg["plan"], astro_result=_meta.get("astro_result") or {}, chapter_content=chapter_content, report_options=report_options)
        _set_generation_step(db, order, "uploading_html", "完成HTMLをCloud Storageへ保存中")
        object_name = save_external_report_html(order, html)

        order.html_storage_path = object_name
        order.html_original_name = f"{order.order_code}_generated.html"
        order.html_uploaded_at = datetime.utcnow()
        order.report_generated_at = datetime.utcnow()
        order.report_generation_status = "completed"
        order.last_error = None
        if order.status == "draft":
            order.status = "html_uploaded"
        db.commit()
        return order
    except Exception as exc:
        order.report_generation_status = "failed"
        order.last_error = str(exc)[:2000]
        print(f"[external_report] order={getattr(order, 'id', None)} failed error={order.last_error}", flush=True)
        db.commit()
        raise
