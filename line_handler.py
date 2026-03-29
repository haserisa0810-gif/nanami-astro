"""
line_handler.py
LINEからの鑑定リクエストを受け取り、占術計算とレポート生成を実行するモジュール。
セッション管理・テキストパース・LINE送受信には関与しない。
"""
from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import HTTPException

try:
    from src.services.western_calc import calc_western_from_payload  # type: ignore
except Exception:
    from services.western_calc import calc_western_from_payload  # type: ignore

try:
    from src.services.ai_report import generate_report  # type: ignore
except Exception:
    from services.ai_report import generate_report  # type: ignore

try:
    from src.services.freeastro import call_freeastro_natal  # type: ignore
except Exception:
    from services.freeastro import call_freeastro_natal  # type: ignore

try:
    from src.web.shared import (  # type: ignore
        _age_years,
        _attach_meta,
        _calc_payload_from_inputs,
        apply_detail_level,
        format_by_style,
    )
except Exception:
    from shared import (  # type: ignore
        _age_years,
        _attach_meta,
        _calc_payload_from_inputs,
        apply_detail_level,
        format_by_style,
    )

from line_parser import normalize_prefecture, sanitize_birth_place

# AI生成失敗時のフォールバックテキスト
_FALLBACK_TEXT = (
    "今は、自分に合うやり方を見極め直すことが大切な時期です。\n"
    "直近3〜6ヶ月は、広げるよりも整理と絞り込みを優先するほど流れが整いやすくなります。"
)


def build_astro_payload(
    merged: dict[str, str],
) -> tuple[dict[str, Any], list[str]]:
    """
    セッションのマージ済みデータから占術計算用ペイロードを構築する。
    戻り値: (payload, unknowns)
    HTTPException / Exception はそのまま呼び出し元へ伝播させる。
    """
    birth_date = merged.get("birth_date")
    birth_time = merged.get("birth_time")
    prefecture = normalize_prefecture(merged.get("prefecture"))
    birth_place = sanitize_birth_place(merged.get("birth_place"))
    if not birth_place and prefecture:
        birth_place = prefecture

    unknowns: list[str] = []
    payload = _calc_payload_from_inputs(
        birth_date=birth_date or "",
        birth_time=birth_time,
        birth_place=birth_place,
        prefecture=prefecture,
        lat=None,
        lon=None,
        unknowns=unknowns,
    )
    return payload, unknowns


def run_astro_calc(payload: dict[str, Any]) -> dict[str, Any]:
    """
    占術計算を実行する。ローカル計算に失敗した場合は freeastro へフォールバック。
    """
    try:
        result = calc_western_from_payload(payload)
        print("LINE handler: used calc_western_from_payload")
        return result
    except Exception as calc_exc:
        print("LINE handler: local calc failed, fallback to freeastro:", repr(calc_exc))
        return call_freeastro_natal(payload)


def build_report(
    astro: dict[str, Any],
    merged: dict[str, str],
    formatted_user_name: str,
    line_display_name: str | None,
) -> tuple[str, dict[str, Any]]:
    """
    鑑定レポートを生成して返す。
    戻り値: (report_text, meta)
    """
    birth_date = merged.get("birth_date")
    today = date.today().isoformat()

    meta: dict[str, Any] = {
        "birth_date": birth_date,
        "today": today,
        "age_years": _age_years(birth_date or "", today),
        "output_style": "line",
        "detail_level": merged.get("detail_level") or "standard",
        "analysis_type": "single",
        "astrology_system": "western",
        "user_name": formatted_user_name,
        "line_display_name": line_display_name or "",
    }
    astro = _attach_meta(astro, meta)

    text = generate_report(astro, style="line", report_type="single_line")
    if (text or "").startswith("AI生成エラー:"):
        print("LINE handler: generation fallback used:", (text or "")[:200])
        text = _FALLBACK_TEXT

    text = format_by_style(apply_detail_level(text, meta["detail_level"]), "line")
    return text, meta


def format_reply(text: str, unknowns: list[str]) -> str:
    """unknowns の警告ヘッダーを付与してLINE返信テキストを組み立てる。"""
    head = ("⚠ " + " / ".join(unknowns) + "\n\n") if unknowns else ""
    return head + text
