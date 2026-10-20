from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal


ReportType = Literal[
    "single_web",
    "single_web_reader",
    "single_line",
    "single_line_reader",
    "compat_web",
    "compat_line",
]


@dataclass
class GuardResult:
    ok: bool
    issues: list[str]
    retries: int = 0
    status: str = "ok"  # ok | retried | failed


_READER_REQUIRED_HEADERS = [
    "### 1) 主要構造と根拠",
    "### 2) 現れ方と条件",
    "### 3) 現状確認の質問",
    "### 4) 伝え方",
    "### 5) 活かし方と注意点",
]

_LEGACY_READER_HEADERS = [
    "### 1) ハイリスク（洗い出し）",
    "### 2) 具体的な“事故り方”",
    "### 3) 観測ポイント（質問）",
    "### 4) 介入（言い方テンプレ）",
    "### 5) リスク対策（現実の手順）",
]


def _extract_planets(astro_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Tolerant extraction for structure_engine."""
    planets: list[Any] = []
    if isinstance(astro_data.get("planets"), list):
        planets = astro_data["planets"]
    elif isinstance(astro_data.get("western"), dict) and isinstance(astro_data["western"].get("planets"), list):
        planets = astro_data["western"]["planets"]

    out: list[dict[str, Any]] = []
    for p in planets:
        if not isinstance(p, dict):
            continue
        name = (p.get("name") or p.get("id") or "").strip()
        lon = p.get("abs_pos")
        if lon is None:
            lon = p.get("lon")
        if lon is None:
            lon = p.get("longitude")
        if not name or lon is None:
            continue
        try:
            out.append({"name": name, "lon": float(lon), "sign": p.get("sign")})
        except Exception:
            continue
    return out


def _extract_house_cusps(astro_data: dict[str, Any]) -> list[float] | None:
    houses: Any = astro_data.get("houses")
    if not isinstance(houses, list) and isinstance(astro_data.get("western"), dict):
        houses = astro_data["western"].get("houses")
    if not isinstance(houses, list):
        return None

    cusps: list[float] = []
    for h in houses:
        if not isinstance(h, dict):
            continue
        lon = h.get("lon")
        if lon is None:
            lon = h.get("abs_pos")
        if lon is None:
            lon = h.get("longitude")
        try:
            cusps.append(float(lon))
        except Exception:
            continue
    return cusps[:12] if len(cusps) >= 12 else None


def derive_risk_flags_from_astro(astro_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort risk flags extraction for guard checks."""
    try:
        from services.structure_engine import (  # type: ignore
            analyze_structure,
            derive_risk_flags,
            analyze_vedic_structure,
            derive_vedic_flags,
        )

        merged: list[dict[str, Any]] = []

        planets = _extract_planets(astro_data)
        cusps = _extract_house_cusps(astro_data)
        if planets:
            structure = analyze_structure(planets, cusps)
            flags = derive_risk_flags(structure)
            if isinstance(flags, list):
                merged.extend(flags)

        vedic_data = None
        if isinstance(astro_data.get("vedic"), dict):
            vedic_data = astro_data.get("vedic")
        elif astro_data.get("system") == "vedic":
            vedic_data = astro_data
        if isinstance(vedic_data, dict):
            vedic_flags = derive_vedic_flags(analyze_vedic_structure(vedic_data))
            if isinstance(vedic_flags, list):
                merged.extend(vedic_flags)

        merged.sort(key=lambda x: int(x.get("severity") or 0), reverse=True)
        return merged
    except Exception:
        return []


def _severity_max(risk_flags: list[dict[str, Any]]) -> int:
    m = 0
    for rf in risk_flags:
        if not isinstance(rf, dict):
            continue
        try:
            m = max(m, int(rf.get("severity") or 0))
        except Exception:
            continue
    return m


def validate_generated_text(
    *,
    text: str,
    report_type: ReportType,
    risk_flags: list[dict[str, Any]] | None = None,
) -> GuardResult:
    """Heuristic format checks; this does not validate astrological evidence."""
    t = (text or "").strip()
    issues: list[str] = []
    if not t:
        return GuardResult(ok=False, issues=["出力が空です"], status="failed")

    rf = risk_flags or []
    sev_max = _severity_max(rf)

    # Reader must keep the fixed headings.
    if report_type == "single_web_reader":
        if not all(h in t for h in _LEGACY_READER_HEADERS):
            for h in _READER_REQUIRED_HEADERS:
                if h not in t:
                    issues.append(f"reader必須見出し欠落: {h}")
    elif report_type == "single_line_reader":
        for h in ("【主要構造と根拠】", "【現れ方と条件】", "【現状確認の質問】", "【伝え方・活かし方】"):
            if h not in t:
                issues.append(f"reader必須見出し欠落: {h}")

    # Client: if high severity exists, require a caution block.
    # ただし本文がまだ短い・見出し不足の段階では fail で潰さず、十分な長さが出た後だけ厳しく見る。
    if report_type in ("single_web", "single_line") and sev_max >= 4:
        has_caution = bool(re.search(r"(注意|気をつけ|リスク|事故|落とし穴)", t))
        visible_headers = len(re.findall(r"^#{1,6}\s+", t, flags=re.MULTILINE))
        if len(t) >= 1200 and visible_headers >= 4 and not has_caution:
            issues.append("高severityリスクがあるのに、注意喚起が文章上に現れていません")

    # Too many hedge words => content becomes empty.
    hedges = len(re.findall(r"(かもしれない|可能性|人による|場合がある)", t))
    if hedges >= 10:
        issues.append("曖昧語が多すぎます（言い逃れ率が高い）")

    ok = len(issues) == 0
    return GuardResult(ok=ok, issues=issues, status="ok" if ok else "failed")


def build_fix_instructions(result: GuardResult, report_type: ReportType) -> str:
    """Return a compact instruction block appended to meta['message'] for retries."""
    if result.ok:
        return ""

    base = [
        "【BIAS_GUARD 修正指示】",
        "- 指摘された形式・不足だけを修正し、主要構造と根拠・候補・保留を維持する",
        "- 曖昧語の多さを断定や深刻さで解消しない。根拠、条件、行動のつながりを具体化する",
        "- 根拠のある厳しい傾向は残し、再生成のために事故や症状、新たな問題を作らない",
    ]
    if report_type == "single_web_reader":
        base += [
            "- 出力フォーマット（### 1)〜### 5)）の見出しを崩さない",
            "- 1)は主要構造と参照根拠、2)は条件ごとの現れ方を記す",
            "- 3)は現状確認の質問、4)は伝え方、5)は活かし方と根拠のある注意点を記す",
        ]
    elif report_type == "single_line_reader":
        base.append("- 主要構造と根拠、現れ方と条件、現状確認の質問、伝え方・活かし方の4つの見出しを維持する")
    if result.issues:
        base.append("- NG検出: " + " / ".join(result.issues[:6]))

    return "\n".join(base).strip()


def compact_guard_meta(gr: GuardResult) -> dict[str, Any]:
    return {
        "status": gr.status,
        "ok": gr.ok,
        "issues": gr.issues,
        "retries": gr.retries,
    }
