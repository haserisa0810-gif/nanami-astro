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
    """Heuristic validation to reduce "positive-only" outputs."""
    t = (text or "").strip()
    issues: list[str] = []
    if not t:
        return GuardResult(ok=False, issues=["出力が空です"], status="failed")

    rf = risk_flags or []
    sev_max = _severity_max(rf)

    # Reader must keep the fixed headings.
    if report_type in ("single_web_reader", "single_line_reader"):
        for h in _READER_REQUIRED_HEADERS:
            if h not in t:
                issues.append(f"reader必須見出し欠落: {h}")

        # Must contain some bullets/questions.
        bullets = len(re.findall(r"^[-・]\s+", t, flags=re.MULTILINE))
        if bullets < 8:
            issues.append("readerの箇条書き量が少なすぎます（実務粒度不足）")

        # Avoid harmlessness language.
        if re.search(r"(大丈夫|問題ない|心配いらない)", t):
            issues.append("readerで無害化ワード（大丈夫/問題ない等）が出ています")

    # Client: if high severity exists, require a caution block.
    if report_type in ("single_web", "single_line") and sev_max >= 4:
        if not re.search(r"(注意|気をつけ|リスク|事故|落とし穴)", t):
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
        "- 良いこと中心の作文をやめ、事故導線（発火条件→行動→結果）を具体化する",
        "- 断定は禁止。ただし『かもしれない』連発は禁止。条件とパターンで書く",
    ]
    if report_type in ("single_web_reader", "single_line_reader"):
        base += [
            "- 出力フォーマット（### 1)〜### 5)）の見出しを崩さない",
            "- 1)はseverityが高い順に並べ、箇条書きを増やす",
            "- 3)は“質問”の形で5〜8個、4)は“言い方テンプレ”を必ず含める",
        ]
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
