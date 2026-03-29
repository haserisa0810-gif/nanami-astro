from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Literal


HandoffMode = Literal["mini", "full", "delta"]


def _truncate(text: str, n: int) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    if len(t) <= n:
        return t
    return t[:n].rstrip() + "…（省略）"


def _parse_observations(text: str) -> dict[str, Any]:
    """Parse user-entered observations lines like 'key: value'."""
    t = (text or "").strip()
    if not t:
        return {"raw": "", "items": {}}
    items: dict[str, Any] = {}
    for line in t.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if ":" in s:
            k, v = s.split(":", 1)
            k = k.strip()
            v = v.strip()
            if k:
                items[k] = v
        else:
            # freeform lines
            items.setdefault("_free", []).append(s)
    return {"raw": t, "items": items}


def _extract_json_obj(v: Any) -> Any:
    if isinstance(v, str) and v.strip().startswith("{"):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def _augment_structure_summary(ss: Any) -> Any:
    ss = _extract_json_obj(ss)
    if not isinstance(ss, dict):
        return ss

    vedic = ss.get("vedic") if isinstance(ss.get("vedic"), dict) else None
    derived = ss.get("_derived") if isinstance(ss.get("_derived"), dict) else {}
    vedic_structure = derived.get("vedic_structure") if isinstance(derived.get("vedic_structure"), dict) else {}
    vedic_flags = derived.get("vedic_flags") if isinstance(derived.get("vedic_flags"), list) else []

    if isinstance(vedic, dict):
        for key in (
            "house_lords",
            "house_lords_placement",
            "planetary_aspects_vedic",
            "yogas",
            "varga",
            "dasha",
            "summary_flags",
            "zodiac_type",
            "ayanamsha",
        ):
            if vedic.get(key) is not None and ss.get(key) is None:
                ss[key] = vedic.get(key)

    if vedic_structure:
        ss["vedic_structure"] = vedic_structure
    if vedic_flags:
        ss["vedic_flags"] = vedic_flags
    return ss


def _slim_structure_summary(ss: Any) -> Any:
    """
    miniモード用: pair_angles と詳細アスペクトリストを除去してサイズを削減する。
    天体位置・ハウス・上位アスペクト・risk_flags は残す。
    """
    ss = _extract_json_obj(ss)
    if not isinstance(ss, dict):
        return ss
    import copy
    s = copy.deepcopy(ss)
    # _derived から pair_angles と大量 aspects を除去
    derived = s.get("_derived") or {}
    if isinstance(derived, dict):
        struct = derived.get("structure") or {}
        if isinstance(struct, dict):
            struct.pop("pair_angles", None)
            # aspects は上位20件に絞る
            if isinstance(struct.get("aspects"), list):
                struct["aspects"] = struct["aspects"][:20]
        derived["structure"] = struct
        s["_derived"] = derived
    # トップレベルの aspects も上位20件に絞る
    if isinstance(s.get("aspects"), list):
        s["aspects"] = s["aspects"][:20]
    return s


def _slim_transit(transit: Any) -> Any:
    """
    miniモード用: transit から past を除去し、today_planets と
    active/upcoming の長期トランジットのみ残す。
    """
    if not isinstance(transit, dict):
        return transit
    import copy
    t = copy.deepcopy(transit)
    # long_term: active/upcoming のみ、最大15件
    if isinstance(t.get("long_term"), list):
        t["long_term"] = [
            x for x in t["long_term"]
            if isinstance(x, dict) and x.get("status") in ("active", "upcoming")
        ][:15]
    # aspects (今日): 上位10件に絞る
    if isinstance(t.get("aspects"), list):
        t["aspects"] = t["aspects"][:10]
    return t


def build_handoff(
    *,
    inputs_view: dict[str, Any],
    payload_view: dict[str, Any],
    unknowns: list[str],
    structure_summary: Any,
    reports: dict[str, str],
    observations_text: str = "",
    bias_guard: dict[str, Any] | None = None,
    mode: HandoffMode = "mini",
    prev: dict[str, Any] | None = None,
    transit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a handoff log for moving conversation to another LLM."""

    now = datetime.now(ZoneInfo("Asia/Tokyo")).isoformat()
    obs = _parse_observations(observations_text)

    base: dict[str, Any] = {
        "version": "handoff-v1.9",
        "mode": mode,
        "generated_at": now,
        "unknowns": unknowns,
        "inputs": inputs_view,
        "payload": payload_view,
        "observations": obs,
        "bias_guard": bias_guard or {},
    }

    if mode == "full":
        base["structure_summary"] = _augment_structure_summary(structure_summary)
        base["reports"] = reports
        if transit is not None:
            base["transit"] = transit  # full は生データそのまま
        return base

    if mode == "delta":
        # Best-effort diff: only include changed observations and a tiny report summary.
        prev_obs = (prev or {}).get("observations", {})
        base["delta"] = {
            "observations_prev": prev_obs,
            "observations_now": obs,
            "reports_summary": {
                "web": _truncate(reports.get("web", ""), 400),
                "reader": _truncate(reports.get("reader", ""), 400),
            },
        }
        return base

    # mini (default) — 肥大化防止のため structure_summary をスリム化
    summary_aug = _augment_structure_summary(structure_summary)
    base["structure_summary"] = _slim_structure_summary(summary_aug)

    # risk_flags top 3-5
    top_risks: list[Any] = []
    try:
        ss = summary_aug if isinstance(summary_aug, dict) else _extract_json_obj(summary_aug)
        derived = (ss or {}).get("_derived") or {}
        risks = []
        if isinstance(derived.get("risk_flags"), list):
            risks.extend(derived.get("risk_flags") or [])
        if isinstance(derived.get("vedic_flags"), list):
            risks.extend(derived.get("vedic_flags") or [])
        risks.sort(key=lambda x: int((x or {}).get("severity") or 0), reverse=True)
        if isinstance(risks, list):
            top_risks = risks[:5]
    except Exception:
        top_risks = []

    base["risk_flags_top"] = top_risks
    base["reports_summary"] = {
        "web": _truncate(reports.get("web", ""), 800),
        "reader": _truncate(reports.get("reader", ""), 800),
        "line": _truncate(reports.get("line", ""), 500),
    }

    # transit: miniではactive/upcomingの長期トランジットのみ（pastと生データを除外）
    if transit is not None:
        base["transit"] = _slim_transit(transit)

    return base


def dumps_json(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def dumps_yaml(obj: dict[str, Any]) -> str:
    try:
        import yaml  # type: ignore

        return yaml.safe_dump(obj, allow_unicode=True, sort_keys=False, width=120)
    except Exception:
        return ""
