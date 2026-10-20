"""Reading roles for existing calculations; no new strength/pattern calculation."""
from __future__ import annotations

from copy import deepcopy
from typing import Any


def build_reading_context(shichu: dict[str, Any]) -> dict[str, Any]:
    structure = shichu.get("structure_report") or {}
    normalized = shichu.get("normalized_data") or {}
    strength = structure.get("strength_v2") or {}
    hour_unknown = (normalized.get("timing") or {}).get("hour_known") is False
    special = bool(strength.get("special_pattern_suspicion"))
    if strength:
        status = "ambiguous" if hour_unknown or special or strength.get("band") == "中庸域" else "candidate"
        source = "structure_report.strength_v2"
    else:
        status = "candidate" if (structure.get("strength_index") or (shichu.get("features") or {}).get("strength")) else "ambiguous"
        source = "structure_report.strength_index" if structure.get("strength_index") else "features.strength" if (shichu.get("features") or {}).get("strength") else None
    fu_yi = (structure.get("yojin_candidates") or {}).get("fu_yi") or {}
    return {
        "version": "shichu_reading_policy_v1",
        "strength": {
            "source": source, "status": status,
            "role": "weighted_candidate" if strength else "legacy_reference",
            "legacy_values_omitted": bool(strength),
        },
        "hour_dependent_materials": "reference_only" if hour_unknown else "follow_source_flags",
        "special_pattern": "review_required" if special else "not_confirmed",
        "pattern_candidates": {"status": "candidate", "confirmation": "not_assessed"},
        "element_recommendation": {
            "selection_status": fu_yi.get("status", "not_available"),
            "status": "ambiguous" if hour_unknown or not fu_yi or fu_yi.get("status") in {"hold", "withheld"} else "candidate",
            "role": "method_specific_suggestion_not_definitive_useful_god",
        },
    }


def project_for_reading(shichu: dict[str, Any]) -> dict[str, Any]:
    """Hide superseded labels only in AI input; stored calculation stays intact."""
    projected = deepcopy(shichu)
    context = build_reading_context(shichu)
    if context["strength"]["legacy_values_omitted"]:
        (projected.get("features") or {}).pop("strength", None)
        (projected.get("structure_report") or {}).pop("strength_index", None)
    projected["reading_context"] = context
    return projected
