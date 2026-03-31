from __future__ import annotations

from datetime import datetime, timezone
import hashlib

from db import get_session
from models import Case
from services.ai_report import call_gemini_report
from services.prompt_builder import build_report_prompt


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_source_payload(case: Case) -> str:
    parts = [
        case.input_name or "",
        case.birth_date or "",
        case.birth_time or "",
        case.birth_place or "",
        case.analysis_yaml or "",
        case.western_summary or "",
        case.vedic_summary or "",
        case.shichu_summary or "",
    ]
    return "\n".join(parts)


def generate_ai_report_for_case(case_id: int, force: bool = False) -> str:
    session = get_session()
    case = session.get(Case, case_id)
    if not case:
        raise ValueError(f"Case not found: {case_id}")

    source_payload = build_source_payload(case)
    source_hash = _hash_text(source_payload)

    if not force:
        if case.ai_report_status == "completed" and case.ai_report_text:
            if case.ai_report_source_hash == source_hash:
                return case.ai_report_text

        if case.ai_report_status == "generating":
            return case.ai_report_text or "生成中です。"

    case.ai_report_status = "generating"
    case.ai_report_error = None
    session.commit()

    try:
        prompt = build_report_prompt(case)
        prompt_hash = _hash_text(prompt)

        report_text = call_gemini_report(prompt)

        case.ai_report_text = report_text
        case.ai_report_status = "completed"
        case.ai_report_generated_at = datetime.now(timezone.utc)
        case.ai_report_model = "gemini-2.0-flash"
        case.ai_report_prompt_hash = prompt_hash
        case.ai_report_source_hash = source_hash
        case.ai_report_version = (case.ai_report_version or 0) + 1
        case.ai_report_error = None
        session.commit()
        return report_text

    except Exception as e:
        case.ai_report_status = "failed"
        case.ai_report_error = str(e)[:2000]
        session.commit()
        raise
