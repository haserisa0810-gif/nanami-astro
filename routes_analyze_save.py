from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from services.analyze_save_service import save_analysis_result_to_order

router = APIRouter()


@router.post("/analyze/save-to-order")
def save_analysis_to_order(
    request: Request,
    from_order_code: str = Form(""),
    ai_text: str = Form(""),
    reader_text: str = Form(""),
    line_text: str = Form(""),
    inputs_json: str = Form("{}"),
    payload_json: str = Form("{}"),
    raw_json: str = Form("{}"),
    structure_summary_json: str = Form("{}"),
    handoff_yaml_full: str = Form(""),
):
    resolved_order_code = save_analysis_result_to_order(
        request=request,
        from_order_code=from_order_code,
        ai_text=ai_text,
        reader_text=reader_text,
        line_text=line_text,
        inputs_json=inputs_json,
        payload_json=payload_json,
        raw_json=raw_json,
        structure_summary_json=structure_summary_json,
        handoff_yaml_full=handoff_yaml_full,
    )
    return RedirectResponse(url=f"/staff/orders/{resolved_order_code}", status_code=303)
