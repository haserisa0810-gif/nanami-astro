from __future__ import annotations

import json
import os
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from prefs import PREF_LABELS
from routes_shared import DEFAULT_INDEX_CONTEXT, build_full_astrologer_summary, templates

router = APIRouter()


def build_index_context(
    request: Request,
    *,
    analysis_type: str = "single",
    astrology_system: str = "western",
) -> dict[str, Any]:
    return {
        "request": request,
        "prefs": PREF_LABELS,
        "analysis_type": analysis_type,
        "astrology_system": astrology_system,
        "google_maps_api_key": os.getenv("GOOGLE_MAPS_API_KEY", ""),
        **DEFAULT_INDEX_CONTEXT,
    }


@router.get("/", response_class=HTMLResponse)
def form_page(
    request: Request,
    analysis_type: str = "single",
    astrology_system: str = "western",
):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=build_index_context(
            request,
            analysis_type=analysis_type,
            astrology_system=astrology_system,
        ),
    )


@router.get("/analyze", response_class=HTMLResponse)
def analyze_page(
    request: Request,
    analysis_type: str = "single",
    astrology_system: str = "western",
):
    return form_page(request, analysis_type=analysis_type, astrology_system=astrology_system)


@router.get("/western", response_class=HTMLResponse)
def western_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="western")


@router.get("/vedic", response_class=HTMLResponse)
def vedic_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="vedic")


@router.get("/integrated", response_class=HTMLResponse)
def integrated_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="integrated3")


@router.get("/shichu", response_class=HTMLResponse)
def shichu_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="shichusuimei")


@router.get("/lite", response_class=HTMLResponse)
def lite_page(request: Request):
    return form_page(request, analysis_type="single", astrology_system="western")


@router.get("/guide", response_class=HTMLResponse)
def guide_page(request: Request):
    return templates.TemplateResponse(request=request, name="guide.html", context={"request": request})


@router.get("/about", response_class=HTMLResponse)
def about_page(request: Request):
    return templates.TemplateResponse(request=request, name="about.html", context={"request": request})


@router.post("/astrologer-result", response_class=HTMLResponse)
async def astrologer_result(request: Request):
    form = await request.form()

    raw_json = form.get("result_json") or "{}"
    try:
        result = json.loads(raw_json) if isinstance(raw_json, str) else {}
    except Exception:
        result = {}

    structure_summary_json = form.get("structure_summary_json") or "{}"
    try:
        structure_summary = json.loads(structure_summary_json) if isinstance(structure_summary_json, str) else {}
    except Exception:
        structure_summary = {}

    reader_text = form.get("reader_text") or ""
    if not isinstance(reader_text, str):
        reader_text = ""

    summary = build_full_astrologer_summary(
        result if isinstance(result, dict) else {},
        structure_summary if isinstance(structure_summary, dict) else {},
    )

    return templates.TemplateResponse(
        request=request,
        name="astrologer_result.html",
        context={
            "request": request,
            "result": result if isinstance(result, dict) else {},
            "summary": summary,
            "structure_summary": structure_summary if isinstance(structure_summary, dict) else {},
            "reader_text": reader_text.strip(),
        },
    )
