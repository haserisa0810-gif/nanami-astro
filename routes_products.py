from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from auth import get_current_staff
from db import get_db
from models import TransitHubJob, TransitHubRequest
from services.transit_hub_service import (
    CHANNEL_OPTIONS,
    STATUS_LABELS,
    create_request,
    default_period_dates,
    generate_request_code,
    generate_request_output,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _parse_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


@router.get("/products/transit", response_class=HTMLResponse)
def products_transit_list(request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    q = (request.query_params.get("q") or "").strip()
    status = (request.query_params.get("status") or "").strip()
    channel = (request.query_params.get("channel") or "").strip()

    stmt = select(TransitHubRequest).order_by(TransitHubRequest.created_at.desc())
    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            TransitHubRequest.request_code.ilike(like)
            | TransitHubRequest.customer_name.ilike(like)
            | TransitHubRequest.customer_email.ilike(like)
        )
    if status:
        stmt = stmt.where(TransitHubRequest.status == status)
    if channel:
        stmt = stmt.where(TransitHubRequest.channel == channel)
    requests = db.scalars(stmt).all()
    counts = {
        "all": db.scalar(select(func.count()).select_from(TransitHubRequest)) or 0,
        "ready": db.scalar(select(func.count()).select_from(TransitHubRequest).where(TransitHubRequest.status == "ready")) or 0,
        "generated": db.scalar(select(func.count()).select_from(TransitHubRequest).where(TransitHubRequest.status == "generated")) or 0,
        "error": db.scalar(select(func.count()).select_from(TransitHubRequest).where(TransitHubRequest.status == "error")) or 0,
    }
    return templates.TemplateResponse(request=request, name="products_transit_list.html", context={
        "request": request, "staff": staff, "requests": requests, "status_labels": STATUS_LABELS,
        "counts": counts, "filters": {"q": q, "status": status, "channel": channel}, "channel_options": CHANNEL_OPTIONS,
        "success": request.query_params.get("success"),
    })


@router.get("/products/transit/new", response_class=HTMLResponse)
def products_transit_new(request: Request, staff: dict = Depends(get_current_staff)):
    start, end = default_period_dates()
    return templates.TemplateResponse(request=request, name="products_transit_new.html", context={
        "request": request, "staff": staff, "channel_options": CHANNEL_OPTIONS,
        "default_period_start": start.isoformat(), "default_period_end": end.isoformat(), "error": request.query_params.get("error"),
    })


@router.post("/products/transit")
def products_transit_create(
    request: Request,
    customer_name: str = Form(...),
    customer_email: str = Form(""),
    birth_date: str = Form(""),
    birth_time: str = Form(""),
    gender: str = Form(""),
    prefecture: str = Form(""),
    birth_place: str = Form(""),
    channel: str = Form("manual"),
    period_label: str = Form("3ヶ月"),
    period_start: str = Form(""),
    period_end: str = Form(""),
    template_name: str = Form("standard_3month"),
    notes: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    if not customer_name.strip():
        return _redirect("/products/transit/new?error=required")
    req = create_request(
        db,
        request_code=generate_request_code(db),
        customer_name=customer_name.strip(),
        customer_email=(customer_email or "").strip() or None,
        birth_date=_parse_date(birth_date),
        birth_time=(birth_time or "").strip() or None,
        gender=(gender or "").strip() or None,
        prefecture=(prefecture or "").strip() or None,
        birth_place=(birth_place or "").strip() or None,
        channel=channel if channel in CHANNEL_OPTIONS else "manual",
        period_label=(period_label or "3ヶ月").strip() or "3ヶ月",
        period_start=_parse_date(period_start),
        period_end=_parse_date(period_end),
        template_name=(template_name or "standard_3month").strip() or "standard_3month",
        notes=(notes or "").strip() or None,
        status="ready",
    )
    return _redirect(f"/products/transit/{req.id}?success=created")


@router.get("/products/transit/{request_id}", response_class=HTMLResponse)
def products_transit_detail(request_id: int, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")
    jobs = db.scalars(select(TransitHubJob).where(TransitHubJob.request_id == req.id).order_by(TransitHubJob.created_at.desc())).all()
    return templates.TemplateResponse(request=request, name="products_transit_detail.html", context={
        "request": request, "staff": staff, "item": req, "jobs": jobs, "status_labels": STATUS_LABELS,
        "channel_options": CHANNEL_OPTIONS, "success": request.query_params.get("success"),
    })


@router.post("/products/transit/{request_id}/update")
def products_transit_update(
    request_id: int,
    customer_name: str = Form(...),
    customer_email: str = Form(""),
    birth_date: str = Form(""),
    birth_time: str = Form(""),
    gender: str = Form(""),
    prefecture: str = Form(""),
    birth_place: str = Form(""),
    channel: str = Form("manual"),
    period_label: str = Form("3ヶ月"),
    period_start: str = Form(""),
    period_end: str = Form(""),
    template_name: str = Form("standard_3month"),
    notes: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")
    req.customer_name = customer_name.strip()
    req.customer_email = (customer_email or "").strip() or None
    req.birth_date = _parse_date(birth_date)
    req.birth_time = (birth_time or "").strip() or None
    req.gender = (gender or "").strip() or None
    req.prefecture = (prefecture or "").strip() or None
    req.birth_place = (birth_place or "").strip() or None
    req.channel = channel if channel in CHANNEL_OPTIONS else "manual"
    req.period_label = (period_label or "3ヶ月").strip() or "3ヶ月"
    req.period_start = _parse_date(period_start)
    req.period_end = _parse_date(period_end)
    req.template_name = (template_name or "standard_3month").strip() or "standard_3month"
    req.notes = (notes or "").strip() or None
    if req.status == "draft":
        req.status = "ready"
    db.add(req)
    db.commit()
    return _redirect(f"/products/transit/{req.id}?success=updated")


@router.post("/products/transit/{request_id}/generate")
def products_transit_generate(request_id: int, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")
    generate_request_output(db, req)
    return _redirect(f"/products/transit/{request_id}?success=generated")


@router.get("/products/daily", response_class=HTMLResponse)
def products_daily_placeholder(request: Request, staff: dict = Depends(get_current_staff)):
    return templates.TemplateResponse(request=request, name="products_daily_placeholder.html", context={"request": request, "staff": staff})


@router.get("/products/templates", response_class=HTMLResponse)
def products_templates_placeholder(request: Request, staff: dict = Depends(get_current_staff)):
    return templates.TemplateResponse(request=request, name="products_templates_placeholder.html", context={"request": request, "staff": staff})


@router.get("/products/jobs", response_class=HTMLResponse)
def products_jobs(request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    jobs = db.scalars(select(TransitHubJob).order_by(TransitHubJob.created_at.desc()).limit(100)).all()
    return templates.TemplateResponse(request=request, name="products_jobs.html", context={"request": request, "staff": staff, "jobs": jobs})
