from __future__ import annotations

import os
import secrets
from datetime import date, datetime

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, inspect, select, text
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



def _ensure_transit_public_columns_runtime(db: Session) -> None:
    """古いDBが残っていても、HTML登録・URL発行に必要なカラムを実行時に補完する。"""
    try:
        inspector = inspect(db.bind)
        existing = {col["name"] for col in inspector.get_columns("transit_hub_requests")}
        ddl_map = {
            "generated_html": "ALTER TABLE transit_hub_requests ADD COLUMN generated_html TEXT",
            "generated_at": "ALTER TABLE transit_hub_requests ADD COLUMN generated_at TIMESTAMP",
            "html_original_name": "ALTER TABLE transit_hub_requests ADD COLUMN html_original_name VARCHAR(255)",
            "html_uploaded_at": "ALTER TABLE transit_hub_requests ADD COLUMN html_uploaded_at TIMESTAMP",
            "public_token": "ALTER TABLE transit_hub_requests ADD COLUMN public_token VARCHAR(128)",
            "public_url": "ALTER TABLE transit_hub_requests ADD COLUMN public_url TEXT",
            "url_issued_at": "ALTER TABLE transit_hub_requests ADD COLUMN url_issued_at TIMESTAMP",
        }
        changed = False
        for name, ddl in ddl_map.items():
            if name not in existing:
                db.execute(text(ddl))
                changed = True
        if changed:
            db.commit()
    except Exception:
        db.rollback()


def _base_url(request: Request) -> str:
    """ユーザーに見せる公開URLのベースを返す。

    Cloud Run 直URLではなく独自ドメインを優先する。
    PUBLIC_TRANSIT_BASE_URL を最優先、次に PUBLIC_BASE_URL、
    どちらも未設定なら pay.nanami-astro.com をデフォルトにする。
    """
    return (
        os.getenv("PUBLIC_TRANSIT_BASE_URL")
        or os.getenv("PUBLIC_BASE_URL")
        or "https://pay.nanami-astro.com"
    ).strip().rstrip("/")


def _public_url(request: Request, token: str) -> str:
    return f"{_base_url(request)}/t/{token}"


def _issue_public_url(db: Session, req: TransitHubRequest, request: Request) -> str:
    if not req.public_token:
        token = secrets.token_urlsafe(24)
        while db.scalar(select(TransitHubRequest.id).where(TransitHubRequest.public_token == token)):
            token = secrets.token_urlsafe(24)
        req.public_token = token
    req.public_url = _public_url(request, req.public_token)
    req.url_issued_at = datetime.utcnow()
    db.add(req)
    db.commit()
    db.refresh(req)
    return req.public_url or _public_url(request, req.public_token)


def _serialize_transit(req: TransitHubRequest) -> dict:
    return {
        "id": req.id,
        "request_code": req.request_code,
        "status": req.status,
        "customer_name": req.customer_name,
        "customer_email": req.customer_email,
        "period_label": req.period_label,
        "period_start": req.period_start.isoformat() if req.period_start else None,
        "period_end": req.period_end.isoformat() if req.period_end else None,
        "template_name": req.template_name,
        "html_original_name": req.html_original_name,
        "html_uploaded_at": req.html_uploaded_at.isoformat() if req.html_uploaded_at else None,
        "public_url": req.public_url,
        "url_issued_at": req.url_issued_at.isoformat() if req.url_issued_at else None,
        "generated_at": req.generated_at.isoformat() if req.generated_at else None,
        "last_error": req.last_error,
    }


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


@router.post("/products/transit/{request_id}/html")
async def products_transit_upload_html(
    request_id: int,
    html_file: UploadFile | None = File(None),
    html_text: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    _ensure_transit_public_columns_runtime(db)
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")

    content = (html_text or "").strip()
    original_name = None
    if html_file and html_file.filename:
        raw = await html_file.read()
        content = raw.decode("utf-8-sig", errors="replace").strip()
        original_name = html_file.filename

    if not content:
        return _redirect(f"/products/transit/{request_id}?success=html_empty")

    req.generated_html = content
    req.html_original_name = original_name or "direct_input.html"
    req.html_uploaded_at = datetime.utcnow()
    req.generated_at = req.generated_at or datetime.utcnow()
    req.status = "generated"
    req.last_error = None
    db.add(req)
    db.commit()
    return _redirect(f"/products/transit/{request_id}?success=html_saved")


@router.post("/products/transit/{request_id}/issue-url")
def products_transit_issue_url(request_id: int, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    try:
        _ensure_transit_public_columns_runtime(db)
        req = db.get(TransitHubRequest, request_id)
        if not req:
            raise HTTPException(status_code=404, detail="not found")
        if not (req.generated_html or "").strip():
            return _redirect(f"/products/transit/{request_id}?success=no_html")
        _issue_public_url(db, req, request)
        return _redirect(f"/products/transit/{request_id}?success=url_issued")
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        print(f"[transit issue-url] failed: {exc}")
        return _redirect(f"/products/transit/{request_id}?success=url_error")


@router.get("/t/{token}", response_class=HTMLResponse)
def public_transit_html(token: str, db: Session = Depends(get_db)):
    _ensure_transit_public_columns_runtime(db)
    req = db.scalar(select(TransitHubRequest).where(TransitHubRequest.public_token == token))
    if not req or not (req.generated_html or "").strip():
        raise HTTPException(status_code=404, detail="not found")
    return HTMLResponse(content=req.generated_html, status_code=200)


@router.get("/api/products/transit/{request_id}")
def api_products_transit_get(request_id: int, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")
    return _serialize_transit(req)


@router.post("/api/products/transit/{request_id}/html")
async def api_products_transit_upload_html(
    request_id: int,
    html_file: UploadFile | None = File(None),
    html_text: str = Form(""),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")
    content = (html_text or "").strip()
    original_name = None
    if html_file and html_file.filename:
        raw = await html_file.read()
        content = raw.decode("utf-8-sig", errors="replace").strip()
        original_name = html_file.filename
    if not content:
        raise HTTPException(status_code=400, detail="html is required")
    req.generated_html = content
    req.html_original_name = original_name or "direct_input.html"
    req.html_uploaded_at = datetime.utcnow()
    req.generated_at = req.generated_at or datetime.utcnow()
    req.status = "generated"
    req.last_error = None
    db.add(req)
    db.commit()
    db.refresh(req)
    return _serialize_transit(req)


@router.post("/api/products/transit/{request_id}/issue-url")
def api_products_transit_issue_url(request_id: int, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    _ensure_transit_public_columns_runtime(db)
    req = db.get(TransitHubRequest, request_id)
    if not req:
        raise HTTPException(status_code=404, detail="not found")
    if not (req.generated_html or "").strip():
        raise HTTPException(status_code=400, detail="html is required before issuing url")
    _issue_public_url(db, req, request)
    return _serialize_transit(req)


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
