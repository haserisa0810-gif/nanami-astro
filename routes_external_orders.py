from __future__ import annotations

from datetime import date, datetime, time, timedelta
import base64
import hashlib
import hmac
import json
import os
import secrets

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from google.cloud import storage
from sqlalchemy.orm import Session

from auth import get_current_staff
from db import get_db, db_session
from models import ExternalOrder, Order, OrderResultView
from services.notification_service import send_mail
from services.report_generation_service import REPORT_OPTION_LABELS, REPORT_PLAN_LABELS, REPORT_PLAN_OPTIONS, default_report_options, generate_external_order_report, normalize_report_options, order_report_options

router = APIRouter()
templates = Jinja2Templates(directory="templates")

EXTERNAL_STATUSES = {
    "draft": "下書き",
    "html_uploaded": "HTML登録済み",
    "url_issued": "URL発行済み",
    "mail_sent": "メール送信済み",
    "delivered": "納品済み",
}
SOURCE_OPTIONS = ["coconala", "stores", "manual"]
GENDER_OPTIONS = ["female", "male", "other", "unknown"]


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _bucket_name() -> str:
    return os.getenv("EXTERNAL_REPORTS_BUCKET", "").strip()


def _storage_client() -> storage.Client:
    return storage.Client()


def _object_name(order_code: str) -> str:
    return f"external_reports/{order_code}/report.html"


def _build_public_url(request: Request, token: str) -> str:
    base = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
    if base:
        return f"{base}/report/share/{token}"
    return str(request.url_for("external_report_share", token=token))


def _html_object_name(order: ExternalOrder) -> str | None:
    raw = (order.html_storage_path or "").strip()
    return raw or None


def _html_exists(order: ExternalOrder) -> bool:
    object_name = _html_object_name(order)
    bucket_name = _bucket_name()
    if not object_name or not bucket_name:
        return False
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    return blob.exists()


def _download_html(order: ExternalOrder) -> str | None:
    object_name = _html_object_name(order)
    bucket_name = _bucket_name()
    if not object_name or not bucket_name:
        return None
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")


def _generate_token() -> str:
    return secrets.token_urlsafe(32)


def _staff_report_bucket_name() -> str:
    return (os.getenv("STAFF_REPORTS_BUCKET") or os.getenv("EXTERNAL_REPORTS_BUCKET") or "").strip()


def _staff_report_object_name(order_code: str) -> str:
    return f"staff_reports/{order_code}/report.html"


def _download_staff_report_html(order_code: str) -> str | None:
    bucket_name = _staff_report_bucket_name()
    if not bucket_name:
        return None
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(_staff_report_object_name(order_code))
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")


def _staff_report_share_secret() -> str:
    return (
        os.getenv("REPORT_SHARE_SECRET")
        or os.getenv("SECRET_KEY")
        or os.getenv("SESSION_SECRET")
        or "nanami-astro-staff-report-share"
    )


def _decode_staff_share_token(token: str) -> str | None:
    if "." not in token:
        return None
    encoded, signature = token.split(".", 1)
    padding = "=" * (-len(encoded) % 4)
    try:
        payload = base64.urlsafe_b64decode(f"{encoded}{padding}").decode("utf-8")
    except Exception:
        return None
    expected = base64.urlsafe_b64encode(
        hmac.new(_staff_report_share_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        data = json.loads(payload)
    except Exception:
        return None
    if not isinstance(data, dict) or data.get("t") != "staff_report":
        return None
    order_code = str(data.get("o") or "").strip()
    return order_code or None


def _generate_order_code(db: Session) -> str:
    prefix = f"EXT-{datetime.utcnow().strftime('%Y%m%d')}-"
    rows = db.scalars(select(ExternalOrder.order_code).where(ExternalOrder.order_code.like(f"{prefix}%"))).all()
    used = set()
    for code in rows:
        try:
            used.add(int(code.rsplit('-', 1)[-1]))
        except Exception:
            continue
    seq = 1
    while seq in used:
        seq += 1
    return f"{prefix}{seq:04d}"


def _parse_date(value: str | None) -> date | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _parse_datetime_local(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _coerce_price(value: str | None) -> int | None:
    raw = (value or "").strip().replace(',', '')
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _checkbox_on(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "on", "yes", "checked"}


def _report_options_from_form(plan: str, option_asteroids: str | None, option_transit: str | None, option_special_points: str | None, option_year_forecast: str | None) -> dict[str, bool]:
    raw = {
        "option_asteroids": _checkbox_on(option_asteroids),
        "option_transit": _checkbox_on(option_transit),
        "option_special_points": _checkbox_on(option_special_points),
        "option_year_forecast": _checkbox_on(option_year_forecast),
    }
    return normalize_report_options(plan, raw)


def _apply_report_options(order: ExternalOrder, options: dict[str, bool]) -> None:
    for key in REPORT_OPTION_LABELS:
        setattr(order, key, bool(options.get(key)))


def _generation_stale_minutes() -> int:
    raw = (os.getenv("EXTERNAL_REPORT_GENERATION_STALE_MINUTES") or "30").strip()
    try:
        return max(5, int(raw))
    except ValueError:
        return 30


def _generation_started_at(order: ExternalOrder) -> datetime | None:
    # 専用の started_at カラムを増やさず、既存カラムで安全に判定する。
    # generating 開始時に report_generated_at へ時刻を入れ、完了時は生成完了時刻として上書きされる想定。
    return order.report_generated_at or order.updated_at or order.created_at


def _mark_stale_generation_if_needed(db: Session, order: ExternalOrder) -> bool:
    """長時間 stuck した generating を failed に落として画面ループを止める。"""
    if (order.report_generation_status or "") != "generating":
        return False
    started_at = _generation_started_at(order)
    if not started_at:
        return False
    # DBがnaive datetime前提なので utcnow で比較する。
    elapsed = datetime.utcnow() - started_at
    limit = timedelta(minutes=_generation_stale_minutes())
    if elapsed <= limit:
        return False
    order.report_generation_status = "failed"
    order.last_error = (
        f"生成開始から{_generation_stale_minutes()}分以上経過したため停止扱いにしました。"
        "再生成する場合は入力内容を確認してから再生成してください。"
    )
    db.commit()
    return True




def _extract_generation_progress(raw: str | None) -> tuple[str, str, str]:
    """Return (step, message, real_error).

    While generating, services/report_generation_service.py stores progress in
    last_error as STEP:<step>|<message>. That value is not an error.
    """
    text = (raw or "").strip()
    if text.startswith("STEP:"):
        body = text[5:]
        if "|" in body:
            step, message = body.split("|", 1)
        else:
            step, message = body, body
        return step.strip(), message.strip(), ""
    return "", "", text

def _run_external_report_generation_background(order_id: int, plan: str, report_options: dict[str, bool]) -> None:
    """外部受注鑑定書をレスポンス後に生成します。"""
    print(f"[external_report][background] start order_id={order_id} plan={plan} options={report_options}", flush=True)
    with db_session() as bg_db:
        order = bg_db.get(ExternalOrder, order_id)
        if not order:
            print(f"[external_report][background] order_not_found order_id={order_id}", flush=True)
            return
        try:
            generate_external_order_report(bg_db, order, plan=plan, report_options=report_options)
            print(f"[external_report][background] completed order_id={order_id}", flush=True)
        except Exception as exc:
            try:
                order.report_generation_status = "failed"
                order.last_error = str(exc)[:2000]
                bg_db.commit()
            except Exception:
                bg_db.rollback()
            print(f"[external_report][background] failed order_id={order_id} error={exc}", flush=True)
            return


def _apply_filters(stmt, q: str, status: str, source_type: str, staff_name: str, created_from: str, created_to: str):
    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            ExternalOrder.order_code.ilike(like)
            | ExternalOrder.customer_name.ilike(like)
            | ExternalOrder.customer_email.ilike(like)
            | ExternalOrder.menu_name.ilike(like)
        )
    if status:
        stmt = stmt.where(ExternalOrder.status == status)
    if source_type:
        stmt = stmt.where(ExternalOrder.source_type == source_type)
    if staff_name:
        stmt = stmt.where(ExternalOrder.staff_name == staff_name)
    dt_from = _parse_date(created_from)
    if dt_from:
        stmt = stmt.where(ExternalOrder.created_at >= datetime.combine(dt_from, time.min))
    dt_to = _parse_date(created_to)
    if dt_to:
        stmt = stmt.where(ExternalOrder.created_at < datetime.combine(dt_to + timedelta(days=1), time.min))
    return stmt


def _default_subject() -> str:
    return "【星月七海】鑑定書をお届けします"


def _default_body(order: ExternalOrder) -> str:
    url = (order.public_url or "").strip()
    return f"""{order.customer_name} 様

このたびはお申し込みいただきありがとうございます。
鑑定書のご用意が整いました。

以下のURLよりご確認ください。
{url}

ご不明点がありましたら、このメールへご返信ください。"""


@router.get("/staff/external-orders", response_class=HTMLResponse)
def external_order_list(request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    q = (request.query_params.get("q") or "").strip()
    status = (request.query_params.get("status") or "").strip()
    source_type = (request.query_params.get("source_type") or "").strip()
    staff_name = (request.query_params.get("staff_name") or "").strip()
    created_from = (request.query_params.get("created_from") or "").strip()
    created_to = (request.query_params.get("created_to") or "").strip()

    stmt = select(ExternalOrder).order_by(ExternalOrder.created_at.desc())
    stmt = _apply_filters(stmt, q, status, source_type, staff_name, created_from, created_to)
    orders = db.scalars(stmt).all()
    staff_names = [x for x in db.scalars(select(ExternalOrder.staff_name).distinct().order_by(ExternalOrder.staff_name.asc())).all() if x]
    return templates.TemplateResponse(
        request=request,
        name="external_orders_list.html",
        context={
            "request": request,
            "staff": staff,
            "orders": orders,
            "order_count": len(orders),
            "status_labels": EXTERNAL_STATUSES,
            "filters": {
                "q": q, "status": status, "source_type": source_type,
                "staff_name": staff_name, "created_from": created_from, "created_to": created_to,
            },
            "source_options": SOURCE_OPTIONS,
            "staff_names": staff_names,
            "success": request.query_params.get("success"),
            "error": request.query_params.get("error"),
        },
    )


@router.get("/staff/external-orders/new", response_class=HTMLResponse)
def external_order_new(request: Request, staff: dict = Depends(get_current_staff)):
    return templates.TemplateResponse(
        request=request,
        name="external_order_new.html",
        context={
            "request": request,
            "staff": staff,
            "source_options": SOURCE_OPTIONS,
            "gender_options": GENDER_OPTIONS,
            "default_staff_name": staff.get("display_name") or "",
            "report_plan_options": REPORT_PLAN_OPTIONS,
            "report_plan_labels": REPORT_PLAN_LABELS,
            "error": request.query_params.get("error"),
        },
    )


@router.post("/staff/external-orders")
async def external_order_create(
    request: Request,
    customer_name: str = Form(...),
    customer_email: str = Form(...),
    birth_date: str = Form(""),
    birth_time: str = Form(""),
    gender: str = Form(""),
    prefecture: str = Form(""),
    birth_place: str = Form(""),
    consultation_text: str = Form(""),
    source_type: str = Form("manual"),
    menu_name: str = Form(""),
    price: str = Form(""),
    staff_name: str = Form(""),
    yaml_log_text: str = Form(""),
    report_plan: str = Form("standard"),
    option_asteroids: str | None = Form(None),
    option_transit: str | None = Form(None),
    option_special_points: str | None = Form(None),
    option_year_forecast: str | None = Form(None),
    html_file: UploadFile | None = File(None),
    staff: dict = Depends(get_current_staff),
    db: Session = Depends(get_db),
):
    name = customer_name.strip()
    email = customer_email.strip()
    if not name or not email:
        return _redirect("/staff/external-orders/new?error=required")

    order = ExternalOrder(
        order_code=_generate_order_code(db),
        source_type=source_type if source_type in SOURCE_OPTIONS else "manual",
        customer_name=name,
        customer_email=email,
        birth_date=_parse_date(birth_date),
        birth_time=(birth_time or "").strip() or None,
        gender=(gender or "").strip() or None,
        prefecture=(prefecture or "").strip() or None,
        birth_place=(birth_place or "").strip() or None,
        consultation_text=(consultation_text or "").strip() or None,
        menu_name=(menu_name or "").strip() or None,
        price=_coerce_price(price),
        staff_name=(staff_name or staff.get("display_name") or "").strip() or None,
        yaml_log_text=yaml_log_text or None,
        status="draft",
        report_generation_plan=report_plan if report_plan in REPORT_PLAN_OPTIONS else "standard",
        report_generation_status="not_started",
        report_generation_model="claude-sonnet-4-6",
    )
    _apply_report_options(order, _report_options_from_form(order.report_generation_plan or "standard", option_asteroids, option_transit, option_special_points, option_year_forecast))
    db.add(order)
    db.flush()

    if html_file and html_file.filename:
        await _save_html_upload(order, html_file)

    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=created")


async def _save_html_upload(order: ExternalOrder, html_file: UploadFile) -> None:
    filename = (html_file.filename or "report.html").strip()
    if not filename.lower().endswith(".html"):
        raise HTTPException(status_code=400, detail="HTMLファイルのみアップロードできます")

    content = await html_file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="HTMLファイルは5MB以下にしてください")

    bucket_name = _bucket_name()
    if not bucket_name:
        raise HTTPException(status_code=500, detail="EXTERNAL_REPORTS_BUCKET が未設定です")

    client = _storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(_object_name(order.order_code))
    blob.upload_from_string(content, content_type="text/html; charset=utf-8")

    order.html_storage_path = blob.name
    order.html_original_name = filename
    order.html_uploaded_at = datetime.utcnow()
    if order.status == "draft":
        order.status = "html_uploaded"


@router.get("/staff/external-orders/{order_id}", response_class=HTMLResponse)
def external_order_detail(order_id: int, request: Request, staff: dict = Depends(get_current_staff), db: Session = Depends(get_db)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    _mark_stale_generation_if_needed(db, order)
    if not order.mail_subject:
        order.mail_subject = _default_subject()
    if not order.mail_body:
        order.mail_body = _default_body(order)
        db.commit()
    return templates.TemplateResponse(
        request=request,
        name="external_order_detail.html",
        context={
            "request": request,
            "staff": staff,
            "order": order,
            "status_labels": EXTERNAL_STATUSES,
            "source_options": SOURCE_OPTIONS,
            "gender_options": GENDER_OPTIONS,
            "report_plan_options": REPORT_PLAN_OPTIONS,
            "report_plan_labels": REPORT_PLAN_LABELS,
            "report_option_labels": REPORT_OPTION_LABELS,
            "report_options": order_report_options(order),
            "success": request.query_params.get("success"),
            "error": request.query_params.get("error"),
            "has_html": _html_exists(order),
        },
    )


@router.post("/staff/external-orders/{order_id}/update")
def external_order_update(
    order_id: int,
    customer_name: str = Form(...),
    customer_email: str = Form(...),
    birth_date: str = Form(""),
    birth_time: str = Form(""),
    gender: str = Form(""),
    prefecture: str = Form(""),
    birth_place: str = Form(""),
    consultation_text: str = Form(""),
    source_type: str = Form("manual"),
    menu_name: str = Form(""),
    price: str = Form(""),
    staff_name: str = Form(""),
    yaml_log_text: str = Form(""),
    report_plan: str = Form("standard"),
    option_asteroids: str | None = Form(None),
    option_transit: str | None = Form(None),
    option_special_points: str | None = Form(None),
    option_year_forecast: str | None = Form(None),
    mail_subject: str = Form(""),
    mail_body: str = Form(""),
    expires_at: str = Form(""),
    db: Session = Depends(get_db),
    staff: dict = Depends(get_current_staff),
):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    order.customer_name = customer_name.strip()
    order.customer_email = customer_email.strip()
    order.birth_date = _parse_date(birth_date)
    order.birth_time = (birth_time or "").strip() or None
    order.gender = (gender or "").strip() or None
    order.prefecture = (prefecture or "").strip() or None
    order.birth_place = (birth_place or "").strip() or None
    order.consultation_text = (consultation_text or "").strip() or None
    order.source_type = source_type if source_type in SOURCE_OPTIONS else order.source_type
    order.menu_name = (menu_name or "").strip() or None
    order.price = _coerce_price(price)
    order.staff_name = (staff_name or staff.get("display_name") or "").strip() or None
    order.yaml_log_text = yaml_log_text or None
    if report_plan in REPORT_PLAN_OPTIONS:
        order.report_generation_plan = report_plan
    _apply_report_options(order, _report_options_from_form(order.report_generation_plan or "standard", option_asteroids, option_transit, option_special_points, option_year_forecast))
    order.mail_subject = (mail_subject or _default_subject()).strip()
    order.mail_body = (mail_body or "").strip() or _default_body(order)
    order.expires_at = _parse_datetime_local(expires_at)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=saved")


@router.post("/staff/external-orders/{order_id}/generate-report")
def external_order_generate_report(
    order_id: int,
    background_tasks: BackgroundTasks,
    report_plan: str = Form("standard"),
    option_asteroids: str | None = Form(None),
    option_transit: str | None = Form(None),
    option_special_points: str | None = Form(None),
    option_year_forecast: str | None = Form(None),
    confirm_generate: str | None = Form(None),
    # 生成ボタンは基本情報フォームとは別フォームなので、未保存の入力値も一緒に受け取る。
    # これが無いと、画面上は生年月日が入っていてもDB未保存のまま生成して「生年月日未入力」になる。
    customer_name: str | None = Form(None),
    customer_email: str | None = Form(None),
    birth_date: str | None = Form(None),
    birth_time: str | None = Form(None),
    gender: str | None = Form(None),
    prefecture: str | None = Form(None),
    birth_place: str | None = Form(None),
    consultation_text: str | None = Form(None),
    source_type: str | None = Form(None),
    menu_name: str | None = Form(None),
    price: str | None = Form(None),
    staff_name: str | None = Form(None),
    yaml_log_text: str | None = Form(None),
    db: Session = Depends(get_db),
    staff: dict = Depends(get_current_staff),
):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)

    # 誤送信対策：生成ボタンのクリックでセットされる確認フラグが無いPOSTは実行しない。
    # 自動更新・Enterキー・別ボタン由来のsubmitで勝手に生成が始まる事故を防ぐ。
    if str(confirm_generate or "").strip() != "1":
        return _redirect(f"/staff/external-orders/{order.id}?error=generate_not_confirmed")

    try:
        # 詳細画面で編集した直後に「保存」を押さず生成しても、現在の入力値を先にDBへ反映する。
        if customer_name is not None:
            order.customer_name = customer_name.strip() or order.customer_name
        if customer_email is not None:
            order.customer_email = customer_email.strip() or order.customer_email
        if birth_date is not None:
            order.birth_date = _parse_date(birth_date)
        if birth_time is not None:
            order.birth_time = (birth_time or "").strip() or None
        if gender is not None:
            order.gender = (gender or "").strip() or None
        if prefecture is not None:
            order.prefecture = (prefecture or "").strip() or None
        if birth_place is not None:
            order.birth_place = (birth_place or "").strip() or None
        if consultation_text is not None:
            order.consultation_text = (consultation_text or "").strip() or None
        if source_type is not None and source_type in SOURCE_OPTIONS:
            order.source_type = source_type
        if menu_name is not None:
            order.menu_name = (menu_name or "").strip() or None
        if price is not None:
            order.price = _coerce_price(price)
        if staff_name is not None:
            order.staff_name = (staff_name or staff.get("display_name") or "").strip() or None
        if yaml_log_text is not None:
            order.yaml_log_text = yaml_log_text or None

        plan = report_plan if report_plan in REPORT_PLAN_OPTIONS else (order.report_generation_plan or "standard")
        options = _report_options_from_form(plan, option_asteroids, option_transit, option_special_points, option_year_forecast)

        if not order.birth_date:
            order.report_generation_status = "failed"
            order.last_error = "生年月日が未入力です。基本情報の生年月日を入力してから再生成してください。"
            _apply_report_options(order, options)
            db.commit()
            return _redirect(f"/staff/external-orders/{order.id}?error=generate_failed")

        # 同期生成しない。画面は即戻し、実処理はBackgroundTask側で新しいDBセッションを使って実行する。
        order.report_generation_plan = plan
        order.report_generation_status = "generating"
        order.report_generated_at = datetime.utcnow()
        order.report_generation_model = os.getenv("EXTERNAL_REPORT_CLAUDE_MODEL") or os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-6"
        order.last_error = None
        _apply_report_options(order, options)
        db.commit()

        background_tasks.add_task(_run_external_report_generation_background, order.id, plan, dict(options))
        return _redirect(f"/staff/external-orders/{order.id}?success=report_queued")
    except Exception as exc:
        order.last_error = str(exc)[:2000]
        order.report_generation_status = "failed"
        db.commit()
        return _redirect(f"/staff/external-orders/{order.id}?error=generate_failed")




@router.get("/staff/external-orders/{order_id}/report-status")
def external_order_report_status(order_id: int, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    """AI鑑定書生成の状態だけを返す軽量API。

    詳細画面全体を自動更新すると、セッション状態によって /login リダイレクトを繰り返すことがあるため、
    生成中のポーリングはこのJSON APIだけを見る。
    """
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)

    _mark_stale_generation_if_needed(db, order)
    status = order.report_generation_status or "not_started"
    progress_step, progress_message, real_error = _extract_generation_progress(order.last_error)
    has_html = _html_exists(order)
    return JSONResponse({
        "status": status,
        "step": progress_step,
        "progress_message": progress_message,
        "has_html": bool(has_html),
        "public_url": order.public_url or "",
        "last_error": real_error if status == "failed" else "",
        "generated_at": order.report_generated_at.isoformat() if order.report_generated_at else "",
        "html_uploaded_at": order.html_uploaded_at.isoformat() if order.html_uploaded_at else "",
    })

@router.post("/staff/external-orders/{order_id}/reset-report-status")
def external_order_reset_report_status(order_id: int, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    order.report_generation_status = "not_started"
    order.last_error = None
    order.report_generated_at = None
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=report_status_reset")


@router.post("/staff/external-orders/{order_id}/upload-html")
async def external_order_upload_html(order_id: int, html_file: UploadFile = File(...), db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    await _save_html_upload(order, html_file)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=html_uploaded")


@router.post("/staff/external-orders/{order_id}/issue-url")
def external_order_issue_url(order_id: int, request: Request, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not _html_exists(order):
        return _redirect(f"/staff/external-orders/{order.id}?error=no_html")
    if not order.public_token:
        order.public_token = _generate_token()
    order.public_url = _build_public_url(request, order.public_token)
    order.url_issued_at = datetime.utcnow()
    if order.status in {"draft", "html_uploaded"}:
        order.status = "url_issued"
    if not order.mail_body:
        order.mail_body = _default_body(order)
    else:
        order.mail_body = order.mail_body.replace("{public_url}", order.public_url)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=url_issued")


@router.post("/staff/external-orders/{order_id}/reissue-url")
def external_order_reissue_url(order_id: int, request: Request, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not _html_exists(order):
        return _redirect(f"/staff/external-orders/{order.id}?error=no_html")
    order.public_token = _generate_token()
    order.public_url = _build_public_url(request, order.public_token)
    order.url_issued_at = datetime.utcnow()
    if order.status in {"draft", "html_uploaded"}:
        order.status = "url_issued"
    if order.mail_body:
        order.mail_body = order.mail_body.replace("{public_url}", order.public_url)
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=url_reissued")


@router.post("/staff/external-orders/{order_id}/send-mail")
def external_order_send_mail(
    order_id: int,
    mail_subject: str = Form(""),
    mail_body: str = Form(""),
    db: Session = Depends(get_db),
    staff: dict = Depends(get_current_staff),
):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    if not order.public_url:
        return _redirect(f"/staff/external-orders/{order.id}?error=no_public_url")

    order.mail_subject = (mail_subject or order.mail_subject or _default_subject()).strip()
    order.mail_body = (mail_body or order.mail_body or _default_body(order)).strip()

    ok = send_mail(order.mail_subject, order.mail_body, [order.customer_email])

    if ok:
        order.mail_sent_at = datetime.utcnow()
        if order.status in {"draft", "html_uploaded", "url_issued"}:
            order.status = "mail_sent"
        order.last_error = None
        db.commit()
        return _redirect(f"/staff/external-orders/{order.id}?success=mail_sent")

    order.last_error = "メール送信に失敗しました"
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?error=mail_failed")


@router.post("/staff/external-orders/{order_id}/mark-delivered")
def external_order_mark_delivered(order_id: int, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    order.status = "delivered"
    order.delivered_at = datetime.utcnow()
    db.commit()
    return _redirect(f"/staff/external-orders/{order.id}?success=delivered")


@router.get("/staff/external-orders/{order_id}/preview", response_class=HTMLResponse)
def external_order_preview(order_id: int, request: Request, db: Session = Depends(get_db), staff: dict = Depends(get_current_staff)):
    order = db.get(ExternalOrder, order_id)
    if not order:
        raise HTTPException(status_code=404)
    body = _download_html(order)
    if not body:
        raise HTTPException(status_code=404)
    return HTMLResponse(body)


@router.get("/report/share/{token}", response_class=HTMLResponse, name="external_report_share")
def external_report_share(token: str, db: Session = Depends(get_db)):
    order = db.scalar(select(ExternalOrder).where(ExternalOrder.public_token == token))
    if order:
        if order.expires_at and datetime.utcnow() > order.expires_at:
            raise HTTPException(status_code=403, detail="このURLは期限切れです")
        body = _download_html(order)
        if not body:
            raise HTTPException(status_code=404)
        headers = {
            "X-Robots-Tag": "noindex, nofollow, noarchive",
            "Cache-Control": "private, no-store",
        }
        return HTMLResponse(content=body, headers=headers)

    staff_order_code = _decode_staff_share_token(token)
    if staff_order_code:
        staff_order = db.scalar(select(Order).where(Order.order_code == staff_order_code))
        if not staff_order:
            raise HTTPException(status_code=404)
        body = _download_staff_report_html(staff_order.order_code)
        if not body:
            latest_view = db.scalar(
                select(OrderResultView)
                .where(OrderResultView.order_id == staff_order.id)
                .order_by(OrderResultView.updated_at.desc(), OrderResultView.created_at.desc())
            )
            if latest_view and latest_view.report_html:
                body = latest_view.report_html
        if not body:
            raise HTTPException(status_code=404)
        headers = {
            "X-Robots-Tag": "noindex, nofollow, noarchive",
            "Cache-Control": "private, no-store",
        }
        return HTMLResponse(content=body, headers=headers)

    raise HTTPException(status_code=404)
