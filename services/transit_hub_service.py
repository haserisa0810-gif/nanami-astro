from __future__ import annotations

from datetime import date, datetime, timedelta
from sqlalchemy import select
from sqlalchemy.orm import Session

from models import TransitHubJob, TransitHubRequest

STATUS_LABELS = {
    "draft": "下書き",
    "ready": "生成待ち",
    "generating": "生成中",
    "generated": "生成済み",
    "error": "エラー",
}

CHANNEL_OPTIONS = ["manual", "stores", "coconala"]


def default_period_dates() -> tuple[date, date]:
    start = date.today()
    end = start + timedelta(days=90)
    return start, end


def generate_request_code(db: Session) -> str:
    prefix = f"TR-{datetime.utcnow().strftime('%Y%m%d')}-"
    rows = db.scalars(select(TransitHubRequest.request_code).where(TransitHubRequest.request_code.like(f"{prefix}%"))).all()
    used = set()
    for code in rows:
        try:
            used.add(int(str(code).rsplit('-', 1)[-1]))
        except Exception:
            continue
    seq = 1
    while seq in used:
        seq += 1
    return f"{prefix}{seq:04d}"


def create_request(db: Session, **payload) -> TransitHubRequest:
    req = TransitHubRequest(**payload)
    db.add(req)
    db.commit()
    db.refresh(req)
    return req


def create_job(db: Session, request_id: int, *, status: str = "pending", log_text: str | None = None) -> TransitHubJob:
    job = TransitHubJob(request_id=request_id, status=status, job_type="generate", log_text=log_text)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def build_preview_html(req: TransitHubRequest) -> str:
    period = f"{req.period_start or '-'} 〜 {req.period_end or '-'}"
    notes = (req.notes or "未入力").replace("\n", "<br>")
    summary = (req.generated_summary or "まだ生成されていません。")
    return f"""<!doctype html>
<html lang='ja'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>{req.customer_name} | トランジットレポート</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#faf8f5;color:#111;margin:0;padding:32px}}.wrap{{max-width:760px;margin:0 auto;background:#fff;border:1px solid #e7e1d9;border-radius:20px;padding:32px}}h1,h2{{margin:0 0 12px}}.muted{{color:#666}}.box{{background:#faf8f5;border:1px solid #eee2d5;border-radius:16px;padding:18px;margin-top:18px;line-height:1.8}}</style>
</head>
<body><div class='wrap'><div class='muted'>Transit Hub Preview</div><h1>{req.customer_name} の{req.period_label}トランジット</h1><p class='muted'>期間: {period}</p><div class='box'><h2>要約</h2><p>{summary}</p></div><div class='box'><h2>メモ</h2><p>{notes}</p></div></div></body></html>"""


def generate_request_output(db: Session, req: TransitHubRequest) -> TransitHubRequest:
    job = create_job(db, req.id, status="running", log_text="transit preview generation start")
    try:
        req.status = "generating"
        db.add(req)
        db.commit()

        start = req.period_start.isoformat() if req.period_start else "未設定"
        end = req.period_end.isoformat() if req.period_end else "未設定"
        req.generated_summary = (
            f"{req.customer_name}向けの{req.period_label}トランジット案件です。"
            f" 期間は {start} から {end}。"
            f" チャネルは {req.channel}。実計算エンジン接続前のため、現在は管理画面導線と生成ジョブのみを分離追加しています。"
        )
        req.generated_html = build_preview_html(req)
        req.generated_at = datetime.utcnow()
        req.status = "generated"
        req.last_error = None

        job = db.get(TransitHubJob, job.id)
        job.status = "completed"
        job.started_at = job.started_at or datetime.utcnow()
        job.finished_at = datetime.utcnow()
        job.log_text = (job.log_text or "") + "\ncompleted"
        db.add_all([req, job])
        db.commit()
        db.refresh(req)
        return req
    except Exception as exc:
        db.rollback()
        req = db.get(TransitHubRequest, req.id)
        if req:
            req.status = "error"
            req.last_error = str(exc)
            db.add(req)
        job = db.get(TransitHubJob, job.id)
        if job:
            job.status = "error"
            job.error_message = str(exc)
            job.finished_at = datetime.utcnow()
            db.add(job)
        db.commit()
        raise
