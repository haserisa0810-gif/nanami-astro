from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_staff
from services.note_article_service import (
    ARTICLE_TYPES,
    CLAUDE_MODELS,
    NoteArticleError,
    default_target_month,
    generate_note_article,
)


router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _context(
    *,
    request: Request,
    staff: dict,
    target_month: str,
    article_type: str,
    custom_theme: str,
    model_key: str,
    result: dict | None = None,
    error: str = "",
) -> dict:
    return {
        "request": request,
        "staff": staff,
        "article_types": ARTICLE_TYPES,
        "claude_models": CLAUDE_MODELS,
        "target_month": target_month,
        "article_type": article_type,
        "custom_theme": custom_theme,
        "model_key": model_key,
        "result": result,
        "error": error,
    }


@router.get("/products/note-articles", response_class=HTMLResponse)
def note_articles_page(
    request: Request,
    staff: dict = Depends(get_current_staff),
):
    return templates.TemplateResponse(
        request=request,
        name="products_note_articles.html",
        context=_context(
            request=request,
            staff=staff,
            target_month=default_target_month(),
            article_type="monthly_reading",
            custom_theme="",
            model_key="haiku",
        ),
    )


@router.post("/products/note-articles/generate", response_class=HTMLResponse)
def note_articles_generate(
    request: Request,
    target_month: str = Form(...),
    article_type: str = Form(...),
    custom_theme: str = Form(""),
    model_key: str = Form("haiku"),
    staff: dict = Depends(get_current_staff),
):
    result = None
    error = ""
    try:
        result = generate_note_article(
            target_month=target_month,
            article_type=article_type,
            custom_theme=custom_theme,
            model_key=model_key,
        )
    except NoteArticleError as exc:
        error = str(exc)
    except Exception:
        error = "note記事生成中にエラーが発生しました。入力とClaude API設定を確認してください。"

    return templates.TemplateResponse(
        request=request,
        name="products_note_articles.html",
        context=_context(
            request=request,
            staff=staff,
            target_month=target_month,
            article_type=article_type,
            custom_theme=custom_theme,
            model_key=model_key,
            result=result,
            error=error,
        ),
    )
