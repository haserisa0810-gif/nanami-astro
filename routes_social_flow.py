from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_staff
from services.social_flow_service import (
    CLAUDE_MODELS,
    OUTPUT_TYPES,
    PERIODS,
    THEMES,
    SocialFlowError,
    default_base_date,
    generate_social_flow,
)


router = APIRouter()
templates = Jinja2Templates(directory="templates")


def _context(
    *,
    request: Request,
    staff: dict,
    base_date: str,
    period: str,
    theme: str,
    output_type: str,
    model_key: str,
    result: dict | None = None,
    error: str = "",
) -> dict:
    return {
        "request": request,
        "staff": staff,
        "periods": PERIODS,
        "themes": THEMES,
        "output_types": OUTPUT_TYPES,
        "claude_models": CLAUDE_MODELS,
        "base_date": base_date,
        "period": period,
        "theme": theme,
        "output_type": output_type,
        "model_key": model_key,
        "result": result,
        "error": error,
    }


@router.get("/products/social-flow", response_class=HTMLResponse)
def social_flow_page(
    request: Request,
    staff: dict = Depends(get_current_staff),
):
    return templates.TemplateResponse(
        request=request,
        name="products_social_flow.html",
        context=_context(
            request=request,
            staff=staff,
            base_date=default_base_date(),
            period="month",
            theme="overall",
            output_type="all",
            model_key="sonnet",
        ),
    )


@router.post("/products/social-flow/generate", response_class=HTMLResponse)
def social_flow_generate(
    request: Request,
    base_date: str = Form(...),
    period: str = Form("month"),
    theme: str = Form("overall"),
    output_type: str = Form("all"),
    model_key: str = Form("sonnet"),
    staff: dict = Depends(get_current_staff),
):
    result = None
    error = ""
    try:
        result = generate_social_flow(
            base_date=base_date,
            period=period,
            theme=theme,
            output_type=output_type,
            model_key=model_key,
        )
    except SocialFlowError as exc:
        error = str(exc)
    except Exception:
        error = "社会の流れ生成中にエラーが発生しました。入力とClaude API設定を確認してください。"

    return templates.TemplateResponse(
        request=request,
        name="products_social_flow.html",
        context=_context(
            request=request,
            staff=staff,
            base_date=base_date,
            period=period,
            theme=theme,
            output_type=output_type,
            model_key=model_key,
            result=result,
            error=error,
        ),
    )
