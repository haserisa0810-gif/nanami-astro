"""Readable errors for the HTML analysis form, without changing JSON APIs."""
from __future__ import annotations

import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException

from routes_shared import templates

logger = logging.getLogger(__name__)

_FIELD_LABELS = {
    "birth_date": "生年月日",
    "birth_time": "出生時刻",
    "lat": "緯度",
    "lon": "経度",
    "birth_date_b": "相手の生年月日",
    "birth_time_b": "相手の出生時刻",
    "lat_b": "相手の緯度",
    "lon_b": "相手の経度",
    "analysis_type": "分析タイプ",
    "astrology_system": "占星術タイプ",
    "day_change_at_23": "四柱推命オプション",
}
_UNAVAILABLE_MESSAGE = "鑑定処理に失敗しました。時間をおいて再試行してください。"


def _validation_messages(exc: RequestValidationError) -> list[str]:
    messages = []
    for error in exc.errors():
        field = str(error.get("loc", ("",))[-1])
        label = _FIELD_LABELS.get(field, "入力内容")
        if error.get("type") == "missing":
            message = f"{label}を入力してください。"
        elif field in {"lat", "lon", "lat_b", "lon_b"}:
            message = f"{label}は数値で入力してください。"
        else:
            message = f"{label}を確認し、入力または選択し直してください。"
        if message not in messages:
            messages.append(message)
    return messages or ["入力内容を確認してください。"]


def _error_page(request: Request, status_code: int, messages: list[str]):
    return templates.TemplateResponse(
        request=request,
        name="analyze_error.html",
        context={
            "request": request,
            "title": "入力内容を確認してください" if status_code < 500 else "鑑定を完了できませんでした",
            "messages": messages,
        },
        status_code=status_code,
        headers={"Cache-Control": "no-store"},
    )


class AnalyzeFormRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()
        # Recommendation and other JSON APIs retain FastAPI's normal responses.
        if self.path != "/analyze" or "POST" not in self.methods:
            return handler

        async def handle(request: Request):
            try:
                return await handler(request)
            except RequestValidationError as exc:
                return _error_page(request, 422, _validation_messages(exc))
            except HTTPException as exc:
                if exc.status_code < 400:
                    raise
                message = (
                    exc.detail
                    if exc.status_code < 500 and isinstance(exc.detail, str)
                    else _UNAVAILABLE_MESSAGE
                )
                return _error_page(request, exc.status_code, [message])
            except Exception:
                logger.exception("analyze_form_failed")
                return _error_page(request, 500, [_UNAVAILABLE_MESSAGE])

        return handle
