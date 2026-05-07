from __future__ import annotations

import traceback
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.api_calc import (
    calc_combined_api,
    calc_shichu_api,
    calc_transit_api,
    calc_western_api,
)

router = APIRouter()


def _json_ok(payload: dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=payload)


def _json_error(code: str, message: str, status_code: int = 400) -> JSONResponse:
    return JSONResponse(
        content={
            "ok": False,
            "error": {
                "code": code,
                "message": message,
            },
        },
        status_code=status_code,
    )


async def _read_json(request: Request) -> dict[str, Any]:
    body = await request.json()
    return body if isinstance(body, dict) else {}


@router.post("/api/calc/western", response_class=JSONResponse)
async def api_calc_western(request: Request):
    try:
        body = await _read_json(request)
        return _json_ok(calc_western_api(body))
    except HTTPException as exc:
        return _json_error("INVALID_INPUT", _safe_detail(exc.detail), exc.status_code)
    except ValueError as exc:
        if str(exc) == "UNSUPPORTED_PERIOD":
            return _json_error("UNSUPPORTED_PERIOD", "period は day または month を指定してください。", 400)
        return _json_error("CALCULATION_FAILED", str(exc), 400)
    except Exception as exc:
        traceback.print_exc()
        return _json_error("INTERNAL_ERROR", str(exc), 500)


@router.post("/api/calc/shichu", response_class=JSONResponse)
async def api_calc_shichu(request: Request):
    try:
        body = await _read_json(request)
        return _json_ok(calc_shichu_api(body))
    except HTTPException as exc:
        return _json_error("INVALID_INPUT", _safe_detail(exc.detail), exc.status_code)
    except ValueError as exc:
        if str(exc) == "UNSUPPORTED_PERIOD":
            return _json_error("UNSUPPORTED_PERIOD", "period は day または month を指定してください。", 400)
        return _json_error("CALCULATION_FAILED", str(exc), 400)
    except Exception as exc:
        traceback.print_exc()
        return _json_error("INTERNAL_ERROR", str(exc), 500)


@router.post("/api/calc/transit", response_class=JSONResponse)
async def api_calc_transit(request: Request):
    try:
        body = await _read_json(request)
        return _json_ok(calc_transit_api(body))
    except HTTPException as exc:
        return _json_error("INVALID_INPUT", _safe_detail(exc.detail), exc.status_code)
    except ValueError as exc:
        if str(exc) == "UNSUPPORTED_PERIOD":
            return _json_error("UNSUPPORTED_PERIOD", "period は day または month を指定してください。", 400)
        return _json_error("CALCULATION_FAILED", str(exc), 400)
    except Exception as exc:
        traceback.print_exc()
        return _json_error("INTERNAL_ERROR", str(exc), 500)


@router.post("/api/calc/combined", response_class=JSONResponse)
async def api_calc_combined(request: Request):
    try:
        body = await _read_json(request)
        return _json_ok(calc_combined_api(body))
    except HTTPException as exc:
        return _json_error("INVALID_INPUT", _safe_detail(exc.detail), exc.status_code)
    except ValueError as exc:
        if str(exc) == "UNSUPPORTED_PERIOD":
            return _json_error("UNSUPPORTED_PERIOD", "period は day または month を指定してください。", 400)
        return _json_error("CALCULATION_FAILED", str(exc), 400)
    except Exception as exc:
        traceback.print_exc()
        return _json_error("INTERNAL_ERROR", str(exc), 500)


def _safe_detail(detail: Any) -> str:
    if isinstance(detail, str):
        return detail
    return str(detail)

