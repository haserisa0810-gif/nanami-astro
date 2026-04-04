from __future__ import annotations

import os
from datetime import date

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from services.daily_card_service import (
    build_card_payload,
    fetch_line_identity,
    get_today_draw,
    load_cards,
    save_today_draw,
    select_card_for_user,
)

router = APIRouter()
templates = Jinja2Templates(directory="templates")


class DailyCardAuthRequest(BaseModel):
    access_token: str


@router.get("/daily-card", response_class=HTMLResponse)
def daily_card_page(request: Request):
    liff_id = (os.getenv("LINE_DAILY_CARD_LIFF_ID") or "").strip()
    add_friend_url = (os.getenv("LINE_ADD_FRIEND_URL") or "https://line.me/R/ti/p/@281jnwon").strip()
    home_url = (os.getenv("HOMEPAGE_BASE_URL") or os.getenv("PUBLIC_BASE_URL") or "https://nanami-astro.com").strip().rstrip("/")
    daily_card_url = (os.getenv("DAILY_CARD_URL") or f"{home_url}/daily-card").strip()
    return templates.TemplateResponse(
        request=request,
        name="daily_card_line_gate.html",
        context={
            "request": request,
            "liff_id": liff_id,
            "add_friend_url": add_friend_url,
            "home_url": home_url,
            "daily_card_url": daily_card_url,
        },
    )


@router.get("/daily-card/cards", response_class=JSONResponse)
def daily_card_cards():
    return {"cards": [build_card_payload(card) for card in load_cards()]}


@router.post("/daily-card/api/status", response_class=JSONResponse)
async def daily_card_status(body: DailyCardAuthRequest):
    access_token = (body.access_token or "").strip()
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token が必要です")

    try:
        identity = await fetch_line_identity(access_token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    if not identity["friend_flag"]:
        return {
            "authenticated": True,
            "friend_required": True,
            "display_name": identity.get("display_name") or "",
        }

    today_row = get_today_draw(identity["line_user_id"], date.today())
    card = select_card_for_user(identity["line_user_id"], date.today())
    return {
        "authenticated": True,
        "friend_required": False,
        "display_name": identity.get("display_name") or "",
        "already_drawn": today_row is not None,
        "card": build_card_payload(card) if today_row is not None else None,
        "draw_date": date.today().isoformat(),
    }


@router.post("/daily-card/api/draw", response_class=JSONResponse)
async def daily_card_draw(body: DailyCardAuthRequest):
    access_token = (body.access_token or "").strip()
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token が必要です")

    try:
        identity = await fetch_line_identity(access_token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    if not identity["friend_flag"]:
        raise HTTPException(status_code=403, detail="LINE友だち登録が必要です")

    line_user_id = identity["line_user_id"]
    card = select_card_for_user(line_user_id, date.today())
    save_today_draw(
        line_user_id=line_user_id,
        card_id=str(card.get("id") or ""),
        display_name=identity.get("display_name") or None,
        target_date=date.today(),
    )
    return {
        "ok": True,
        "already_drawn": True,
        "draw_date": date.today().isoformat(),
        "display_name": identity.get("display_name") or "",
        "card": build_card_payload(card),
    }
