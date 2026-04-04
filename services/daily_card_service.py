from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select

from db import SessionLocal
from models import DailyCardDraw

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "daily_cards.json"
LINE_PROFILE_URL = "https://api.line.me/v2/profile"
LINE_FRIENDSHIP_URL = "https://api.line.me/friendship/v1/status"


def load_cards() -> list[dict[str, Any]]:
    data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("daily_cards.json must be a list")
    return data


def select_card_for_user(line_user_id: str, target_date: date | None = None) -> dict[str, Any]:
    cards = load_cards()
    if not cards:
        raise ValueError("daily cards are empty")
    target_date = target_date or date.today()
    key = f"{line_user_id}:{target_date.isoformat()}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    index = int(digest[:8], 16) % len(cards)
    return cards[index]


async def fetch_line_identity(access_token: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        profile_res = await client.get(LINE_PROFILE_URL, headers=headers)
        if profile_res.status_code != 200:
            raise ValueError("LINEプロフィールを取得できませんでした。")
        friendship_res = await client.get(LINE_FRIENDSHIP_URL, headers=headers)
        if friendship_res.status_code != 200:
            raise ValueError("友だち状態を確認できませんでした。")

    profile = profile_res.json()
    friendship = friendship_res.json()
    line_user_id = str(profile.get("userId") or "").strip()
    if not line_user_id:
        raise ValueError("LINE userId を取得できませんでした。")
    return {
        "line_user_id": line_user_id,
        "display_name": str(profile.get("displayName") or "").strip(),
        "picture_url": str(profile.get("pictureUrl") or "").strip(),
        "friend_flag": bool(friendship.get("friendFlag")),
    }



def get_today_draw(line_user_id: str, target_date: date | None = None) -> DailyCardDraw | None:
    target_date = target_date or date.today()
    with SessionLocal() as db:
        return db.scalar(
            select(DailyCardDraw).where(
                DailyCardDraw.line_user_id == line_user_id,
                DailyCardDraw.draw_date == target_date,
            )
        )



def save_today_draw(*, line_user_id: str, card_id: str, display_name: str | None = None, target_date: date | None = None) -> DailyCardDraw:
    target_date = target_date or date.today()
    now = datetime.utcnow()
    with SessionLocal() as db:
        row = db.scalar(
            select(DailyCardDraw).where(
                DailyCardDraw.line_user_id == line_user_id,
                DailyCardDraw.draw_date == target_date,
            )
        )
        if row is None:
            row = DailyCardDraw(
                line_user_id=line_user_id,
                display_name=display_name,
                draw_date=target_date,
                card_id=card_id,
                drawn_at=now,
            )
            db.add(row)
        else:
            row.card_id = card_id
            row.display_name = display_name or row.display_name
            row.drawn_at = row.drawn_at or now
        db.commit()
        db.refresh(row)
        return row



def build_card_payload(card: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": card.get("id"),
        "title": card.get("title"),
        "image": card.get("image"),
        "message": card.get("message"),
        "detail": card.get("detail"),
        "action": card.get("action"),
    }
