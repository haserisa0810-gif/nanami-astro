"""
routes.py
FastAPI の起動・共通設定・router 登録のみを担当する薄い層。
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import HTTPConnection

from auth import staff_nav_role
from line_webhook import router as line_router
from routes_admin import router as admin_router
from routes_analyze import router as analyze_router
from routes_analyze_save import router as analyze_save_router
from routes_daily_card import router as daily_card_router
from routes_daily_theme import router as daily_theme_router
from routes_public_orders import router as public_orders_router
from routes_public_pages import router as public_pages_router
from routes_api_calc import router as api_calc_router
from routes_reader import router as reader_router
from routes_staff import router as staff_router
from routes_shared import startup_platform_safe
from routes_transit import router as transit_router
from routes_external_orders import router as external_orders_router
from routes_note_articles import router as note_articles_router
from routes_products import router as products_router
from routes_social_flow import router as social_flow_router


class StaffNavMiddleware:
    """テンプレートの共通ナビ（_staff_nav.html）がログイン中の role を参照できるようにする。"""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            scope.setdefault("state", {})["nav_role"] = staff_nav_role(HTTPConnection(scope).cookies)
        await self.app(scope, receive, send)


app = FastAPI()
app.add_middleware(StaffNavMiddleware)
# 解析結果ページは1MBを超えるため圧縮して返す。
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/favicon.ico")
def favicon():
    return RedirectResponse("/static/favicon.svg", status_code=307)


@app.on_event("startup")
def _startup_platform() -> None:
    startup_platform_safe()


@app.get("/healthz")
def healthz():
    return {"ok": True}


app.include_router(line_router)
app.include_router(public_pages_router)
app.include_router(api_calc_router)
app.include_router(analyze_router)
app.include_router(analyze_save_router)
app.include_router(transit_router)
app.include_router(public_orders_router)
app.include_router(reader_router)
app.include_router(admin_router)
app.include_router(staff_router)
app.include_router(external_orders_router)
app.include_router(daily_theme_router)
app.include_router(daily_card_router)
app.include_router(products_router)
app.include_router(note_articles_router)
app.include_router(social_flow_router)
