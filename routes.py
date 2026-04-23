"""
routes.py
FastAPI の起動・共通設定・router 登録のみを担当する薄い層。
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from line_webhook import router as line_router
from routes_admin import router as admin_router
from routes_analyze import router as analyze_router
from routes_analyze_save import router as analyze_save_router
from routes_daily_card import router as daily_card_router
from routes_daily_theme import router as daily_theme_router
from routes_public_orders import router as public_orders_router
from routes_public_pages import router as public_pages_router
from routes_reader import router as reader_router
from routes_staff import router as staff_router
from routes_shared import startup_platform_safe
from routes_transit import router as transit_router
from routes_external_orders import router as external_orders_router
from routes_products import router as products_router

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def _startup_platform() -> None:
    startup_platform_safe()


@app.get("/healthz")
def healthz():
    return {"ok": True}


app.include_router(line_router)
app.include_router(public_pages_router)
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
