from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from routes_public_orders import (
    PAYHIP_FROM_DEFAULT,
    _ensure_stores_payment_table,
    _parse_stores_mail,
    _upsert_stores_payment,
    _verify_stores_payment,
)


PAYHIP_BODY = """Buyer: buyer@example.com
Date: 8th July 2026
Order ID: LWR6I4Y4Wa
AI-Readable Natal Data Core Pack [NP-WB]
Total: $7.00
"""


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    return Session(engine)


def test_parse_payhip_mail_extracts_order_id_and_buyer_email():
    parsed = _parse_stores_mail("You've Sold An Item!", PAYHIP_BODY, "msg-1", datetime(2026, 7, 8, 12, 0, 0))

    assert parsed is not None
    assert parsed["stores_order_no"] == "LWR6I4Y4Wa"
    assert parsed["buyer_email"] == "buyer@example.com"
    assert parsed["item_name"] == "AI-Readable Natal Data Core Pack [NP-WB]"
    assert parsed["amount"] == 7
    assert parsed["currency"] == "usd"
    assert parsed["payment_status"] == "paid"
    assert parsed["source"] == "payhip_email"


def test_parse_payhip_mail_requires_only_order_id_for_payment_identity():
    body = """Date: 8th July 2026
Order ID: LWR6I4Y4Wa
Total: $7.00
"""

    parsed = _parse_stores_mail("You've Sold An Item!", body, "msg-1", datetime(2026, 7, 8, 12, 0, 0))

    assert parsed is not None
    assert parsed["stores_order_no"] == "LWR6I4Y4Wa"
    assert parsed["buyer_email"] is None
    assert parsed["item_name"] is None
    assert parsed["payment_status"] == "paid"


def test_payhip_mail_search_defaults_to_contact_sender():
    assert PAYHIP_FROM_DEFAULT == "contact@payhip.com"


def test_upsert_uses_order_id_as_unique_key_and_preserves_buyer_email():
    db = _session()
    _ensure_stores_payment_table(db)
    parsed = _parse_stores_mail("You've Sold An Item!", PAYHIP_BODY, "msg-1", datetime(2026, 7, 8, 12, 0, 0))
    assert parsed is not None

    _upsert_stores_payment(db, parsed)
    _upsert_stores_payment(db, {**parsed, "raw_message_id": "msg-2"})
    db.commit()

    rows = db.execute(text("SELECT stores_order_no, buyer_email FROM stores_payments")).mappings().all()
    assert len(rows) == 1
    assert rows[0]["stores_order_no"] == "LWR6I4Y4Wa"
    assert rows[0]["buyer_email"] == "buyer@example.com"


def test_verify_payment_does_not_use_email_as_identity():
    db = _session()
    _ensure_stores_payment_table(db)
    parsed = _parse_stores_mail("You've Sold An Item!", PAYHIP_BODY, "msg-1", datetime(2026, 7, 8, 12, 0, 0))
    assert parsed is not None
    _upsert_stores_payment(db, parsed)
    db.commit()

    status, row = _verify_stores_payment(
        db,
        stores_order_no="LWR6I4Y4Wa",
        payment_email="different@example.com",
    )

    assert status == "verified"
    assert row is not None
    assert row["buyer_email"] == "buyer@example.com"


def test_legacy_mail_without_order_id_is_not_parsed():
    body = """Buyer: buyer@example.com
Date: 8th July 2026
AI-Readable Natal Data Core Pack [NP-WB]
Total: $7.00
"""

    assert _parse_stores_mail("You've Sold An Item!", body, "msg-1", datetime(2026, 7, 8, 12, 0, 0)) is None
