from __future__ import annotations

import os

import stripe

from models import Menu, Order


def _base_url() -> str:
    return (os.getenv('BASE_URL') or 'http://localhost:8000').rstrip('/')


def configure_stripe() -> None:
    stripe.api_key = os.getenv('STRIPE_SECRET_KEY', '')


def create_checkout_session(order: Order, menu: Menu) -> stripe.checkout.Session:
    configure_stripe()
    success_url = f"{_base_url()}/thanks/{order.order_code}?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{_base_url()}/payment/{order.order_code}?cancelled=1"
    return stripe.checkout.Session.create(
        mode='payment',
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            'order_code': order.order_code,
            'menu_id': str(menu.id),
            'source': order.source,
            'customer_id': str(order.customer_id or ''),
        },
        line_items=[{
            'price_data': {
                'currency': 'jpy',
                'product_data': {'name': menu.name, 'description': menu.description or ''},
                'unit_amount': int(order.price),
            },
            'quantity': 1,
        }],
    )


def build_payment_link(order_code: str) -> str:
    return f"{_base_url()}/payment/{order_code}"
