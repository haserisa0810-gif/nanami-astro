from __future__ import annotations

import os

import stripe

from models import Menu, Order


def _public_base_url() -> str:
    return (os.getenv('PUBLIC_BASE_URL') or os.getenv('BASE_URL') or 'http://localhost:8000').rstrip('/')


def configure_stripe() -> None:
    secret_key = (os.getenv('STRIPE_SECRET_KEY') or '').strip()
    if not secret_key:
        raise RuntimeError('STRIPE_SECRET_KEY is not set')
    stripe.api_key = secret_key


def create_checkout_session(order: Order, menu: Menu) -> stripe.checkout.Session:
    configure_stripe()
    success_url = f"{_public_base_url()}/thanks/{order.order_code}?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{_public_base_url()}/payment/{order.order_code}?cancelled=1"
    params = {
        'mode': 'payment',
        'success_url': success_url,
        'cancel_url': cancel_url,
        'client_reference_id': order.order_code,
        'metadata': {
            'order_code': order.order_code,
            'menu_id': str(menu.id),
            'source': order.source,
            'customer_id': str(order.customer_id or ''),
        },
        'line_items': [{
            'price_data': {
                'currency': 'jpy',
                'product_data': {'name': menu.name, 'description': menu.description or ''},
                'unit_amount': int(order.price),
            },
            'quantity': 1,
        }],
    }
    customer_email = (order.user_contact or '').strip() if '@' in (order.user_contact or '') else ''
    if customer_email:
        params['customer_email'] = customer_email
    return stripe.checkout.Session.create(**params)


def retrieve_checkout_session(session_id: str) -> stripe.checkout.Session:
    configure_stripe()
    return stripe.checkout.Session.retrieve(session_id)


def build_payment_link(order_code: str) -> str:
    return f"{_public_base_url()}/payment/{order_code}"
