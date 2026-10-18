from __future__ import annotations

from services.zoho_mail_api import ZohoMailClient, ZohoMailConfig


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, *, post_responses, get_responses):
        self.post_responses = list(post_responses)
        self.get_responses = list(get_responses)
        self.posts = []
        self.gets = []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.post_responses.pop(0)

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return self.get_responses.pop(0)


def make_config(**overrides):
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "refresh_token": "refresh-token",
        "account_id": "12345",
        "username": "support@nanami-astro.com",
        "accounts_base_url": "https://accounts.zoho.jp",
        "mail_api_base_url": "https://mail.zoho.jp/api",
        "timeout_seconds": 15,
    }
    values.update(overrides)
    return ZohoMailConfig(**values)


def test_fetch_original_messages_returns_mime_without_update_requests():
    raw_message = "From: hello@stores.jp\r\nSubject: order\r\n\r\nbody\r\n"
    session = FakeSession(
        post_responses=[FakeResponse({"access_token": "access-token"})],
        get_responses=[
            FakeResponse(
                {
                    "status": {"code": 200},
                    "data": [{"messageId": "2", "receivedtime": 200}],
                }
            ),
            FakeResponse(
                {"status": {"code": 200}, "data": {"content": raw_message}}
            ),
        ],
    )

    messages = ZohoMailClient(make_config(), session=session).fetch_original_messages(
        senders=["hello@stores.jp"], limit=1
    )

    assert messages == [raw_message.encode("utf-8")]
    assert len(session.posts) == 1
    assert len(session.gets) == 2
    assert session.gets[0][1]["params"]["searchKey"] == "sender:hello@stores.jp"


def test_account_id_resolves_from_support_mailbox():
    session = FakeSession(
        post_responses=[FakeResponse({"access_token": "access-token"})],
        get_responses=[
            FakeResponse(
                {
                    "status": {"code": 200},
                    "data": [
                        {
                            "accountId": "67890",
                            "mailboxAddress": "support@nanami-astro.com",
                        }
                    ],
                }
            )
        ],
    )

    assert ZohoMailClient(make_config(account_id=""), session=session).account_id() == "67890"
