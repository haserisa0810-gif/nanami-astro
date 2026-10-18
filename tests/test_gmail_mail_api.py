from __future__ import annotations

import base64
from email.message import EmailMessage

import pytest

from services.gmail_mail_api import GmailApiClient, GmailApiConfig, GmailApiError


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


def config(**overrides):
    values = {
        "client_id": "client-id",
        "client_secret": "client-secret",
        "refresh_token": "refresh-token",
        "expected_email": "nanami.hoshitsuki@gmail.com",
        "token_url": "https://oauth2.googleapis.com/token",
        "api_base_url": "https://gmail.googleapis.com/gmail/v1",
        "timeout_seconds": 15,
    }
    values.update(overrides)
    return GmailApiConfig(**values)


def raw_payload(message: EmailMessage) -> str:
    return base64.urlsafe_b64encode(message.as_bytes()).decode().rstrip("=")


def test_fetch_original_messages_uses_get_only_and_returns_mime_bytes():
    mime = EmailMessage()
    mime["From"] = "hello@stores.jp"
    mime["Subject"] = "order"
    mime.set_content("body")
    session = FakeSession(
        post_responses=[FakeResponse({"access_token": "access-token"})],
        get_responses=[
            FakeResponse({"emailAddress": "nanami.hoshitsuki@gmail.com"}),
            FakeResponse({"messages": [{"id": "2"}, {"id": "1"}]}),
            FakeResponse({"id": "2", "raw": raw_payload(mime)}),
            FakeResponse({"id": "1", "raw": raw_payload(mime)}),
        ],
    )

    messages = GmailApiClient(config(), session=session).fetch_original_messages(
        senders=["hello@stores.jp", "emails@mail.etsy.com"],
        limit=2,
    )

    assert len(messages) == 2
    assert b"Subject: order" in messages[0]
    assert session.posts[0][0] == "https://oauth2.googleapis.com/token"
    assert session.posts[0][1]["data"]["grant_type"] == "refresh_token"
    list_request = session.gets[1]
    assert list_request[0].endswith("/users/me/messages")
    assert list_request[1]["params"]["q"] == (
        "{from:hello@stores.jp from:emails@mail.etsy.com}"
    )
    assert list_request[1]["params"]["maxResults"] == 2
    assert all(call[1]["params"] == {"format": "raw"} for call in session.gets[2:])


def test_profile_mismatch_stops_before_reading_messages():
    session = FakeSession(
        post_responses=[FakeResponse({"access_token": "access-token"})],
        get_responses=[FakeResponse({"emailAddress": "wrong@gmail.com"})],
    )

    with pytest.raises(GmailApiError, match="does not match"):
        GmailApiClient(config(), session=session).fetch_original_messages(
            senders=["hello@stores.jp"], limit=1
        )

    assert len(session.gets) == 1


def test_missing_oauth_configuration_fails_without_exposing_values(monkeypatch):
    monkeypatch.delenv("GMAIL_API_CLIENT_ID", raising=False)
    monkeypatch.delenv("GMAIL_API_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("GMAIL_API_REFRESH_TOKEN", raising=False)
    monkeypatch.setenv("STORES_MAIL_USERNAME", "nanami.hoshitsuki@gmail.com")

    with pytest.raises(GmailApiError, match="GMAIL_API_CLIENT_ID"):
        GmailApiConfig.from_env()


def test_refresh_failure_does_not_include_secret_values():
    session = FakeSession(
        post_responses=[FakeResponse({"error": "invalid_client"}, status_code=400)],
        get_responses=[],
    )

    with pytest.raises(GmailApiError) as exc_info:
        GmailApiClient(config(), session=session).fetch_original_messages(
            senders=["hello@stores.jp"], limit=1
        )

    message = str(exc_info.value)
    assert "invalid_client" in message
    assert "client-secret" not in message
    assert "refresh-token" not in message
