"""Read-only Gmail API client used by the order intake jobs."""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Any, Iterable

import requests


class GmailApiError(RuntimeError):
    """Raised when Gmail OAuth or API access fails."""


@dataclass(frozen=True)
class GmailApiConfig:
    client_id: str
    client_secret: str
    refresh_token: str
    expected_email: str
    token_url: str
    api_base_url: str
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> "GmailApiConfig":
        config = cls(
            client_id=(os.getenv("GMAIL_API_CLIENT_ID") or "").strip(),
            client_secret=(os.getenv("GMAIL_API_CLIENT_SECRET") or "").strip(),
            refresh_token=(os.getenv("GMAIL_API_REFRESH_TOKEN") or "").strip(),
            expected_email=(
                os.getenv("GMAIL_API_EXPECTED_EMAIL")
                or os.getenv("STORES_MAIL_USERNAME")
                or ""
            ).strip(),
            token_url=(
                os.getenv("GMAIL_API_TOKEN_URL") or "https://oauth2.googleapis.com/token"
            ).strip(),
            api_base_url=(
                os.getenv("GMAIL_API_BASE_URL")
                or "https://gmail.googleapis.com/gmail/v1"
            ).rstrip("/"),
            timeout_seconds=float(os.getenv("GMAIL_API_TIMEOUT", "15")),
        )
        missing = [
            name
            for name, value in (
                ("GMAIL_API_CLIENT_ID", config.client_id),
                ("GMAIL_API_CLIENT_SECRET", config.client_secret),
                ("GMAIL_API_REFRESH_TOKEN", config.refresh_token),
                ("GMAIL_API_EXPECTED_EMAIL or STORES_MAIL_USERNAME", config.expected_email),
            )
            if not value
        ]
        if missing:
            raise GmailApiError(f"Gmail API configuration missing: {', '.join(missing)}")
        return config


class GmailApiClient:
    def __init__(self, config: GmailApiConfig, *, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()
        self._access_token: str | None = None
        self._profile_verified = False

    def _refresh_access_token(self) -> str:
        response = self.session.post(
            self.config.token_url,
            data={
                "grant_type": "refresh_token",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "refresh_token": self.config.refresh_token,
            },
            timeout=self.config.timeout_seconds,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise GmailApiError(
                f"Google OAuth returned a non-JSON response (HTTP {response.status_code})"
            ) from exc
        access_token = str(payload.get("access_token") or "").strip()
        if response.status_code >= 400 or not access_token:
            error_name = str(payload.get("error") or "token_refresh_failed")
            raise GmailApiError(
                f"Google OAuth token refresh failed (HTTP {response.status_code}, {error_name})"
            )
        self._access_token = access_token
        return access_token

    def _headers(self) -> dict[str, str]:
        token = self._access_token or self._refresh_access_token()
        return {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    def _get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{self.config.api_base_url}{path}",
            params=params,
            headers=self._headers(),
            timeout=self.config.timeout_seconds,
        )
        if response.status_code == 401:
            self._access_token = None
            response = self.session.get(
                f"{self.config.api_base_url}{path}",
                params=params,
                headers=self._headers(),
                timeout=self.config.timeout_seconds,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise GmailApiError(
                f"Gmail API returned a non-JSON response (HTTP {response.status_code})"
            ) from exc
        if response.status_code >= 400:
            error = payload.get("error") if isinstance(payload, dict) else None
            error_name = (
                str(error.get("status") or error.get("message") or "request_failed")
                if isinstance(error, dict)
                else "request_failed"
            )
            raise GmailApiError(
                f"Gmail API request failed (HTTP {response.status_code}, {error_name})"
            )
        return payload

    def verify_profile(self) -> None:
        if self._profile_verified:
            return
        payload = self._get_json("/users/me/profile")
        actual_email = str(payload.get("emailAddress") or "").strip().lower()
        if actual_email != self.config.expected_email.lower():
            raise GmailApiError("Gmail OAuth account does not match configured mailbox")
        self._profile_verified = True

    def fetch_original_messages(self, *, senders: Iterable[str], limit: int) -> list[bytes]:
        self.verify_profile()
        unique_senders = list(dict.fromkeys(sender.strip() for sender in senders if sender.strip()))
        if not unique_senders:
            raise GmailApiError("At least one sender filter is required for Gmail API sync")
        query = "{" + " ".join(f"from:{sender}" for sender in unique_senders) + "}"
        payload = self._get_json(
            "/users/me/messages",
            params={
                "q": query,
                "maxResults": max(1, min(int(limit), 500)),
                "includeSpamTrash": "false",
            },
        )
        rows = payload.get("messages") or []
        if not isinstance(rows, list):
            raise GmailApiError("Gmail messages response did not contain a list")

        messages: list[bytes] = []
        for row in rows[:limit]:
            message_id = str(row.get("id") or "").strip() if isinstance(row, dict) else ""
            if not message_id:
                continue
            message = self._get_json(
                f"/users/me/messages/{message_id}", params={"format": "raw"}
            )
            raw = str(message.get("raw") or "")
            if not raw:
                raise GmailApiError("Gmail raw message response was missing content")
            try:
                messages.append(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
            except (ValueError, TypeError) as exc:
                raise GmailApiError("Gmail raw message content was invalid") from exc
        return messages


def fetch_order_messages(*, senders: Iterable[str], limit: int) -> list[bytes]:
    """Fetch matching order emails without changing labels or read/unread state."""
    return GmailApiClient(GmailApiConfig.from_env()).fetch_original_messages(
        senders=senders,
        limit=limit,
    )
