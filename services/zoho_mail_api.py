"""Read-only Zoho Mail API client used by the order intake jobs."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests


class ZohoMailApiError(RuntimeError):
    """Raised when Zoho OAuth or Mail API access fails."""


@dataclass(frozen=True)
class ZohoMailConfig:
    client_id: str
    client_secret: str
    refresh_token: str
    account_id: str
    username: str
    accounts_base_url: str
    mail_api_base_url: str
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> "ZohoMailConfig":
        config = cls(
            client_id=(os.getenv("ZOHO_MAIL_CLIENT_ID") or "").strip(),
            client_secret=(os.getenv("ZOHO_MAIL_CLIENT_SECRET") or "").strip(),
            refresh_token=(os.getenv("ZOHO_MAIL_REFRESH_TOKEN") or "").strip(),
            account_id=(os.getenv("ZOHO_MAIL_ACCOUNT_ID") or "").strip(),
            username=(os.getenv("STORES_MAIL_USERNAME") or "").strip(),
            accounts_base_url=(
                os.getenv("ZOHO_ACCOUNTS_BASE_URL") or "https://accounts.zoho.jp"
            ).rstrip("/"),
            mail_api_base_url=(
                os.getenv("ZOHO_MAIL_API_BASE_URL") or "https://mail.zoho.jp/api"
            ).rstrip("/"),
            timeout_seconds=float(os.getenv("ZOHO_MAIL_API_TIMEOUT", "15")),
        )
        missing = [
            name
            for name, value in (
                ("ZOHO_MAIL_CLIENT_ID", config.client_id),
                ("ZOHO_MAIL_CLIENT_SECRET", config.client_secret),
                ("ZOHO_MAIL_REFRESH_TOKEN", config.refresh_token),
            )
            if not value
        ]
        if missing:
            raise ZohoMailApiError(f"Zoho Mail API configuration missing: {', '.join(missing)}")
        if not config.account_id and not config.username:
            raise ZohoMailApiError(
                "ZOHO_MAIL_ACCOUNT_ID or STORES_MAIL_USERNAME must be configured"
            )
        return config


class ZohoMailClient:
    def __init__(self, config: ZohoMailConfig, *, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()
        self._access_token: str | None = None
        self._account_id: str | None = config.account_id or None

    def _refresh_access_token(self) -> str:
        response = self.session.post(
            f"{self.config.accounts_base_url}/oauth/v2/token",
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
            raise ZohoMailApiError(
                f"Zoho OAuth returned a non-JSON response (HTTP {response.status_code})"
            ) from exc
        access_token = str(payload.get("access_token") or "").strip()
        if response.status_code >= 400 or not access_token:
            error_name = str(payload.get("error") or "token_refresh_failed")
            raise ZohoMailApiError(
                f"Zoho OAuth token refresh failed (HTTP {response.status_code}, {error_name})"
            )
        self._access_token = access_token
        return access_token

    def _headers(self) -> dict[str, str]:
        token = self._access_token or self._refresh_access_token()
        return {
            "Accept": "application/json",
            "Authorization": f"Zoho-oauthtoken {token}",
        }

    def _get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        response = self.session.get(
            f"{self.config.mail_api_base_url}{path}",
            params=params,
            headers=self._headers(),
            timeout=self.config.timeout_seconds,
        )
        if response.status_code == 401:
            self._access_token = None
            response = self.session.get(
                f"{self.config.mail_api_base_url}{path}",
                params=params,
                headers=self._headers(),
                timeout=self.config.timeout_seconds,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise ZohoMailApiError(
                f"Zoho Mail API returned a non-JSON response (HTTP {response.status_code})"
            ) from exc
        api_status = payload.get("status") if isinstance(payload, dict) else None
        api_code = api_status.get("code") if isinstance(api_status, dict) else None
        if response.status_code >= 400 or (api_code is not None and int(api_code) >= 400):
            description = (
                str(api_status.get("description") or "request_failed")
                if isinstance(api_status, dict)
                else "request_failed"
            )
            raise ZohoMailApiError(
                f"Zoho Mail API request failed (HTTP {response.status_code}, {description})"
            )
        return payload.get("data") if isinstance(payload, dict) else None

    def account_id(self) -> str:
        if self._account_id:
            return self._account_id
        accounts = self._get_json("/accounts")
        if not isinstance(accounts, list):
            raise ZohoMailApiError("Zoho Mail accounts response did not contain a list")
        username = self.config.username.lower()
        for account in accounts:
            if not isinstance(account, dict):
                continue
            addresses = {
                str(account.get(key) or "").strip().lower()
                for key in ("mailboxAddress", "emailAddress", "accountName", "primaryEmailAddress")
            }
            if username in addresses:
                account_id = str(account.get("accountId") or "").strip()
                if account_id:
                    self._account_id = account_id
                    return account_id
        if len(accounts) == 1 and isinstance(accounts[0], dict):
            account_id = str(accounts[0].get("accountId") or "").strip()
            if account_id:
                self._account_id = account_id
                return account_id
        raise ZohoMailApiError("Could not resolve Zoho Mail account ID for configured username")

    def fetch_original_messages(self, *, senders: Iterable[str], limit: int) -> list[bytes]:
        account_id = self.account_id()
        unique_senders = list(dict.fromkeys(sender.strip() for sender in senders if sender.strip()))
        if not unique_senders:
            raise ZohoMailApiError("At least one sender filter is required for Zoho Mail API sync")
        search_key = "::or:".join(f"sender:{sender}" for sender in unique_senders)
        message_rows = self._get_json(
            f"/accounts/{account_id}/messages/search",
            params={
                "searchKey": search_key,
                "receivedTime": int(time.time() * 1000),
                "start": 1,
                "limit": max(1, min(int(limit), 200)),
                "includeto": "true",
            },
        )
        if message_rows is None:
            return []
        if not isinstance(message_rows, list):
            raise ZohoMailApiError("Zoho Mail search response did not contain a list")
        ordered = sorted(
            (row for row in message_rows if isinstance(row, dict)),
            key=lambda row: int(row.get("receivedtime") or row.get("sentDateInGMT") or 0),
            reverse=True,
        )[:limit]
        messages: list[bytes] = []
        for row in ordered:
            message_id = str(row.get("messageId") or "").strip()
            if not message_id:
                continue
            original = self._get_json(
                f"/accounts/{account_id}/messages/{message_id}/originalmessage"
            )
            if not isinstance(original, dict):
                raise ZohoMailApiError(
                    f"Zoho original-message response missing content for message {message_id}"
                )
            content = original.get("content")
            if not isinstance(content, str):
                raise ZohoMailApiError(
                    f"Zoho original-message content was invalid for message {message_id}"
                )
            messages.append(content.encode("utf-8"))
        return messages


def fetch_order_messages(*, senders: Iterable[str], limit: int) -> list[bytes]:
    """Fetch matching order emails without changing read/unread state."""
    return ZohoMailClient(ZohoMailConfig.from_env()).fetch_original_messages(
        senders=senders,
        limit=limit,
    )
