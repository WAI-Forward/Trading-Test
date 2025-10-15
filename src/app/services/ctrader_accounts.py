# src/app/services/ctrader_accounts.py v6 (final)
"""Helpers for retrieving trading account information from cTrader (REST-compatible)."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping
import requests

__all__ = ["CTraderAccountError", "TraderAccount", "fetch_trading_accounts"]

_DEFAULT_REST_BASE_URL = "https://api.spotware.com/connect"
_DEFAULT_TIMEOUT = 10.0


class CTraderAccountError(RuntimeError):
    """Raised when the cTrader API returns an error for account lookups."""


@dataclass
class TraderAccount:
    """Minimal representation of a cTrader trading account."""
    ctid_trader_account_id: int
    trader_login: str | None = None
    account_type: str | None = None
    currency: str | None = None
    broker_name: str | None = None
    live: bool | None = None


def fetch_trading_accounts(
    *,
    access_token: str,
    base_url: str | None = None,
    request_timeout: float = _DEFAULT_TIMEOUT,
) -> list[TraderAccount]:
    """Return the trading accounts associated with the given OAuth access token."""
    if not access_token:
        raise ValueError("An OAuth access token is required to query cTrader.")

    api_base_url = base_url or _DEFAULT_REST_BASE_URL
    url = f"{api_base_url.rstrip('/')}/tradingaccounts"
    params = {"oauth_token": access_token}

    try:
        response = requests.get(url, params=params, timeout=request_timeout)
    except requests.RequestException as exc:
        raise CTraderAccountError("Unable to contact the cTrader trading accounts endpoint.") from exc

    if response.status_code >= 400:
        raise CTraderAccountError(
            f"cTrader API call failed with status {response.status_code}: {response.text}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise CTraderAccountError("Unable to parse JSON response from cTrader.") from exc

    raw_accounts = _extract_account_list(data)
    return [_build_trader_account(a) for a in raw_accounts]


def _extract_account_list(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Locate and return the list of accounts from the payload."""
    if not isinstance(payload, Mapping):
        raise CTraderAccountError("Unexpected response structure from cTrader.")

    for key in ("data", "accounts", "tradingAccounts", "traderAccounts", "traderAccountList"):
        candidate = payload.get(key)
        if isinstance(candidate, list):
            return candidate

    if isinstance(payload, list):
        return payload

    raise CTraderAccountError("cTrader response did not include any trading accounts.")


def _build_trader_account(raw: Mapping[str, Any]) -> TraderAccount:
    """Convert a raw API account object into a TraderAccount."""
    account_id = raw.get("accountId") or raw.get("ctidTraderAccountId")
    if account_id is None:
        raise CTraderAccountError("cTrader account payload missing required 'accountId' field.")

    try:
        account_id = int(account_id)
    except (TypeError, ValueError) as exc:
        raise CTraderAccountError(f"Invalid accountId in cTrader payload: {account_id}") from exc

    trader_login = _optional_str(raw.get("accountNumber") or raw.get("login"))
    account_type = _optional_str(raw.get("traderAccountType") or raw.get("accountType"))
    currency = _optional_str(raw.get("depositCurrency") or raw.get("currency"))
    broker_name = _optional_str(raw.get("brokerName"))
    live = raw.get("live", None)

    return TraderAccount(
        ctid_trader_account_id=account_id,
        trader_login=trader_login,
        account_type=account_type,
        currency=currency,
        broker_name=broker_name,
        live=live,
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        text = str(value).strip()
        return text or None
    except Exception:
        return None
