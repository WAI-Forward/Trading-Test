"""Helpers for retrieving trading account information from cTrader."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableMapping

import requests

__all__ = ["CTraderAccountError", "TraderAccount", "fetch_trading_accounts"]

_DEFAULT_REST_BASE_URL = "https://api.spotware.com/connect/openapi/trading/v3"
_DEFAULT_TIMEOUT = 10.0


class CTraderAccountError(RuntimeError):
    """Raised when the cTrader API returns an error for account lookups."""


@dataclass(slots=True)
class TraderAccount:
    """Minimal representation of a cTrader trading account."""

    ctid_trader_account_id: int
    trader_login: str | None = None
    account_type: str | None = None
    currency: str | None = None


def fetch_trading_accounts(
    *,
    access_token: str,
    base_url: str | None = None,
    request_timeout: float = _DEFAULT_TIMEOUT,
) -> list[TraderAccount]:
    """Return the trading accounts associated with the OAuth ``access_token``.

    Args:
        access_token: OAuth access token obtained from the cTrader login flow.
        base_url: Optional override of the REST base URL.
        request_timeout: Timeout in seconds for the upstream HTTP request.

    Returns:
        A list of :class:`TraderAccount` objects ordered as provided by cTrader.

    Raises:
        ValueError: If ``access_token`` is empty.
        CTraderAccountError: If the upstream request fails or the payload is
            malformed.
    """

    if not access_token:
        raise ValueError("An OAuth access token is required to query cTrader.")

    api_base_url = base_url or _DEFAULT_REST_BASE_URL

    payload = _call_ctrader_endpoint(
        api_base_url,
        "GetTradingAccounts",
        {},
        access_token,
        timeout=request_timeout,
    )

    raw_accounts = _extract_account_list(payload)
    accounts: list[TraderAccount] = []

    for raw_account in raw_accounts:
        accounts.append(_build_trader_account(raw_account))

    return accounts


def _call_ctrader_endpoint(
    base_url: str,
    endpoint: str,
    payload: Mapping[str, object],
    access_token: str,
    *,
    timeout: float,
) -> MutableMapping[str, Any]:
    url = _join_url(base_url, endpoint)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(url, json=dict(payload), headers=headers, timeout=timeout)
    except requests.RequestException as exc:  # pragma: no cover - network failure
        raise CTraderAccountError("Unable to contact the cTrader trading accounts endpoint.") from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - requires live HTTP call
        raise CTraderAccountError(
            "cTrader API call to '{0}' failed with status {1}: {2}".format(
                endpoint, response.status_code, response.text
            )
        ) from exc

    try:
        parsed = response.json()
    except ValueError as exc:  # pragma: no cover - requires invalid remote response
        raise CTraderAccountError(
            "Unable to parse JSON response from cTrader endpoint '{0}'.".format(endpoint)
        ) from exc

    if isinstance(parsed, Mapping) and "error" in parsed:
        error_payload = parsed.get("error")
        raise CTraderAccountError(
            "cTrader returned an error for '{0}': {1}".format(endpoint, error_payload)
        )

    if not isinstance(parsed, MutableMapping):
        raise CTraderAccountError(
            "Unexpected payload structure returned by cTrader endpoint '{0}'.".format(endpoint)
        )

    return parsed


def _extract_account_list(payload: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    for key in ("accounts", "tradingAccounts", "traderAccounts", "traderAccountList"):
        candidate = payload.get(key)
        if isinstance(candidate, Iterable) and not isinstance(candidate, (str, bytes)):
            return candidate

    raise CTraderAccountError("cTrader response did not include any trading accounts.")


def _build_trader_account(raw_account: Mapping[str, Any]) -> TraderAccount:
    try:
        account_id = int(raw_account["ctidTraderAccountId"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CTraderAccountError(
            "cTrader account payload missing required 'ctidTraderAccountId'."
        ) from exc

    trader_login = _optional_str(
        raw_account.get("traderLogin") or raw_account.get("login")
    )
    account_type = _optional_str(
        raw_account.get("accountType") or raw_account.get("type")
    )
    currency = _optional_str(
        raw_account.get("accountCurrency")
        or raw_account.get("depositCurrency")
        or raw_account.get("currency")
    )

    return TraderAccount(
        ctid_trader_account_id=account_id,
        trader_login=trader_login,
        account_type=account_type,
        currency=currency,
    )


def _join_url(base_url: str, endpoint: str) -> str:
    trimmed_base = base_url.rstrip("/")
    trimmed_endpoint = endpoint.lstrip("/")
    return f"{trimmed_base}/{trimmed_endpoint}"


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None

    try:
        text = str(value)
    except Exception:  # pragma: no cover - defensive programming
        return None

    return text if text else None

