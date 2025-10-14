"""Route for handling cTrader OAuth redirects."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from urllib.parse import urlencode

from flask import Request, Response, redirect, request, url_for

from src.app import app
from src.app.credentials import load_ctrader_credentials
from src.app.services.ctrader_accounts import (
    CTraderAccountError,
    fetch_trading_accounts,
)

# NOTE:
# Historically the token endpoint lived under ``/connect/oauth/token``.  Spotware
# have since consolidated the OAuth flows under ``https://connect.spotware.com``
# which serves ``/apps/token`` for exchanging authorisation codes.  The old URL
# now returns ``404`` which in turn bubbled up to the UI as a fatal error.  Using
# the documented endpoint keeps both sandbox and live environments working.
_DEFAULT_TOKEN_URL = "https://connect.spotware.com/apps/token"
_TOKEN_STORAGE_KEY = "ctrader_access_token"
_EXPIRY_STORAGE_KEY = "ctrader_access_token_expires_at"
_ACCOUNT_STORAGE_KEY = "ctrader_account_id"
_TOKEN_REQUEST_TIMEOUT = 10.0
_ACCOUNT_REQUEST_TIMEOUT = 10.0


class CTraderOAuthError(RuntimeError):
    """Raised when the cTrader OAuth token exchange fails."""


@app.route("/redirect", methods=["GET"])
def ctrader_redirect() -> Response:
    """Handle the cTrader redirect callback and persist the access token.

    Once cTrader redirects the user back to the application with an
    authorisation ``code`` the handler exchanges it for an access token and
    finally redirects the browser back to the index page with the token details
    encoded as query string parameters so the dashboard can persist them in
    ``localStorage``.
    """

    error = request.args.get("error")
    state = request.args.get("state")

    if error is not None:
        description = request.args.get("error_description")
        error_message = _format_oauth_error(error, description)
        return _redirect_to_index(error=error_message, state=state)

    code = request.args.get("code")

    if not code:
        return _redirect_to_index(
            error="Missing 'code' query parameter in OAuth callback.",
            state=state,
        )

    redirect_uri = url_for("ctrader_redirect", _external=True)

    try:
        token_payload = _exchange_authorisation_code(request, code, redirect_uri)
    except CTraderOAuthError as exc:
        return _redirect_to_index(error=str(exc), state=state)

    access_token = token_payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        return _redirect_to_index(
            error="cTrader token response did not include an access token.",
            state=state,
        )

    expires_in = token_payload.get("expires_in")
    expires_at = _compute_expiry_timestamp(expires_in)

    account_id: int | None = None
    account_error: str | None = None

    try:
        account_id = _fetch_primary_account(access_token)
    except CTraderOAuthError as exc:
        account_error = str(exc)
        account_id = ""

    return _redirect_to_index(
        access_token=access_token,
        expires_at=expires_at,
        account_id=account_id,
        state=state,
        error=account_error,
    )


def _exchange_authorisation_code(
    flask_request: Request, code: str, redirect_uri: str
) -> dict[str, Any]:
    credentials = load_ctrader_credentials()

    token_url = flask_request.args.get("token_url") or app.config.get(
        "CTRADER_TOKEN_URL", _DEFAULT_TOKEN_URL
    )
    timeout = app.config.get("CTRADER_TOKEN_TIMEOUT", _TOKEN_REQUEST_TIMEOUT)

    form_data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": credentials["client_id"],
        "client_secret": credentials["secret"],
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(
            token_url, data=form_data, headers=headers, timeout=timeout
        )
    except requests.RequestException as exc:  # pragma: no cover - network failure
        raise CTraderOAuthError("Unable to contact cTrader token endpoint.") from exc

    if response.status_code >= 400:
        raise CTraderOAuthError(
            "cTrader token endpoint responded with status {0}: {1}".format(
                response.status_code, response.text
            )
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise CTraderOAuthError("Unable to parse cTrader token response.") from exc

    if not isinstance(payload, dict):
        raise CTraderOAuthError("Unexpected payload structure from token endpoint.")

    if payload.get("error"):
        description = payload.get("error_description")
        raise CTraderOAuthError(
            _format_oauth_error(str(payload["error"]), description)
        )

    return payload


def _compute_expiry_timestamp(expires_in: Any) -> str | None:
    try:
        expires_seconds = int(expires_in)
    except (TypeError, ValueError):
        return None

    expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expires_seconds)
    return expiry_time.isoformat()


def _redirect_to_index(
    *,
    access_token: str | None = None,
    expires_at: str | None = None,
    account_id: int | str | None = None,
    state: str | None = None,
    error: str | None = None,
) -> Response:
    params: dict[str, str] = {}

    if access_token:
        params[_TOKEN_STORAGE_KEY] = access_token

    if expires_at:
        params[_EXPIRY_STORAGE_KEY] = expires_at
    elif expires_at is not None:
        params[_EXPIRY_STORAGE_KEY] = ""

    if account_id:
        params[_ACCOUNT_STORAGE_KEY] = str(account_id)
    elif account_id is not None:
        params[_ACCOUNT_STORAGE_KEY] = ""

    if state:
        params["state"] = state

    if error:
        params["error"] = error

    base_url = url_for("index")
    location = base_url

    if params:
        query_string = urlencode(params)
        location = f"{base_url}?{query_string}"

    return redirect(location)


def _format_oauth_error(error: str, description: str | None) -> str:
    if description:
        return f"{error}: {description}"
    return error


def _fetch_primary_account(access_token: str) -> int:
    try:
        accounts = fetch_trading_accounts(
            access_token=access_token,
            base_url=app.config.get("CTRADER_REST_BASE_URL"),
            request_timeout=app.config.get(
                "CTRADER_ACCOUNT_TIMEOUT", _ACCOUNT_REQUEST_TIMEOUT
            ),
        )
    except (ValueError, CTraderAccountError) as exc:
        raise CTraderOAuthError(
            "Access token retrieved but trading account lookup failed: {0}".format(exc)
        ) from exc

    if not accounts:
        raise CTraderOAuthError(
            "Access token retrieved but no trading accounts were returned."
        )

    return accounts[0].ctid_trader_account_id
