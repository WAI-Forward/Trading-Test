"""Route for initiating the cTrader OAuth login flow."""
from __future__ import annotations

from urllib.parse import urlencode

from flask import Response, redirect, request, url_for

from src.app import app
from src.app.credentials import load_ctrader_credentials


_CTRADER_AUTHORIZE_URL = "https://connect.spotware.com/apps/authorize"


@app.route("/login", methods=["GET"])
def ctrader_login() -> Response:
    """Redirect the user to the cTrader OAuth consent screen.

    Optional ``scope`` and ``state`` query parameters provided to the login
    endpoint are forwarded to cTrader so integrators can request specific
    permissions and maintain request state.
    """

    credentials = load_ctrader_credentials()
    redirect_uri = url_for("ctrader_redirect", _external=True)

    query_params: dict[str, str] = {
        "response_type": "code",
        "client_id": credentials["client_id"],
        "redirect_uri": redirect_uri,
    }

    scope = request.args.get("scope")
    if scope:
        query_params["scope"] = scope

    state = request.args.get("state")
    if state:
        query_params["state"] = state

    authorize_url = f"{_CTRADER_AUTHORIZE_URL}?{urlencode(query_params)}"

    return redirect(authorize_url)
