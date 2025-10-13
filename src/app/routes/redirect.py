"""Route for handling cTrader OAuth redirects."""
from __future__ import annotations

from flask import request

from src.app import app
from src.app.credentials import load_ctrader_credentials


@app.route("/redirect", methods=["GET"])
def ctrader_redirect() -> tuple[dict[str, object], int]:
    """Handle the cTrader redirect callback.

    The handler loads the configured client identifier so that the integration can
    verify the correct configuration and echoes the OAuth parameters supplied by
    cTrader for debugging purposes.
    """

    credentials = load_ctrader_credentials()

    code = request.args.get("code")
    state = request.args.get("state")

    if not code:
        return {"error": "Missing 'code' query parameter."}, 400

    response_payload: dict[str, object] = {
        "message": "cTrader redirect received.",
        "client_id": credentials["client_id"],
        "code": code,
    }

    if state is not None:
        response_payload["state"] = state

    return response_payload, 200
