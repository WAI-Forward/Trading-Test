"""Route for initiating the cTrader OAuth login flow (SDK-based)."""
from __future__ import annotations

import json
from flask import Response, redirect, url_for
from src.app import app

# Official Spotware SDK
from ctrader_open_api import Auth, EndPoints

CREDENTIALS_PATH = r"C:\Users\44771\PycharmProjects\Trading test\data\ctrader.json"


def load_ctrader_credentials() -> dict[str, str]:
    with open(CREDENTIALS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@app.route("/login", methods=["GET"])
def ctrader_login() -> Response:
    """Redirect the user to the cTrader OAuth consent screen using the SDK."""
    creds = load_ctrader_credentials()
    redirect_uri = url_for("ctrader_redirect", _external=True)

    # Initialize Auth helper (uses production by default)
    auth = Auth(creds["client_id"], creds["secret"], redirect_uri)

    # You can choose environment:
    #   - Production: EndPoints.AUTH_URI  (default)
    #   - Sandbox: EndPoints.SANDBOX_AUTH_URI
    # Uncomment this line if your app is still sandbox-only:
    # base_uri = EndPoints.SANDBOX_AUTH_URI
    # auth_url = auth.getAuthUri(baseUri=base_uri)
    # Otherwise:
    auth_url = auth.getAuthUri(scope="trading")

    print("=== Redirecting to authorization URL ===")
    print(auth_url)

    return redirect(auth_url)
