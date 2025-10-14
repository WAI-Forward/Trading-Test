"""Routes for the root landing page of the Trading-Test application."""
from __future__ import annotations

from flask import render_template

from src.app import app


@app.route("/")
def index() -> str:
    """Render the HTML dashboard for launching authentication flows."""
    return render_template("index.html")
