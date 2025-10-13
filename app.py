"""A minimal Flask application."""
from __future__ import annotations

from flask import Flask

app = Flask(__name__)


@app.route("/")
def index() -> tuple[dict[str, str], int]:
    """Return a JSON greeting for the root endpoint."""
    return {"message": "Welcome to the Trading-Test Flask app!"}, 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
