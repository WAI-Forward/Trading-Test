"""A minimal Flask application."""
from __future__ import annotations

from flask import Flask

app = Flask(__name__)

# Import route modules to ensure their handlers are registered with the Flask app.
from src.app.routes import index as _index  # noqa: E402,F401
from src.app.routes import login as _login  # noqa: E402,F401
from src.app.routes import redirect as _redirect  # noqa: E402,F401


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8888)
