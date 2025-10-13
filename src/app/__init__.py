"""Application package for the Trading-Test Flask service."""
from __future__ import annotations

from flask import Flask

# Create the shared Flask application instance. Importing ``app`` from this
# module will always return the same object regardless of how the process was
# started (``python -m``, ``python`` file execution, ``flask run`` etc.).
app = Flask(__name__)


def _register_routes() -> None:
    """Import route modules so their view functions are registered.

    Importing within the function avoids circular-import issues while ensuring
    the decorators in each module attach to the single global :data:`app`
    instance defined above.
    """

    from src.app.routes import index as _index  # noqa: F401
    from src.app.routes import login as _login  # noqa: F401
    from src.app.routes import redirect as _redirect  # noqa: F401


_register_routes()

__all__ = ["app"]
