from src.app import app


@app.route("/")
def index() -> tuple[dict[str, str], int]:
    """Return a JSON greeting for the root endpoint."""
    return {"message": "Welcome to the Trading-Test Flask app!"}, 200
