"""Utilities for loading credential files."""
from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict


class CTraderCredentials(TypedDict):
    """Structure of the cTrader credential file."""

    client_id: str
    secret: str


_DEFAULT_CTRADER_PATH = Path(__file__).resolve().parents[2] / "data" / "ctrader.json"


def load_ctrader_credentials(path: str | Path | None = None) -> CTraderCredentials:
    """Load cTrader credentials from ``data/ctrader.json``.

    Args:
        path: Optional override of the credentials file location.

    Returns:
        The parsed credentials as a typed dictionary.

    Raises:
        FileNotFoundError: If the credential file is missing.
        ValueError: If the credential file cannot be parsed or is missing keys.
    """

    credentials_path = Path(path) if path is not None else _DEFAULT_CTRADER_PATH

    try:
        raw_contents = credentials_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Unable to locate cTrader credentials at '{credentials_path}'."
        ) from None

    try:
        parsed_contents: dict[str, object] = json.loads(raw_contents)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"The credentials file at '{credentials_path}' is not valid JSON."
        ) from exc

    missing_keys = {"client_id", "secret"} - parsed_contents.keys()
    if missing_keys:
        raise ValueError(
            "The credentials file is missing required keys: " + ", ".join(sorted(missing_keys))
        )

    client_id = parsed_contents.get("client_id")
    secret = parsed_contents.get("secret")

    if not isinstance(client_id, str) or not isinstance(secret, str):
        raise ValueError("Credential values must be strings.")

    return CTraderCredentials(client_id=client_id, secret=secret)


__all__ = ["CTraderCredentials", "load_ctrader_credentials"]
