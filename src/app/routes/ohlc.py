"""Routes for retrieving OHLC data through the cTrader integration."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from flask import Response, jsonify, request

from src.app import app
from src.app.services.ctrader_market_data import (
    CTraderMarketDataError,
    OHLCBar,
    fetch_ohlc_data,
)


@app.route("/ohlc/<string:ticker>", methods=["GET"])
def get_ohlc(ticker: str) -> tuple[Response, int]:
    """Return OHLC candles for ``ticker`` using the cTrader REST API.

    Query Parameters
    ----------------
    access_token:
        OAuth access token obtained via the cTrader login flow (required).
    account_id:
        Numeric ``ctidTraderAccountId`` that owns the requested instrument (required).
    symbol_id:
        Optional cTrader ``symbolId``.  Supplying this skips the symbol lookup.
    timeframe:
        Requested timeframe string (default ``M1``).
    limit:
        Number of candles to return (default 100, max 500).
    start:
        Optional ISO-8601 timestamp specifying the inclusive start of the window.
    end:
        Optional ISO-8601 timestamp specifying the inclusive end of the window.
    base_url:
        Optional override of the cTrader REST base URL.
    timeout:
        Optional HTTP timeout (seconds) for the upstream requests.
    """

    access_token = request.args.get("access_token")
    account_id = request.args.get("account_id", type=int)
    symbol_id = request.args.get("symbol_id", type=int)
    timeframe = request.args.get("timeframe", default="M1")
    limit = request.args.get("limit", default=100, type=int)
    start = request.args.get("start")
    end = request.args.get("end")
    base_url = request.args.get("base_url")
    timeout = request.args.get("timeout", type=float)

    if access_token is None:
        return jsonify({"error": "Missing required 'access_token' query parameter."}), 400

    if account_id is None:
        return jsonify({"error": "Missing required 'account_id' query parameter."}), 400

    try:
        start_dt = _parse_iso8601(start) if start else None
        end_dt = _parse_iso8601(end) if end else None
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    try:
        bars = fetch_ohlc_data(
            access_token=access_token,
            ctid_trader_account_id=account_id,
            symbol_name=ticker,
            symbol_id=symbol_id,
            timeframe=timeframe,
            limit=limit,
            start_time=start_dt,
            end_time=end_dt,
            base_url=base_url,
            request_timeout=timeout if timeout is not None else 10.0,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except CTraderMarketDataError as exc:
        return jsonify({"error": str(exc)}), 502

    payload: dict[str, Any] = {
        "ticker": ticker,
        "timeframe": timeframe,
        "bars": [_serialise_bar(bar) for bar in bars],
    }

    return jsonify(payload), 200


def _parse_iso8601(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Unable to parse ISO-8601 timestamp '{value}'."
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError("Timestamps must include a timezone (expected UTC).")

    return parsed.astimezone(timezone.utc)


def _serialise_bar(bar: OHLCBar) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "timestamp": bar.timestamp,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
    }

    if bar.volume is not None:
        payload["volume"] = bar.volume

    return payload


__all__ = ["get_ohlc"]

