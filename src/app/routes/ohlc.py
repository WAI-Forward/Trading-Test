# src/app/routes/ohlc.py v2 (WebSocket-compatible)
"""Routes for retrieving OHLC data through the cTrader integration (WebSocket-based)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

from flask import Response, jsonify, request, stream_with_context

from src.app import app
from src.app.services.ctrader_market_data import (  # updated import
    CTraderMarketDataError,
    OHLCBar,
    fetch_ohlc_data,
)

_DEFAULT_MARKET_DATA_TIMEOUT = 10.0


@app.route("/ohlc/<string:ticker>", methods=["GET"])
def get_ohlc(ticker: str) -> tuple[Response, int]:
    """Return OHLC candles for ``ticker`` using the cTrader WebSocket API."""
    access_token = request.args.get("access_token")
    account_id = request.args.get("account_id", type=int)
    timeframe = request.args.get("timeframe", default="M1")
    limit = request.args.get("limit", default=100, type=int)

    if access_token is None:
        return jsonify({"error": "Missing required 'access_token' query parameter."}), 400
    if account_id is None:
        return jsonify({"error": "Missing required 'account_id' query parameter."}), 400

    try:
        bars = fetch_ohlc_data(
            access_token=access_token,
            account_id=account_id,
            symbol=ticker,
            timeframe=timeframe,
            limit=limit,
        )
    except (ValueError, CTraderMarketDataError) as exc:
        return jsonify({"error": str(exc)}), 502

    payload: dict[str, Any] = {
        "ticker": ticker,
        "timeframe": timeframe,
        "bars": [_serialise_bar(bar) for bar in bars],
    }

    return jsonify(payload), 200


@app.route("/ohlc-stream/<string:ticker>", methods=["GET"])
def stream_ohlc(ticker: str) -> Response | tuple[Response, int]:
    """Stream OHLC candles for ``ticker`` using server-sent events (SSE)."""
    access_token = request.args.get("access_token")
    account_id = request.args.get("account_id", type=int)
    timeframe = request.args.get("timeframe", default="M1")
    limit = request.args.get("limit", default=100, type=int)

    if access_token is None:
        return jsonify({"error": "Missing required 'access_token' query parameter."}), 400
    if account_id is None:
        return jsonify({"error": "Missing required 'account_id' query parameter."}), 400

    def _event_stream() -> Iterable[str]:
        try:
            bars = fetch_ohlc_data(
                access_token=access_token,
                account_id=account_id,
                symbol=ticker,
                timeframe=timeframe,
                limit=limit,
            )
        except (ValueError, CTraderMarketDataError) as exc:
            yield _sse_event("error", {"message": str(exc)})
            return

        yield _sse_event("meta", {"ticker": ticker, "timeframe": timeframe, "count": len(bars)})
        for bar in bars:
            yield _sse_event("bar", _serialise_bar(bar))
        yield _sse_event("complete", {"ticker": ticker, "count": len(bars)})

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }

    return Response(
        stream_with_context(_event_stream()),
        mimetype="text/event-stream",
        headers=headers,
    )


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


def _sse_event(event: str, data: Any) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n"


__all__ = ["get_ohlc", "stream_ohlc"]
