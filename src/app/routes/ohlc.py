"""Routes for retrieving OHLC data through the cTrader integration."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

from flask import Response, jsonify, request, stream_with_context

from src.app import app
from src.app.services.ctrader_market_data import (
    CTraderMarketDataError,
    OHLCBar,
    fetch_ohlc_data,
)


_DEFAULT_MARKET_DATA_TIMEOUT = 10.0


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
            **_build_fetch_request(
                access_token=access_token,
                account_id=account_id,
                ticker=ticker,
                symbol_id=symbol_id,
                timeframe=timeframe,
                limit=limit,
                start=start_dt,
                end=end_dt,
                base_url=base_url,
                timeout=timeout,
            )
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


@app.route("/ohlc-stream/<string:ticker>", methods=["GET"])
def stream_ohlc(ticker: str) -> Response | tuple[Response, int]:
    """Stream OHLC candles for ``ticker`` using server-sent events.

    The query parameters mirror :func:`get_ohlc`.  Successful responses emit
    a ``text/event-stream`` payload containing three types of events:

    ``meta``
        Sent once at the start of the stream describing the request.
    ``bar``
        Emitted for each OHLC candle returned by cTrader.
    ``complete``
        Sent once all candles have been streamed.

    Any upstream error is reported via a terminal ``error`` event.
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

    def _event_stream() -> Iterable[str]:
        try:
            bars = fetch_ohlc_data(
                **_build_fetch_request(
                    access_token=access_token,
                    account_id=account_id,
                    ticker=ticker,
                    symbol_id=symbol_id,
                    timeframe=timeframe,
                    limit=limit,
                    start=start_dt,
                    end=end_dt,
                    base_url=base_url,
                    timeout=timeout,
                )
            )
        except ValueError as exc:
            yield _sse_event("error", {"message": str(exc)})
            return
        except CTraderMarketDataError as exc:
            yield _sse_event("error", {"message": str(exc)})
            return

        yield _sse_event(
            "meta",
            {
                "ticker": ticker,
                "timeframe": timeframe,
                "count": len(bars),
            },
        )

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


def _build_fetch_request(
    *,
    access_token: str,
    account_id: int,
    ticker: str,
    symbol_id: int | None,
    timeframe: str,
    limit: int,
    start: datetime | None,
    end: datetime | None,
    base_url: str | None,
    timeout: float | None,
) -> dict[str, Any]:
    request_timeout = (
        timeout if timeout is not None else _DEFAULT_MARKET_DATA_TIMEOUT
    )

    fetch_kwargs: dict[str, Any] = {
        "access_token": access_token,
        "ctid_trader_account_id": account_id,
        "symbol_name": ticker,
        "timeframe": timeframe,
        "limit": limit,
        "request_timeout": request_timeout,
    }

    if symbol_id is not None:
        fetch_kwargs["symbol_id"] = symbol_id

    if start is not None:
        fetch_kwargs["start_time"] = start

    if end is not None:
        fetch_kwargs["end_time"] = end

    if base_url:
        fetch_kwargs["base_url"] = base_url

    return fetch_kwargs


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

