"""WebSocket helpers for streaming cTrader OHLC data."""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging
import ssl
from collections.abc import AsyncGenerator
from typing import Any, Iterable, Mapping

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed

from .common import (
    CTraderMarketDataError,
    OHLCBar,
    build_auth_headers,
    build_bar,
    ensure_iterable,
    format_timestamp,
    looks_like_trendbar,
)
from .rest import fetch_ohlc_data

__all__ = ["stream_ohlc_data"]

logger = logging.getLogger(__name__)

_WS_URL = "wss://api.spotware.com/connect/openapi/trading/v3/ws"
_DEFAULT_TIMEOUT = 10.0
_MAX_RECONNECT_DELAY = 30.0


async def stream_ohlc_data(
    access_token: str,
    account_id: int,
    symbol: str,
    timeframe: str = "M1",
) -> AsyncGenerator[OHLCBar, None]:
    """Yield OHLC bars in real time using the WebSocket streaming API."""

    timeframe_code = timeframe.upper()
    logger.info(
        "Initialising cTrader OHLC stream for %s (%s) on account %s",
        symbol,
        timeframe_code,
        account_id,
    )

    initial_bars, last_timestamp = await _fetch_initial_history(
        access_token=access_token,
        account_id=account_id,
        symbol=symbol,
        timeframe_code=timeframe_code,
    )

    for bar in initial_bars:
        last_timestamp = bar.timestamp
        logger.debug("Emitting historical bar %s", bar)
        yield bar

    reconnect_delay = 1.0
    ssl_context = ssl.create_default_context()

    while True:
        ws_url = f"{_WS_URL}?access_token={access_token}"
        logger.info("Connecting to cTrader WebSocket at %s", ws_url)
        try:
            async with websockets.connect(
                ws_url,
                extra_headers=build_auth_headers(access_token),
                ssl=ssl_context,
                ping_interval=None,
                close_timeout=_DEFAULT_TIMEOUT,
            ) as ws:
                logger.info("Connected to cTrader WebSocket for %s (%s)", symbol, timeframe_code)
                await _authenticate_and_subscribe(
                    ws,
                    access_token,
                    account_id,
                    symbol,
                    timeframe_code,
                    last_timestamp,
                )
                reconnect_delay = 1.0

                async for bar in _consume_trendbar_messages(ws, symbol):
                    if last_timestamp and bar.timestamp <= last_timestamp:
                        logger.debug(
                            "Skipping bar with timestamp %s because it is not newer than %s",
                            bar.timestamp,
                            last_timestamp,
                        )
                        continue
                    last_timestamp = bar.timestamp
                    yield bar
        except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
            logger.warning("WebSocket connection lost (%s). Reconnecting...", exc)
        except CTraderMarketDataError as exc:
            logger.exception("Failed to parse trendbar message from cTrader; reconnecting")
            logger.debug("Offending exception: %s", exc)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Unexpected error while streaming OHLC data: %s", exc)
        await asyncio.sleep(reconnect_delay)
        logger.info("Reconnecting after %.1f seconds", reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, _MAX_RECONNECT_DELAY)


async def _fetch_initial_history(
    *,
    access_token: str,
    account_id: int,
    symbol: str,
    timeframe_code: str,
) -> tuple[list[OHLCBar], _dt.datetime | None]:
    """Fetch the latest batch of trendbars before streaming begins."""

    logger.info("Fetching initial OHLC history before opening stream")
    initial_bars = await asyncio.to_thread(
        fetch_ohlc_data,
        access_token,
        account_id,
        symbol,
        timeframe_code,
        100,
    )
    logger.info("Retrieved %s historical bars before streaming", len(initial_bars))
    if not initial_bars:
        logger.warning("No historical bars were returned from the REST API before streaming")
    last_timestamp = initial_bars[-1].timestamp if initial_bars else None
    return initial_bars, last_timestamp


async def _authenticate_and_subscribe(
    ws: WebSocketClientProtocol,
    access_token: str,
    account_id: int,
    symbol: str,
    timeframe: str,
    last_timestamp: _dt.datetime | None,
) -> None:
    """Send the authentication and subscription messages over the WebSocket."""

    auth_message = {
        "type": "authenticate",
        "payload": {
            "accessToken": access_token,
            "application": "connect",
        },
    }
    await _send_json(ws, auth_message)
    logger.debug("Authentication message acknowledged for account %s", account_id)

    subscribe_payload: dict[str, Any] = {
        "type": "subscribeTrendbars",
        "payload": {
            "accountId": int(account_id),
            "symbol": symbol,
            "timeframe": timeframe,
        },
    }
    if last_timestamp:
        subscribe_payload["payload"]["from"] = format_timestamp(last_timestamp)
        logger.info("Requesting trendbars from %s", subscribe_payload["payload"]["from"])
    await _send_json(ws, subscribe_payload)
    logger.info("Subscription message sent for %s (%s)", symbol, timeframe)


async def _consume_trendbar_messages(
    ws: WebSocketClientProtocol, symbol: str
) -> AsyncGenerator[OHLCBar, None]:
    """Yield OHLC bars parsed from the raw WebSocket stream."""

    async for raw in ws:
        logger.debug("Received WebSocket message: %s", raw)
        message = _decode_message(raw)
        logger.debug("Decoded message type: %s", message.get("type"))

        msg_type = message.get("type")
        if msg_type in {"ping", "heartbeat"}:
            await _respond_to_ping(ws, message)
            continue
        if msg_type in {"authenticated", "subscriptionConfirmed", "info"}:
            logger.debug("Received control message: %s", msg_type)
            continue

        payload = message.get("payload")
        if payload is None:
            payload = message.get("data")
        if payload is None:
            logger.debug("Ignoring message without payload: %s", message)
            continue

        bars_payload: Iterable[Mapping[str, Any]]
        if isinstance(payload, Mapping) and "trendbars" in payload:
            bars_payload = ensure_iterable(payload.get("trendbars"))
        elif isinstance(payload, Mapping) and "trendbar" in payload:
            bars_payload = ensure_iterable(payload.get("trendbar"))
        elif isinstance(payload, Mapping) and looks_like_trendbar(payload):
            bars_payload = [payload]
        else:
            logger.debug("Ignoring non-trendbar payload: %s", payload)
            continue

        for bar_payload in bars_payload:
            bar = build_bar(bar_payload)
            logger.debug("Yielding trendbar for %s: %s", symbol, bar)
            yield bar


async def _respond_to_ping(ws: WebSocketClientProtocol, message: Mapping[str, Any]) -> None:
    """Send a pong response for JSON ping/heartbeat frames."""

    response = {"type": "pong"}
    if "payload" in message:
        response["payload"] = message["payload"]
    await _send_json(ws, response)
    logger.debug("Sent pong response to ping/heartbeat message")


async def _send_json(ws: WebSocketClientProtocol, message: Mapping[str, Any]) -> None:
    """Serialise and send a JSON payload to the WebSocket."""

    logger.debug("Sending WebSocket message: %s", message)
    await ws.send(json.dumps(message))


def _decode_message(raw: Any) -> Mapping[str, Any]:
    """Decode a raw WebSocket message into a mapping."""

    if isinstance(raw, Mapping):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CTraderMarketDataError("Invalid JSON payload received from cTrader.") from exc
        if isinstance(data, Mapping):
            return data
    raise CTraderMarketDataError("Unexpected message type received from cTrader.")


if __name__ == "__main__":  # pragma: no cover - diagnostic helper
    import argparse

    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Stream OHLC data via the cTrader WebSocket API.")
    parser.add_argument("access_token", help="OAuth access token")
    parser.add_argument("account_id", type=int, help="cTrader account identifier")
    parser.add_argument("symbol", help="Symbol to subscribe to, e.g. EURUSD")
    parser.add_argument("--timeframe", default="M1", help="Trendbar timeframe (default: M1)")
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of initial historical bars to display before streaming",
    )

    args = parser.parse_args()

    async def _runner() -> None:
        bars = []
        async for bar in stream_ohlc_data(
            access_token=args.access_token,
            account_id=args.account_id,
            symbol=args.symbol,
            timeframe=args.timeframe,
        ):
            print(bar)
            bars.append(bar)
            if len(bars) >= args.limit:
                break

    asyncio.run(_runner())
