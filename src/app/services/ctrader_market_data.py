"""Modern cTrader market-data helpers based on the Spotware Connect Open API v3."""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging
import ssl
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Iterable, Mapping

import requests
import websockets
from websockets.client import WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed

__all__ = ["CTraderMarketDataError", "OHLCBar", "fetch_ohlc_data", "stream_ohlc_data"]

logger = logging.getLogger(__name__)

_REST_BASE_URL = "https://api.spotware.com/connect/openapi/trading/v3"
_WS_URL = "wss://api.spotware.com/connect/openapi/trading/v3/ws"
_DEFAULT_TIMEOUT = 10.0
_MAX_RECONNECT_DELAY = 30.0


class CTraderMarketDataError(RuntimeError):
    """Raised when cTrader returns an error for a market-data request."""


@dataclass
class OHLCBar:
    """Representation of an OHLC (trendbar) datapoint returned by cTrader."""

    timestamp: _dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


def fetch_ohlc_data(
    access_token: str,
    account_id: int,
    symbol: str,
    timeframe: str = "M1",
    limit: int = 100,
) -> list[OHLCBar]:
    """Return the latest OHLC bars for ``symbol`` using the REST trading API."""

    if not access_token:
        raise ValueError("An OAuth access token is required to query cTrader.")
    if not symbol:
        raise ValueError("A symbol must be supplied when requesting OHLC data.")
    if limit <= 0:
        raise ValueError("The number of trendbars requested must be positive.")

    timeframe_code = timeframe.upper()
    url = (
        f"{_REST_BASE_URL}/accounts/{int(account_id)}/symbols/{symbol}/trendbars"
    )
    params = {"timeframe": timeframe_code, "limit": limit}
    headers = _build_auth_headers(access_token)

    logger.info(
        "Requesting %s trendbars for %s (account %s) via REST",
        timeframe_code,
        symbol,
        account_id,
    )

    try:
        response = requests.get(url, params=params, headers=headers, timeout=_DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        raise CTraderMarketDataError("Unable to contact the cTrader trendbars endpoint.") from exc

    if response.status_code >= 400:
        raise CTraderMarketDataError(
            f"cTrader API call failed with status {response.status_code}: {response.text}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise CTraderMarketDataError("Unable to parse JSON response from cTrader.") from exc

    raw_bars = _extract_trendbars(payload)
    bars = [_build_bar(obj) for obj in raw_bars]
    bars.sort(key=lambda bar: bar.timestamp)
    return bars[-limit:]


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

    last_timestamp: _dt.datetime | None = None

    # Fetch initial history in a worker thread so we do not block the event loop.
    initial_bars = await asyncio.to_thread(
        fetch_ohlc_data,
        access_token,
        account_id,
        symbol,
        timeframe_code,
        100,
    )
    for bar in initial_bars:
        last_timestamp = bar.timestamp
        yield bar

    reconnect_delay = 1.0
    ssl_context = ssl.create_default_context()

    while True:
        ws_url = f"{_WS_URL}?access_token={access_token}"
        try:
            async with websockets.connect(
                ws_url,
                extra_headers=_build_auth_headers(access_token),
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
                        continue
                    last_timestamp = bar.timestamp
                    yield bar
        except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
            logger.warning("WebSocket connection lost (%s). Reconnecting...", exc)
        except CTraderMarketDataError:
            logger.exception("Failed to parse trendbar message from cTrader; reconnecting")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, _MAX_RECONNECT_DELAY)


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

    subscribe_payload: dict[str, Any] = {
        "type": "subscribeTrendbars",
        "payload": {
            "accountId": int(account_id),
            "symbol": symbol,
            "timeframe": timeframe,
        },
    }
    if last_timestamp:
        subscribe_payload["payload"]["from"] = _format_timestamp(last_timestamp)
    await _send_json(ws, subscribe_payload)


async def _consume_trendbar_messages(
    ws: WebSocketClientProtocol, symbol: str
) -> AsyncGenerator[OHLCBar, None]:
    """Yield OHLC bars parsed from the raw WebSocket stream."""

    async for raw in ws:
        logger.debug("Received WebSocket message: %s", raw)
        message = _decode_message(raw)

        msg_type = message.get("type")
        if msg_type in {"ping", "heartbeat"}:
            await _respond_to_ping(ws, message)
            continue
        if msg_type in {"authenticated", "subscriptionConfirmed", "info"}:
            continue

        payload = message.get("payload")
        if payload is None:
            payload = message.get("data")
        if payload is None:
            logger.debug("Ignoring message without payload: %s", message)
            continue

        bars_payload: Iterable[Mapping[str, Any]]
        if isinstance(payload, Mapping) and "trendbars" in payload:
            bars_payload = _ensure_iterable(payload.get("trendbars"))
        elif isinstance(payload, Mapping) and "trendbar" in payload:
            bars_payload = _ensure_iterable(payload.get("trendbar"))
        elif isinstance(payload, Mapping) and _looks_like_trendbar(payload):
            bars_payload = [payload]
        else:
            logger.debug("Ignoring non-trendbar payload: %s", payload)
            continue

        for bar_payload in bars_payload:
            bar = _build_bar(bar_payload)
            yield bar


async def _respond_to_ping(ws: WebSocketClientProtocol, message: Mapping[str, Any]) -> None:
    """Send a pong response for JSON ping/heartbeat frames."""

    response = {"type": "pong"}
    if "payload" in message:
        response["payload"] = message["payload"]
    await _send_json(ws, response)


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


def _extract_trendbars(payload: Mapping[str, Any] | Iterable[Any]) -> list[Mapping[str, Any]]:
    """Extract a list of trendbar dictionaries from the REST payload."""

    if isinstance(payload, Mapping):
        for key in ("data", "trendbars", "bars", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, Mapping)]
        if isinstance(payload.get("trendbar"), list):
            return [item for item in payload["trendbar"] if isinstance(item, Mapping)]
        if _looks_like_trendbar(payload):
            return [payload]
    elif isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    raise CTraderMarketDataError("cTrader response did not contain any trendbars.")


def _build_bar(raw: Mapping[str, Any]) -> OHLCBar:
    """Convert a trendbar mapping into an :class:`OHLCBar`."""

    timestamp_value = (
        raw.get("timestamp")
        or raw.get("time")
        or raw.get("openTimestamp")
        or raw.get("startTimestamp")
        or raw.get("utcTimestamp")
    )
    if timestamp_value is None:
        raise CTraderMarketDataError("Trendbar payload missing timestamp field.")

    timestamp = _parse_timestamp(timestamp_value)
    open_ = _parse_float(raw, "open") or _parse_float(raw, "openPrice")
    high = _parse_float(raw, "high") or _parse_float(raw, "highPrice")
    low = _parse_float(raw, "low") or _parse_float(raw, "lowPrice")
    close = _parse_float(raw, "close") or _parse_float(raw, "closePrice")

    if None in (open_, high, low, close):
        raise CTraderMarketDataError("Trendbar payload missing OHLC fields.")

    volume = _parse_float(raw, "volume")
    return OHLCBar(timestamp=timestamp, open=open_, high=high, low=low, close=close, volume=volume)


def _parse_timestamp(value: Any) -> _dt.datetime:
    """Parse a timestamp value supplied by the API."""

    if isinstance(value, (int, float)):
        # Spotware timestamps are often in milliseconds since epoch.
        if value > 1e12:
            value /= 1000.0
        return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc)

    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = _dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise CTraderMarketDataError(f"Invalid timestamp format received: {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt

    raise CTraderMarketDataError(f"Unsupported timestamp value: {value!r}")


def _parse_float(mapping: Mapping[str, Any], key: str) -> float | None:
    value = mapping.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_timestamp(ts: _dt.datetime) -> str:
    return ts.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _looks_like_trendbar(candidate: Mapping[str, Any]) -> bool:
    keys = set(candidate.keys())
    required = {"open", "high", "low", "close"}
    alt_required = {"openPrice", "highPrice", "lowPrice", "closePrice"}
    return bool((required <= keys) or (alt_required <= keys))


def _ensure_iterable(value: Any) -> Iterable[Mapping[str, Any]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Iterable):  # type: ignore[arg-type]
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _build_auth_headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}
