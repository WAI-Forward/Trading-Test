"""Utilities for retrieving market data from the cTrader Open API."""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableMapping

import requests

__all__ = [
    "CTraderMarketDataError",
    "OHLCBar",
    "fetch_ohlc_data",
]


# Default REST base URL for the public cTrader Connect Open API.
_DEFAULT_REST_BASE_URL = "https://api.spotware.com/connect/openapi/trading/v3"

# cTrader names its OHLC timeframes as strings such as "M1", "H1", etc.  The API is
# case-sensitive so we normalise user input to upper-case and validate it against this
# allow-list.  Additional granularities can be added here as needed.
_VALID_TIMEFRAMES = {
    "M1",
    "M5",
    "M15",
    "M30",
    "H1",
    "H4",
    "D1",
    "W1",
    "MN1",
}


class CTraderMarketDataError(RuntimeError):
    """Raised when cTrader returns an error for a market-data request."""


@dataclass(slots=True)
class OHLCBar:
    """Represents a single OHLCV candle returned by cTrader."""

    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None


def fetch_ohlc_data(
    *,
    access_token: str,
    ctid_trader_account_id: int,
    symbol_name: str,
    symbol_id: int | None = None,
    timeframe: str = "M1",
    limit: int = 100,
    start_time: _dt.datetime | None = None,
    end_time: _dt.datetime | None = None,
    base_url: str | None = None,
    request_timeout: float = 10.0,
) -> list[OHLCBar]:
    """Fetch OHLC candles for ``symbol_name`` using the cTrader REST API.

    Args:
        access_token: OAuth access token obtained via the cTrader integration.
        ctid_trader_account_id: The trader account identifier that owns ``symbol_name``.
        symbol_name: Human-friendly symbol name (e.g. ``"EURUSD"``).  Used to resolve
            the numeric ``symbol_id`` when it is not supplied explicitly.
        symbol_id: Optional numeric cTrader symbol identifier.  Providing this skips a
            symbol lookup.
        timeframe: Requested timeframe (``"M1"``, ``"H1"``, ...).
        limit: Maximum number of candles to return.  Constrained between 1 and 500 by
            the function.
        start_time: Optional inclusive lower bound as a timezone-aware ``datetime``.
        end_time: Optional inclusive upper bound as a timezone-aware ``datetime``.
        base_url: Override of the REST base URL.  Defaults to the production
            Spotware endpoint.
        request_timeout: HTTP timeout in seconds for each API call.

    Returns:
        A list of :class:`OHLCBar` objects ordered chronologically (oldest first).

    Raises:
        ValueError: If inputs are invalid.
        CTraderMarketDataError: If the cTrader API responds with an error payload.
    """

    if not access_token:
        raise ValueError("An OAuth access token is required to query cTrader.")

    if limit <= 0:
        raise ValueError("The 'limit' parameter must be greater than zero.")

    if limit > 500:
        # The public REST API caps the response size to keep payloads reasonable.
        limit = 500

    normalised_timeframe = _normalise_timeframe(timeframe)

    api_base_url = base_url or _DEFAULT_REST_BASE_URL

    if symbol_id is None:
        symbol_id = _resolve_symbol_id(
            base_url=api_base_url,
            access_token=access_token,
            ctid_trader_account_id=ctid_trader_account_id,
            symbol_name=symbol_name,
            timeout=request_timeout,
        )

    payload: dict[str, object] = {
        "ctidTraderAccountId": int(ctid_trader_account_id),
        "symbolId": int(symbol_id),
        "period": normalised_timeframe,
        "num": int(limit),
    }

    if start_time is not None:
        payload["fromTimestamp"] = _to_epoch_millis(start_time)

    if end_time is not None:
        payload["toTimestamp"] = _to_epoch_millis(end_time)

    response_payload = _call_ctrader_endpoint(
        api_base_url,
        "GetTrendbars",
        payload,
        access_token,
        timeout=request_timeout,
    )

    trendbars = _extract_trendbars(response_payload)

    return [_build_ohlc_bar(raw_bar) for raw_bar in trendbars]


def _normalise_timeframe(timeframe: str) -> str:
    if not timeframe:
        raise ValueError("A timeframe must be provided when requesting OHLC data.")

    normalised = timeframe.upper()

    if normalised not in _VALID_TIMEFRAMES:
        raise ValueError(
            "Unsupported timeframe '{0}'. Valid values: {1}".format(
                timeframe, ", ".join(sorted(_VALID_TIMEFRAMES))
            )
        )

    return normalised


def _resolve_symbol_id(
    *,
    base_url: str,
    access_token: str,
    ctid_trader_account_id: int,
    symbol_name: str,
    timeout: float,
) -> int:
    lookup_payload = {
        "ctidTraderAccountId": int(ctid_trader_account_id),
        "symbolName": symbol_name,
    }

    response_payload = _call_ctrader_endpoint(
        base_url,
        "GetSymbolByName",
        lookup_payload,
        access_token,
        timeout=timeout,
    )

    symbol_info = response_payload.get("symbol")
    if not isinstance(symbol_info, Mapping):
        raise CTraderMarketDataError(
            "Unexpected response when resolving symbol '{0}'.".format(symbol_name)
        )

    try:
        resolved_id = int(symbol_info["symbolId"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CTraderMarketDataError(
            "cTrader response missing 'symbolId' when resolving symbol '{0}'.".format(
                symbol_name
            )
        ) from exc

    return resolved_id


def _call_ctrader_endpoint(
    base_url: str,
    endpoint: str,
    payload: Mapping[str, object],
    access_token: str,
    *,
    timeout: float,
) -> MutableMapping[str, Any]:
    url = _join_url(base_url, endpoint)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=dict(payload), headers=headers, timeout=timeout)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - requires live HTTP call
        raise CTraderMarketDataError(
            "cTrader API call to '{0}' failed with status {1}: {2}".format(
                endpoint, response.status_code, response.text
            )
        ) from exc

    try:
        parsed = response.json()
    except ValueError as exc:  # pragma: no cover - requires invalid remote response
        raise CTraderMarketDataError(
            "Unable to parse JSON response from cTrader endpoint '{0}'.".format(
                endpoint
            )
        ) from exc

    if isinstance(parsed, Mapping) and "error" in parsed:
        error_payload = parsed.get("error")
        raise CTraderMarketDataError(
            "cTrader returned an error for '{0}': {1}".format(endpoint, error_payload)
        )

    if not isinstance(parsed, MutableMapping):
        raise CTraderMarketDataError(
            "Unexpected payload structure returned by cTrader endpoint '{0}'.".format(
                endpoint
            )
        )

    return parsed


def _extract_trendbars(payload: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    # The REST API may return either ``trendbars`` or ``trendbarList`` depending on the
    # environment.  We support both to increase compatibility.
    for key in ("trendbars", "trendbarList", "trendbar"):
        candidate = payload.get(key)
        if isinstance(candidate, Iterable) and not isinstance(candidate, (str, bytes)):
            return candidate

    raise CTraderMarketDataError("cTrader response did not include any trendbars.")


def _build_ohlc_bar(raw_bar: Mapping[str, Any]) -> OHLCBar:
    try:
        timestamp_ms = int(raw_bar["utcTimestamp"])
        open_price = float(raw_bar["open"])
        high_price = float(raw_bar["high"])
        low_price = float(raw_bar["low"])
        close_price = float(raw_bar["close"])
    except (KeyError, TypeError, ValueError) as exc:
        raise CTraderMarketDataError(
            "cTrader trendbar payload missing required OHLC fields."
        ) from exc

    volume = raw_bar.get("volume")
    volume_value = float(volume) if volume is not None else None

    timestamp_iso = _format_timestamp(timestamp_ms)

    return OHLCBar(
        timestamp=timestamp_iso,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=volume_value,
    )


def _join_url(base_url: str, endpoint: str) -> str:
    trimmed_base = base_url.rstrip("/")
    trimmed_endpoint = endpoint.lstrip("/")
    return f"{trimmed_base}/{trimmed_endpoint}"


def _to_epoch_millis(dt_value: _dt.datetime) -> int:
    if dt_value.tzinfo is None:
        raise ValueError("Timestamps must be timezone-aware and in UTC.")

    return int(dt_value.timestamp() * 1000)


def _format_timestamp(epoch_ms: int) -> str:
    return _dt.datetime.fromtimestamp(epoch_ms / 1000, tz=_dt.timezone.utc).isoformat()

