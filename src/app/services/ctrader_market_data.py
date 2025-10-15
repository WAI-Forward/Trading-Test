# src/app/services/market_data.py v2 (REST-compatible)
"""Utilities for retrieving market data from the cTrader Connect REST API."""

from __future__ import annotations
import datetime as _dt
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
import requests

__all__ = ["CTraderMarketDataError", "OHLCBar", "fetch_ohlc_data"]

_DEFAULT_REST_BASE_URL = "https://api.spotware.com/connect"
_VALID_TIMEFRAMES = {"M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"}


class CTraderMarketDataError(RuntimeError):
    """Raised when cTrader returns an error for a market-data request."""


@dataclass
class OHLCBar:
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
    timeframe: str = "M1",
    limit: int = 100,
    start_time: _dt.datetime | None = None,
    end_time: _dt.datetime | None = None,
    base_url: str | None = None,
    request_timeout: float = 10.0,
) -> list[OHLCBar]:
    """Fetch OHLC candles for ``symbol_name`` via cTrader's REST API."""
    if not access_token:
        raise ValueError("An OAuth access token is required to query cTrader.")
    if timeframe.upper() not in _VALID_TIMEFRAMES:
        raise ValueError(f"Unsupported timeframe '{timeframe}'.")
    if limit <= 0:
        raise ValueError("Limit must be positive.")
    if limit > 500:
        limit = 500

    api_base_url = base_url or _DEFAULT_REST_BASE_URL
    url = (
        f"{api_base_url.rstrip('/')}/public/tradingaccounts/"
        f"{ctid_trader_account_id}/symbols/{symbol_name}/trendbars"
    )

    params: dict[str, Any] = {
        "oauth_token": access_token,
        "period": timeframe.upper(),
        "num": limit,
    }

    if start_time is not None:
        params["fromTimestamp"] = _to_epoch_millis(start_time)
    if end_time is not None:
        params["toTimestamp"] = _to_epoch_millis(end_time)

    try:
        response = requests.get(url, params=params, timeout=request_timeout)
    except requests.RequestException as exc:
        raise CTraderMarketDataError("Unable to contact the cTrader OHLC endpoint.") from exc

    if response.status_code >= 400:
        raise CTraderMarketDataError(
            f"cTrader OHLC request failed with {response.status_code}: {response.text}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise CTraderMarketDataError("Unable to parse JSON response from cTrader.") from exc

    trendbars = _extract_trendbars(payload)
    return [_build_ohlc_bar(tb) for tb in trendbars]


def _extract_trendbars(payload: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    for key in ("data", "trendbars", "trendbarList", "trendbar"):
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
        raise CTraderMarketDataError("Invalid trendbar payload.") from exc

    volume = raw_bar.get("volume")
    volume_value = float(volume) if volume is not None else None
    timestamp_iso = _dt.datetime.fromtimestamp(
        timestamp_ms / 1000, tz=_dt.timezone.utc
    ).isoformat()

    return OHLCBar(
        timestamp=timestamp_iso,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=volume_value,
    )


def _to_epoch_millis(dt_value: _dt.datetime) -> int:
    if dt_value.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware.")
    return int(dt_value.timestamp() * 1000)
