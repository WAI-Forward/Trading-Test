"""Shared models and helpers for interacting with the cTrader market-data APIs."""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

__all__ = [
    "CTraderMarketDataError",
    "OHLCBar",
    "build_auth_headers",
    "build_bar",
    "format_timestamp",
    "ensure_iterable",
    "looks_like_trendbar",
]

logger = logging.getLogger(__name__)


class CTraderMarketDataError(RuntimeError):
    """Raised when cTrader returns an error for a market-data request."""


@dataclass(slots=True)
class OHLCBar:
    """Representation of an OHLC (trendbar) datapoint returned by cTrader."""

    timestamp: _dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


def build_bar(raw: Mapping[str, Any]) -> OHLCBar:
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

    timestamp = parse_timestamp(timestamp_value)
    open_ = _parse_float(raw, "open") or _parse_float(raw, "openPrice")
    high = _parse_float(raw, "high") or _parse_float(raw, "highPrice")
    low = _parse_float(raw, "low") or _parse_float(raw, "lowPrice")
    close = _parse_float(raw, "close") or _parse_float(raw, "closePrice")

    if None in (open_, high, low, close):
        raise CTraderMarketDataError("Trendbar payload missing OHLC fields.")

    volume = _parse_float(raw, "volume")
    bar = OHLCBar(timestamp=timestamp, open=open_, high=high, low=low, close=close, volume=volume)
    logger.debug("Constructed OHLCBar from payload: %s", bar)
    return bar


def parse_timestamp(value: Any) -> _dt.datetime:
    """Parse a timestamp value supplied by the API."""

    if isinstance(value, (int, float)):
        logger.debug("Parsing numeric timestamp value: %s", value)
        if value > 1e12:
            value /= 1000.0
        return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc)

    if isinstance(value, str):
        text = value.strip()
        logger.debug("Parsing string timestamp value: %s", text)
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


def format_timestamp(ts: _dt.datetime) -> str:
    """Format a timestamp suitable for subscription requests."""

    return ts.astimezone(_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def looks_like_trendbar(candidate: Mapping[str, Any]) -> bool:
    """Return ``True`` if a mapping resembles a trendbar payload."""

    keys = set(candidate.keys())
    required = {"open", "high", "low", "close"}
    alt_required = {"openPrice", "highPrice", "lowPrice", "closePrice"}
    result = bool((required <= keys) or (alt_required <= keys))
    logger.debug("Payload %s looks like trendbar: %s", candidate, result)
    return result


def ensure_iterable(value: Any) -> Iterable[Mapping[str, Any]]:
    """Return an iterable of mappings from a payload field."""

    if value is None:
        return []
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Iterable):  # type: ignore[arg-type]
        return [item for item in value if isinstance(item, Mapping)]
    return []


def build_auth_headers(access_token: str) -> dict[str, str]:
    """Return the HTTP headers required to authenticate with cTrader."""

    return {"Authorization": f"Bearer {access_token}"}


def _parse_float(mapping: Mapping[str, Any], key: str) -> float | None:
    value = mapping.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        logger.debug("Unable to parse float for key %s from payload %s", key, mapping)
        return None


if __name__ == "__main__":  # pragma: no cover - diagnostic helper
    logging.basicConfig(level=logging.DEBUG)
    sample_payload = {
        "timestamp": "2024-01-01T00:00:00Z",
        "open": 1.0,
        "high": 1.1,
        "low": 0.9,
        "close": 1.05,
        "volume": 1000,
    }
    print("Parsed OHLC bar:", build_bar(sample_payload))
