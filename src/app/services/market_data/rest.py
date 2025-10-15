"""REST helpers for retrieving cTrader OHLC data."""
from __future__ import annotations

import logging
from typing import Iterable, Mapping

import requests

from .common import (
    CTraderMarketDataError,
    OHLCBar,
    build_auth_headers,
    build_bar,
    looks_like_trendbar,
)

__all__ = ["fetch_ohlc_data"]

logger = logging.getLogger(__name__)

_REST_BASE_URL = "https://api.spotware.com/connect/openapi/trading/v3"
_DEFAULT_TIMEOUT = 10.0


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
    url = f"{_REST_BASE_URL}/accounts/{int(account_id)}/symbols/{symbol}/trendbars"
    params = {"timeframe": timeframe_code, "limit": limit}
    headers = build_auth_headers(access_token)

    logger.info(
        "Requesting %s trendbars for %s (account %s) via REST",
        timeframe_code,
        symbol,
        account_id,
    )
    logger.debug("REST request details: url=%s params=%s headers=%s", url, params, headers)

    try:
        response = requests.get(url, params=params, headers=headers, timeout=_DEFAULT_TIMEOUT)
    except requests.RequestException as exc:
        logger.exception("Unable to contact the cTrader trendbars endpoint.")
        raise CTraderMarketDataError("Unable to contact the cTrader trendbars endpoint.") from exc

    logger.debug("Received REST response with status %s", response.status_code)
    if response.status_code >= 400:
        logger.error("cTrader API call failed: %s", response.text)
        raise CTraderMarketDataError(
            f"cTrader API call failed with status {response.status_code}: {response.text}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        logger.exception("Unable to parse JSON response from cTrader.")
        raise CTraderMarketDataError("Unable to parse JSON response from cTrader.") from exc

    raw_bars = _extract_trendbars(payload)
    bars = [build_bar(obj) for obj in raw_bars]
    bars.sort(key=lambda bar: bar.timestamp)
    logger.info("Fetched %s OHLC bars via REST", len(bars))
    return bars[-limit:]


def _extract_trendbars(payload: Mapping[str, object] | Iterable[object]) -> list[Mapping[str, object]]:
    """Extract a list of trendbar dictionaries from the REST payload."""

    if isinstance(payload, Mapping):
        for key in ("data", "trendbars", "bars", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                logger.debug("Found trendbars in payload key '%s'", key)
                return [item for item in candidate if isinstance(item, Mapping)]
        if isinstance(payload.get("trendbar"), list):
            logger.debug("Found trendbar list under 'trendbar' key")
            return [item for item in payload["trendbar"] if isinstance(item, Mapping)]
        if looks_like_trendbar(payload):
            logger.debug("Payload appears to be a single trendbar object")
            return [payload]
    elif isinstance(payload, list):
        logger.debug("Payload is a list; filtering trendbar-like entries")
        return [item for item in payload if isinstance(item, Mapping)]

    logger.error("cTrader response did not contain any trendbars: %s", payload)
    raise CTraderMarketDataError("cTrader response did not contain any trendbars.")


if __name__ == "__main__":  # pragma: no cover - diagnostic helper
    import argparse
    import json

    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Fetch OHLC data via the cTrader REST API.")
    parser.add_argument("access_token", help="OAuth access token")
    parser.add_argument("account_id", type=int, help="cTrader account identifier")
    parser.add_argument("symbol", help="Symbol to query, e.g. EURUSD")
    parser.add_argument("--timeframe", default="M1", help="Trendbar timeframe (default: M1)")
    parser.add_argument("--limit", type=int, default=10, help="Number of bars to fetch")

    args = parser.parse_args()
    bars = fetch_ohlc_data(
        access_token=args.access_token,
        account_id=args.account_id,
        symbol=args.symbol,
        timeframe=args.timeframe,
        limit=args.limit,
    )
    print(json.dumps([bar.__dict__ for bar in bars], default=str, indent=2))
