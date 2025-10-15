"""Modular helpers for working with cTrader market data."""
from __future__ import annotations

from .common import CTraderMarketDataError, OHLCBar
from .rest import fetch_ohlc_data
from .streaming import stream_ohlc_data

__all__ = ["CTraderMarketDataError", "OHLCBar", "fetch_ohlc_data", "stream_ohlc_data"]


if __name__ == "__main__":  # pragma: no cover - diagnostic helper
    import argparse
    import asyncio
    import logging

    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Quick access to cTrader market data helpers")
    parser.add_argument("access_token", help="OAuth access token")
    parser.add_argument("account_id", type=int, help="cTrader account identifier")
    parser.add_argument("symbol", help="Symbol to query, e.g. EURUSD")
    parser.add_argument("--timeframe", default="M1", help="Trendbar timeframe (default: M1)")
    parser.add_argument("--mode", choices={"rest", "stream"}, default="rest")

    args = parser.parse_args()

    if args.mode == "rest":
        for bar in fetch_ohlc_data(
            access_token=args.access_token,
            account_id=args.account_id,
            symbol=args.symbol,
            timeframe=args.timeframe,
            limit=5,
        ):
            print(bar)
    else:
        async def _runner() -> None:
            async for bar in stream_ohlc_data(
                access_token=args.access_token,
                account_id=args.account_id,
                symbol=args.symbol,
                timeframe=args.timeframe,
            ):
                print(bar)
                break

        asyncio.run(_runner())
