# src/app/services/ctrader_market_data.py v17
"""Retrieve OHLC data from cTrader Open API using Twisted client (ctrader_open_api 0.9.2, reactor thread-safe)."""

from __future__ import annotations
import datetime as _dt
import json
import threading
import time
from dataclasses import dataclass
from typing import List, Optional

from twisted.internet import reactor
from ctrader_open_api import Client, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOASymbolsListReq,
    ProtoOAGetTrendbarsReq,
)

__all__ = ["CTraderMarketDataError", "OHLCBar", "fetch_ohlc_data"]

_CREDENTIALS_PATH = r"C:\Users\44771\PycharmProjects\Trading test\data\ctrader.json"


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


_TIMEFRAME_MAP = {
    "M1": 1, "M5": 2, "M15": 3, "M30": 4, "H1": 5, "H4": 6, "D1": 7, "W1": 8, "MN1": 9,
}


def _load_credentials() -> dict[str, str]:
    with open(_CREDENTIALS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _start_reactor_in_background():
    """Start Twisted reactor in a background thread if not already running."""
    if not reactor.running:
        threading.Thread(target=reactor.run, kwargs={"installSignalHandlers": False}, daemon=True).start()
        time.sleep(0.2)


def fetch_ohlc_data(
    *,
    access_token: str,
    ctid_trader_account_id: int,
    symbol_name: str,
    timeframe: str = "M1",
    limit: int = 100,
) -> List[OHLCBar]:
    """Fetch OHLC data synchronously using Twisted-based Client (ctrader_open_api 0.9.2)."""

    creds = _load_credentials()
    # host = "demo.ctraderapi.com"
    host = "hconnect.ctrader.com"
    port = 5035  # SSL handled automatically by Client

    _start_reactor_in_background()

    result: dict[str, Optional[List[OHLCBar]]] = {"bars": None, "error": None}
    done = threading.Event()

    def on_connect(client_inst):
        print("[cTrader] CONNECTED → sending application auth")
        req = ProtoOAApplicationAuthReq()
        req.clientId = creds["client_id"]
        req.clientSecret = creds["secret"]
        client_inst.send(req)

    def on_message(client_inst, message):
        pt = message.payloadType
        if pt == "ProtoOAApplicationAuthRes":
            print("[cTrader] App authenticated")
            client_inst.send(ProtoOAAccountAuthReq(ctidTraderAccountId=ctid_trader_account_id))
        elif pt == "ProtoOAAccountAuthRes":
            print(f"[cTrader] Account {ctid_trader_account_id} authenticated")
            client_inst.send(ProtoOASymbolsListReq(ctidTraderAccountId=ctid_trader_account_id))
        elif pt == "ProtoOASymbolsListRes":
            found = None
            for s in message.payload.symbol:
                if s.symbolName.upper() == symbol_name.upper():
                    found = s
                    break
            if not found:
                result["error"] = CTraderMarketDataError(f"Symbol '{symbol_name}' not found")
                done.set()
                return
            period = _TIMEFRAME_MAP.get(timeframe.upper())
            if not period:
                result["error"] = ValueError(f"Unsupported timeframe '{timeframe}'")
                done.set()
                return
            client_inst.send(
                ProtoOAGetTrendbarsReq(
                    ctidTraderAccountId=ctid_trader_account_id,
                    symbolId=found.symbolId,
                    period=period,
                    count=limit,
                )
            )
        elif pt == "ProtoOAGetTrendbarsRes":
            bars = []
            for tb in message.payload.trendbar:
                ts = _dt.datetime.fromtimestamp(tb.utcTimestampInMinutes * 60, tz=_dt.timezone.utc)
                bars.append(
                    OHLCBar(
                        timestamp=ts.isoformat(),
                        open=tb.open,
                        high=tb.high,
                        low=tb.low,
                        close=tb.close,
                        volume=tb.volume,
                    )
                )
            result["bars"] = bars
            done.set()

    def on_disconnect(client_inst, reason):
        print(f"[cTrader] DISCONNECTED: {reason}")

    print(f"[cTrader] Connecting securely to {host}:{port}")
    client = Client(host, port, TcpProtocol)
    client.setConnectedCallback(on_connect)
    client.setMessageReceivedCallback(on_message)
    client.setDisconnectedCallback(on_disconnect)
    client.startService()

    # Wait for result or timeout
    done.wait(timeout=25)

    client.stopService()

    if result["error"]:
        raise result["error"]
    if not result["bars"]:
        raise CTraderMarketDataError("No trendbars received (timeout)")

    return result["bars"]
