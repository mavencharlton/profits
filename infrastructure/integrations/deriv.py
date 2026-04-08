import json
import threading
import asyncio
import websocket
import os

DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id={app_id}"

_GRANULARITY = {
    "M1": 60,    "M5": 300,  "M15": 900,
    "M30": 1800, "H1": 3600, "D1": 86400
}


class DerivError(Exception):
    pass


class DerivClient:
    def __init__(self):
        app_id = os.getenv("DERIV_APP_ID", "1089")
        self.url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"

    def _fetch_sync(self, payload: dict) -> dict:
        result = {}
        done = threading.Event()

        def on_message(ws, message):
            data = json.loads(message)
            if data.get("error"):
                result["error"] = data["error"].get("message", "Unknown error")
            else:
                result["data"] = data
            done.set()
            ws.close()

        def on_open(ws):
            ws.send(json.dumps(payload))

        def on_error(ws, error):
            result["error"] = str(error)
            done.set()
            ws.close()

        ws = websocket.WebSocketApp(
            self.url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            header={"Origin": "https://smarttrader.deriv.com"}
        )
        thread = threading.Thread(target=ws.run_forever)
        thread.start()
        done.wait(timeout=10)
        thread.join(timeout=2)

        if "error" in result:
            raise DerivError(result["error"])
        if "data" not in result:
            raise DerivError("Timeout — no response from Deriv")
        return result["data"]

    async def connect(self):
        pass

    async def disconnect(self):
        pass

    async def get_candles(self, symbol: str, timeframe: str, count: int) -> list[dict]:
        data = await asyncio.to_thread(self._fetch_sync, {
            "ticks_history": symbol,
            "style":         "candles",
            "granularity":   _granularity(timeframe),
            "count":         count,
            "end":           "latest",
        })
        return data.get("candles", [])

    async def get_candles_since(self, symbol: str, timeframe: str, since_epoch: int) -> list[dict]:
        data = await asyncio.to_thread(self._fetch_sync, {
            "ticks_history": symbol,
            "style":         "candles",
            "granularity":   _granularity(timeframe),
            "start":         since_epoch,
            "end":           "latest",
        })
        return data.get("candles", [])

    async def subscribe_candles(self, symbol: str, timeframe: str, on_candle) -> str:
        sub_id_holder = {}
        started = threading.Event()

        def on_message(ws, message):
            data = json.loads(message)
            if data.get("msg_type") == "ohlc":
                asyncio.run_coroutine_threadsafe(
                    on_candle(data.get("ohlc", {})),
                    asyncio.get_event_loop()
                )
            elif "subscription" in data:
                sub_id_holder["id"] = data["subscription"].get("id")
                started.set()

        def on_open(ws):
            ws.send(json.dumps({
                "ticks_history": symbol,
                "style":         "candles",
                "granularity":   _granularity(timeframe),
                "count":         1,
                "end":           "latest",
                "subscribe":     1,
            }))

        def on_error(ws, error):
            started.set()

        ws = websocket.WebSocketApp(
            self.url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            header={"Origin": "https://smarttrader.deriv.com"}
        )
        thread = threading.Thread(target=ws.run_forever, daemon=True)
        thread.start()
        started.wait(timeout=10)

        return sub_id_holder.get("id", "")

    async def unsubscribe(self, sub_id: str):
        await asyncio.to_thread(self._fetch_sync, {"forget": sub_id})


def _granularity(timeframe: str) -> int:
    if timeframe not in _GRANULARITY:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return _GRANULARITY[timeframe]
