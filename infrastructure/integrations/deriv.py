import asyncio
import json
import websockets

DERIV_WS_URL = "wss://ws.derivws.com/websockets/v3?app_id={app_id}"

_GRANULARITY = {
    "M1": 60,    "M5": 300,  "M15": 900,
    "M30": 1800, "H1": 3600, "D1": 86400
}


class DerivError(Exception):
    pass


class DerivClient:
    def __init__(self, app_id: str):
        self.url = DERIV_WS_URL.format(app_id=app_id)
        self._ws = None

    async def connect(self, api_token: str = None):
        self._ws = await websockets.connect(self.url)
        if api_token:
            await self._send({"authorize": api_token})  # ← Fix: authorize first

    async def disconnect(self):
        if self._ws:
            await self._ws.close()

    async def _send(self, payload: dict) -> dict:
        await self._ws.send(json.dumps(payload))
        response = await self._ws.recv()
        data = json.loads(response)
        if data.get("error"):
            raise DerivError(data["error"].get("message", "Unknown Deriv error"))
        return data

    async def get_candles(self, symbol: str, timeframe: str, count: int) -> list[dict]:
        response = await self._send({
            "ticks_history": symbol,
            "style":         "candles",
            "granularity":   _granularity(timeframe),
            "count":         count,
            "end":           "latest",
        })
        return response.get("candles", [])

    async def get_candles_since(self, symbol: str, timeframe: str, since_epoch: int) -> list[dict]:
        response = await self._send({
            "ticks_history": symbol,
            "style":         "candles",
            "granularity":   _granularity(timeframe),
            "start":         since_epoch,
            "end":           "latest",
        })
        return response.get("candles", [])

    async def subscribe_candles(self, symbol: str, timeframe: str, on_candle) -> str:
        response = await self._send({
            "ticks_history": symbol,
            "style":         "candles",
            "granularity":   _granularity(timeframe),
            "count":         1,
            "end":           "latest",
            "subscribe":     1,
        })
        sub_id = response.get("subscription", {}).get("id")
        asyncio.create_task(self._listen(on_candle))
        return sub_id

    async def _listen(self, on_candle):
        async for raw in self._ws:
            data = json.loads(raw)
            if data.get("msg_type") == "ohlc":
                await on_candle(data.get("ohlc", {}))

    async def unsubscribe(self, sub_id: str):
        await self._send({"forget": sub_id})


def _granularity(timeframe: str) -> int:
    if timeframe not in _GRANULARITY:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return _GRANULARITY[timeframe]
