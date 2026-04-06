from domain.entities import Candle
from domain.value_objects import CandleType, CandleTypes, Sentiment
from datetime import datetime


class CandleRepository:
    def __init__(self, firebase):
        self.db = firebase

    def save(self, symbol: str, timeframe: str, candle: Candle):
        doc_id = f"{symbol}_{timeframe}_{int(candle.timestamp.timestamp())}"
        self.db.save("candles", doc_id, self._to_dict(
            symbol, timeframe, candle))

    def save_batch(self, symbol: str, timeframe: str, candles: list[Candle]):
        for candle in candles:
            self.save(symbol, timeframe, candle)

    def get_latest(self, symbol: str, timeframe: str, count: int) -> list[Candle]:
        raw = self.db.query("candles", [
            ("symbol",    "==", symbol),
            ("timeframe", "==", timeframe),
        ])
        raw_sorted = sorted(raw, key=lambda r: r["timestamp"])[-count:]
        return [self._from_dict(r) for r in raw_sorted]

    def get_unclassified(self, symbol: str, timeframe: str) -> list[Candle]:
        raw = self.db.query("candles", [
            ("symbol",      "==", symbol),
            ("timeframe",   "==", timeframe),
            ("candle_type", "==", None),
        ])
        return [self._from_dict(r) for r in raw]

    def update_batch(self, symbol: str, timeframe: str, candles: list[Candle]):
        for candle in candles:
            doc_id = f"{symbol}_{timeframe}_{int(candle.timestamp.timestamp())}"
            self.db.save("candles", doc_id, self._to_dict(
                symbol, timeframe, candle))

    @staticmethod
    def _to_dict(symbol: str, timeframe: str, candle: Candle) -> dict:
        return {
            "symbol":    symbol,
            "timeframe": timeframe,
            "open":      candle.open,
            "high":      candle.high,
            "low":       candle.low,
            "close":     candle.close,
            "volume":    candle.volume,
            "timestamp": candle.timestamp.isoformat(),
            "candle_type": {
                "name":      candle.candle_type.name,
                "sentiment": candle.candle_type.sentiment.value,
            } if candle.candle_type else None,
        }

    @staticmethod
    def _from_dict(raw: dict) -> Candle:
        candle = Candle(
            open=raw["open"],
            high=raw["high"],
            low=raw["low"],
            close=raw["close"],
            volume=raw["volume"],
            timestamp=datetime.fromisoformat(raw["timestamp"]),
        )
        ct = raw.get("candle_type")
        if ct:
            candle.candle_type = CandleType(
                name=ct["name"],
                sentiment=Sentiment(ct["sentiment"]),
            )
        return candle
