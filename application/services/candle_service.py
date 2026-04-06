from datetime import datetime
from domain.entities import Candle
from application.events import EventBus, CandleReceived
from utilities.candle_classifiers import get_classifiers


class CandleService:
    def __init__(self, deriv, candle_repo, event_bus: EventBus):
        self.deriv = deriv
        self.repo = candle_repo
        self.event_bus = event_bus

    async def start(self, symbol: str, timeframe: str):
        """
        Subscribe to Deriv M1 stream.
        On each tick: classify → save → emit.
        """
        await self.deriv.subscribe_candles(
            symbol, timeframe,
            on_candle=lambda raw: self._handle(raw, symbol, timeframe)
        )

    async def bootstrap(self, symbol: str, timeframe: str, count: int = 1000):
        """
        Pull historic candles on startup.
        Classify and save all — no event emitted (no pattern check on history).
        """
        raw_candles = await self.deriv.get_candles(symbol, timeframe, count)
        candles = [self._to_candle(r) for r in raw_candles]
        classifiers = get_classifiers()

        for candle in candles:
            candle.classify(classifiers)

        self.repo.save_batch(symbol, timeframe, candles)

    async def _handle(self, raw: dict, symbol: str, timeframe: str):
        candle = self._to_candle(raw)
        classifiers = get_classifiers()
        candle.classify(classifiers)

        self.repo.save(symbol, timeframe, candle)

        await self.event_bus.publish(CandleReceived(
            symbol=symbol,
            timeframe=timeframe,
            candle=candle,
            timestamp=datetime.utcnow(),
        ))

    @staticmethod
    def _to_candle(raw: dict) -> Candle:
        return Candle(
            open=float(raw["open"]),
            high=float(raw["high"]),
            low=float(raw["low"]),
            close=float(raw["close"]),
            volume=float(raw.get("volume", 0)),
            timestamp=datetime.utcfromtimestamp(raw["epoch"]),
        )
