from collections import defaultdict
from typing import Callable
from dataclasses import dataclass
from datetime import datetime
from domain.entities import Candle

@dataclass
class CandleReceived:
    symbol:    str
    timeframe: str
    candle:    Candle
    timestamp: datetime

class EventBus:
    def __init__(self):
        self._listeners: dict[type, list[Callable]] = defaultdict(list)

    def subscribe(self, event_type: type, listener: Callable):
        self._listeners[event_type].append(listener)

    async def publish(self, event):
        for listener in self._listeners[type(event)]:
            await listener(event)