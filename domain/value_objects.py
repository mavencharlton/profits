from dataclasses import dataclass
from enum import Enum


class Timeframe(str, Enum):
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    D1 = "D1"


class Direction(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class Sentiment(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    UNCERTAINTY = "uncertainty"


class ContractType(str, Enum):
    RISE = "RISE"
    FALL = "FALL"


class SignalStatus(str, Enum):
    FORMING = "forming"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"


@dataclass(frozen=True)
class CandleType:
    name:      str
    sentiment: Sentiment

    def __str__(self):
        return f"{self.name} ({self.sentiment.value})"


class CandleTypes:
    DOJI = CandleType("doji",             Sentiment.UNCERTAINTY)
    DRAGONFLY_DOJI = CandleType("dragonfly_doji",   Sentiment.BULLISH)
    GRAVESTONE_DOJI = CandleType("gravestone_doji",  Sentiment.BEARISH)
    HAMMER = CandleType("hammer",           Sentiment.BULLISH)
    INVERTED_HAMMER = CandleType("inverted_hammer",  Sentiment.BULLISH)
    SHOOTING_STAR = CandleType("shooting_star",    Sentiment.BEARISH)
    HANGING_MAN = CandleType("hanging_man",      Sentiment.BEARISH)
    MARUBOZU_BULL = CandleType("marubozu_bullish", Sentiment.BULLISH)
    MARUBOZU_BEAR = CandleType("marubozu_bearish", Sentiment.BEARISH)
    SPINNING_TOP = CandleType("spinning_top",     Sentiment.UNCERTAINTY)
    PIN_BAR_BULL = CandleType("pin_bar_bullish",  Sentiment.BULLISH)
    PIN_BAR_BEAR = CandleType("pin_bar_bearish",  Sentiment.BEARISH)
    HIGH_WAVE = CandleType("high_wave",        Sentiment.UNCERTAINTY)
    UNKNOWN = CandleType("unknown",          Sentiment.UNCERTAINTY)
