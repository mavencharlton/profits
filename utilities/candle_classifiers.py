from domain.value_objects import CandleType, CandleTypes
from domain.entities import Candle


def _body(c: Candle) -> float:
    return abs(c.close - c.open)


def _range(c: Candle) -> float:
    return c.high - c.low or 1e-10


def _upper_wick(c: Candle) -> float:
    return c.high - max(c.open, c.close)


def _lower_wick(c: Candle) -> float:
    return min(c.open, c.close) - c.low


def _body_ratio(c: Candle) -> float:
    return _body(c) / _range(c)


def classify_doji(c: Candle) -> CandleType | None:
    if _body_ratio(c) <= 0.10:
        uw, lw = _upper_wick(c), _lower_wick(c)
        bd = _body(c) or 1e-10
        if lw >= bd * 2 and uw / _range(c) <= 0.05:
            return CandleTypes.DRAGONFLY_DOJI
        if uw >= bd * 2 and lw / _range(c) <= 0.05:
            return CandleTypes.GRAVESTONE_DOJI
        return CandleTypes.DOJI
    return None


def classify_hammer(c: Candle) -> CandleType | None:
    if _body_ratio(c) <= 0.25:
        bd = _body(c) or 1e-10
        if _lower_wick(c) >= bd * 2 and _upper_wick(c) / _range(c) <= 0.10:
            return CandleTypes.HAMMER
    return None


def classify_inverted_hammer(c: Candle) -> CandleType | None:
    if _body_ratio(c) <= 0.25:
        bd = _body(c) or 1e-10
        if _upper_wick(c) >= bd * 2 and _lower_wick(c) / _range(c) <= 0.10:
            return CandleTypes.INVERTED_HAMMER
    return None


def classify_shooting_star(c: Candle) -> CandleType | None:
    return classify_inverted_hammer(c) and CandleTypes.SHOOTING_STAR or None


def classify_hanging_man(c: Candle) -> CandleType | None:
    return classify_hammer(c) and CandleTypes.HANGING_MAN or None


def classify_marubozu(c: Candle) -> CandleType | None:
    if _body_ratio(c) >= 0.90:
        return CandleTypes.MARUBOZU_BULL if c.close > c.open else CandleTypes.MARUBOZU_BEAR
    return None


def classify_spinning_top(c: Candle) -> CandleType | None:
    if 0.10 <= _body_ratio(c) <= 0.25:
        uw, lw = _upper_wick(c), _lower_wick(c)
        if uw > _body(c) and lw > _body(c):
            return CandleTypes.SPINNING_TOP
    return None


def classify_pin_bar(c: Candle) -> CandleType | None:
    if _body_ratio(c) <= 0.15:
        lw, uw = _lower_wick(c), _upper_wick(c)
        rng = _range(c)
        if lw / rng >= 2/3:
            return CandleTypes.PIN_BAR_BULL
        if uw / rng >= 2/3:
            return CandleTypes.PIN_BAR_BEAR
    return None


def classify_high_wave(c: Candle) -> CandleType | None:
    if _body_ratio(c) <= 0.20:
        bd = _body(c) or 1e-10
        if _upper_wick(c) >= bd * 2 and _lower_wick(c) >= bd * 2:
            return CandleTypes.HIGH_WAVE
    return None


def get_classifiers() -> list:
    """
    Ordered — first match wins.
    Order matters: doji before hammer, marubozu last.
    """
    return [
        classify_doji,
        classify_pin_bar,
        classify_hammer,
        classify_inverted_hammer,
        classify_spinning_top,
        classify_high_wave,
        classify_marubozu,
    ]
