from domain.entities import Candle


def _highs(candles): return [c.high for c in candles]
def _lows(candles): return [c.low for c in candles]


def double_top(candles: list[Candle], tolerance: float = 0.002) -> bool:
    if len(candles) < 20:
        return False
    highs = _highs(candles)
    peak1 = max(highs[:-10])
    peak2 = max(highs[-10:])
    return abs(peak1 - peak2) / peak1 <= tolerance and highs[-1] < peak2


def double_bottom(candles: list[Candle], tolerance: float = 0.002) -> bool:
    if len(candles) < 20:
        return False
    lows = _lows(candles)
    trough1 = min(lows[:-10])
    trough2 = min(lows[-10:])
    return abs(trough1 - trough2) / (trough1 or 1e-10) <= tolerance and lows[-1] > trough2


def rising_staircase(candles: list[Candle], lookback: int = 10) -> bool:
    if len(candles) < lookback:
        return False
    window = candles[-lookback:]
    highs = _highs(window)
    lows = _lows(window)
    return all(highs[i] > highs[i-1] for i in range(1, len(highs))) and \
        all(lows[i] > lows[i-1] for i in range(1, len(lows)))


def falling_staircase(candles: list[Candle], lookback: int = 10) -> bool:
    if len(candles) < lookback:
        return False
    window = candles[-lookback:]
    highs = _highs(window)
    lows = _lows(window)
    return all(highs[i] < highs[i-1] for i in range(1, len(highs))) and \
        all(lows[i] < lows[i-1] for i in range(1, len(lows)))


def make_market_pattern_analyzer(pattern_names: list[str]):
    _map = {
        "double_top":        double_top,
        "double_bottom":     double_bottom,
        "rising_staircase":  rising_staircase,
        "falling_staircase": falling_staircase,
    }

    def analyzer(candles: list[Candle]) -> dict:
        fired = [n for n in pattern_names if _map.get(n) and _map[n](candles)]
        return {"market_patterns": fired}
    return analyzer


def get_market_pattern_analyzers(names: list[str]) -> list:
    return [make_market_pattern_analyzer(names)]
