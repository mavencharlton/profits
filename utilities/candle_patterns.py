import pandas as pd
from domain.entities import Candle

# your existing file — no changes
from .candlestick_patterns import PATTERNS
from .candlestick_utils import normalise


def _to_df(candles: list[Candle]) -> pd.DataFrame:
    return pd.DataFrame([{
        "open": c.open, "high": c.high,
        "low":  c.low,  "close": c.close,
        "volume": c.volume,
    } for c in candles])


def make_candle_pattern_analyzer(pattern_names: list[str]):
    """
    Returns a single analyzer callable that checks
    only the requested patterns against the candle window.
    """
    def analyzer(candles: list[Candle]) -> dict:
        df = normalise(_to_df(candles))
        fired = []
        for name in pattern_names:
            fn = PATTERNS.get(name)
            if fn is None:
                continue
            try:
                if fn(df).iloc[-1]:
                    fired.append(name)
            except Exception:
                pass
        return {"candle_patterns": fired}

    return analyzer


def get_candle_pattern_analyzers(names: list[str]) -> list:
    return [make_candle_pattern_analyzer(names)]
