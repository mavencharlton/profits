import pandas as pd
import pandas_ta as ta
from domain.entities import Candle


def _to_df(candles: list[Candle]) -> pd.DataFrame:
    return pd.DataFrame([{
        "open":  c.open, "high": c.high,
        "low":   c.low,  "close": c.close,
        "volume": c.volume,
    } for c in candles])


def rsi_analyzer(candles: list[Candle]) -> dict:
    df = _to_df(candles)
    rsi = ta.rsi(df["close"], length=14)
    val = round(float(rsi.iloc[-1]), 2)
    return {
        "rsi":        val,
        "rsi_signal": "overbought" if val >= 65 else "oversold" if val <= 35 else "neutral",
    }


def macd_analyzer(candles: list[Candle]) -> dict:
    df = _to_df(candles)
    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    hist = round(float(macd["MACDh_12_26_9"].iloc[-1]), 5)
    return {
        "macd_hist":   hist,
        "macd_signal": "bullish" if hist > 0 else "bearish",
    }


def bollinger_bands_analyzer(candles: list[Candle]) -> dict:
    df = _to_df(candles)
    bb = ta.bbands(df["close"], length=20, std=2)
    return {
        "bb_upper": round(float(bb["BBU_20_2.0"].iloc[-1]), 5),
        "bb_lower": round(float(bb["BBL_20_2.0"].iloc[-1]), 5),
        "bb_mid":   round(float(bb["BBM_20_2.0"].iloc[-1]), 5),
    }


def squeeze_analyzer(candles: list[Candle]) -> dict:
    df = _to_df(candles)
    bb = ta.bbands(df["close"], length=20, std=2)
    kc = ta.kc(df["high"], df["low"], df["close"], length=20, scalar=1.5)
    squeeze = (
        float(bb["BBU_20_2.0"].iloc[-1]) < float(kc["KCUe_20_1.5"].iloc[-1]) and
        float(bb["BBL_20_2.0"].iloc[-1]) > float(kc["KCLe_20_1.5"].iloc[-1])
    )
    return {"squeeze": squeeze}


def ema_trend_analyzer(candles: list[Candle]) -> dict:
    df = _to_df(candles)
    close = df["close"]
    ema_fast = ta.ema(close, length=21)
    ema_slow = ta.ema(close, length=55)
    ema_base = ta.ema(close, length=200)
    last = close.iloc[-1]
    return {
        "ema_fast":  round(float(ema_fast.iloc[-1]), 5),
        "ema_slow":  round(float(ema_slow.iloc[-1]), 5),
        "ema_base":  round(float(ema_base.iloc[-1]), 5),
        "trend":     "bullish" if last > float(ema_base.iloc[-1]) else "bearish",
        "ema_cross": (
            "bullish" if float(ema_fast.iloc[-1]) > float(ema_slow.iloc[-1])
            else "bearish"
        ),
    }


_REGISTRY = {
    "rsi":             rsi_analyzer,
    "macd":            macd_analyzer,
    "bollinger_bands": bollinger_bands_analyzer,
    "squeeze":         squeeze_analyzer,
    "ema_trend":       ema_trend_analyzer,
}


def get_indicator_analyzers(names: list[str]) -> list:
    return [_REGISTRY[n] for n in names if n in _REGISTRY]
