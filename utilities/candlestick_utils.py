import pandas as pd
import numpy as np


def normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase columns, ensure required columns exist."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    return df.reset_index(drop=True)


def body(df: pd.DataFrame) -> pd.Series:
    return (df["close"] - df["open"]).abs()


def total_range(df: pd.DataFrame) -> pd.Series:
    return (df["high"] - df["low"]).replace(0, 1e-10)


def upper_wick(df: pd.DataFrame) -> pd.Series:
    return df["high"] - df[["open", "close"]].max(axis=1)


def lower_wick(df: pd.DataFrame) -> pd.Series:
    return df[["open", "close"]].min(axis=1) - df["low"]


def body_ratio(df: pd.DataFrame) -> pd.Series:
    return body(df) / total_range(df)


def is_bullish(df: pd.DataFrame) -> pd.Series:
    return df["close"] > df["open"]


def is_bearish(df: pd.DataFrame) -> pd.Series:
    return df["close"] < df["open"]


def candle_top(df: pd.DataFrame) -> pd.Series:
    """Top of the body (not the wick)."""
    return df[["open", "close"]].max(axis=1)


def candle_bottom(df: pd.DataFrame) -> pd.Series:
    """Bottom of the body (not the wick)."""
    return df[["open", "close"]].min(axis=1)


def shift(series: pd.Series, n: int) -> pd.Series:
    return series.shift(n)


def avg_volume(df: pd.DataFrame, n: int = 20) -> pd.Series:
    return df["volume"].rolling(n).mean()


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    tr    = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def within_pct(a: pd.Series, b: pd.Series, pct: float = 0.001) -> pd.Series:
    return ((a - b).abs() / b.abs().replace(0, 1e-10)) <= pct


def gap_up(df: pd.DataFrame, n: int = 1, min_pct: float = 0.002) -> pd.Series:
    return df["low"] > candle_top(df).shift(n) * (1 + min_pct)


def gap_down(df: pd.DataFrame, n: int = 1, min_pct: float = 0.002) -> pd.Series:
    return df["high"] < candle_bottom(df).shift(n) * (1 - min_pct)


def engulfs(df: pd.DataFrame, n: int = 1, ratio: float = 1.0) -> pd.Series:
    """Current body engulfs the body n bars ago."""
    return (
        (candle_top(df)    >= candle_top(df).shift(n)    * ratio) &
        (candle_bottom(df) <= candle_bottom(df).shift(n) * ratio)
    )


def contained_in(df: pd.DataFrame, n: int = 1) -> pd.Series:
    """Current body is contained within the body n bars ago."""
    return (
        (candle_top(df)    <= candle_top(df).shift(n)) &
        (candle_bottom(df) >= candle_bottom(df).shift(n))
    )


def is_doji(df, max_body_ratio: float = 0.10) -> pd.Series:
    if isinstance(df, pd.DataFrame):
        return body_ratio(df) <= max_body_ratio
    # shifted dataframe passed in — handle gracefully
    return body_ratio(df) <= max_body_ratio


def is_spinning_top(
    df: pd.DataFrame,
    min_body_ratio: float = 0.10,
    max_body_ratio: float = 0.25,
) -> pd.Series:
    bd = body(df).clip(lower=1e-10)
    return (
        (body_ratio(df) >= min_body_ratio) &
        (body_ratio(df) <= max_body_ratio) &
        (upper_wick(df) > bd) &
        (lower_wick(df) > bd)
    )


def vol_confirm(df: pd.DataFrame, mult: float = 1.0, n: int = 20) -> pd.Series:
    """Volume is above average by a multiplier."""
    return df["volume"] >= avg_volume(df, n) * mult
