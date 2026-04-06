"""
candlestick_patterns.py
───────────────────────
60 candlestick pattern detectors.

Each function signature:
    pattern_name(df: pd.DataFrame, **kwargs) -> pd.Series[bool]

- df must contain columns: open, high, low, close, volume
  (call normalise(df) once before using these functions)
- Returns a boolean Series aligned to df's index.
- True at index i means the pattern completes on bar i.
"""

import numpy as np
import pandas as pd

from .candlestick_utils import (
    normalise,
    body, total_range, upper_wick, lower_wick,
    body_ratio, is_bullish, is_bearish,
    candle_top, candle_bottom,
    shift, avg_volume, atr,
    within_pct, gap_up, gap_down,
    engulfs, contained_in,
    is_doji, is_spinning_top,
    vol_confirm,
)


# ═══════════════════════════════════════════════════════════════
# SINGLE-CANDLE PATTERNS  (1–10)
# ═══════════════════════════════════════════════════════════════

def marubozu(df: pd.DataFrame, min_body_ratio: float = 0.90) -> pd.Series:
    """
    #1 Marubozu – no (or negligible) wicks; body ≥ 90 % of range.
    Returns two columns merged into one: both bullish and bearish fire.
    Use marubozu_bullish / marubozu_bearish for directional variants.
    """
    return marubozu_bullish(df, min_body_ratio) | marubozu_bearish(df, min_body_ratio)


def marubozu_bullish(df: pd.DataFrame, min_body_ratio: float = 0.90) -> pd.Series:
    """Bullish Marubozu – long green candle, body ≥ min_body_ratio of range."""
    return is_bullish(df) & (body_ratio(df) >= min_body_ratio)


def marubozu_bearish(df: pd.DataFrame, min_body_ratio: float = 0.90) -> pd.Series:
    """Bearish Marubozu – long red candle, body ≥ min_body_ratio of range."""
    return is_bearish(df) & (body_ratio(df) >= min_body_ratio)


def doji(df: pd.DataFrame, max_body_ratio: float = 0.10) -> pd.Series:
    """#2 Doji – open ≈ close (body ≤ max_body_ratio of range)."""
    return is_doji(df, max_body_ratio)


def dragonfly_doji(
    df: pd.DataFrame,
    max_body_ratio: float = 0.10,
    min_lower_wick_mult: float = 2.0,
    max_upper_wick_ratio: float = 0.05,
) -> pd.Series:
    """#3 Dragonfly Doji – long lower wick, open/close at top, no upper wick."""
    bd = body(df).clip(lower=1e-10)
    lw = lower_wick(df)
    uw = upper_wick(df)
    rng = total_range(df)
    return (
        is_doji(df, max_body_ratio)
        & (lw >= bd * min_lower_wick_mult)
        & (uw / rng <= max_upper_wick_ratio)
    )


def gravestone_doji(
    df: pd.DataFrame,
    max_body_ratio: float = 0.10,
    min_upper_wick_mult: float = 2.0,
    max_lower_wick_ratio: float = 0.05,
) -> pd.Series:
    """#4 Gravestone Doji – long upper wick, open/close at bottom, no lower wick."""
    bd = body(df).clip(lower=1e-10)
    uw = upper_wick(df)
    lw = lower_wick(df)
    rng = total_range(df)
    return (
        is_doji(df, max_body_ratio)
        & (uw >= bd * min_upper_wick_mult)
        & (lw / rng <= max_lower_wick_ratio)
    )


def long_legged_doji(
    df: pd.DataFrame,
    max_body_ratio: float = 0.10,
    min_wick_mult: float = 2.0,
) -> pd.Series:
    """#5 Long-Legged Doji – very long upper AND lower wicks."""
    bd = body(df).clip(lower=1e-10)
    return (
        is_doji(df, max_body_ratio)
        & (upper_wick(df) >= bd * min_wick_mult)
        & (lower_wick(df) >= bd * min_wick_mult)
    )


def bullish_spinning_top(
    df: pd.DataFrame,
    min_body_ratio: float = 0.10,
    max_body_ratio: float = 0.25,
) -> pd.Series:
    """#6 Bullish Spinning Top – small green body, long roughly equal wicks."""
    return is_bullish(df) & is_spinning_top(df, min_body_ratio, max_body_ratio)


def bearish_spinning_top(
    df: pd.DataFrame,
    min_body_ratio: float = 0.10,
    max_body_ratio: float = 0.25,
) -> pd.Series:
    """#7 Bearish Spinning Top – small red body, long roughly equal wicks."""
    return is_bearish(df) & is_spinning_top(df, min_body_ratio, max_body_ratio)


def high_wave(
    df: pd.DataFrame,
    max_body_ratio: float = 0.20,
    min_wick_mult: float = 2.0,
) -> pd.Series:
    """#8 High-Wave – very small body, very long upper AND lower wicks (any colour)."""
    bd = body(df).clip(lower=1e-10)
    return (
        (body_ratio(df) <= max_body_ratio)
        & (upper_wick(df) >= bd * min_wick_mult)
        & (lower_wick(df) >= bd * min_wick_mult)
    )


def long_wick_bullish(
    df: pd.DataFrame,
    min_wick_mult: float = 2.0,
) -> pd.Series:
    """#9a Long Lower Wick – rejection of lower prices."""
    bd = body(df).clip(lower=1e-10)
    return lower_wick(df) >= bd * min_wick_mult


def long_wick_bearish(
    df: pd.DataFrame,
    min_wick_mult: float = 2.0,
) -> pd.Series:
    """#9b Long Upper Wick – rejection of higher prices."""
    bd = body(df).clip(lower=1e-10)
    return upper_wick(df) >= bd * min_wick_mult


def pin_bar_bullish(
    df: pd.DataFrame,
    max_body_ratio: float = 0.15,
    min_tail_ratio: float = 2 / 3,
) -> pd.Series:
    """#10a Bullish Pin Bar – long lower tail ≥ 2/3 of candle range."""
    rng = total_range(df)
    lw = lower_wick(df)
    return (
        (body_ratio(df) <= max_body_ratio)
        & (lw / rng >= min_tail_ratio)
    )


def pin_bar_bearish(
    df: pd.DataFrame,
    max_body_ratio: float = 0.15,
    min_tail_ratio: float = 2 / 3,
) -> pd.Series:
    """#10b Bearish Pin Bar – long upper tail ≥ 2/3 of candle range."""
    rng = total_range(df)
    uw = upper_wick(df)
    return (
        (body_ratio(df) <= max_body_ratio)
        & (uw / rng >= min_tail_ratio)
    )


# ═══════════════════════════════════════════════════════════════
# SINGLE-CANDLE REVERSAL PATTERNS  (11–14)
# ═══════════════════════════════════════════════════════════════

def hammer(
    df: pd.DataFrame,
    max_body_ratio: float = 0.25,
    min_lower_wick_mult: float = 2.0,
    max_upper_wick_ratio: float = 0.10,
) -> pd.Series:
    """#11 Hammer – small body near top, long lower wick ≥ 2× body."""
    bd = body(df).clip(lower=1e-10)
    rng = total_range(df)
    return (
        (body_ratio(df) <= max_body_ratio)
        & (lower_wick(df) >= bd * min_lower_wick_mult)
        & (upper_wick(df) / rng <= max_upper_wick_ratio)
        # body in upper 25 % of range
        & (candle_bottom(df) >= df["low"] + rng * 0.75)
    )


def hanging_man(
    df: pd.DataFrame,
    max_body_ratio: float = 0.25,
    min_lower_wick_mult: float = 2.0,
    max_upper_wick_ratio: float = 0.10,
) -> pd.Series:
    """
    #12 Hanging Man – identical shape to Hammer but context is uptrend.
    Detection is shape-only; trend context should be applied externally.
    """
    return hammer(df, max_body_ratio, min_lower_wick_mult, max_upper_wick_ratio)


def inverted_hammer(
    df: pd.DataFrame,
    max_body_ratio: float = 0.25,
    min_upper_wick_mult: float = 2.0,
    max_lower_wick_ratio: float = 0.10,
) -> pd.Series:
    """#13 Inverted Hammer – small body near bottom, long upper wick ≥ 2× body."""
    bd = body(df).clip(lower=1e-10)
    rng = total_range(df)
    return (
        (body_ratio(df) <= max_body_ratio)
        & (upper_wick(df) >= bd * min_upper_wick_mult)
        & (lower_wick(df) / rng <= max_lower_wick_ratio)
        & (candle_top(df) <= df["low"] + rng * 0.25)
    )


def shooting_star(
    df: pd.DataFrame,
    max_body_ratio: float = 0.25,
    min_upper_wick_mult: float = 2.0,
    max_lower_wick_ratio: float = 0.10,
) -> pd.Series:
    """
    #14 Shooting Star – same shape as Inverted Hammer but context is uptrend.
    Detection is shape-only.
    """
    return inverted_hammer(df, max_body_ratio, min_upper_wick_mult, max_lower_wick_ratio)


# ═══════════════════════════════════════════════════════════════
# TWO-CANDLE PATTERNS  (15–24)
# ═══════════════════════════════════════════════════════════════

def bullish_engulfing(df: pd.DataFrame, ratio: float = 1.0) -> pd.Series:
    """#15 Bullish Engulfing – green C2 body fully engulfs red C1 body."""
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & engulfs(df, n=1, ratio=ratio)
    )


def bearish_engulfing(df: pd.DataFrame, ratio: float = 1.0) -> pd.Series:
    """#16 Bearish Engulfing – red C2 body fully engulfs green C1 body."""
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & engulfs(df, n=1, ratio=ratio)
    )


def piercing_line(df: pd.DataFrame, min_penetration: float = 0.50) -> pd.Series:
    """#17 Piercing Line – C2 green opens below C1 low, closes ≥50 % into C1 body."""
    c1_body_top = candle_top(df).shift(1)
    c1_body_bottom = candle_bottom(df).shift(1)
    midpoint = c1_body_bottom + \
        (c1_body_top - c1_body_bottom) * min_penetration
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & (df["open"] < df["low"].shift(1))
        & (df["close"] >= midpoint)
        & (df["close"] < c1_body_top)
    )


def dark_cloud_cover(df: pd.DataFrame, min_penetration: float = 0.50) -> pd.Series:
    """#18 Dark Cloud Cover – C2 red opens above C1 high, closes ≥50 % into C1 body."""
    c1_body_top = candle_top(df).shift(1)
    c1_body_bottom = candle_bottom(df).shift(1)
    midpoint = c1_body_top - (c1_body_top - c1_body_bottom) * min_penetration
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & (df["open"] > df["high"].shift(1))
        & (df["close"] <= midpoint)
        & (df["close"] > c1_body_bottom)
    )


def bullish_harami(df: pd.DataFrame) -> pd.Series:
    """#19 Bullish Harami – small green C2 body contained within large red C1 body."""
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & contained_in(df, n=1)
        & (body(df) <= body(df).shift(1) * 0.5)
    )


def bearish_harami(df: pd.DataFrame) -> pd.Series:
    """#20 Bearish Harami – small red C2 body contained within large green C1 body."""
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & contained_in(df, n=1)
        & (body(df) <= body(df).shift(1) * 0.5)
    )


def tweezer_bottom(df: pd.DataFrame, tol_atr_mult: float = 0.20) -> pd.Series:
    """#21 Tweezer Bottom – two consecutive candles with matching lows."""
    _atr = atr(df)
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & ((df["low"] - df["low"].shift(1)).abs() <= _atr * tol_atr_mult)
    )


def tweezer_top(df: pd.DataFrame, tol_atr_mult: float = 0.20) -> pd.Series:
    """#22 Tweezer Top – two consecutive candles with matching highs."""
    _atr = atr(df)
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & ((df["high"] - df["high"].shift(1)).abs() <= _atr * tol_atr_mult)
    )


def bullish_counterattack(df: pd.DataFrame, gap_pct: float = 0.002) -> pd.Series:
    """#23 Bullish Counterattack – C2 gaps down then closes at/near C1 close."""
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & (df["open"] < df["close"].shift(1) * (1 - gap_pct))
        & within_pct(df["close"], df["close"].shift(1), pct=0.001)
    )


def bearish_counterattack(df: pd.DataFrame, gap_pct: float = 0.002) -> pd.Series:
    """#24 Bearish Counterattack – C2 gaps up then closes at/near C1 close."""
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & (df["open"] > df["close"].shift(1) * (1 + gap_pct))
        & within_pct(df["close"], df["close"].shift(1), pct=0.001)
    )


# ═══════════════════════════════════════════════════════════════
# THREE-CANDLE PATTERNS  (25–30, 37–40)
# ═══════════════════════════════════════════════════════════════

def _three_candle_star(
    df: pd.DataFrame,
    c1_bullish: bool,
    min_c3_penetration: float = 0.50,
) -> pd.Series:
    """
    Shared logic for Morning Star / Evening Star variants.
    c1_bullish=False → Morning Star (c1 bearish, c3 bullish).
    c1_bullish=True  → Evening Star (c1 bullish, c3 bearish).
    """
    c1_is = is_bullish if c1_bullish else is_bearish
    c3_is = is_bearish if c1_bullish else is_bullish

    c1_body_top = candle_top(df).shift(2)
    c1_body_bottom = candle_bottom(df).shift(2)
    c1_body_size = body(df).shift(2)

    # small middle candle (body ≤ 25 % of C1)
    small_c2 = body(df).shift(1) <= c1_body_size * 0.25

    # C3 closes deep into C1 body
    if c1_bullish:
        penetration_level = c1_body_top - c1_body_size * min_c3_penetration
        c3_deep = df["close"] <= penetration_level
    else:
        penetration_level = c1_body_bottom + c1_body_size * min_c3_penetration
        c3_deep = df["close"] >= penetration_level

    return (
        c1_is(df).shift(2)
        & small_c2
        & c3_is(df)
        & c3_deep
    )


def morning_star(df: pd.DataFrame, min_c3_penetration: float = 0.50) -> pd.Series:
    """#25 Morning Star – bearish C1, small C2, bullish C3 closes into C1."""
    return _three_candle_star(df, c1_bullish=False, min_c3_penetration=min_c3_penetration)


def evening_star(df: pd.DataFrame, min_c3_penetration: float = 0.50) -> pd.Series:
    """#26 Evening Star – bullish C1, small C2, bearish C3 closes into C1."""
    return _three_candle_star(df, c1_bullish=True, min_c3_penetration=min_c3_penetration)


def morning_star_doji(df: pd.DataFrame, min_c3_penetration: float = 0.60) -> pd.Series:
    """#27 Morning Star Doji – C2 is specifically a Doji."""
    base = morning_star(df, min_c3_penetration)
    return base & is_doji(df.shift(1))


def evening_star_doji(df: pd.DataFrame, min_c3_penetration: float = 0.60) -> pd.Series:
    """#28 Evening Star Doji – C2 is specifically a Doji."""
    base = evening_star(df, min_c3_penetration)
    return base & is_doji(df.shift(1))


def three_white_soldiers(
    df: pd.DataFrame,
    min_body_ratio: float = 0.60,
    max_upper_wick_ratio: float = 0.25,
) -> pd.Series:
    """#29 Three White Soldiers – three consecutive long bullish candles."""
    c1 = is_bullish(df).shift(2) & (body_ratio(df).shift(2) >= min_body_ratio)
    c2 = (
        is_bullish(df).shift(1)
        & (body_ratio(df).shift(1) >= min_body_ratio)
        & (df["open"].shift(1) >= candle_bottom(df).shift(2))
        & (df["open"].shift(1) <= candle_top(df).shift(2))
    )
    c3 = (
        is_bullish(df)
        & (body_ratio(df) >= min_body_ratio)
        & (df["open"] >= candle_bottom(df).shift(1))
        & (df["open"] <= candle_top(df).shift(1))
        & (upper_wick(df) / total_range(df) <= max_upper_wick_ratio)
    )
    return c1 & c2 & c3


def three_black_crows(
    df: pd.DataFrame,
    min_body_ratio: float = 0.60,
    max_lower_wick_ratio: float = 0.25,
) -> pd.Series:
    """#30 Three Black Crows – three consecutive long bearish candles."""
    c1 = is_bearish(df).shift(2) & (body_ratio(df).shift(2) >= min_body_ratio)
    c2 = (
        is_bearish(df).shift(1)
        & (body_ratio(df).shift(1) >= min_body_ratio)
        & (df["open"].shift(1) <= candle_top(df).shift(2))
        & (df["open"].shift(1) >= candle_bottom(df).shift(2))
    )
    c3 = (
        is_bearish(df)
        & (body_ratio(df) >= min_body_ratio)
        & (df["open"] <= candle_top(df).shift(1))
        & (df["open"] >= candle_bottom(df).shift(1))
        & (lower_wick(df) / total_range(df) <= max_lower_wick_ratio)
    )
    return c1 & c2 & c3


def three_outside_up(df: pd.DataFrame) -> pd.Series:
    """#37 Three Outside Up – bearish C1, bullish engulf C2, bullish C3 higher."""
    return (
        bearish_engulfing(df.iloc[:].assign(
            **{c: df[c].shift(-1) for c in ["open", "high", "low", "close", "volume"]}
        ))
        .shift(1)
        | (
            is_bearish(df).shift(2)
            & is_bullish(df).shift(1)
            & engulfs(df.shift(-1), n=1).shift(1)   # C2 engulfs C1
            & is_bullish(df)
            & (df["close"] > candle_top(df).shift(1))
        )
    )


def three_outside_up(df: pd.DataFrame) -> pd.Series:
    """#37 Three Outside Up – C1 bearish, C2 bullish engulfing C1, C3 bullish higher."""
    c2_engulfs_c1 = (
        candle_top(df).shift(1) >= candle_top(df).shift(2)
    ) & (
        candle_bottom(df).shift(1) <= candle_bottom(df).shift(2)
    )
    return (
        is_bearish(df).shift(2)
        & is_bullish(df).shift(1)
        & c2_engulfs_c1
        & is_bullish(df)
        & (df["close"] > candle_top(df).shift(1))
    )


def three_outside_down(df: pd.DataFrame) -> pd.Series:
    """#38 Three Outside Down – C1 bullish, C2 bearish engulfing C1, C3 bearish lower."""
    c2_engulfs_c1 = (
        candle_top(df).shift(1) >= candle_top(df).shift(2)
    ) & (
        candle_bottom(df).shift(1) <= candle_bottom(df).shift(2)
    )
    return (
        is_bullish(df).shift(2)
        & is_bearish(df).shift(1)
        & c2_engulfs_c1
        & is_bearish(df)
        & (df["close"] < candle_bottom(df).shift(1))
    )


def three_inside_up(df: pd.DataFrame) -> pd.Series:
    """#39 Three Inside Up – large bearish C1, small bullish C2 (Harami), bullish C3 breakout."""
    c2_in_c1 = contained_in(df.shift(-1), n=1).shift(1)  # C2 inside C1
    return (
        is_bearish(df).shift(2)
        & is_bullish(df).shift(1)
        & (candle_top(df).shift(1) <= candle_top(df).shift(2))
        & (candle_bottom(df).shift(1) >= candle_bottom(df).shift(2))
        & is_bullish(df)
        & (df["close"] > candle_top(df).shift(2))
    )


def three_inside_down(df: pd.DataFrame) -> pd.Series:
    """#40 Three Inside Down – large bullish C1, small bearish C2 (Harami), bearish C3 breakout."""
    return (
        is_bullish(df).shift(2)
        & is_bearish(df).shift(1)
        & (candle_top(df).shift(1) <= candle_top(df).shift(2))
        & (candle_bottom(df).shift(1) >= candle_bottom(df).shift(2))
        & is_bearish(df)
        & (df["close"] < candle_bottom(df).shift(2))
    )


# ═══════════════════════════════════════════════════════════════
# FIVE-CANDLE CONTINUATION PATTERNS  (31–32, 34)
# ═══════════════════════════════════════════════════════════════

def _five_candle_method(
    df: pd.DataFrame,
    c1_bullish: bool,
    min_body_ratio: float = 0.60,
    pullback_candles: int = 3,
) -> pd.Series:
    """
    Shared logic for Rising Three / Falling Three / Mat Hold.
    c1_bullish=True → Rising Three; False → Falling Three.
    """
    c5_is = is_bullish if c1_bullish else is_bearish
    c1_is = is_bullish if c1_bullish else is_bearish

    c1_body_top = candle_top(df).shift(4)
    c1_body_bottom = candle_bottom(df).shift(4)

    # Middle candles contained within C1 body
    mid_contained = (
        (candle_top(df).shift(3) <= c1_body_top)
        & (candle_bottom(df).shift(3) >= c1_body_bottom)
        & (candle_top(df).shift(2) <= c1_body_top)
        & (candle_bottom(df).shift(2) >= c1_body_bottom)
        & (candle_top(df).shift(1) <= c1_body_top)
        & (candle_bottom(df).shift(1) >= c1_body_bottom)
    )

    if c1_bullish:
        c5_confirms = df["close"] > c1_body_top
    else:
        c5_confirms = df["close"] < c1_body_bottom

    return (
        c1_is(df).shift(4)
        & (body_ratio(df).shift(4) >= min_body_ratio)
        & mid_contained
        & c5_is(df)
        & (body_ratio(df) >= min_body_ratio)
        & c5_confirms
    )


def rising_three(df: pd.DataFrame, min_body_ratio: float = 0.60) -> pd.Series:
    """#31 Rising Three Methods – bullish continuation."""
    return _five_candle_method(df, c1_bullish=True, min_body_ratio=min_body_ratio)


def falling_three(df: pd.DataFrame, min_body_ratio: float = 0.60) -> pd.Series:
    """#32 Falling Three Methods – bearish continuation."""
    return _five_candle_method(df, c1_bullish=False, min_body_ratio=min_body_ratio)


def mat_hold(df: pd.DataFrame, min_body_ratio: float = 0.60) -> pd.Series:
    """
    #34 Mat Hold – like Rising Three but middle candles allowed to be slightly
    below C1 open (shallower retracement is not enforced as strictly).
    Uses the same five-candle rising logic as Rising Three.
    """
    return _five_candle_method(df, c1_bullish=True, min_body_ratio=min_body_ratio)


# ═══════════════════════════════════════════════════════════════
# THREE-CANDLE GAP / CONTINUATION  (33)
# ═══════════════════════════════════════════════════════════════

def tasuki_gap_bullish(df: pd.DataFrame) -> pd.Series:
    """
    #33a Bullish Tasuki Gap – upward gap between C1 and C2, C3 bearish
    but does NOT close the gap.
    """
    gap_open = df["low"].shift(1) > df["high"].shift(2)   # gap between C1 & C2
    c3_bearish = is_bearish(df)
    gap_intact = df["close"] > df["high"].shift(2)          # gap still open
    return (
        is_bullish(df).shift(2)
        & is_bullish(df).shift(1)
        & gap_open
        & c3_bearish
        & gap_intact
    )


def tasuki_gap_bearish(df: pd.DataFrame) -> pd.Series:
    """
    #33b Bearish Tasuki Gap – downward gap between C1 and C2, C3 bullish
    but does NOT close the gap.
    """
    gap_open = df["high"].shift(1) < df["low"].shift(2)
    c3_bullish = is_bullish(df)
    gap_intact = df["close"] < df["low"].shift(2)
    return (
        is_bearish(df).shift(2)
        & is_bearish(df).shift(1)
        & gap_open
        & c3_bullish
        & gap_intact
    )


# ═══════════════════════════════════════════════════════════════
# ABANDONED BABY PATTERNS  (35–36)
# ═══════════════════════════════════════════════════════════════

def bullish_abandoned_baby(df: pd.DataFrame) -> pd.Series:
    """#35 Bullish Abandoned Baby – bearish C1, gapped-down Doji C2, bullish gapped-up C3."""
    c2_gaps_down = df["high"].shift(1) < df["low"].shift(2)
    c3_gaps_up = df["low"] > df["high"].shift(1)
    return (
        is_bearish(df).shift(2)
        & is_doji(df.shift(1))
        & c2_gaps_down
        & is_bullish(df)
        & c3_gaps_up
    )


def bearish_abandoned_baby(df: pd.DataFrame) -> pd.Series:
    """#36 Bearish Abandoned Baby – bullish C1, gapped-up Doji C2, bearish gapped-down C3."""
    c2_gaps_up = df["low"].shift(1) > df["high"].shift(2)
    c3_gaps_down = df["high"] < df["low"].shift(1)
    return (
        is_bullish(df).shift(2)
        & is_doji(df.shift(1))
        & c2_gaps_up
        & is_bearish(df)
        & c3_gaps_down
    )


# ═══════════════════════════════════════════════════════════════
# KICKER PATTERNS  (41–42)
# ═══════════════════════════════════════════════════════════════

def bullish_kicker(df: pd.DataFrame, min_gap_atr_mult: float = 1.0) -> pd.Series:
    """#41 Bullish Kicker – red C1, large gap-up green C2 with no overlap."""
    _atr = atr(df)
    gap = df["open"] - df["open"].shift(1)
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & (gap >= _atr * min_gap_atr_mult)
        & (df["low"] > candle_top(df).shift(1))   # no overlap
    )


def bearish_kicker(df: pd.DataFrame, min_gap_atr_mult: float = 1.0) -> pd.Series:
    """#42 Bearish Kicker – green C1, large gap-down red C2 with no overlap."""
    _atr = atr(df)
    gap = df["open"].shift(1) - df["open"]
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & (gap >= _atr * min_gap_atr_mult)
        & (df["high"] < candle_bottom(df).shift(1))  # no overlap
    )


# ═══════════════════════════════════════════════════════════════
# BELT HOLD PATTERNS  (43–44)
# ═══════════════════════════════════════════════════════════════

def bullish_belt_hold(
    df: pd.DataFrame,
    min_body_ratio: float = 0.80,
    max_lower_wick_ratio: float = 0.05,
) -> pd.Series:
    """#43 Bullish Belt Hold – long green candle opening at low, no lower wick."""
    rng = total_range(df)
    return (
        is_bullish(df)
        & (body_ratio(df) >= min_body_ratio)
        & (lower_wick(df) / rng <= max_lower_wick_ratio)
    )


def bearish_belt_hold(
    df: pd.DataFrame,
    min_body_ratio: float = 0.80,
    max_upper_wick_ratio: float = 0.05,
) -> pd.Series:
    """#44 Bearish Belt Hold – long red candle opening at high, no upper wick."""
    rng = total_range(df)
    return (
        is_bearish(df)
        & (body_ratio(df) >= min_body_ratio)
        & (upper_wick(df) / rng <= max_upper_wick_ratio)
    )


# ═══════════════════════════════════════════════════════════════
# DOJI STAR PATTERNS  (45–46)
# ═══════════════════════════════════════════════════════════════

def doji_star(df: pd.DataFrame, min_gap_pct: float = 0.002) -> pd.Series:
    """#45 Doji Star – large candle followed by a gapped Doji."""
    strong_c1 = body_ratio(df).shift(1) >= 0.60
    gapped = (
        gap_up(df, n=1, min_pct=min_gap_pct)
        | gap_down(df, n=1, min_pct=min_gap_pct)
    )
    return strong_c1 & is_doji(df) & gapped


def bearish_doji_star(df: pd.DataFrame, min_gap_pct: float = 0.002) -> pd.Series:
    """#46 Bearish Doji Star – bullish C1 followed by gap-up Doji."""
    return (
        is_bullish(df).shift(1)
        & (body_ratio(df).shift(1) >= 0.60)
        & is_doji(df)
        & gap_up(df, n=1, min_pct=min_gap_pct)
    )


# ═══════════════════════════════════════════════════════════════
# MATCHING CLOSE PATTERNS  (47)
# ═══════════════════════════════════════════════════════════════

def matching_low(df: pd.DataFrame, tol_pct: float = 0.0015) -> pd.Series:
    """#47a Matching Low – two consecutive candles with nearly identical closes (downtrend)."""
    return (
        is_bearish(df).shift(1)
        & within_pct(df["close"], df["close"].shift(1), pct=tol_pct)
    )


def matching_high(df: pd.DataFrame, tol_pct: float = 0.0015) -> pd.Series:
    """#47b Matching High – two consecutive candles with nearly identical closes (uptrend)."""
    return (
        is_bullish(df).shift(1)
        & within_pct(df["close"], df["close"].shift(1), pct=tol_pct)
    )


# ═══════════════════════════════════════════════════════════════
# INSIDE BAR  (48)
# ═══════════════════════════════════════════════════════════════

def inside_bar(df: pd.DataFrame, max_size_ratio: float = 0.80) -> pd.Series:
    """#48 Inside Bar – current high/low fully within prior candle's high/low."""
    return (
        (df["high"] <= df["high"].shift(1))
        & (df["low"] >= df["low"].shift(1))
        & (total_range(df) <= total_range(df).shift(1) * max_size_ratio)
    )


# ═══════════════════════════════════════════════════════════════
# FOUR-CANDLE PATTERNS  (49)
# ═══════════════════════════════════════════════════════════════

def three_line_strike_bullish(df: pd.DataFrame) -> pd.Series:
    """
    #49a Bullish Three-Line Strike – three consecutive bearish closes,
    then large bullish C4 engulfing all three.
    """
    three_down = (
        is_bearish(df).shift(3)
        & is_bearish(df).shift(2)
        & is_bearish(df).shift(1)
        & (df["close"].shift(2) < df["close"].shift(3))
        & (df["close"].shift(1) < df["close"].shift(2))
    )
    c4_engulfs = (
        is_bullish(df)
        & (df["close"] > candle_top(df).shift(3))
        & (df["open"] < candle_bottom(df).shift(1))
    )
    return three_down & c4_engulfs


def three_line_strike_bearish(df: pd.DataFrame) -> pd.Series:
    """
    #49b Bearish Three-Line Strike – three consecutive bullish closes,
    then large bearish C4 engulfing all three.
    """
    three_up = (
        is_bullish(df).shift(3)
        & is_bullish(df).shift(2)
        & is_bullish(df).shift(1)
        & (df["close"].shift(2) > df["close"].shift(3))
        & (df["close"].shift(1) > df["close"].shift(2))
    )
    c4_engulfs = (
        is_bearish(df)
        & (df["close"] < candle_bottom(df).shift(3))
        & (df["open"] > candle_top(df).shift(1))
    )
    return three_up & c4_engulfs


# ═══════════════════════════════════════════════════════════════
# SEPARATING LINES  (50–51)
# ═══════════════════════════════════════════════════════════════

def bullish_separating_lines(df: pd.DataFrame, tol_pct: float = 0.001) -> pd.Series:
    """#50 Bullish Separating Lines – red C1, green C2 opens at same open."""
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & within_pct(df["open"], df["open"].shift(1), pct=tol_pct)
        & (body_ratio(df) >= 0.60)
    )


def bearish_separating_lines(df: pd.DataFrame, tol_pct: float = 0.001) -> pd.Series:
    """#51 Bearish Separating Lines – green C1, red C2 opens at same open."""
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & within_pct(df["open"], df["open"].shift(1), pct=tol_pct)
        & (body_ratio(df) >= 0.60)
    )


# ═══════════════════════════════════════════════════════════════
# MEETING LINES  (52)
# ═══════════════════════════════════════════════════════════════

def bullish_meeting_lines(df: pd.DataFrame, tol_pct: float = 0.003) -> pd.Series:
    """#52a Bullish Meeting Lines – bearish C1, bullish C2 closing at same level."""
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & within_pct(df["close"], df["close"].shift(1), pct=tol_pct)
    )


def bearish_meeting_lines(df: pd.DataFrame, tol_pct: float = 0.003) -> pd.Series:
    """#52b Bearish Meeting Lines – bullish C1, bearish C2 closing at same level."""
    return (
        is_bullish(df).shift(1)
        & is_bearish(df)
        & within_pct(df["close"], df["close"].shift(1), pct=tol_pct)
    )


# ═══════════════════════════════════════════════════════════════
# ON-NECK / IN-NECK / THRUSTING  (53–55)
# ═══════════════════════════════════════════════════════════════

def on_neck(df: pd.DataFrame, tol_pct: float = 0.0015) -> pd.Series:
    """
    #53 On-Neck – bearish C1, small bullish C2 opens below C1 low
    and closes near (at) C1 low.
    """
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & (df["open"] < df["low"].shift(1))
        & within_pct(df["close"], df["low"].shift(1), pct=tol_pct)
    )


def in_neck(df: pd.DataFrame, max_penetration: float = 0.25) -> pd.Series:
    """
    #54 In-Neck – like On-Neck but C2 closes slightly above C1 low
    (within lower 25 % of C1 body).
    """
    c1_body_bottom = candle_bottom(df).shift(1)
    c1_body_size = body(df).shift(1)
    threshold = c1_body_bottom + c1_body_size * max_penetration
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & (df["open"] < df["low"].shift(1))
        & (df["close"] > df["low"].shift(1))
        & (df["close"] <= threshold)
    )


def thrusting(df: pd.DataFrame) -> pd.Series:
    """
    #55 Thrusting – bearish C1, bullish C2 opens below C1 low
    but closes below midpoint of C1 body.
    """
    c1_mid = candle_bottom(df).shift(1) + body(df).shift(1) * 0.5
    return (
        is_bearish(df).shift(1)
        & is_bullish(df)
        & (df["open"] < df["low"].shift(1))
        & (df["close"] > df["low"].shift(1))
        & (df["close"] < c1_mid)
    )


# ═══════════════════════════════════════════════════════════════
# UPSIDE GAP TWO CROWS  (56)
# ═══════════════════════════════════════════════════════════════

def upside_gap_two_crows(df: pd.DataFrame) -> pd.Series:
    """
    #56 Upside Gap Two Crows – bullish C1, bearish C2 gaps up above C1 high,
    bearish C3 opens inside C2 and closes within C1 body (filling gap).
    """
    c1_top = candle_top(df).shift(2)
    c1_bottom = candle_bottom(df).shift(2)
    c2_top = candle_top(df).shift(1)

    c2_gaps_above_c1 = df["low"].shift(1) > df["high"].shift(2)
    c3_opens_in_c2 = (df["open"] <= c2_top) & (
        df["open"] >= candle_bottom(df).shift(1))
    c3_closes_in_c1 = (df["close"] < c1_top) & (df["close"] >= c1_bottom)

    return (
        is_bullish(df).shift(2)
        & is_bearish(df).shift(1)
        & c2_gaps_above_c1
        & is_bearish(df)
        & c3_opens_in_c2
        & c3_closes_in_c1
    )


# ═══════════════════════════════════════════════════════════════
# TRI-STAR  (57)
# ═══════════════════════════════════════════════════════════════

def tri_star_bullish(df: pd.DataFrame, min_gap_pct: float = 0.001) -> pd.Series:
    """#57a Bullish Tri-Star – three Dojis, middle gaps down, third reverses up."""
    middle_gaps_down = df["high"].shift(1) < df["low"].shift(2)
    third_reverses_up = df["low"] > df["high"].shift(1)
    return (
        is_doji(df.shift(2))
        & is_doji(df.shift(1))
        & middle_gaps_down
        & is_doji(df)
        & third_reverses_up
    )


def tri_star_bearish(df: pd.DataFrame) -> pd.Series:
    """#57b Bearish Tri-Star – three Dojis, middle gaps up, third reverses down."""
    middle_gaps_up = df["low"].shift(1) > df["high"].shift(2)
    third_reverses_down = df["high"] < df["low"].shift(1)
    return (
        is_doji(df.shift(2))
        & is_doji(df.shift(1))
        & middle_gaps_up
        & is_doji(df)
        & third_reverses_down
    )


# ═══════════════════════════════════════════════════════════════
# HIKKAKE  (58)
# ═══════════════════════════════════════════════════════════════

def hikkake_bullish(df: pd.DataFrame) -> pd.Series:
    """
    #58a Bullish Hikkake – inside bar followed by false downside breakout
    and then reversal above the inside bar high.
    Requires at least 4 bars: mother bar, inside bar, false break, reversal.
    """
    mother_high = df["high"].shift(3)
    mother_low = df["low"].shift(3)
    ib_high = df["high"].shift(2)
    ib_low = df["low"].shift(2)

    is_inside = (ib_high < mother_high) & (ib_low > mother_low)
    false_break = (df["low"].shift(1) < ib_low)  # breaks below IB low
    reversal = (df["close"] > ib_high)         # closes back above IB high
    return is_inside & false_break & reversal


def hikkake_bearish(df: pd.DataFrame) -> pd.Series:
    """
    #58b Bearish Hikkake – inside bar, false upside breakout, reversal below IB low.
    """
    mother_high = df["high"].shift(3)
    mother_low = df["low"].shift(3)
    ib_high = df["high"].shift(2)
    ib_low = df["low"].shift(2)

    is_inside = (ib_high < mother_high) & (ib_low > mother_low)
    false_break = (df["high"].shift(1) > ib_high)
    reversal = (df["close"] < ib_low)
    return is_inside & false_break & reversal


# ═══════════════════════════════════════════════════════════════
# LADDER BOTTOM  (59)
# ═══════════════════════════════════════════════════════════════

def ladder_bottom(df: pd.DataFrame, min_body_ratio: float = 0.55) -> pd.Series:
    """
    #59 Ladder Bottom – three long bearish candles, indecision C4, strong bullish C5.
    """
    three_long_bears = (
        is_bearish(df).shift(4) & (body_ratio(df).shift(4) >= min_body_ratio)
        & is_bearish(df).shift(3) & (body_ratio(df).shift(3) >= min_body_ratio)
        & is_bearish(df).shift(2) & (body_ratio(df).shift(2) >= min_body_ratio)
        & (df["close"].shift(3) < df["close"].shift(4))
        & (df["close"].shift(2) < df["close"].shift(3))
    )
    indecision_c4 = (body_ratio(df).shift(1) <= 0.30)  # small body
    strong_c5 = (
        is_bullish(df)
        & (body_ratio(df) >= min_body_ratio)
        & (df["close"] > candle_top(df).shift(1))
    )
    return three_long_bears & indecision_c4 & strong_c5


# ═══════════════════════════════════════════════════════════════
# CONCEALING BABY SWALLOW  (60)
# ═══════════════════════════════════════════════════════════════

def concealing_baby_swallow(df: pd.DataFrame) -> pd.Series:
    """
    #60 Concealing Baby Swallow – four bearish candles: two long ones gap down,
    third is small/doji, fourth engulfs the third (signals climax exhaustion).
    """
    c1_c2_bear = is_bearish(df).shift(3) & is_bearish(df).shift(2)
    c1_long = body_ratio(df).shift(3) >= 0.60
    c2_long = body_ratio(df).shift(2) >= 0.60
    c2_gap_down = df["high"].shift(
        2) < df["low"].shift(3)  # gap between C1 and C2

    c3_small = body_ratio(df).shift(1) <= 0.25
    c3_gap = df["high"].shift(1) < df["low"].shift(2)      # C3 gaps below C2

    c4_engulfs_c3 = (
        is_bearish(df)
        & (candle_top(df) >= candle_top(df).shift(1))
        & (candle_bottom(df) <= candle_bottom(df).shift(1))
    )

    return c1_c2_bear & c1_long & c2_long & c2_gap_down & c3_small & c3_gap & c4_engulfs_c3


# ═══════════════════════════════════════════════════════════════
# CONVENIENCE: registry of all patterns
# ═══════════════════════════════════════════════════════════════

PATTERNS: dict = {
    # single-candle
    "marubozu_bullish":          marubozu_bullish,
    "marubozu_bearish":          marubozu_bearish,
    "doji":                      doji,
    "dragonfly_doji":            dragonfly_doji,
    "gravestone_doji":           gravestone_doji,
    "long_legged_doji":          long_legged_doji,
    "bullish_spinning_top":      bullish_spinning_top,
    "bearish_spinning_top":      bearish_spinning_top,
    "high_wave":                 high_wave,
    "long_wick_bullish":         long_wick_bullish,
    "long_wick_bearish":         long_wick_bearish,
    "pin_bar_bullish":           pin_bar_bullish,
    "pin_bar_bearish":           pin_bar_bearish,
    "hammer":                    hammer,
    "hanging_man":               hanging_man,
    "inverted_hammer":           inverted_hammer,
    "shooting_star":             shooting_star,
    # two-candle
    "bullish_engulfing":         bullish_engulfing,
    "bearish_engulfing":         bearish_engulfing,
    "piercing_line":             piercing_line,
    "dark_cloud_cover":          dark_cloud_cover,
    "bullish_harami":            bullish_harami,
    "bearish_harami":            bearish_harami,
    "tweezer_bottom":            tweezer_bottom,
    "tweezer_top":               tweezer_top,
    "bullish_counterattack":     bullish_counterattack,
    "bearish_counterattack":     bearish_counterattack,
    "bullish_kicker":            bullish_kicker,
    "bearish_kicker":            bearish_kicker,
    "bullish_belt_hold":         bullish_belt_hold,
    "bearish_belt_hold":         bearish_belt_hold,
    "doji_star":                 doji_star,
    "bearish_doji_star":         bearish_doji_star,
    "matching_low":              matching_low,
    "matching_high":             matching_high,
    "inside_bar":                inside_bar,
    "bullish_separating_lines":  bullish_separating_lines,
    "bearish_separating_lines":  bearish_separating_lines,
    "bullish_meeting_lines":     bullish_meeting_lines,
    "bearish_meeting_lines":     bearish_meeting_lines,
    "on_neck":                   on_neck,
    "in_neck":                   in_neck,
    "thrusting":                 thrusting,
    # three-candle
    "morning_star":              morning_star,
    "evening_star":              evening_star,
    "morning_star_doji":         morning_star_doji,
    "evening_star_doji":         evening_star_doji,
    "three_white_soldiers":      three_white_soldiers,
    "three_black_crows":         three_black_crows,
    "tasuki_gap_bullish":        tasuki_gap_bullish,
    "tasuki_gap_bearish":        tasuki_gap_bearish,
    "three_outside_up":          three_outside_up,
    "three_outside_down":        three_outside_down,
    "three_inside_up":           three_inside_up,
    "three_inside_down":         three_inside_down,
    "upside_gap_two_crows":      upside_gap_two_crows,
    "tri_star_bullish":          tri_star_bullish,
    "tri_star_bearish":          tri_star_bearish,
    # four-candle
    "three_line_strike_bullish": three_line_strike_bullish,
    "three_line_strike_bearish": three_line_strike_bearish,
    "concealing_baby_swallow":   concealing_baby_swallow,
    "hikkake_bullish":           hikkake_bullish,
    "hikkake_bearish":           hikkake_bearish,
    # five-candle
    "rising_three":              rising_three,
    "falling_three":             falling_three,
    "mat_hold":                  mat_hold,
    # six-candle
    "bullish_abandoned_baby":    bullish_abandoned_baby,
    "bearish_abandoned_baby":    bearish_abandoned_baby,
    "ladder_bottom":             ladder_bottom,
}


def scan_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run every pattern in PATTERNS against df.
    Returns a boolean DataFrame with one column per pattern.
    """
    df = normalise(df)
    return pd.DataFrame({name: fn(df) for name, fn in PATTERNS.items()}, index=df.index)
