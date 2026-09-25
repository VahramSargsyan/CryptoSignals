from __future__ import annotations

import math
from typing import Iterable

import pandas as pd


def _numeric_series(values: Iterable[float] | pd.Series, *, name: str) -> pd.Series:
    series = pd.Series(values, copy=True, dtype="float64")
    series.name = name
    return series


def bollinger_bands(
    close: Iterable[float] | pd.Series,
    *,
    window: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    if window <= 1:
        raise ValueError("window must be greater than 1")
    if not math.isfinite(num_std) or num_std < 0:
        raise ValueError("num_std must be finite and non-negative")

    close_series = _numeric_series(close, name="close")
    mid = close_series.rolling(window=window).mean()
    std = close_series.rolling(window=window).std()
    return pd.DataFrame(
        {
            "bb_mid": mid,
            "bb_std": std,
            "bb_upper": mid + num_std * std,
            "bb_lower": mid - num_std * std,
        },
        index=close_series.index,
    )


def vahram_close_range_oscillator(
    close: Iterable[float] | pd.Series,
    *,
    window: int = 14,
    smooth: int = 3,
) -> pd.Series:
    """Frozen historical Close-range normalization formerly named StochRSI."""
    if window <= 1 or smooth <= 0:
        raise ValueError("window must be > 1 and smooth must be > 0")

    close_series = _numeric_series(close, name="close")
    rolling_min = close_series.rolling(window=window).min()
    rolling_max = close_series.rolling(window=window).max()
    denominator = rolling_max - rolling_min
    raw = (close_series - rolling_min) / denominator
    result = raw.rolling(window=smooth).mean() * 100.0
    result.name = "vahram_close_range"
    return result


def rsi_wilder(
    close: Iterable[float] | pd.Series,
    *,
    window: int = 14,
) -> pd.Series:
    """Wilder RSI with an SMA seed followed by Wilder recursive smoothing."""
    if window <= 1:
        raise ValueError("window must be greater than 1")

    close_series = _numeric_series(close, name="close")
    result = pd.Series(float("nan"), index=close_series.index, dtype="float64", name="rsi")
    if len(close_series) <= window:
        return result

    delta = close_series.diff()
    gains = delta.clip(lower=0.0)
    losses = -delta.clip(upper=0.0)

    avg_gain = gains.iloc[1 : window + 1].mean()
    avg_loss = losses.iloc[1 : window + 1].mean()

    def rsi_from_averages(gain: float, loss: float) -> float:
        if gain == 0 and loss == 0:
            return 50.0
        if loss == 0:
            return 100.0
        if gain == 0:
            return 0.0
        rs = gain / loss
        return 100.0 - (100.0 / (1.0 + rs))

    result.iloc[window] = rsi_from_averages(avg_gain, avg_loss)

    for position in range(window + 1, len(close_series)):
        avg_gain = ((avg_gain * (window - 1)) + gains.iloc[position]) / window
        avg_loss = ((avg_loss * (window - 1)) + losses.iloc[position]) / window
        result.iloc[position] = rsi_from_averages(avg_gain, avg_loss)

    return result


def true_stoch_rsi(
    close: Iterable[float] | pd.Series,
    *,
    rsi_window: int = 14,
    stoch_window: int = 14,
    smooth_k: int = 3,
    smooth_d: int = 3,
) -> pd.DataFrame:
    if stoch_window <= 1 or smooth_k <= 0 or smooth_d <= 0:
        raise ValueError("stoch_window must be > 1 and smoothing windows must be > 0")

    rsi = rsi_wilder(close, window=rsi_window)
    rsi_min = rsi.rolling(window=stoch_window).min()
    rsi_max = rsi.rolling(window=stoch_window).max()
    denominator = rsi_max - rsi_min
    raw = ((rsi - rsi_min) / denominator) * 100.0
    k = raw.rolling(window=smooth_k).mean()
    d = k.rolling(window=smooth_d).mean()
    return pd.DataFrame(
        {
            "rsi": rsi,
            "stoch_rsi_raw": raw,
            "stoch_rsi_k": k,
            "stoch_rsi_d": d,
        },
        index=rsi.index,
    )


def ema_sma_seed(
    values: Iterable[float] | pd.Series,
    *,
    window: int,
    name: str = "ema",
) -> pd.Series:
    """EMA seeded from the first complete SMA window, then recursively smoothed."""
    if window <= 1:
        raise ValueError("window must be greater than 1")

    series = _numeric_series(values, name=name)
    result = pd.Series(float("nan"), index=series.index, dtype="float64", name=name)
    valid_positions = [position for position, value in enumerate(series) if pd.notna(value)]
    if len(valid_positions) < window:
        return result

    seed_positions = valid_positions[:window]
    seed_position = seed_positions[-1]
    seed = float(series.iloc[seed_positions].mean())
    result.iloc[seed_position] = seed
    alpha = 2.0 / (window + 1.0)
    previous = seed

    for position in range(seed_position + 1, len(series)):
        value = series.iloc[position]
        if pd.isna(value):
            continue
        previous = ((float(value) - previous) * alpha) + previous
        result.iloc[position] = previous

    return result


def macd(
    close: Iterable[float] | pd.Series,
    *,
    fast_window: int = 12,
    slow_window: int = 26,
    signal_window: int = 9,
) -> pd.DataFrame:
    if fast_window <= 1 or slow_window <= 1 or signal_window <= 1:
        raise ValueError("MACD windows must be greater than 1")
    if fast_window >= slow_window:
        raise ValueError("fast_window must be smaller than slow_window")

    close_series = _numeric_series(close, name="close")
    fast = ema_sma_seed(close_series, window=fast_window, name="ema_fast")
    slow = ema_sma_seed(close_series, window=slow_window, name="ema_slow")
    line = (fast - slow).rename("macd")
    signal = ema_sma_seed(line, window=signal_window, name="macd_signal")
    histogram = (line - signal).rename("macd_hist")
    return pd.DataFrame(
        {
            "ema_fast": fast,
            "ema_slow": slow,
            "macd": line,
            "macd_signal": signal,
            "macd_hist": histogram,
        },
        index=close_series.index,
    )


def simple_moving_average(
    values: Iterable[float] | pd.Series,
    *,
    window: int,
    name: str = "sma",
) -> pd.Series:
    if window <= 0:
        raise ValueError("window must be greater than 0")
    result = _numeric_series(values, name=name).rolling(window=window).mean()
    result.name = name
    return result


def moving_average_trend_features(
    close: Iterable[float] | pd.Series,
    *,
    fast_window: int = 50,
    medium_window: int = 100,
    slow_window: int = 200,
) -> pd.DataFrame:
    if not (0 < fast_window < medium_window < slow_window):
        raise ValueError("moving-average windows must satisfy fast < medium < slow")

    close_series = _numeric_series(close, name="close")
    sma_fast = simple_moving_average(close_series, window=fast_window, name=f"sma_{fast_window}")
    sma_medium = simple_moving_average(
        close_series,
        window=medium_window,
        name=f"sma_{medium_window}",
    )
    sma_slow = simple_moving_average(close_series, window=slow_window, name=f"sma_{slow_window}")

    return pd.DataFrame(
        {
            f"sma_{fast_window}": sma_fast,
            f"sma_{medium_window}": sma_medium,
            f"sma_{slow_window}": sma_slow,
            f"close_above_sma_{fast_window}": close_series > sma_fast,
            f"close_above_sma_{medium_window}": close_series > sma_medium,
            f"close_above_sma_{slow_window}": close_series > sma_slow,
            "ma_bull_stack": (sma_fast > sma_medium) & (sma_medium > sma_slow),
            "ma_bear_stack": (sma_fast < sma_medium) & (sma_medium < sma_slow),
        },
        index=close_series.index,
    )


def volume_moving_average(
    volume: Iterable[float] | pd.Series,
    *,
    window: int = 20,
) -> pd.Series:
    if window <= 0:
        raise ValueError("window must be greater than 0")
    result = _numeric_series(volume, name="volume").rolling(window=window).mean()
    result.name = "volume_ma"
    return result


def candle_body_strength(
    open_: Iterable[float] | pd.Series,
    high: Iterable[float] | pd.Series,
    low: Iterable[float] | pd.Series,
    close: Iterable[float] | pd.Series,
) -> pd.Series:
    open_series = _numeric_series(open_, name="open")
    high_series = _numeric_series(high, name="high")
    low_series = _numeric_series(low, name="low")
    close_series = _numeric_series(close, name="close")

    candle_range = high_series - low_series
    body = (close_series - open_series).abs()
    strength = body / candle_range.where(candle_range != 0)
    strength = strength.fillna(0.0)
    strength.name = "candle_body_strength"
    return strength


def build_standard_features(candles: pd.DataFrame) -> pd.DataFrame:
    required = {"open", "high", "low", "close", "volume"}
    missing = sorted(required.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing candle columns for indicators: {missing}")

    features = pd.DataFrame(index=candles.index)
    features = features.join(bollinger_bands(candles["close"]))
    features["vahram_close_range"] = vahram_close_range_oscillator(candles["close"])
    stoch = true_stoch_rsi(candles["close"])
    features = features.join(stoch)
    macd_values = macd(candles["close"])
    features = features.join(macd_values)
    ma_trend = moving_average_trend_features(candles["close"])
    features = features.join(ma_trend)
    features["volume_ma"] = volume_moving_average(candles["volume"])
    features["candle_body_strength"] = candle_body_strength(
        candles["open"],
        candles["high"],
        candles["low"],
        candles["close"],
    )
    return features
