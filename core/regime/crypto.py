from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Mapping

import pandas as pd

from core.data.candles import HistoricalDataset


@dataclass(frozen=True)
class CryptoRegimeConfig:
    benchmark_symbol: str = "BTCUSDT"
    trend_fast_window: int = 50
    trend_slow_window: int = 200
    breadth_window: int = 50
    volume_window: int = 20
    volatility_window: int = 20
    volatility_baseline_window: int = 252
    minimum_breadth_assets: int = 5
    breadth_bull_threshold: float = 0.60
    breadth_bear_threshold: float = 0.40
    volume_high_threshold: float = 0.60
    volume_low_threshold: float = 0.40
    volatility_high_multiple: float = 1.25
    volatility_low_multiple: float = 0.80

    def __post_init__(self) -> None:
        if self.trend_fast_window <= 1 or self.trend_slow_window <= self.trend_fast_window:
            raise ValueError("trend windows must satisfy 1 < fast < slow")
        if min(
            self.breadth_window,
            self.volume_window,
            self.volatility_window,
            self.volatility_baseline_window,
            self.minimum_breadth_assets,
        ) <= 0:
            raise ValueError("regime windows and minimum asset count must be positive")
        if not 0 <= self.breadth_bear_threshold < self.breadth_bull_threshold <= 1:
            raise ValueError("breadth thresholds must be ordered inside [0, 1]")
        if not 0 <= self.volume_low_threshold < self.volume_high_threshold <= 1:
            raise ValueError("volume thresholds must be ordered inside [0, 1]")
        if not 0 < self.volatility_low_multiple < self.volatility_high_multiple:
            raise ValueError("volatility multiples must be positive and ordered")


def _series_from_dataset(dataset: HistoricalDataset, column: str) -> pd.Series:
    candles = dataset.candles.copy()
    if column not in candles.columns:
        raise ValueError(f"Dataset {dataset.symbol} is missing {column}")
    if dataset.quality.has_critical_issues:
        raise ValueError(f"Dataset {dataset.symbol} has critical quality issues")
    timestamps = pd.to_datetime(candles["timestamp"], utc=True)
    if timestamps.duplicated().any():
        raise ValueError(f"Dataset {dataset.symbol} has duplicate timestamps")
    series = pd.Series(
        pd.to_numeric(candles[column], errors="raise").to_numpy(),
        index=timestamps,
        name=dataset.symbol,
        dtype="float64",
    )
    return series.sort_index(kind="stable")


def _assemble_panel(
    datasets: Mapping[str, HistoricalDataset],
    column: str,
) -> pd.DataFrame:
    series = []
    for symbol in sorted(datasets):
        series.append(_series_from_dataset(datasets[symbol], column).rename(symbol.upper()))
    if not series:
        raise ValueError("At least one dataset is required")
    return pd.concat(series, axis=1).sort_index(kind="stable")


def _three_way_label(
    values: pd.Series,
    *,
    high: float,
    low: float,
    high_label: str,
    mid_label: str,
    low_label: str,
) -> pd.Series:
    result = pd.Series("UNKNOWN", index=values.index, dtype="object")
    valid = values.notna()
    result.loc[valid & (values >= high)] = high_label
    result.loc[valid & (values <= low)] = low_label
    result.loc[valid & (values < high) & (values > low)] = mid_label
    return result


def build_crypto_regime(
    datasets: Mapping[str, HistoricalDataset],
    *,
    config: CryptoRegimeConfig | None = None,
) -> pd.DataFrame:
    """Build trailing-only market regimes from canonical daily datasets."""
    cfg = config or CryptoRegimeConfig()
    benchmark = cfg.benchmark_symbol.upper()
    normalized = {symbol.upper(): dataset for symbol, dataset in datasets.items()}
    if benchmark not in normalized:
        raise ValueError(f"Benchmark dataset {benchmark} is required")

    close_panel = _assemble_panel(normalized, "close")
    volume_panel = _assemble_panel(normalized, "volume")
    btc_close = close_panel[benchmark]

    btc_fast = btc_close.rolling(cfg.trend_fast_window).mean()
    btc_slow = btc_close.rolling(cfg.trend_slow_window).mean()
    btc_trend = pd.Series("UNKNOWN", index=close_panel.index, dtype="object")
    trend_ready = btc_close.notna() & btc_fast.notna() & btc_slow.notna()
    btc_trend.loc[
        trend_ready & (btc_close > btc_slow) & (btc_fast > btc_slow)
    ] = "BTC_BULL"
    btc_trend.loc[
        trend_ready & (btc_close < btc_slow) & (btc_fast < btc_slow)
    ] = "BTC_BEAR"
    btc_trend.loc[
        trend_ready & (btc_trend == "UNKNOWN")
    ] = "BTC_TRANSITION"

    breadth_sma = close_panel.rolling(cfg.breadth_window).mean()
    breadth_eligible = close_panel.notna() & breadth_sma.notna()
    breadth_assets = breadth_eligible.sum(axis=1)
    breadth_above = ((close_panel > breadth_sma) & breadth_eligible).sum(axis=1)
    market_breadth = (breadth_above / breadth_assets).where(
        breadth_assets >= cfg.minimum_breadth_assets
    )
    breadth_regime = _three_way_label(
        market_breadth,
        high=cfg.breadth_bull_threshold,
        low=cfg.breadth_bear_threshold,
        high_label="BREADTH_STRONG",
        mid_label="BREADTH_MIXED",
        low_label="BREADTH_WEAK",
    )
    broad_crypto_trend = breadth_regime.map(
        {
            "BREADTH_STRONG": "BROAD_BULL",
            "BREADTH_MIXED": "BROAD_SIDEWAYS",
            "BREADTH_WEAK": "BROAD_BEAR",
            "UNKNOWN": "UNKNOWN",
        }
    )

    volume_ma = volume_panel.rolling(cfg.volume_window).mean()
    volume_eligible = volume_panel.notna() & volume_ma.notna()
    volume_assets = volume_eligible.sum(axis=1)
    volume_above = ((volume_panel > volume_ma) & volume_eligible).sum(axis=1)
    volume_breadth = (volume_above / volume_assets).where(
        volume_assets >= cfg.minimum_breadth_assets
    )
    volume_regime = _three_way_label(
        volume_breadth,
        high=cfg.volume_high_threshold,
        low=cfg.volume_low_threshold,
        high_label="VOLUME_HIGH",
        mid_label="VOLUME_NORMAL",
        low_label="VOLUME_LOW",
    )

    btc_returns = btc_close.pct_change(fill_method=None)
    btc_realized_vol = btc_returns.rolling(cfg.volatility_window).std() * sqrt(365.0)
    btc_vol_baseline = btc_realized_vol.rolling(cfg.volatility_baseline_window).median()
    volatility_regime = pd.Series("UNKNOWN", index=close_panel.index, dtype="object")
    volatility_ready = (
        btc_realized_vol.notna()
        & btc_vol_baseline.notna()
        & (btc_vol_baseline > 0)
    )
    vol_ratio = btc_realized_vol / btc_vol_baseline
    volatility_regime.loc[
        volatility_ready & (vol_ratio >= cfg.volatility_high_multiple)
    ] = "VOL_HIGH"
    volatility_regime.loc[
        volatility_ready & (vol_ratio <= cfg.volatility_low_multiple)
    ] = "VOL_LOW"
    volatility_regime.loc[
        volatility_ready & (volatility_regime == "UNKNOWN")
    ] = "VOL_NORMAL"

    frame = pd.DataFrame(
        {
            "timestamp": close_panel.index,
            "btc_close": btc_close.to_numpy(),
            "btc_sma_fast": btc_fast.to_numpy(),
            "btc_sma_slow": btc_slow.to_numpy(),
            "btc_trend": btc_trend.to_numpy(),
            "btc_realized_vol": btc_realized_vol.to_numpy(),
            "btc_vol_baseline": btc_vol_baseline.to_numpy(),
            "volatility_regime": volatility_regime.to_numpy(),
            "market_breadth": market_breadth.to_numpy(),
            "breadth_assets": breadth_assets.to_numpy(),
            "breadth_regime": breadth_regime.to_numpy(),
            "broad_crypto_trend": broad_crypto_trend.to_numpy(),
            "volume_breadth": volume_breadth.to_numpy(),
            "volume_assets": volume_assets.to_numpy(),
            "volume_regime": volume_regime.to_numpy(),
        }
    )
    frame["regime_id"] = (
        frame["btc_trend"].astype(str)
        + "|"
        + frame["broad_crypto_trend"].astype(str)
        + "|"
        + frame["volatility_regime"].astype(str)
        + "|"
        + frame["volume_regime"].astype(str)
    )
    return frame.reset_index(drop=True)
