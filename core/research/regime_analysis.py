from __future__ import annotations

from typing import Iterable

import pandas as pd

REGIME_DIMENSIONS = (
    "btc_trend",
    "broad_crypto_trend",
    "volatility_regime",
    "breadth_regime",
    "volume_regime",
    "regime_id",
)


def _regime_lookup(regime_frame: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", *REGIME_DIMENSIONS}
    missing = sorted(required.difference(regime_frame.columns))
    if missing:
        raise ValueError(f"Missing regime columns: {missing}")
    lookup = regime_frame.loc[:, ["timestamp", *REGIME_DIMENSIONS]].copy()
    lookup["timestamp"] = pd.to_datetime(lookup["timestamp"], utc=True)
    if lookup["timestamp"].duplicated().any():
        raise ValueError("Regime timestamps must be unique")
    return lookup


def attach_regime_to_events(
    events: pd.DataFrame,
    regime_frame: pd.DataFrame,
) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    if "signal_timestamp" not in events.columns:
        raise ValueError("Event observations require signal_timestamp")
    lookup = _regime_lookup(regime_frame)
    result = events.copy()
    result["signal_timestamp"] = pd.to_datetime(result["signal_timestamp"], utc=True)
    result = result.merge(
        lookup,
        left_on="signal_timestamp",
        right_on="timestamp",
        how="left",
        validate="many_to_one",
    ).drop(columns=["timestamp"])
    for column in REGIME_DIMENSIONS:
        result[column] = result[column].fillna("UNKNOWN")
    result["calendar_year"] = result["signal_timestamp"].dt.year.astype(int)
    return result


def attach_regime_to_trades(
    trades: pd.DataFrame,
    regime_frame: pd.DataFrame,
) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    if "entry_signal_timestamp" not in trades.columns:
        raise ValueError("Trade records require entry_signal_timestamp")
    lookup = _regime_lookup(regime_frame)
    result = trades.copy()
    result["entry_signal_timestamp"] = pd.to_datetime(
        result["entry_signal_timestamp"],
        utc=True,
    )
    result = result.merge(
        lookup,
        left_on="entry_signal_timestamp",
        right_on="timestamp",
        how="left",
        validate="many_to_one",
    ).drop(columns=["timestamp"])
    for column in REGIME_DIMENSIONS:
        result[column] = result[column].fillna("UNKNOWN")
    result["calendar_year"] = result["entry_signal_timestamp"].dt.year.astype(int)
    return result


def summarize_events_by_regime(
    events: pd.DataFrame,
    *,
    dimensions: Iterable[str] = REGIME_DIMENSIONS,
    group_by_symbol: bool = True,
    group_by_year: bool = False,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    rows = []
    for dimension in dimensions:
        if dimension not in events.columns:
            raise ValueError(f"Missing regime dimension: {dimension}")
        group_columns = [
            "strategy_id",
            "strategy_version",
            "timeframe",
            "signal",
            "horizon_candles",
        ]
        if group_by_symbol:
            group_columns.insert(2, "symbol")
        if group_by_year:
            if "calendar_year" not in events.columns:
                raise ValueError("calendar_year is required for yearly regime summary")
            group_columns.append("calendar_year")
        group_columns.append(dimension)

        for keys, group in events.groupby(group_columns, dropna=False):
            values = dict(zip(group_columns, keys if isinstance(keys, tuple) else (keys,)))
            directional = group["directional_return"].astype(float)
            row = {
                "regime_dimension": dimension,
                "regime_value": str(values.pop(dimension)),
                **values,
                "observation_count": int(len(group)),
                "win_rate": float((directional > 0).mean()),
                "average_directional_return": float(directional.mean()),
                "median_directional_return": float(directional.median()),
            }
            row["symbol_scope"] = row.get("symbol", "ALL_SYMBOLS_POOLED")
            rows.append(row)
    return pd.DataFrame(rows)


def _profit_factor(returns: pd.Series) -> float | None:
    positive = float(returns[returns > 0].sum())
    negative = float(-returns[returns < 0].sum())
    if negative > 0:
        return positive / negative
    return None


def summarize_trades_by_regime(
    trades: pd.DataFrame,
    *,
    dimensions: Iterable[str] = REGIME_DIMENSIONS,
    group_by_symbol: bool = True,
    group_by_year: bool = False,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    rows = []
    for dimension in dimensions:
        if dimension not in trades.columns:
            raise ValueError(f"Missing regime dimension: {dimension}")
        group_columns = [
            "strategy_id",
            "strategy_version",
            "timeframe",
        ]
        if group_by_symbol:
            group_columns.insert(2, "symbol")
        if group_by_year:
            if "calendar_year" not in trades.columns:
                raise ValueError("calendar_year is required for yearly regime summary")
            group_columns.append("calendar_year")
        group_columns.append(dimension)

        for keys, group in trades.groupby(group_columns, dropna=False):
            values = dict(zip(group_columns, keys if isinstance(keys, tuple) else (keys,)))
            returns = group["net_return"].astype(float)
            row = {
                "regime_dimension": dimension,
                "regime_value": str(values.pop(dimension)),
                **values,
                "trade_count": int(len(group)),
                "win_rate": float((returns > 0).mean()),
                "average_return": float(returns.mean()),
                "median_return": float(returns.median()),
                "profit_factor": _profit_factor(returns),
            }
            row["symbol_scope"] = row.get("symbol", "ALL_SYMBOLS_POOLED")
            rows.append(row)
    return pd.DataFrame(rows)
