from __future__ import annotations

import argparse
import json
import math
import zipfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

MID_TARGET_PCTS = (
    0.26, 0.28, 0.31, 0.35, 0.43, 0.53, 0.61, 0.70,
    0.80, 0.92, 1.02, 1.17, 1.30, 1.50, 1.75, 2.12,
)
MAIN_LEVELS = 16
TOTAL_SUBLEVELS = 64
EVALUATION_START = pd.Timestamp("2023-09-25T00:00:00Z")
FEE_RATE = 0.001
SLIPPAGE_RATE = 0.0005
MID_RECOVERY_SUBLEVELS = 18


@dataclass(frozen=True)
class Result:
    total_return: float
    max_drawdown: float
    closed_trades: int


def load_artifact(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        matches = [name for name in archive.namelist() if name.endswith("canonical_candles.csv")]
        if len(matches) != 1:
            raise ValueError(f"{path}: expected one canonical_candles.csv, found {matches}")
        with archive.open(matches[0]) as handle:
            frame = pd.read_csv(handle)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame.sort_values("timestamp", kind="stable").reset_index(drop=True)


def _weights() -> np.ndarray:
    values = np.arange(1, MAIN_LEVELS + 1, dtype=float)
    return values / values.sum()


def _boundary(grid: tuple[float, float], sublevel: int) -> float:
    high, low = grid
    return high - ((high - low) * (sublevel / TOTAL_SUBLEVELS))


def _buy_limit_fill(candle: pd.Series, limit_price: float) -> float:
    raw = min(float(candle["open"]), limit_price)
    return min(limit_price, raw * (1.0 + SLIPPAGE_RATE))


def _sell_limit_fill(candle: pd.Series, limit_price: float) -> float:
    raw = max(float(candle["open"]), limit_price)
    return max(limit_price, raw * (1.0 - SLIPPAGE_RATE))


def _buy_stop_fill(candle: pd.Series, trigger: float) -> float:
    return max(float(candle["open"]), trigger) * (1.0 + SLIPPAGE_RATE)


def _sell_stop_fill(candle: pd.Series, trigger: float) -> float:
    return min(float(candle["open"]), trigger) * (1.0 - SLIPPAGE_RATE)


def _atr(frame: pd.DataFrame, period: int = 14) -> pd.Series:
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    previous = close.shift(1)
    true_range = pd.concat(
        [(high - low).abs(), (high - previous).abs(), (low - previous).abs()], axis=1
    ).max(axis=1)
    return true_range.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def causal_hl_schedule(
    frame: pd.DataFrame,
    *,
    lookback: int = 1095,
    refresh_candles: int = 30,
) -> list[tuple[float, float] | None]:
    schedule: list[tuple[float, float] | None] = [None] * len(frame)
    active = None
    last_refresh = None
    for position, candle in frame.iterrows():
        if candle["timestamp"] < EVALUATION_START:
            continue
        history_start = max(0, position - lookback)
        if position - history_start < lookback:
            schedule[position] = active
            continue
        if active is None or last_refresh is None or position - last_refresh >= refresh_candles:
            history = frame.iloc[history_start:position]
            active = (float(history["high"].max()), float(history["low"].min()))
            last_refresh = position
        schedule[position] = active
    return schedule


def atr_gated_hl_schedule(
    frame: pd.DataFrame,
    *,
    lookback: int = 1095,
    cooldown_candles: int = 60,
    atr_period: int = 14,
    atr_regrid_threshold: float = 0.50,
) -> list[tuple[float, float] | None]:
    """MID adaptation of the OSS dynamic-regrid idea.

    H/L remains our 1095-candle causal range. Unlike the canonical monthly refresh,
    range geometry is rebuilt only when ATR has moved far enough from the ATR at the
    previous rebuild (or price escapes the current range).
    """
    atr = _atr(frame, atr_period)
    schedule: list[tuple[float, float] | None] = [None] * len(frame)
    active = None
    last_refresh = None
    atr_anchor = None
    for position, candle in frame.iterrows():
        if candle["timestamp"] < EVALUATION_START:
            continue
        history_start = max(0, position - lookback)
        if position - history_start < lookback:
            schedule[position] = active
            continue
        if active is None:
            history = frame.iloc[history_start:position]
            active = (float(history["high"].max()), float(history["low"].min()))
            last_refresh = position
            atr_anchor = float(atr.iloc[position])
            schedule[position] = active
            continue
        if position - int(last_refresh) >= cooldown_candles:
            atr_now = float(atr.iloc[position])
            close = float(candle["close"])
            atr_shift = (
                abs((atr_now / atr_anchor) - 1.0)
                if atr_anchor and math.isfinite(atr_now) and atr_anchor > 0
                else 0.0
            )
            if atr_shift > atr_regrid_threshold or close > active[0] or close < active[1]:
                history = frame.iloc[history_start:position]
                active = (float(history["high"].max()), float(history["low"].min()))
                last_refresh = position
                atr_anchor = atr_now
        schedule[position] = active
    return schedule


def run_mid(
    frame: pd.DataFrame,
    *,
    schedule: list[tuple[float, float] | None] | None = None,
    entry_retracement: float | None = None,
    exit_retracement: float | None = None,
    exposure_cap: float | None = None,
    initial_capital: float = 1000.0,
) -> Result:
    if schedule is None:
        schedule = causal_hl_schedule(frame)
    first = int(frame.index[frame["timestamp"] >= EVALUATION_START][0])
    weights = _weights()
    cash = {level: initial_capital * weights[level - 1] for level in range(1, 17)}
    lots: dict[int, dict] = {}
    armed_entries: dict[int, dict] = {}
    peak_equity = initial_capital
    max_drawdown = 0.0
    closed_trades = 0
    final_equity = initial_capital

    for position, candle in frame.iterrows():
        if position < first:
            continue
        grid = schedule[position]
        exited_today: set[int] = set()

        for level, lot in list(lots.items()):
            if lot["entry_position"] >= position:
                continue
            if exit_retracement is None:
                if float(candle["high"]) < lot["target"]:
                    continue
                fill = _sell_limit_fill(candle, lot["target"])
            else:
                if lot.get("exit_armed", False):
                    stop = lot["peak"] * (1.0 - exit_retracement)
                    if float(candle["low"]) > stop:
                        lot["peak"] = max(lot["peak"], float(candle["high"]))
                        continue
                    fill = _sell_stop_fill(candle, stop)
                else:
                    if float(candle["high"]) < lot["target"]:
                        continue
                    lot["exit_armed"] = True
                    lot["peak"] = float(candle["high"])
                    continue

            proceeds = lot["units"] * fill * (1.0 - FEE_RATE)
            cash[level] = proceeds
            del lots[level]
            exited_today.add(level)
            closed_trades += 1

        if entry_retracement is not None:
            for level, armed in list(armed_entries.items()):
                if level in lots or level in exited_today:
                    armed_entries.pop(level, None)
                    continue
                trigger = armed["trough"] * (1.0 + entry_retracement)
                if float(candle["high"]) >= trigger:
                    budget = cash[level]
                    fill = _buy_stop_fill(candle, trigger)
                    units = (budget * (1.0 - FEE_RATE)) / fill
                    entry_grid = armed["grid"]
                    sublevel = level * 4
                    percent_target = fill * (1.0 + MID_TARGET_PCTS[level - 1])
                    grid_target = (
                        _boundary(entry_grid, max(0, sublevel - MID_RECOVERY_SUBLEVELS))
                        if level >= 7
                        else None
                    )
                    target = min(percent_target, grid_target) if grid_target is not None else percent_target
                    lots[level] = {
                        "entry_position": position,
                        "entry_price": fill,
                        "units": units,
                        "invested": budget,
                        "target": target,
                        "exit_armed": False,
                    }
                    cash[level] = 0.0
                    armed_entries.pop(level, None)
                else:
                    armed["trough"] = min(armed["trough"], float(candle["low"]))

        if grid is not None:
            close = float(candle["close"])
            marked_equity = sum(cash.values()) + sum(
                lot["units"] * close * (1.0 - FEE_RATE) for lot in lots.values()
            )
            for level in range(1, 17):
                if level in lots or level in exited_today or level in armed_entries:
                    continue
                sublevel = level * 4
                limit_price = _boundary(grid, sublevel)
                if float(candle["low"]) > limit_price:
                    continue
                budget = cash[level]
                if budget <= 0:
                    continue
                if exposure_cap is not None:
                    deployed_cost = sum(lot["invested"] for lot in lots.values())
                    if deployed_cost + budget > exposure_cap * max(marked_equity, 1e-12):
                        continue
                if entry_retracement is not None:
                    armed_entries[level] = {
                        "trough": float(candle["low"]),
                        "grid": grid,
                    }
                    continue

                fill = _buy_limit_fill(candle, limit_price)
                units = (budget * (1.0 - FEE_RATE)) / fill
                percent_target = fill * (1.0 + MID_TARGET_PCTS[level - 1])
                grid_target = (
                    _boundary(grid, max(0, sublevel - MID_RECOVERY_SUBLEVELS))
                    if level >= 7
                    else None
                )
                target = min(percent_target, grid_target) if grid_target is not None else percent_target
                lots[level] = {
                    "entry_position": position,
                    "entry_price": fill,
                    "units": units,
                    "invested": budget,
                    "target": target,
                    "exit_armed": False,
                }
                cash[level] = 0.0

        close = float(candle["close"])
        final_equity = sum(cash.values()) + sum(
            lot["units"] * close * (1.0 - FEE_RATE) for lot in lots.values()
        )
        peak_equity = max(peak_equity, final_equity)
        max_drawdown = max(max_drawdown, 1.0 - (final_equity / peak_equity))

    return Result(
        total_return=(final_equity / initial_capital) - 1.0,
        max_drawdown=max_drawdown,
        closed_trades=closed_trades,
    )


def run_jordan_dynamic_grid_port(
    frame: pd.DataFrame,
    *,
    initial_capital: float = 1000.0,
    num_grids: int = 8,
    atr_period: int = 14,
    atr_spacing_multiplier: float = 0.5,
    regrid_threshold: float = 0.30,
    cooldown_bars: int = 60,
) -> Result:
    """Daily OHLC port of the public dynamic simple-grid settings.

    This is NOT execution of the upstream package. Its live state/order manager is
    intentionally replaced by a deterministic daily-OHLC cell model so the same
    frozen datasets can be used.
    """
    atr = _atr(frame, atr_period)
    first = int(frame.index[frame["timestamp"] >= EVALUATION_START][0])
    quote = initial_capital
    base = 0.0
    cells: list[dict] = []
    atr_anchor = None
    bars_since_regrid = 0
    peak_equity = initial_capital
    max_drawdown = 0.0
    closed_events = 0

    def market_buy_quote(quote_amount: float, price: float) -> None:
        nonlocal quote, base
        spend = min(quote_amount, quote)
        fill = price * (1.0 + SLIPPAGE_RATE)
        units = (spend * (1.0 - FEE_RATE)) / fill
        quote -= spend
        base += units

    def rebuild(center: float, atr_value: float) -> bool:
        nonlocal cells, atr_anchor, bars_since_regrid
        spacing = atr_spacing_multiplier * atr_value
        bottom = center - ((num_grids - 1) / 2.0) * spacing
        if bottom <= 0:
            return False
        levels = [bottom + index * spacing for index in range(num_grids)]
        total_equity = quote + base * center * (1.0 - FEE_RATE)
        size_units = (total_equity / num_grids) / center
        cells = []
        for lower, upper in zip(levels[:-1], levels[1:]):
            state = "QUOTE" if upper <= center or lower < center < upper else "BASE"
            cells.append(
                {
                    "lower": lower,
                    "upper": upper,
                    "state": state,
                    "units": size_units if state == "BASE" else 0.0,
                    "size_units": size_units,
                }
            )
        atr_anchor = atr_value
        bars_since_regrid = 0
        return True

    first_candle = frame.iloc[first]
    initial_atr = float(atr.iloc[first])
    market_buy_quote(initial_capital * 0.5, float(first_candle["close"]))
    rebuild(float(first_candle["close"]), initial_atr)
    final_equity = initial_capital

    for position, candle in frame.iterrows():
        if position < first:
            continue
        for cell in cells:
            if cell["state"] == "QUOTE" and float(candle["low"]) <= cell["lower"]:
                fill = _buy_limit_fill(candle, cell["lower"])
                units = cell["size_units"]
                required_quote = (units * fill) / (1.0 - FEE_RATE)
                if quote >= required_quote:
                    quote -= required_quote
                    base += units
                    cell["units"] = units
                    cell["state"] = "BASE"
                    closed_events += 1
            elif cell["state"] == "BASE" and float(candle["high"]) >= cell["upper"]:
                units = min(cell["units"], base)
                if units > 0:
                    fill = _sell_limit_fill(candle, cell["upper"])
                    quote += units * fill * (1.0 - FEE_RATE)
                    base -= units
                    cell["units"] = 0.0
                    cell["state"] = "QUOTE"
                    closed_events += 1

        atr_now = float(atr.iloc[position])
        bars_since_regrid += 1
        if (
            atr_anchor
            and math.isfinite(atr_now)
            and atr_now > 0
            and bars_since_regrid >= cooldown_bars
            and abs((atr_now / atr_anchor) - 1.0) > regrid_threshold
        ):
            rebuild(float(candle["close"]), atr_now)

        final_equity = quote + base * float(candle["close"]) * (1.0 - FEE_RATE)
        peak_equity = max(peak_equity, final_equity)
        max_drawdown = max(max_drawdown, 1.0 - (final_equity / peak_equity))

    return Result(
        total_return=(final_equity / initial_capital) - 1.0,
        max_drawdown=max_drawdown,
        closed_trades=closed_events,
    )


def aggregate(results: dict[str, Result]) -> dict:
    growth = [1.0 + result.total_return for result in results.values()]
    return {
        "geometric_total_return": float(np.prod(growth) ** (1.0 / len(growth)) - 1.0),
        "median_max_drawdown": float(np.median([r.max_drawdown for r in results.values()])),
        "closed_trades": int(sum(r.closed_trades for r in results.values())),
    }


def _parse_artifact(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("artifact must be SYMBOL=/path/to.zip")
    symbol, raw = value.split("=", 1)
    return symbol.upper(), Path(raw)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", action="append", type=_parse_artifact, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    datasets = {symbol: load_artifact(path) for symbol, path in args.artifact}

    candidates: dict[str, dict[str, Result]] = {}
    candidates["MID_BASELINE"] = {symbol: run_mid(frame) for symbol, frame in datasets.items()}

    for value in (0.005, 0.01, 0.02, 0.03, 0.05):
        candidates[f"MID_ENTRY_RETRACE_{value:.3f}"] = {
            symbol: run_mid(frame, entry_retracement=value) for symbol, frame in datasets.items()
        }
    for value in (0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10):
        candidates[f"MID_EXIT_RETRACE_{value:.2f}"] = {
            symbol: run_mid(frame, exit_retracement=value) for symbol, frame in datasets.items()
        }
    for value in (0.15, 0.20, 0.30, 0.40, 0.50):
        candidates[f"MID_ATR_REGRID_GATE_{value:.2f}"] = {
            symbol: run_mid(
                frame,
                schedule=atr_gated_hl_schedule(frame, atr_regrid_threshold=value),
            )
            for symbol, frame in datasets.items()
        }
    for value in (0.50, 0.60, 0.70, 0.80, 0.90):
        candidates[f"MID_EXPOSURE_CAP_{value:.2f}"] = {
            symbol: run_mid(frame, exposure_cap=value) for symbol, frame in datasets.items()
        }
    for exit_retrace in (0.05, 0.06, 0.07, 0.08, 0.09):
        candidates[f"MID_OSS_COMBO_ATR50_EXIT_{exit_retrace:.2f}"] = {
            symbol: run_mid(
                frame,
                schedule=atr_gated_hl_schedule(frame, atr_regrid_threshold=0.50),
                exit_retracement=exit_retrace,
            )
            for symbol, frame in datasets.items()
        }

    candidates["JORDAN_DYNAMIC_GRID_PORT"] = {
        symbol: run_jordan_dynamic_grid_port(frame) for symbol, frame in datasets.items()
    }

    payload = {
        "evaluation_start": EVALUATION_START.isoformat(),
        "fee_bps": FEE_RATE * 10_000,
        "slippage_bps": SLIPPAGE_RATE * 10_000,
        "candidates": {
            name: {
                "aggregate": aggregate(results),
                "per_asset": {
                    symbol: {
                        "total_return": result.total_return,
                        "max_drawdown": result.max_drawdown,
                        "closed_trades": result.closed_trades,
                    }
                    for symbol, result in results.items()
                },
            }
            for name, results in candidates.items()
        },
    }

    text = json.dumps(payload, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
