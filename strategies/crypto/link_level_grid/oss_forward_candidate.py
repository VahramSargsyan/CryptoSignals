from __future__ import annotations

import math
from hashlib import sha256
from dataclasses import dataclass
from typing import Optional

import pandas as pd

MAIN_LEVELS = 16
TOTAL_SUBLEVELS = 64
MID_TARGET_PCTS = (
    0.26, 0.28, 0.31, 0.35, 0.43, 0.53, 0.61, 0.70,
    0.80, 0.92, 1.02, 1.17, 1.30, 1.50, 1.75, 2.12,
)


@dataclass(frozen=True)
class OssMidCandidateConfig:
    initial_capital: float = 2000.0
    lookback_candles: int = 1095
    atr_period: int = 14
    atr_regrid_threshold: float = 0.50
    regrid_cooldown_candles: int = 60
    exit_retracement: float = 0.07
    mid_recovery_sublevels: int = 18
    ten_sublevel_from_main: int = 7
    fee_bps: float = 10.0
    slippage_bps: float = 5.0

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if self.lookback_candles <= 1:
            raise ValueError("lookback_candles must be greater than 1")
        if self.atr_period <= 1:
            raise ValueError("atr_period must be greater than 1")
        if not 0 < self.atr_regrid_threshold < 10:
            raise ValueError("atr_regrid_threshold must be positive")
        if self.regrid_cooldown_candles <= 0:
            raise ValueError("regrid_cooldown_candles must be positive")
        if not 0 < self.exit_retracement < 1:
            raise ValueError("exit_retracement must be between 0 and 1")
        if not 1 <= self.mid_recovery_sublevels <= TOTAL_SUBLEVELS:
            raise ValueError("mid_recovery_sublevels out of range")
        if not 1 <= self.ten_sublevel_from_main <= MAIN_LEVELS:
            raise ValueError("ten_sublevel_from_main out of range")
        if self.fee_bps < 0 or self.slippage_bps < 0:
            raise ValueError("fee/slippage must be non-negative")


@dataclass(frozen=True)
class OssMidCandidateResult:
    summary: dict
    events: pd.DataFrame
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    range_history: pd.DataFrame


def _utc(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _prepare_candles(candles: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = sorted(required.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing candle columns: {missing}")
    frame = candles.loc[:, ["timestamp", "open", "high", "low", "close", "volume"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if frame["timestamp"].duplicated().any():
        raise ValueError("Candidate backtest requires unique timestamps")
    numeric = frame[["open", "high", "low", "close", "volume"]].astype(float)
    if (numeric[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC values must be positive")
    frame[["open", "high", "low", "close", "volume"]] = numeric
    return frame


def _atr(frame: pd.DataFrame, period: int) -> pd.Series:
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    close = frame["close"].astype(float)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def _boundary(grid: tuple[float, float], sublevel: int) -> float:
    high, low = grid
    return high - ((high - low) * (sublevel / TOTAL_SUBLEVELS))


def _buy_limit_fill(candle: pd.Series, limit_price: float, slippage_rate: float) -> float:
    raw = min(float(candle["open"]), limit_price)
    return min(limit_price, raw * (1.0 + slippage_rate))


def _sell_stop_fill(candle: pd.Series, stop_price: float, slippage_rate: float) -> float:
    raw = min(float(candle["open"]), stop_price)
    return raw * (1.0 - slippage_rate)


def _weights() -> tuple[float, ...]:
    denominator = sum(range(1, MAIN_LEVELS + 1))
    return tuple(level / denominator for level in range(1, MAIN_LEVELS + 1))


def _make_run_id(
    *,
    dataset_id: str,
    source_commit_sha: str,
    evaluation_start: pd.Timestamp,
    config: OssMidCandidateConfig,
) -> str:
    payload = repr(
        (
            "VAHRAM_LINK_LEVEL_GRID_OSS_FORWARD_CANDIDATE_V1",
            dataset_id,
            source_commit_sha,
            evaluation_start.isoformat(),
            config,
        )
    ).encode("utf-8")
    return "RUN-OSS-" + sha256(payload).hexdigest()[:20]


def build_atr_gated_hl_schedule(
    candles: pd.DataFrame,
    *,
    evaluation_start,
    config: OssMidCandidateConfig | None = None,
) -> tuple[list[Optional[tuple[float, float]]], pd.DataFrame]:
    cfg = config or OssMidCandidateConfig()
    frame = _prepare_candles(candles)
    start = _utc(evaluation_start)
    atr = _atr(frame, cfg.atr_period)
    schedule: list[Optional[tuple[float, float]]] = [None] * len(frame)
    active: Optional[tuple[float, float]] = None
    last_refresh: Optional[int] = None
    atr_anchor: Optional[float] = None
    rows: list[dict] = []

    for position, candle in frame.iterrows():
        timestamp = pd.Timestamp(candle["timestamp"])
        if timestamp < start:
            continue

        history_start = max(0, position - cfg.lookback_candles)
        if position - history_start < cfg.lookback_candles:
            schedule[position] = active
            continue

        reason = None
        atr_now = float(atr.iloc[position])

        if active is None:
            reason = "INITIAL"
        elif last_refresh is not None and position - last_refresh >= cfg.regrid_cooldown_candles:
            close = float(candle["close"])
            atr_shift = (
                abs((atr_now / atr_anchor) - 1.0)
                if atr_anchor is not None
                and math.isfinite(atr_now)
                and math.isfinite(atr_anchor)
                and atr_anchor > 0
                else 0.0
            )
            if atr_shift > cfg.atr_regrid_threshold:
                reason = "ATR_SHIFT"
            elif close > active[0] or close < active[1]:
                reason = "PRICE_ESCAPE"

        if reason is not None:
            history = frame.iloc[history_start:position]
            high = float(history["high"].max())
            low = float(history["low"].min())
            if not (math.isfinite(high) and math.isfinite(low) and high > low > 0):
                raise ValueError("Invalid H/L range")
            active = (high, low)
            last_refresh = position
            atr_anchor = atr_now
            rows.append(
                {
                    "timestamp": timestamp,
                    "position": position,
                    "high": high,
                    "low": low,
                    "atr": atr_now,
                    "reason": reason,
                }
            )

        schedule[position] = active

    return schedule, pd.DataFrame(rows)


def run_oss_mid_candidate(
    candles: pd.DataFrame,
    *,
    evaluation_start,
    dataset_id: str = "",
    source_commit_sha: str = "",
    config: OssMidCandidateConfig | None = None,
) -> OssMidCandidateResult:
    cfg = config or OssMidCandidateConfig()
    frame = _prepare_candles(candles)
    start = _utc(evaluation_start)
    eligible = frame.index[frame["timestamp"] >= start].tolist()
    if not eligible:
        raise ValueError("evaluation_start is after the available dataset")
    first = int(eligible[0])
    if first < cfg.lookback_candles:
        raise ValueError(
            f"Insufficient prehistory: need {cfg.lookback_candles}, have {first}"
        )

    schedule, range_history = build_atr_gated_hl_schedule(
        frame,
        evaluation_start=start,
        config=cfg,
    )
    if schedule[first] is None:
        raise ValueError("No initial H/L range at evaluation_start")

    run_id = _make_run_id(
        dataset_id=dataset_id,
        source_commit_sha=source_commit_sha,
        evaluation_start=start,
        config=cfg,
    )

    fee_rate = cfg.fee_bps / 10_000.0
    slippage_rate = cfg.slippage_bps / 10_000.0
    weights = _weights()
    cash = {
        level: cfg.initial_capital * weights[level - 1]
        for level in range(1, MAIN_LEVELS + 1)
    }
    lots: dict[int, dict] = {}
    events: list[dict] = []
    trades: list[dict] = []
    equity_rows: list[dict] = []

    peak_equity = cfg.initial_capital
    max_drawdown = 0.0

    for position, candle in frame.iterrows():
        if position < first:
            continue

        timestamp = pd.Timestamp(candle["timestamp"])
        grid = schedule[position]
        exited_today: set[int] = set()

        # Exit logic intentionally preserves the research-harness OHLC ordering rule:
        # target touch arms trailing on that candle; a trailing stop may fire only on
        # a later candle. A same-candle new high followed by retracement is not assumed.
        for level, lot in list(lots.items()):
            if lot["entry_position"] >= position:
                continue

            if lot["exit_armed"]:
                stop = lot["peak"] * (1.0 - cfg.exit_retracement)
                if float(candle["low"]) <= stop:
                    fill = _sell_stop_fill(candle, stop, slippage_rate)
                    proceeds = lot["units"] * fill * (1.0 - fee_rate)
                    realized_profit = proceeds - lot["invested_cash"]
                    cash[level] = proceeds
                    events.append(
                        {
                            "run_id": run_id,
                            "timestamp": timestamp,
                            "event_type": "SELL",
                            "layer": "MID",
                            "slot_id": level,
                            "main_level": level,
                            "sublevel": level * 4,
                            "label": f"{level}A",
                            "fill_price": fill,
                            "units": lot["units"],
                            "cash_value": proceeds,
                            "target_price": lot["target"],
                            "reason": "OSS_TRAILING_EXIT",
                        }
                    )
                    trades.append(
                        {
                            "run_id": run_id,
                            "layer": "MID",
                            "slot_id": level,
                            "main_level": level,
                            "entry_timestamp": lot["entry_timestamp"],
                            "exit_timestamp": timestamp,
                            "entry_price": lot["entry_price"],
                            "exit_price": fill,
                            "invested_cash": lot["invested_cash"],
                            "proceeds": proceeds,
                            "realized_profit": realized_profit,
                            "net_return": (proceeds / lot["invested_cash"]) - 1.0,
                            "holding_candles": position - lot["entry_position"],
                            "target_price": lot["target"],
                            "armed_peak": lot["peak"],
                            "exit_retracement": cfg.exit_retracement,
                            "exit_reason": "OSS_TRAILING_EXIT",
                        }
                    )
                    del lots[level]
                    exited_today.add(level)
                    continue

                lot["peak"] = max(lot["peak"], float(candle["high"]))
                continue

            if float(candle["high"]) >= lot["target"]:
                lot["exit_armed"] = True
                lot["peak"] = float(candle["high"])
                events.append(
                    {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "event_type": "ARM_EXIT",
                        "layer": "MID",
                        "slot_id": level,
                        "main_level": level,
                        "sublevel": level * 4,
                        "label": f"{level}A",
                        "fill_price": math.nan,
                        "units": lot["units"],
                        "cash_value": 0.0,
                        "target_price": lot["target"],
                        "reason": "OSS_TARGET_REACHED_TRAIL_ARMED",
                    }
                )

        if grid is not None:
            for level in range(1, MAIN_LEVELS + 1):
                if level in lots or level in exited_today:
                    continue
                sublevel = level * 4
                limit_price = _boundary(grid, sublevel)
                if float(candle["low"]) > limit_price:
                    continue
                budget = cash[level]
                if budget <= 0:
                    continue

                fill = _buy_limit_fill(candle, limit_price, slippage_rate)
                units = (budget * (1.0 - fee_rate)) / fill
                percent_target = fill * (1.0 + MID_TARGET_PCTS[level - 1])
                grid_target = (
                    _boundary(grid, max(0, sublevel - cfg.mid_recovery_sublevels))
                    if level >= cfg.ten_sublevel_from_main
                    else None
                )
                target = min(percent_target, grid_target) if grid_target is not None else percent_target
                lots[level] = {
                    "entry_position": position,
                    "entry_timestamp": timestamp,
                    "entry_price": fill,
                    "units": units,
                    "invested_cash": budget,
                    "target": target,
                    "percent_target": percent_target,
                    "grid_target": grid_target,
                    "exit_armed": False,
                    "peak": None,
                }
                cash[level] = 0.0
                events.append(
                    {
                        "run_id": run_id,
                        "timestamp": timestamp,
                        "event_type": "BUY",
                        "layer": "MID",
                        "slot_id": level,
                        "main_level": level,
                        "sublevel": sublevel,
                        "label": f"{level}A",
                        "fill_price": fill,
                        "units": units,
                        "cash_value": budget,
                        "target_price": target,
                        "reason": "OSS_ATR50_GRID_ENTRY",
                    }
                )

        close = float(candle["close"])
        cash_total = sum(cash.values())
        open_value = sum(
            lot["units"] * close * (1.0 - fee_rate)
            for lot in lots.values()
        )
        equity = cash_total + open_value
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, 1.0 - (equity / peak_equity))
        equity_rows.append(
            {
                "timestamp": timestamp,
                "mid_equity": equity,
                "cash_total": cash_total,
                "open_value": open_value,
                "open_mid_lots": len(lots),
                "armed_exit_lots": sum(1 for lot in lots.values() if lot["exit_armed"]),
            }
        )

    equity_df = pd.DataFrame(equity_rows)
    events_df = pd.DataFrame(events)
    trades_df = pd.DataFrame(trades)
    final_equity = float(equity_df.iloc[-1]["mid_equity"])

    summary = {
        "run_id": run_id,
        "strategy_id": "VAHRAM_LINK_LEVEL_GRID_OSS_FORWARD_CANDIDATE_V1",
        "dataset_id": dataset_id,
        "source_commit_sha": source_commit_sha,
        "period_start": start.isoformat(),
        "period_end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
        "initial_capital": cfg.initial_capital,
        "final_equity": final_equity,
        "total_return": (final_equity / cfg.initial_capital) - 1.0,
        "max_drawdown": max_drawdown,
        "closed_trade_count": len(trades_df),
        "open_mid_lots_end": int(equity_df.iloc[-1]["open_mid_lots"]),
        "armed_exit_lots_end": int(equity_df.iloc[-1]["armed_exit_lots"]),
        "range_refresh_count": len(range_history),
        "atr_regrid_threshold": cfg.atr_regrid_threshold,
        "regrid_cooldown_candles": cfg.regrid_cooldown_candles,
        "exit_retracement": cfg.exit_retracement,
        "mid_recovery_sublevels": cfg.mid_recovery_sublevels,
        "fee_bps": cfg.fee_bps,
        "slippage_bps": cfg.slippage_bps,
        "real_orders": False,
    }

    return OssMidCandidateResult(
        summary=summary,
        events=events_df,
        trades=trades_df,
        equity_curve=equity_df,
        range_history=range_history,
    )
