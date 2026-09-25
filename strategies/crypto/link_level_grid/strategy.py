from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Iterable, Optional

import pandas as pd

STRATEGY_ID = "VAHRAM_LINK_LEVEL_GRID_V1"
STRATEGY_NAME = "VAHRAM_LINK_LEVEL_GRID"
STRATEGY_VERSION = "1.0.0-experimental"

MAIN_LEVELS = 16
SUBLEVELS_PER_MAIN = 4
TOTAL_SUBLEVELS = MAIN_LEVELS * SUBLEVELS_PER_MAIN

# Current TradingView targets documented by the owner. They are kept explicit
# until a canonical generating formula is confirmed.
MID_TARGET_PCTS = (
    0.26,
    0.28,
    0.31,
    0.35,
    0.43,
    0.53,
    0.61,
    0.70,
    0.80,
    0.92,
    1.02,
    1.17,
    1.30,
    1.50,
    1.75,
    2.12,
)

HISTORICAL_MICRO_MAIN_PCTS = (
    0.01,
    0.02,
    0.03,
    0.04,
    0.04,
    0.04,
    0.06,
    0.06,
    0.06,
    0.06,
    0.08,
    0.10,
    0.11,
    0.11,
    0.11,
    0.07,
)

HISTORICAL_MID_PCTS = (
    0.0050,
    0.0200,
    0.0300,
    0.0400,
    0.0400,
    0.0500,
    0.0550,
    0.0550,
    0.0640,
    0.0789,
    0.0887,
    0.0986,
    0.1035,
    0.1134,
    0.1134,
    0.0446,
)


@dataclass(frozen=True)
class GridDefinition:
    high: float
    low: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.high) or not math.isfinite(self.low):
            raise ValueError("Grid anchors must be finite")
        if self.high <= self.low:
            raise ValueError("Grid high must be greater than grid low")
        if self.low <= 0:
            raise ValueError("Grid low must be positive")

    @property
    def span(self) -> float:
        return self.high - self.low

    @property
    def main_step(self) -> float:
        return self.span / MAIN_LEVELS

    @property
    def sub_step(self) -> float:
        return self.span / TOTAL_SUBLEVELS

    def boundary_price(self, sublevel_index: int) -> float:
        """Price of boundary 0..64; 0=H and 64=L."""
        if not 0 <= sublevel_index <= TOTAL_SUBLEVELS:
            raise ValueError("sublevel_index must be between 0 and 64")
        return self.high - (self.span * (sublevel_index / TOTAL_SUBLEVELS))

    def main_lower_price(self, main_level: int) -> float:
        if not 1 <= main_level <= MAIN_LEVELS:
            raise ValueError("main_level must be between 1 and 16")
        return self.boundary_price(main_level * SUBLEVELS_PER_MAIN)


@dataclass(frozen=True)
class RollingRangePolicy:
    lookback_candles: int = 1095
    min_history_candles: int = 90
    refresh_candles: int = 30

    def __post_init__(self) -> None:
        if self.lookback_candles <= 0:
            raise ValueError("lookback_candles must be positive")
        if self.min_history_candles <= 1:
            raise ValueError("min_history_candles must be greater than 1")
        if self.min_history_candles > self.lookback_candles:
            raise ValueError("min_history_candles cannot exceed lookback_candles")
        if self.refresh_candles <= 0:
            raise ValueError("refresh_candles must be positive")


@dataclass(frozen=True)
class GridBacktestConfig:
    micro_capital: float = 1000.0
    mid_capital: float = 1000.0
    allocation_preset: str = "linear_depth_reserved"
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    rolling_range: RollingRangePolicy = RollingRangePolicy()
    ten_sublevel_from_main: int = 7
    liquidate_at_end: bool = False

    def __post_init__(self) -> None:
        if self.micro_capital <= 0 or self.mid_capital <= 0:
            raise ValueError("Capital pools must be positive")
        if self.fee_bps < 0 or self.slippage_bps < 0:
            raise ValueError("Fees and slippage must be non-negative")
        if self.allocation_preset not in {
            "linear_depth_reserved",
            "equal_reserved",
            "historical_observed",
        }:
            raise ValueError("Unknown allocation_preset")
        if not 1 <= self.ten_sublevel_from_main <= MAIN_LEVELS:
            raise ValueError("ten_sublevel_from_main must be between 1 and 16")


@dataclass
class OpenLot:
    layer: str
    slot_id: int
    main_level: int
    entry_sublevel: int
    entry_timestamp: pd.Timestamp
    entry_position: int
    entry_price: float
    units: float
    invested_cash: float
    target_price: float
    percent_target_price: Optional[float]
    grid_target_price: Optional[float]
    entry_grid_high: float
    entry_grid_low: float


@dataclass(frozen=True)
class GridBacktestResult:
    run_id: str
    summary: dict
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    range_history: pd.DataFrame
    allocation_table: pd.DataFrame


def _normalize(weights: Iterable[float]) -> tuple[float, ...]:
    values = tuple(float(value) for value in weights)
    if any(value < 0 or not math.isfinite(value) for value in values):
        raise ValueError("Allocation weights must be finite and non-negative")
    total = sum(values)
    if total <= 0:
        raise ValueError("Allocation weights must sum to a positive value")
    return tuple(value / total for value in values)


def main_level_allocations(preset: str, *, layer: str) -> tuple[float, ...]:
    if layer not in {"MICRO", "MID"}:
        raise ValueError("layer must be MICRO or MID")

    if preset == "equal_reserved":
        return tuple(1.0 / MAIN_LEVELS for _ in range(MAIN_LEVELS))

    if preset == "linear_depth_reserved":
        # Parameter-free research baseline: deeper levels receive linearly
        # increasing reserved capital. This is NOT yet owner-confirmed canon.
        return _normalize(range(1, MAIN_LEVELS + 1))

    if preset == "historical_observed":
        return _normalize(
            HISTORICAL_MICRO_MAIN_PCTS if layer == "MICRO" else HISTORICAL_MID_PCTS
        )

    raise ValueError(f"Unknown allocation preset: {preset}")


def micro_sublevel_allocations(preset: str) -> tuple[float, ...]:
    main = main_level_allocations(preset, layer="MICRO")
    return tuple(
        main[(sublevel - 1) // SUBLEVELS_PER_MAIN] / SUBLEVELS_PER_MAIN
        for sublevel in range(1, TOTAL_SUBLEVELS + 1)
    )


def main_for_sublevel(sublevel_index: int) -> int:
    if not 1 <= sublevel_index <= TOTAL_SUBLEVELS:
        raise ValueError("sublevel_index must be between 1 and 64")
    return ((sublevel_index - 1) // SUBLEVELS_PER_MAIN) + 1


def sublevel_label(sublevel_index: int) -> str:
    remainder = ((sublevel_index - 1) % SUBLEVELS_PER_MAIN) + 1
    letter = {1: "D", 2: "C", 3: "B", 4: "A"}[remainder]
    return f"{main_for_sublevel(sublevel_index)}{letter}"


def _prepare_candles(candles: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = sorted(required.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing candle columns: {missing}")

    frame = candles.loc[:, ["timestamp", "open", "high", "low", "close", "volume"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if frame["timestamp"].duplicated().any():
        raise ValueError("Grid backtest requires unique timestamps")
    numeric = frame[["open", "high", "low", "close", "volume"]].astype(float)
    if (numeric[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC values must be positive")
    frame[["open", "high", "low", "close", "volume"]] = numeric
    return frame


def build_causal_range_schedule(
    candles: pd.DataFrame,
    policy: RollingRangePolicy,
) -> list[Optional[GridDefinition]]:
    """Build an infrequently refreshed trailing range using past candles only."""
    frame = _prepare_candles(candles)
    schedule: list[Optional[GridDefinition]] = [None] * len(frame)
    active: Optional[GridDefinition] = None
    last_refresh_position: Optional[int] = None

    for position in range(len(frame)):
        history_end = position
        history_start = max(0, history_end - policy.lookback_candles)
        history_count = history_end - history_start

        should_refresh = (
            history_count >= policy.min_history_candles
            and (
                active is None
                or last_refresh_position is None
                or position - last_refresh_position >= policy.refresh_candles
            )
        )

        if should_refresh:
            history = frame.iloc[history_start:history_end]
            high = float(history["high"].max())
            low = float(history["low"].min())
            active = GridDefinition(high=high, low=low)
            last_refresh_position = position

        schedule[position] = active

    return schedule


def _buy_limit_fill(candle: pd.Series, limit_price: float, slippage_bps: float) -> float:
    if float(candle["low"]) > limit_price:
        raise ValueError("Buy limit cannot fill when candle low is above limit")
    raw = min(float(candle["open"]), limit_price)
    adverse = raw * (1.0 + slippage_bps / 10_000.0)
    return min(limit_price, adverse)


def _sell_limit_fill(candle: pd.Series, limit_price: float, slippage_bps: float) -> float:
    if float(candle["high"]) < limit_price:
        raise ValueError("Sell limit cannot fill when candle high is below limit")
    raw = max(float(candle["open"]), limit_price)
    adverse = raw * (1.0 - slippage_bps / 10_000.0)
    return max(limit_price, adverse)


def _mid_target(
    *,
    grid: GridDefinition,
    main_level: int,
    entry_sublevel: int,
    entry_price: float,
    ten_sublevel_from_main: int,
) -> tuple[float, float, Optional[float]]:
    percent_target = entry_price * (1.0 + MID_TARGET_PCTS[main_level - 1])
    grid_target = None
    if main_level >= ten_sublevel_from_main:
        grid_index = max(0, entry_sublevel - 10)
        grid_target = grid.boundary_price(grid_index)
    target = min(percent_target, grid_target) if grid_target is not None else percent_target
    return target, percent_target, grid_target


def _make_run_id(
    *,
    dataset_id: str,
    config: GridBacktestConfig,
    source_commit_sha: str,
) -> str:
    payload = repr(
        (
            STRATEGY_ID,
            STRATEGY_VERSION,
            dataset_id,
            source_commit_sha,
            asdict(config),
        )
    ).encode("utf-8")
    return "RUN-" + sha256(payload).hexdigest()[:20]


def run_grid_backtest(
    candles: pd.DataFrame,
    *,
    dataset_id: str,
    source_commit_sha: str,
    config: GridBacktestConfig | None = None,
) -> GridBacktestResult:
    """
    Portfolio-aware research backtest.

    Important: this implements a deterministic RESEARCH interpretation of the
    partially frozen strategy. The default monthly-ish range refresh, linear
    depth allocation, and Mid entry at each main A boundary are explicit
    assumptions, not claims that the owner has frozen them as canonical.
    """
    cfg = config or GridBacktestConfig()
    frame = _prepare_candles(candles)
    if len(frame) < cfg.rolling_range.min_history_candles + 2:
        raise ValueError("Not enough candles for configured range warmup")

    schedule = build_causal_range_schedule(frame, cfg.rolling_range)
    run_id = _make_run_id(
        dataset_id=dataset_id,
        config=cfg,
        source_commit_sha=source_commit_sha,
    )

    fee_rate = cfg.fee_bps / 10_000.0

    micro_alloc = micro_sublevel_allocations(cfg.allocation_preset)
    mid_alloc = main_level_allocations(cfg.allocation_preset, layer="MID")

    micro_cash = {
        index: cfg.micro_capital * micro_alloc[index - 1]
        for index in range(1, TOTAL_SUBLEVELS + 1)
    }
    mid_cash = {
        level: cfg.mid_capital * mid_alloc[level - 1]
        for level in range(1, MAIN_LEVELS + 1)
    }
    micro_lots: dict[int, OpenLot] = {}
    mid_lots: dict[int, OpenLot] = {}

    trades: list[dict] = []
    equity_rows: list[dict] = []
    range_rows: list[dict] = []
    previous_grid: Optional[GridDefinition] = None
    exited_today: set[tuple[str, int]] = set()

    peak_equity = cfg.micro_capital + cfg.mid_capital
    max_drawdown = 0.0
    peak_deployed = 0.0

    for position, candle in frame.iterrows():
        grid = schedule[position]
        exited_today.clear()

        if grid is not None and (
            previous_grid is None
            or grid.high != previous_grid.high
            or grid.low != previous_grid.low
        ):
            range_rows.append(
                {
                    "timestamp": pd.Timestamp(candle["timestamp"]),
                    "position": position,
                    "high": grid.high,
                    "low": grid.low,
                    "main_step": grid.main_step,
                    "sub_step": grid.sub_step,
                }
            )
            previous_grid = grid

        # Exits are evaluated before new entries. Same-day re-entry is blocked.
        for layer, lots, cash_map in (
            ("MICRO", micro_lots, micro_cash),
            ("MID", mid_lots, mid_cash),
        ):
            for slot_id, lot in list(lots.items()):
                if lot.entry_position >= position:
                    continue
                if float(candle["high"]) < lot.target_price:
                    continue

                fill = _sell_limit_fill(candle, lot.target_price, cfg.slippage_bps)
                proceeds = lot.units * fill * (1.0 - fee_rate)
                cash_map[slot_id] = proceeds
                gross_return = (fill / lot.entry_price) - 1.0
                net_return = (proceeds / lot.invested_cash) - 1.0
                if layer == "MID" and lot.grid_target_price is not None:
                    if lot.grid_target_price <= lot.percent_target_price:
                        exit_reason = "MID_FIRST_OF_PERCENT_OR_10_SUBLEVELS:10_SUBLEVELS"
                    else:
                        exit_reason = "MID_FIRST_OF_PERCENT_OR_10_SUBLEVELS:PERCENT"
                elif layer == "MID":
                    exit_reason = "MID_PERCENT_TARGET"
                else:
                    exit_reason = "MICRO_ONE_SUBLEVEL_RECOVERY"

                trades.append(
                    {
                        "run_id": run_id,
                        "layer": layer,
                        "slot_id": slot_id,
                        "entry_label": sublevel_label(lot.entry_sublevel),
                        "main_level": lot.main_level,
                        "entry_sublevel": lot.entry_sublevel,
                        "entry_timestamp": lot.entry_timestamp,
                        "exit_timestamp": pd.Timestamp(candle["timestamp"]),
                        "entry_price": lot.entry_price,
                        "exit_price": fill,
                        "invested_cash": lot.invested_cash,
                        "proceeds": proceeds,
                        "gross_return": gross_return,
                        "net_return": net_return,
                        "holding_candles": position - lot.entry_position,
                        "target_price": lot.target_price,
                        "percent_target_price": lot.percent_target_price,
                        "grid_target_price": lot.grid_target_price,
                        "exit_reason": exit_reason,
                        "entry_grid_high": lot.entry_grid_high,
                        "entry_grid_low": lot.entry_grid_low,
                    }
                )
                del lots[slot_id]
                exited_today.add((layer, slot_id))

        if grid is not None:
            # Micro: one independent reserved slot at every sublevel.
            for sublevel in range(1, TOTAL_SUBLEVELS + 1):
                if sublevel in micro_lots or ("MICRO", sublevel) in exited_today:
                    continue
                limit_price = grid.boundary_price(sublevel)
                if float(candle["low"]) > limit_price:
                    continue
                budget = micro_cash[sublevel]
                if budget <= 0:
                    continue

                fill = _buy_limit_fill(candle, limit_price, cfg.slippage_bps)
                units = (budget * (1.0 - fee_rate)) / fill
                target = grid.boundary_price(sublevel - 1)
                micro_lots[sublevel] = OpenLot(
                    layer="MICRO",
                    slot_id=sublevel,
                    main_level=main_for_sublevel(sublevel),
                    entry_sublevel=sublevel,
                    entry_timestamp=pd.Timestamp(candle["timestamp"]),
                    entry_position=position,
                    entry_price=fill,
                    units=units,
                    invested_cash=budget,
                    target_price=target,
                    percent_target_price=None,
                    grid_target_price=target,
                    entry_grid_high=grid.high,
                    entry_grid_low=grid.low,
                )
                micro_cash[sublevel] = 0.0

            # Mid research interpretation: one reserved slot per main level,
            # entered at the lower A boundary (sublevel 4, 8, ..., 64).
            for main_level in range(1, MAIN_LEVELS + 1):
                if main_level in mid_lots or ("MID", main_level) in exited_today:
                    continue
                entry_sublevel = main_level * SUBLEVELS_PER_MAIN
                limit_price = grid.boundary_price(entry_sublevel)
                if float(candle["low"]) > limit_price:
                    continue
                budget = mid_cash[main_level]
                if budget <= 0:
                    continue

                fill = _buy_limit_fill(candle, limit_price, cfg.slippage_bps)
                units = (budget * (1.0 - fee_rate)) / fill
                target, percent_target, grid_target = _mid_target(
                    grid=grid,
                    main_level=main_level,
                    entry_sublevel=entry_sublevel,
                    entry_price=fill,
                    ten_sublevel_from_main=cfg.ten_sublevel_from_main,
                )
                mid_lots[main_level] = OpenLot(
                    layer="MID",
                    slot_id=main_level,
                    main_level=main_level,
                    entry_sublevel=entry_sublevel,
                    entry_timestamp=pd.Timestamp(candle["timestamp"]),
                    entry_position=position,
                    entry_price=fill,
                    units=units,
                    invested_cash=budget,
                    target_price=target,
                    percent_target_price=percent_target,
                    grid_target_price=grid_target,
                    entry_grid_high=grid.high,
                    entry_grid_low=grid.low,
                )
                mid_cash[main_level] = 0.0

        close = float(candle["close"])
        micro_cash_total = sum(micro_cash.values())
        mid_cash_total = sum(mid_cash.values())
        micro_open_value = sum(lot.units * close * (1.0 - fee_rate) for lot in micro_lots.values())
        mid_open_value = sum(lot.units * close * (1.0 - fee_rate) for lot in mid_lots.values())
        micro_equity = micro_cash_total + micro_open_value
        mid_equity = mid_cash_total + mid_open_value
        total_equity = micro_equity + mid_equity

        peak_equity = max(peak_equity, total_equity)
        drawdown = (total_equity / peak_equity) - 1.0
        max_drawdown = max(max_drawdown, abs(drawdown))

        deployed = micro_open_value + mid_open_value
        peak_deployed = max(peak_deployed, deployed)

        equity_rows.append(
            {
                "timestamp": pd.Timestamp(candle["timestamp"]),
                "micro_equity": micro_equity,
                "mid_equity": mid_equity,
                "total_equity": total_equity,
                "micro_open_lots": len(micro_lots),
                "mid_open_lots": len(mid_lots),
                "cash_total": micro_cash_total + mid_cash_total,
                "deployed_mark_value": deployed,
            }
        )

    final_candle = frame.iloc[-1]
    final_close = float(final_candle["close"])

    if cfg.liquidate_at_end:
        # Optional diagnostic policy; default keeps open lots marked to market.
        for layer, lots, cash_map in (
            ("MICRO", micro_lots, micro_cash),
            ("MID", mid_lots, mid_cash),
        ):
            for slot_id, lot in list(lots.items()):
                fill = final_close * (1.0 - cfg.slippage_bps / 10_000.0)
                proceeds = lot.units * fill * (1.0 - fee_rate)
                cash_map[slot_id] = proceeds
                trades.append(
                    {
                        "run_id": run_id,
                        "layer": layer,
                        "slot_id": slot_id,
                        "entry_label": sublevel_label(lot.entry_sublevel),
                        "main_level": lot.main_level,
                        "entry_sublevel": lot.entry_sublevel,
                        "entry_timestamp": lot.entry_timestamp,
                        "exit_timestamp": pd.Timestamp(final_candle["timestamp"]),
                        "entry_price": lot.entry_price,
                        "exit_price": fill,
                        "invested_cash": lot.invested_cash,
                        "proceeds": proceeds,
                        "gross_return": (fill / lot.entry_price) - 1.0,
                        "net_return": (proceeds / lot.invested_cash) - 1.0,
                        "holding_candles": len(frame) - 1 - lot.entry_position,
                        "target_price": lot.target_price,
                        "percent_target_price": lot.percent_target_price,
                        "grid_target_price": lot.grid_target_price,
                        "exit_reason": "END_OF_TEST_LIQUIDATION",
                        "entry_grid_high": lot.entry_grid_high,
                        "entry_grid_low": lot.entry_grid_low,
                    }
                )
                del lots[slot_id]

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_rows)
    range_df = pd.DataFrame(range_rows)

    total_initial = cfg.micro_capital + cfg.mid_capital
    final_micro = float(equity_df.iloc[-1]["micro_equity"])
    final_mid = float(equity_df.iloc[-1]["mid_equity"])
    final_total = float(equity_df.iloc[-1]["total_equity"])

    if trades_df.empty:
        closed_trade_count = 0
        win_rate = 0.0
        avg_trade_return = 0.0
    else:
        closed_trade_count = len(trades_df)
        returns = trades_df["net_return"].astype(float)
        win_rate = float((returns > 0).mean())
        avg_trade_return = float(returns.mean())

    benchmark_return = (
        (final_close / float(frame.iloc[0]["open"])) - 1.0
    )

    allocation_rows = []
    for level in range(1, MAIN_LEVELS + 1):
        allocation_rows.append(
            {
                "main_level": level,
                "micro_main_allocation": main_level_allocations(
                    cfg.allocation_preset, layer="MICRO"
                )[level - 1],
                "mid_allocation": mid_alloc[level - 1],
                "mid_target_pct": MID_TARGET_PCTS[level - 1],
                "mid_entry_sublevel": level * SUBLEVELS_PER_MAIN,
                "mid_entry_label": f"{level}A",
                "ten_sublevel_alternative": level >= cfg.ten_sublevel_from_main,
            }
        )

    summary = {
        "run_id": run_id,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "dataset_id": dataset_id,
        "source_commit_sha": source_commit_sha,
        "period_start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
        "period_end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
        "candles": len(frame),
        "allocation_preset": cfg.allocation_preset,
        "micro_initial_capital": cfg.micro_capital,
        "mid_initial_capital": cfg.mid_capital,
        "total_initial_capital": total_initial,
        "micro_final_equity": final_micro,
        "mid_final_equity": final_mid,
        "total_final_equity": final_total,
        "micro_total_return": (final_micro / cfg.micro_capital) - 1.0,
        "mid_total_return": (final_mid / cfg.mid_capital) - 1.0,
        "total_return": (final_total / total_initial) - 1.0,
        "benchmark_buy_hold_return": benchmark_return,
        "max_drawdown": max_drawdown,
        "peak_deployed_mark_value": peak_deployed,
        "peak_deployed_pct_of_initial": peak_deployed / total_initial,
        "closed_trade_count": closed_trade_count,
        "closed_trade_win_rate": win_rate,
        "average_closed_trade_return": avg_trade_return,
        "open_micro_lots_end": int(equity_df.iloc[-1]["micro_open_lots"]),
        "open_mid_lots_end": int(equity_df.iloc[-1]["mid_open_lots"]),
        "range_refresh_count": len(range_df),
        "research_assumptions": {
            "timeframe": "1D",
            "range_is_causal": True,
            "range_uses_prior_candles_only": True,
            "range_lookback_candles": cfg.rolling_range.lookback_candles,
            "range_min_history_candles": cfg.rolling_range.min_history_candles,
            "range_refresh_candles": cfg.rolling_range.refresh_candles,
            "range_refresh_rule_is_owner_confirmed": False,
            "default_allocation_is_owner_confirmed": False,
            "mid_entry_at_A_boundary_is_owner_confirmed": False,
            "micro_exit_one_sublevel_up_is_historical_observed": True,
            "mid_percent_targets_are_current_chart_values": True,
            "mid_10_sublevel_rule_is_owner_confirmed": True,
            "capital_is_reserved_per_slot": True,
            "open_lots_keep_entry_time_targets_after_grid_refresh": True,
            "same_day_entry_and_exit_is_blocked": True,
        },
    }

    return GridBacktestResult(
        run_id=run_id,
        summary=summary,
        trades=trades_df,
        equity_curve=equity_df,
        range_history=range_df,
        allocation_table=pd.DataFrame(allocation_rows),
    )
