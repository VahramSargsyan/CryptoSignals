from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Iterable, Optional

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.execution import next_candle_open_fill
from core.contracts.strategy import StrategyOutput

ENGINE_NAME = "LONG_ONLY_SIGNAL_FLIP_V1"
END_POLICY_FINAL_CLOSE = "LIQUIDATE_AT_FINAL_CLOSE"


@dataclass(frozen=True)
class LongOnlyTradingPolicy:
    entry_strength_min: float = 0.0
    exit_strength_min: float = 0.0
    end_of_test_policy: str = END_POLICY_FINAL_CLOSE

    def __post_init__(self) -> None:
        for value in (self.entry_strength_min, self.exit_strength_min):
            if not math.isfinite(value) or not 0 <= value <= 100:
                raise ValueError("strength thresholds must be finite and between 0 and 100")
        if self.end_of_test_policy != END_POLICY_FINAL_CLOSE:
            raise ValueError("Unsupported end-of-test policy")

    def to_config(self) -> dict:
        return {
            "entry_strength_min": float(self.entry_strength_min),
            "exit_strength_min": float(self.exit_strength_min),
            "end_of_test_policy": self.end_of_test_policy,
            "position_mode": "LONG_ONLY_FULL_EQUITY",
            "entry_signal": "BUY",
            "exit_signal": "SELL",
        }


@dataclass(frozen=True)
class TradeRecord:
    run_id: str
    strategy_id: str
    strategy_version: str
    symbol: str
    timeframe: str
    entry_signal_id: str
    exit_signal_id: Optional[str]
    entry_signal_timestamp: pd.Timestamp
    exit_signal_timestamp: Optional[pd.Timestamp]
    entry_timestamp: pd.Timestamp
    exit_timestamp: pd.Timestamp
    entry_position: int
    exit_position: int
    raw_entry_price: float
    raw_exit_price: float
    entry_price: float
    exit_price: float
    entry_strength: float
    exit_strength: Optional[float]
    exit_reason: str
    holding_candles: int
    gross_return: float
    net_return: float

    def to_record(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class BacktestMetrics:
    trade_count: int
    win_rate: float
    average_return: float
    median_return: float
    profit_factor: Optional[float]
    expectancy: float
    max_drawdown: float
    total_return: float
    benchmark_return: float
    average_holding_period: float
    exposure: float

    def to_record(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class TradingBacktestResult:
    run_id: str
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    metrics: BacktestMetrics


def _validate_manifest_policy(manifest: BacktestRunManifest, policy: LongOnlyTradingPolicy) -> None:
    if manifest.engine_name != ENGINE_NAME:
        raise ValueError(f"manifest.engine_name must be {ENGINE_NAME}")
    if dict(manifest.engine_config) != policy.to_config():
        raise ValueError("manifest.engine_config must exactly match the trading policy")


def _prepare_candles(candles: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "open", "close"}
    missing = sorted(required.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing candle columns for trading backtest: {missing}")

    ordered = candles.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
    ordered["timestamp"] = pd.to_datetime(ordered["timestamp"], utc=True)
    if ordered["timestamp"].duplicated().any():
        raise ValueError("Trading backtest requires unique candle timestamps")
    if (ordered["open"] <= 0).any() or (ordered["close"] <= 0).any():
        raise ValueError("Trading backtest requires positive open/close prices")
    return ordered


def _validate_outputs(
    outputs: Iterable[StrategyOutput],
    *,
    manifest: BacktestRunManifest,
    positions: dict[pd.Timestamp, int],
) -> list[tuple[int, StrategyOutput]]:
    prepared = []
    seen_positions = set()
    for output in outputs:
        if output.signal == "HOLD":
            continue
        if (
            output.strategy_id != manifest.strategy_id
            or output.strategy_version != manifest.strategy_version
            or output.symbol != manifest.symbol.upper()
            or output.timeframe != manifest.timeframe.upper()
        ):
            raise ValueError("Strategy output identity does not match run manifest")
        if output.run_id != manifest.run_id:
            raise ValueError("Strategy output run_id does not match run manifest")
        if output.source_commit_sha != manifest.source_commit_sha:
            raise ValueError("Strategy output source_commit_sha does not match run manifest")

        timestamp = pd.Timestamp(output.timestamp)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        if timestamp not in positions:
            raise ValueError(f"Signal timestamp not found in candle dataset: {timestamp}")

        position = positions[timestamp]
        if position in seen_positions:
            raise ValueError("Multiple non-HOLD signals at one candle are ambiguous")
        seen_positions.add(position)
        prepared.append((position, output))

    return sorted(prepared, key=lambda item: item[0])


def _round_trip_net_return(
    *,
    entry_price: float,
    exit_price: float,
    fee_rate: float,
) -> tuple[float, float]:
    gross_return = (exit_price / entry_price) - 1.0
    capital_after = (1.0 - fee_rate) * (exit_price / entry_price) * (1.0 - fee_rate)
    return gross_return, capital_after - 1.0


def _benchmark_return(candles: pd.DataFrame, execution: ExecutionPolicy) -> float:
    fee_rate = execution.fee_bps / 10_000.0
    slippage_rate = execution.slippage_bps / 10_000.0
    entry_price = float(candles.iloc[0]["open"]) * (1.0 + slippage_rate)
    exit_price = float(candles.iloc[-1]["close"]) * (1.0 - slippage_rate)
    _, net_return = _round_trip_net_return(
        entry_price=entry_price,
        exit_price=exit_price,
        fee_rate=fee_rate,
    )
    return net_return


def run_long_only_backtest(
    candles: pd.DataFrame,
    outputs: Iterable[StrategyOutput],
    *,
    manifest: BacktestRunManifest,
    policy: LongOnlyTradingPolicy,
) -> TradingBacktestResult:
    _validate_manifest_policy(manifest, policy)
    ordered = _prepare_candles(candles)
    if ordered.empty:
        raise ValueError("Trading backtest requires at least one candle")

    positions = {timestamp: position for position, timestamp in enumerate(ordered["timestamp"])}
    prepared_outputs = _validate_outputs(outputs, manifest=manifest, positions=positions)
    fee_rate = manifest.execution.fee_bps / 10_000.0
    slippage_rate = manifest.execution.slippage_bps / 10_000.0

    trades: list[TradeRecord] = []
    open_trade = None

    for signal_position, output in prepared_outputs:
        if open_trade is None:
            if output.signal != "BUY" or output.strength < policy.entry_strength_min:
                continue
            if signal_position + 1 >= len(ordered):
                continue
            fill = next_candle_open_fill(
                ordered,
                signal_position=signal_position,
                side="BUY",
                policy=manifest.execution,
            )
            open_trade = {
                "entry_signal_id": output.output_id,
                "entry_signal_timestamp": pd.Timestamp(output.timestamp),
                "entry_position": signal_position + 1,
                "raw_entry_price": fill.raw_open_price,
                "entry_price": fill.execution_price,
                "entry_strength": output.strength,
            }
            continue

        if output.signal != "SELL" or output.strength < policy.exit_strength_min:
            continue
        if signal_position + 1 >= len(ordered):
            continue

        fill = next_candle_open_fill(
            ordered,
            signal_position=signal_position,
            side="SELL",
            policy=manifest.execution,
        )
        holding_candles = (signal_position + 1) - open_trade["entry_position"]
        gross_return, net_return = _round_trip_net_return(
            entry_price=open_trade["entry_price"],
            exit_price=fill.execution_price,
            fee_rate=fee_rate,
        )
        trades.append(
            TradeRecord(
                run_id=manifest.run_id,
                strategy_id=manifest.strategy_id,
                strategy_version=manifest.strategy_version,
                symbol=manifest.symbol.upper(),
                timeframe=manifest.timeframe.upper(),
                entry_signal_id=open_trade["entry_signal_id"],
                exit_signal_id=output.output_id,
                entry_signal_timestamp=open_trade["entry_signal_timestamp"],
                exit_signal_timestamp=pd.Timestamp(output.timestamp),
                entry_timestamp=pd.Timestamp(ordered.iloc[open_trade["entry_position"]]["timestamp"]),
                exit_timestamp=fill.entry_timestamp,
                entry_position=open_trade["entry_position"],
                exit_position=signal_position + 1,
                raw_entry_price=open_trade["raw_entry_price"],
                raw_exit_price=fill.raw_open_price,
                entry_price=open_trade["entry_price"],
                exit_price=fill.execution_price,
                entry_strength=open_trade["entry_strength"],
                exit_strength=output.strength,
                exit_reason="SELL_SIGNAL_NEXT_OPEN",
                holding_candles=holding_candles,
                gross_return=gross_return,
                net_return=net_return,
            )
        )
        open_trade = None

    if open_trade is not None:
        final_position = len(ordered) - 1
        raw_exit_price = float(ordered.iloc[final_position]["close"])
        exit_price = raw_exit_price * (1.0 - slippage_rate)
        holding_candles = final_position - open_trade["entry_position"] + 1
        gross_return, net_return = _round_trip_net_return(
            entry_price=open_trade["entry_price"],
            exit_price=exit_price,
            fee_rate=fee_rate,
        )
        trades.append(
            TradeRecord(
                run_id=manifest.run_id,
                strategy_id=manifest.strategy_id,
                strategy_version=manifest.strategy_version,
                symbol=manifest.symbol.upper(),
                timeframe=manifest.timeframe.upper(),
                entry_signal_id=open_trade["entry_signal_id"],
                exit_signal_id=None,
                entry_signal_timestamp=open_trade["entry_signal_timestamp"],
                exit_signal_timestamp=None,
                entry_timestamp=pd.Timestamp(ordered.iloc[open_trade["entry_position"]]["timestamp"]),
                exit_timestamp=pd.Timestamp(ordered.iloc[final_position]["timestamp"]),
                entry_position=open_trade["entry_position"],
                exit_position=final_position,
                raw_entry_price=open_trade["raw_entry_price"],
                raw_exit_price=raw_exit_price,
                entry_price=open_trade["entry_price"],
                exit_price=exit_price,
                entry_strength=open_trade["entry_strength"],
                exit_strength=None,
                exit_reason=END_POLICY_FINAL_CLOSE,
                holding_candles=holding_candles,
                gross_return=gross_return,
                net_return=net_return,
            )
        )

    trades_df = pd.DataFrame([trade.to_record() for trade in trades])
    equity_curve = _build_equity_curve(
        ordered,
        trades,
        fee_rate=fee_rate,
    )
    metrics = _calculate_metrics(
        ordered,
        trades_df,
        equity_curve,
        execution=manifest.execution,
    )
    return TradingBacktestResult(
        run_id=manifest.run_id,
        trades=trades_df,
        equity_curve=equity_curve,
        metrics=metrics,
    )


def _build_equity_curve(
    candles: pd.DataFrame,
    trades: list[TradeRecord],
    *,
    fee_rate: float,
) -> pd.DataFrame:
    cash = 1.0
    units = 0.0
    current_trade = None
    trades_by_entry = {trade.entry_position: trade for trade in trades}
    trades_by_exit = {
        trade.exit_position: trade
        for trade in trades
        if trade.exit_reason != END_POLICY_FINAL_CLOSE
    }
    terminal_trade = next(
        (trade for trade in trades if trade.exit_reason == END_POLICY_FINAL_CLOSE),
        None,
    )

    rows = []
    for position, candle in candles.iterrows():
        if position in trades_by_exit and current_trade is not None:
            trade = trades_by_exit[position]
            gross_proceeds = units * trade.exit_price
            cash = gross_proceeds * (1.0 - fee_rate)
            units = 0.0
            current_trade = None

        if position in trades_by_entry:
            trade = trades_by_entry[position]
            if units != 0.0:
                raise ValueError("Overlapping trades are not supported")
            units = (cash * (1.0 - fee_rate)) / trade.entry_price
            cash = 0.0
            current_trade = trade

        exposed_during_candle = units != 0.0
        if exposed_during_candle:
            equity = units * float(candle["close"]) * (1.0 - fee_rate)
        else:
            equity = cash

        if (
            terminal_trade is not None
            and position == terminal_trade.exit_position
            and current_trade is not None
            and current_trade.entry_signal_id == terminal_trade.entry_signal_id
        ):
            gross_proceeds = units * terminal_trade.exit_price
            cash = gross_proceeds * (1.0 - fee_rate)
            units = 0.0
            current_trade = None
            equity = cash

        rows.append(
            {
                "timestamp": pd.Timestamp(candle["timestamp"]),
                "equity": float(equity),
                "in_position": exposed_during_candle,
            }
        )

    return pd.DataFrame(rows)


def _calculate_metrics(
    candles: pd.DataFrame,
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
    *,
    execution: ExecutionPolicy,
) -> BacktestMetrics:
    if trades.empty:
        returns = pd.Series(dtype="float64")
        trade_count = 0
        win_rate = 0.0
        average_return = 0.0
        median_return = 0.0
        profit_factor = None
        expectancy = 0.0
        average_holding = 0.0
    else:
        returns = trades["net_return"].astype(float)
        trade_count = len(trades)
        win_rate = float((returns > 0).mean())
        average_return = float(returns.mean())
        median_return = float(returns.median())
        positive = float(returns[returns > 0].sum())
        negative = float(-returns[returns < 0].sum())
        profit_factor = (positive / negative) if negative > 0 else None
        average_win = float(returns[returns > 0].mean()) if (returns > 0).any() else 0.0
        average_loss = float(returns[returns < 0].mean()) if (returns < 0).any() else 0.0
        expectancy = (win_rate * average_win) + ((1.0 - win_rate) * average_loss)
        average_holding = float(trades["holding_candles"].mean())

    running_max = equity_curve["equity"].cummax()
    drawdown = (equity_curve["equity"] / running_max) - 1.0
    max_drawdown = float(abs(drawdown.min())) if len(drawdown) else 0.0
    total_return = float(equity_curve.iloc[-1]["equity"] - 1.0)
    exposure = float(equity_curve["in_position"].mean()) if len(equity_curve) else 0.0

    return BacktestMetrics(
        trade_count=trade_count,
        win_rate=win_rate,
        average_return=average_return,
        median_return=median_return,
        profit_factor=profit_factor,
        expectancy=float(expectancy),
        max_drawdown=max_drawdown,
        total_return=total_return,
        benchmark_return=float(_benchmark_return(candles, execution)),
        average_holding_period=average_holding,
        exposure=exposure,
    )
