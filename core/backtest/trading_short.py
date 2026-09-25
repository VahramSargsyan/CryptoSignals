from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from core.backtest.contracts import BacktestRunManifest, ExecutionPolicy
from core.backtest.execution import next_candle_open_fill
from core.backtest.trading import BacktestMetrics, TradeRecord, TradingBacktestResult
from core.contracts.strategy import StrategyOutput

ENGINE_NAME = "SHORT_ONLY_SIGNAL_FLIP_V1"
END_POLICY_FINAL_CLOSE = "COVER_AT_FINAL_CLOSE"
ZERO_EQUITY_LIQUIDATION = "ZERO_EQUITY_LIQUIDATION_AT_INTRADAY_HIGH"


@dataclass(frozen=True)
class ShortOnlyTradingPolicy:
    entry_strength_min: float = 0.0
    exit_strength_min: float = 0.0
    end_of_test_policy: str = END_POLICY_FINAL_CLOSE
    bankruptcy_policy: str = ZERO_EQUITY_LIQUIDATION

    def __post_init__(self) -> None:
        for value in (self.entry_strength_min, self.exit_strength_min):
            if not math.isfinite(value) or not 0 <= value <= 100:
                raise ValueError("strength thresholds must be finite and between 0 and 100")
        if self.end_of_test_policy != END_POLICY_FINAL_CLOSE:
            raise ValueError("Unsupported end-of-test policy")
        if self.bankruptcy_policy != ZERO_EQUITY_LIQUIDATION:
            raise ValueError("Unsupported bankruptcy policy")

    def to_config(self) -> dict:
        return {
            "entry_strength_min": float(self.entry_strength_min),
            "exit_strength_min": float(self.exit_strength_min),
            "end_of_test_policy": self.end_of_test_policy,
            "bankruptcy_policy": self.bankruptcy_policy,
            "position_mode": "SHORT_ONLY_FULL_EQUITY_1X_NOTIONAL",
            "entry_signal": "SELL",
            "exit_signal": "BUY",
        }


def _prepare_candles(candles: pd.DataFrame) -> pd.DataFrame:
    required = {"timestamp", "open", "high", "close"}
    missing = sorted(required.difference(candles.columns))
    if missing:
        raise ValueError(f"Missing candle columns for short backtest: {missing}")

    ordered = candles.sort_values("timestamp", kind="stable").reset_index(drop=True).copy()
    ordered["timestamp"] = pd.to_datetime(ordered["timestamp"], utc=True)
    if ordered["timestamp"].duplicated().any():
        raise ValueError("Short backtest requires unique candle timestamps")
    if (ordered[["open", "high", "close"]] <= 0).any().any():
        raise ValueError("Short backtest requires positive open/high/close prices")
    if (ordered["high"] < ordered[["open", "close"]].max(axis=1)).any():
        raise ValueError("Short backtest requires valid high prices")
    return ordered


def _validate_manifest_policy(
    manifest: BacktestRunManifest,
    policy: ShortOnlyTradingPolicy,
) -> None:
    if manifest.engine_name != ENGINE_NAME:
        raise ValueError(f"manifest.engine_name must be {ENGINE_NAME}")
    if dict(manifest.engine_config) != policy.to_config():
        raise ValueError("manifest.engine_config must exactly match the short trading policy")


def _validate_outputs(
    outputs: Iterable[StrategyOutput],
    *,
    manifest: BacktestRunManifest,
    positions: dict[pd.Timestamp, int],
) -> dict[int, StrategyOutput]:
    prepared: dict[int, StrategyOutput] = {}
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
        if position in prepared:
            raise ValueError("Multiple non-HOLD signals at one candle are ambiguous")
        prepared[position] = output
    return prepared


def _capital_after_cover(
    *,
    entry_capital: float,
    units: float,
    entry_price: float,
    exit_price: float,
    fee_rate: float,
) -> float:
    entry_fee = entry_capital * fee_rate
    exit_fee = units * exit_price * fee_rate
    return (
        entry_capital
        - entry_fee
        + units * (entry_price - exit_price)
        - exit_fee
    )


def _mark_equity(
    *,
    entry_capital: float,
    units: float,
    entry_price: float,
    mark_price: float,
    fee_rate: float,
) -> float:
    return _capital_after_cover(
        entry_capital=entry_capital,
        units=units,
        entry_price=entry_price,
        exit_price=mark_price,
        fee_rate=fee_rate,
    )


def _short_gross_return(entry_price: float, exit_price: float) -> float:
    return 1.0 - (exit_price / entry_price)


def _short_benchmark_return(
    candles: pd.DataFrame,
    execution: ExecutionPolicy,
) -> float:
    fee_rate = execution.fee_bps / 10_000.0
    slip = execution.slippage_bps / 10_000.0
    entry_price = float(candles.iloc[0]["open"]) * (1.0 - slip)
    entry_capital = 1.0
    units = entry_capital / entry_price

    for _, candle in candles.iterrows():
        adverse_cover = float(candle["high"]) * (1.0 + slip)
        if _mark_equity(
            entry_capital=entry_capital,
            units=units,
            entry_price=entry_price,
            mark_price=adverse_cover,
            fee_rate=fee_rate,
        ) <= 0:
            return -1.0

    exit_price = float(candles.iloc[-1]["close"]) * (1.0 + slip)
    capital = _capital_after_cover(
        entry_capital=entry_capital,
        units=units,
        entry_price=entry_price,
        exit_price=exit_price,
        fee_rate=fee_rate,
    )
    return max(capital, 0.0) - 1.0


def run_short_only_backtest(
    candles: pd.DataFrame,
    outputs: Iterable[StrategyOutput],
    *,
    manifest: BacktestRunManifest,
    policy: ShortOnlyTradingPolicy,
) -> TradingBacktestResult:
    _validate_manifest_policy(manifest, policy)
    ordered = _prepare_candles(candles)
    if ordered.empty:
        raise ValueError("Short backtest requires at least one candle")

    positions = {timestamp: position for position, timestamp in enumerate(ordered["timestamp"])}
    outputs_by_position = _validate_outputs(outputs, manifest=manifest, positions=positions)
    fee_rate = manifest.execution.fee_bps / 10_000.0
    slip = manifest.execution.slippage_bps / 10_000.0

    cash = 1.0
    open_trade: dict | None = None
    pending_output: StrategyOutput | None = None
    bankrupt = False
    trades: list[TradeRecord] = []
    equity_rows: list[dict] = []

    for position, candle in ordered.iterrows():
        timestamp = pd.Timestamp(candle["timestamp"])

        if not bankrupt and pending_output is not None:
            if open_trade is None:
                if (
                    pending_output.signal == "SELL"
                    and pending_output.strength >= policy.entry_strength_min
                ):
                    signal_position = position - 1
                    fill = next_candle_open_fill(
                        ordered,
                        signal_position=signal_position,
                        side="SELL",
                        policy=manifest.execution,
                    )
                    entry_capital = cash
                    units = entry_capital / fill.execution_price
                    open_trade = {
                        "entry_signal_id": pending_output.output_id,
                        "entry_signal_timestamp": pd.Timestamp(pending_output.timestamp),
                        "entry_position": position,
                        "raw_entry_price": fill.raw_open_price,
                        "entry_price": fill.execution_price,
                        "entry_strength": pending_output.strength,
                        "entry_capital": entry_capital,
                        "units": units,
                    }
            elif (
                pending_output.signal == "BUY"
                and pending_output.strength >= policy.exit_strength_min
            ):
                signal_position = position - 1
                fill = next_candle_open_fill(
                    ordered,
                    signal_position=signal_position,
                    side="BUY",
                    policy=manifest.execution,
                )
                capital_after = _capital_after_cover(
                    entry_capital=open_trade["entry_capital"],
                    units=open_trade["units"],
                    entry_price=open_trade["entry_price"],
                    exit_price=fill.execution_price,
                    fee_rate=fee_rate,
                )
                capital_after = max(capital_after, 0.0)
                gross_return = _short_gross_return(
                    open_trade["entry_price"],
                    fill.execution_price,
                )
                net_return = (capital_after / open_trade["entry_capital"]) - 1.0
                trades.append(
                    TradeRecord(
                        run_id=manifest.run_id,
                        strategy_id=manifest.strategy_id,
                        strategy_version=manifest.strategy_version,
                        symbol=manifest.symbol.upper(),
                        timeframe=manifest.timeframe.upper(),
                        entry_signal_id=open_trade["entry_signal_id"],
                        exit_signal_id=pending_output.output_id,
                        entry_signal_timestamp=open_trade["entry_signal_timestamp"],
                        exit_signal_timestamp=pd.Timestamp(pending_output.timestamp),
                        entry_timestamp=pd.Timestamp(
                            ordered.iloc[open_trade["entry_position"]]["timestamp"]
                        ),
                        exit_timestamp=fill.entry_timestamp,
                        entry_position=open_trade["entry_position"],
                        exit_position=position,
                        raw_entry_price=open_trade["raw_entry_price"],
                        raw_exit_price=fill.raw_open_price,
                        entry_price=open_trade["entry_price"],
                        exit_price=fill.execution_price,
                        entry_strength=open_trade["entry_strength"],
                        exit_strength=pending_output.strength,
                        exit_reason="BUY_SIGNAL_NEXT_OPEN",
                        holding_candles=position - open_trade["entry_position"],
                        gross_return=gross_return,
                        net_return=net_return,
                    )
                )
                cash = capital_after
                open_trade = None
                if cash <= 0:
                    bankrupt = True

        pending_output = None

        exposed_during_candle = open_trade is not None
        if open_trade is not None:
            adverse_cover = float(candle["high"]) * (1.0 + slip)
            adverse_equity = _mark_equity(
                entry_capital=open_trade["entry_capital"],
                units=open_trade["units"],
                entry_price=open_trade["entry_price"],
                mark_price=adverse_cover,
                fee_rate=fee_rate,
            )
            if adverse_equity <= 0:
                gross_return = _short_gross_return(
                    open_trade["entry_price"],
                    adverse_cover,
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
                        entry_timestamp=pd.Timestamp(
                            ordered.iloc[open_trade["entry_position"]]["timestamp"]
                        ),
                        exit_timestamp=timestamp,
                        entry_position=open_trade["entry_position"],
                        exit_position=position,
                        raw_entry_price=open_trade["raw_entry_price"],
                        raw_exit_price=float(candle["high"]),
                        entry_price=open_trade["entry_price"],
                        exit_price=adverse_cover,
                        entry_strength=open_trade["entry_strength"],
                        exit_strength=None,
                        exit_reason=ZERO_EQUITY_LIQUIDATION,
                        holding_candles=position - open_trade["entry_position"] + 1,
                        gross_return=gross_return,
                        net_return=-1.0,
                    )
                )
                cash = 0.0
                open_trade = None
                bankrupt = True

        if position == len(ordered) - 1 and open_trade is not None:
            raw_exit_price = float(candle["close"])
            exit_price = raw_exit_price * (1.0 + slip)
            capital_after = _capital_after_cover(
                entry_capital=open_trade["entry_capital"],
                units=open_trade["units"],
                entry_price=open_trade["entry_price"],
                exit_price=exit_price,
                fee_rate=fee_rate,
            )
            capital_after = max(capital_after, 0.0)
            gross_return = _short_gross_return(open_trade["entry_price"], exit_price)
            net_return = (capital_after / open_trade["entry_capital"]) - 1.0
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
                    entry_timestamp=pd.Timestamp(
                        ordered.iloc[open_trade["entry_position"]]["timestamp"]
                    ),
                    exit_timestamp=timestamp,
                    entry_position=open_trade["entry_position"],
                    exit_position=position,
                    raw_entry_price=open_trade["raw_entry_price"],
                    raw_exit_price=raw_exit_price,
                    entry_price=open_trade["entry_price"],
                    exit_price=exit_price,
                    entry_strength=open_trade["entry_strength"],
                    exit_strength=None,
                    exit_reason=END_POLICY_FINAL_CLOSE,
                    holding_candles=position - open_trade["entry_position"] + 1,
                    gross_return=gross_return,
                    net_return=net_return,
                )
            )
            cash = capital_after
            open_trade = None
            if cash <= 0:
                bankrupt = True

        if open_trade is not None:
            close_cover = float(candle["close"]) * (1.0 + slip)
            equity = max(
                _mark_equity(
                    entry_capital=open_trade["entry_capital"],
                    units=open_trade["units"],
                    entry_price=open_trade["entry_price"],
                    mark_price=close_cover,
                    fee_rate=fee_rate,
                ),
                0.0,
            )
        else:
            equity = cash

        equity_rows.append(
            {
                "timestamp": timestamp,
                "equity": float(equity),
                "in_position": bool(exposed_during_candle),
            }
        )

        output = outputs_by_position.get(position)
        if (
            not bankrupt
            and position + 1 < len(ordered)
            and output is not None
        ):
            pending_output = output

    trades_df = pd.DataFrame([trade.to_record() for trade in trades])
    equity_curve = pd.DataFrame(equity_rows)
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
        benchmark_return=float(_short_benchmark_return(candles, execution)),
        average_holding_period=average_holding,
        exposure=exposure,
    )
