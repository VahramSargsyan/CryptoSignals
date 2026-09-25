import unittest

import pandas as pd

from strategies.crypto.link_level_grid.strategy import (
    GridBacktestConfig,
    GridDefinition,
    RollingRangePolicy,
    _mid_target,
    build_causal_range_schedule,
    main_level_allocations,
    micro_sublevel_allocations,
    run_grid_backtest,
    sublevel_label,
)


class LinkLevelGridStrategyTests(unittest.TestCase):
    def test_grid_reproduces_workbook_geometry(self):
        grid = GridDefinition(high=31.03, low=8.17)
        self.assertAlmostEqual(grid.main_step, (31.03 - 8.17) / 16)
        self.assertAlmostEqual(grid.sub_step, (31.03 - 8.17) / 64)
        self.assertAlmostEqual(grid.boundary_price(1), 30.6728125)
        self.assertAlmostEqual(grid.boundary_price(2), 30.315625)
        self.assertAlmostEqual(grid.boundary_price(3), 29.9584375)
        self.assertAlmostEqual(grid.boundary_price(4), 29.60125)
        self.assertAlmostEqual(grid.boundary_price(64), 8.17)
        self.assertEqual(sublevel_label(1), "1D")
        self.assertEqual(sublevel_label(4), "1A")
        self.assertEqual(sublevel_label(64), "16A")

    def test_reserved_allocation_presets_sum_to_one(self):
        linear = main_level_allocations("linear_depth_reserved", layer="MID")
        self.assertAlmostEqual(sum(linear), 1.0)
        self.assertGreater(linear[-1], linear[0])

        micro = micro_sublevel_allocations("linear_depth_reserved")
        self.assertEqual(len(micro), 64)
        self.assertAlmostEqual(sum(micro), 1.0)

        equal = main_level_allocations("equal_reserved", layer="MID")
        self.assertAlmostEqual(sum(equal), 1.0)
        self.assertTrue(all(abs(value - 1 / 16) < 1e-12 for value in equal))

    def test_rolling_range_uses_only_past_candles(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-01-01", "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1},
                {"timestamp": "2026-01-02", "open": 10, "high": 12, "low": 8, "close": 11, "volume": 1},
                {"timestamp": "2026-01-03", "open": 11, "high": 13, "low": 7, "close": 12, "volume": 1},
                {"timestamp": "2026-01-04", "open": 12, "high": 100, "low": 6, "close": 90, "volume": 1},
            ]
        )
        policy = RollingRangePolicy(
            lookback_candles=10,
            min_history_candles=2,
            refresh_candles=1,
        )
        schedule = build_causal_range_schedule(candles, policy)

        self.assertIsNone(schedule[0])
        self.assertIsNone(schedule[1])
        self.assertEqual(schedule[2].high, 12.0)
        self.assertEqual(schedule[2].low, 8.0)
        # The huge high=100 on Jan 4 is the current candle and must not leak
        # into the range used on that same day.
        self.assertEqual(schedule[3].high, 13.0)
        self.assertEqual(schedule[3].low, 7.0)

    def test_deep_mid_exit_uses_first_of_percent_or_ten_sublevels(self):
        # span=64 -> one dollar per sublevel
        grid = GridDefinition(high=100.0, low=36.0)
        entry = grid.boundary_price(64)
        target, percent_target, grid_target = _mid_target(
            grid=grid,
            main_level=16,
            entry_sublevel=64,
            entry_price=entry,
            ten_sublevel_from_main=7,
        )
        self.assertAlmostEqual(entry, 36.0)
        self.assertAlmostEqual(grid_target, grid.boundary_price(54))
        self.assertGreater(percent_target, grid_target)
        self.assertAlmostEqual(target, grid.boundary_price(54))

    def test_upper_mid_exit_has_no_ten_sublevel_alternative(self):
        grid = GridDefinition(high=100.0, low=36.0)
        entry_sublevel = 24  # main level 6 A
        entry = grid.boundary_price(entry_sublevel)
        target, percent_target, grid_target = _mid_target(
            grid=grid,
            main_level=6,
            entry_sublevel=entry_sublevel,
            entry_price=entry,
            ten_sublevel_from_main=7,
        )
        self.assertIsNone(grid_target)
        self.assertAlmostEqual(target, percent_target)
        self.assertAlmostEqual(target, entry * 1.53)

    def test_evaluation_start_requires_full_prehistory_and_blocks_early_trades(self):
        candles = []
        for i, ts in enumerate(pd.date_range("2025-01-01", periods=20, freq="D", tz="UTC")):
            candles.append(
                {
                    "timestamp": ts,
                    "open": 100.0,
                    "high": 110.0 + i,
                    "low": 90.0 - i,
                    "close": 100.0,
                    "volume": 1000.0,
                }
            )
        frame = pd.DataFrame(candles)
        cfg = GridBacktestConfig(
            micro_capital=1000.0,
            mid_capital=1000.0,
            allocation_preset="equal_reserved",
            fee_bps=0.0,
            slippage_bps=0.0,
            rolling_range=RollingRangePolicy(
                lookback_candles=10,
                min_history_candles=10,
                refresh_candles=30,
            ),
        )

        result = run_grid_backtest(
            frame,
            dataset_id="TEST:PREHISTORY",
            source_commit_sha="abc123",
            config=cfg,
            evaluation_start=pd.Timestamp("2025-01-11T00:00:00Z"),
        )

        self.assertEqual(result.summary["prehistory_candles"], 10)
        self.assertEqual(result.summary["candles"], 10)
        self.assertEqual(
            result.summary["period_start"],
            "2025-01-11T00:00:00+00:00",
        )
        self.assertTrue((result.trades["entry_timestamp"] >= pd.Timestamp("2025-01-11T00:00:00Z")).all() if not result.trades.empty else True)

        first_range = result.range_history.iloc[0]
        self.assertEqual(first_range["timestamp"], pd.Timestamp("2025-01-11T00:00:00Z"))
        # Jan 11 grid must use only Jan 1..Jan 10 history.
        self.assertEqual(first_range["high"], 119.0)
        self.assertEqual(first_range["low"], 81.0)

    def test_evaluation_start_rejects_insufficient_prehistory(self):
        frame = pd.DataFrame(
            [
                {
                    "timestamp": ts,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                }
                for ts in pd.date_range("2025-01-01", periods=12, freq="D", tz="UTC")
            ]
        )
        cfg = GridBacktestConfig(
            rolling_range=RollingRangePolicy(
                lookback_candles=10,
                min_history_candles=10,
                refresh_candles=30,
            )
        )
        with self.assertRaisesRegex(ValueError, "Insufficient prehistory"):
            run_grid_backtest(
                frame,
                dataset_id="TEST:SHORT",
                source_commit_sha="abc123",
                config=cfg,
                evaluation_start=pd.Timestamp("2025-01-06T00:00:00Z"),
            )

    def test_backtest_keeps_micro_and_mid_capital_separate(self):
        candles = []
        # 140 daily candles, oscillating enough to create fills/exits after
        # a 30-candle warmup without requiring future information.
        for i, ts in enumerate(pd.date_range("2026-01-01", periods=140, freq="D", tz="UTC")):
            phase = i % 20
            center = 80.0 + (10.0 if phase < 10 else -10.0)
            candles.append(
                {
                    "timestamp": ts,
                    "open": center,
                    "high": center + 8.0,
                    "low": center - 8.0,
                    "close": center + (2.0 if phase % 2 == 0 else -2.0),
                    "volume": 1000.0,
                }
            )
        frame = pd.DataFrame(candles)
        cfg = GridBacktestConfig(
            micro_capital=700.0,
            mid_capital=1300.0,
            allocation_preset="linear_depth_reserved",
            fee_bps=0.0,
            slippage_bps=0.0,
            rolling_range=RollingRangePolicy(
                lookback_candles=120,
                min_history_candles=30,
                refresh_candles=30,
            ),
        )
        result = run_grid_backtest(
            frame,
            dataset_id="TEST:LINKUSDT:1D",
            source_commit_sha="abc123",
            config=cfg,
            evaluation_start=pd.Timestamp("2026-01-31T00:00:00Z"),
        )
        self.assertEqual(result.summary["micro_initial_capital"], 700.0)
        self.assertEqual(result.summary["mid_initial_capital"], 1300.0)
        self.assertEqual(result.summary["total_initial_capital"], 2000.0)
        self.assertTrue(result.summary["research_assumptions"]["capital_is_reserved_per_slot"])
        self.assertGreater(len(result.equity_curve), 0)
        self.assertGreater(len(result.range_history), 0)
        self.assertTrue(pd.notna(result.summary["total_return"]))


if __name__ == "__main__":
    unittest.main()
