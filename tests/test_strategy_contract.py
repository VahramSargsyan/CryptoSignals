from pathlib import Path
import unittest

import pandas as pd

from core.contracts.strategy import make_strategy_output
from core.data.candles import normalize_candles
from strategies.crypto.true_stochrsi.strategy import (
    STRATEGY_ID as TRUE_STOCHRSI_ID,
    generate_true_stochrsi_outputs,
)
from strategies.crypto.vahram_original.contract_adapter import (
    generate_vahram_original_outputs,
)

FIXTURE = Path(__file__).parent / "fixtures" / "vahram_original_v1_saga_2025_03_03_2025_05_02.csv"


class StrategyContractTests(unittest.TestCase):
    def test_output_id_is_deterministic(self):
        first = make_strategy_output(
            strategy_id="TEST",
            strategy_version="1.0.0",
            symbol="BTCUSDT",
            timeframe="1D",
            timestamp="2026-01-01T00:00:00Z",
            signal="BUY",
            strength=50,
            reasons=("a",),
            source_commit_sha="abc",
        )
        second = make_strategy_output(
            strategy_id="TEST",
            strategy_version="1.0.0",
            symbol="btcusdt",
            timeframe="1d",
            timestamp=pd.Timestamp("2026-01-01T00:00:00Z"),
            signal="buy",
            strength=80,
            reasons=("different_reason",),
            source_commit_sha="def",
        )
        self.assertEqual(first.output_id, second.output_id)

    def test_invalid_strength_is_rejected(self):
        for strength in (-0.1, 100.1, float("nan"), float("inf")):
            with self.subTest(strength=strength):
                with self.assertRaises(ValueError):
                    make_strategy_output(
                        strategy_id="TEST",
                        strategy_version="1.0.0",
                        symbol="BTCUSDT",
                        timeframe="1D",
                        timestamp="2026-01-01",
                        signal="BUY",
                        strength=strength,
                        reasons=(),
                        source_commit_sha="abc",
                    )

    def test_original_contract_adapter_preserves_frozen_signals(self):
        canonical = normalize_candles(pd.read_csv(FIXTURE))
        outputs = generate_vahram_original_outputs(
            canonical,
            symbol="SAGAUSDT",
            timeframe="1D",
            source_commit_sha="test",
        )
        actual = [
            (output.timestamp.strftime("%Y-%m-%d"), output.signal, output.strength)
            for output in outputs
        ]
        self.assertEqual(
            actual,
            [
                ("2025-04-02", "BUY", 67.2),
                ("2025-04-25", "SELL", 74.9),
                ("2025-04-26", "SELL", 87.3),
                ("2025-04-28", "SELL", 66.7),
            ],
        )

    def test_true_stochrsi_is_a_separate_strategy_identity(self):
        canonical = normalize_candles(pd.read_csv(FIXTURE))
        outputs = generate_true_stochrsi_outputs(
            canonical,
            symbol="SAGAUSDT",
            timeframe="1D",
            source_commit_sha="test",
            include_hold=True,
        )
        self.assertTrue(outputs)
        self.assertTrue(all(output.strategy_id == TRUE_STOCHRSI_ID for output in outputs))
        self.assertTrue(all(output.strategy_id != "VAHRAM_ORIGINAL_V1" for output in outputs))


if __name__ == "__main__":
    unittest.main()
