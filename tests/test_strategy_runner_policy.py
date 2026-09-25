import unittest

from scripts.run_strategy_comparison import _resolve_trading_policy


class StrategyRunnerPolicyTests(unittest.TestCase):
    def test_absent_policy_keeps_legacy_defaults(self):
        self.assertIsNone(_resolve_trading_policy("LONG_ONLY", {}))
        self.assertIsNone(_resolve_trading_policy("SHORT_ONLY", {}))

    def test_short_policy_resolves_entry_and_exit_thresholds(self):
        policy = _resolve_trading_policy(
            "SHORT_ONLY",
            {
                "trading_policy": {
                    "entry_strength_min": 100,
                    "exit_strength_min": 0,
                }
            },
        )
        self.assertEqual(policy.entry_strength_min, 100.0)
        self.assertEqual(policy.exit_strength_min, 0.0)
        self.assertEqual(
            policy.to_config()["position_mode"],
            "SHORT_ONLY_FULL_EQUITY_1X_NOTIONAL",
        )

    def test_long_policy_resolves_thresholds_without_changing_mode(self):
        policy = _resolve_trading_policy(
            "LONG_ONLY",
            {
                "trading_policy": {
                    "entry_strength_min": 80,
                    "exit_strength_min": 20,
                }
            },
        )
        self.assertEqual(policy.entry_strength_min, 80.0)
        self.assertEqual(policy.exit_strength_min, 20.0)
        self.assertEqual(
            policy.to_config()["position_mode"],
            "LONG_ONLY_FULL_EQUITY",
        )

    def test_unknown_policy_key_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "Unsupported trading_policy keys"):
            _resolve_trading_policy(
                "SHORT_ONLY",
                {"trading_policy": {"entry_strength_min": 100, "leverage": 3}},
            )

    def test_invalid_strength_is_rejected_by_policy(self):
        with self.assertRaises(ValueError):
            _resolve_trading_policy(
                "SHORT_ONLY",
                {"trading_policy": {"entry_strength_min": 101}},
            )


if __name__ == "__main__":
    unittest.main()
