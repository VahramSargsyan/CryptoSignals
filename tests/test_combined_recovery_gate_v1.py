from __future__ import annotations

import unittest
import pandas as pd

from scripts.research_combined_recovery_gate_v1 import (
    BTC_CANDIDATES,
    RELEASE_DATES_2021,
    add_causal_m2,
    build_gate_map,
)


class CombinedRecoveryGateV1Tests(unittest.TestCase):
    def test_btc_candidates_are_frozen(self):
        self.assertEqual(BTC_CANDIDATES, ((25,100),(30,100),(12,100)))

    def test_2021_release_schedule_contains_first_monthly_and_august(self):
        self.assertIn(pd.Timestamp("2021-02-23"), RELEASE_DATES_2021)
        self.assertIn(pd.Timestamp("2021-08-24"), RELEASE_DATES_2021)

    def test_gate_requires_m2_and_breadth(self):
        dates=pd.date_range("2024-01-01",periods=3,freq="D",tz="UTC")
        panel=pd.DataFrame({
            "timestamp":dates,
            "breadth_sma200":[4,3,5],
            "m2_expansion_all":[True,True,False],
        })
        crosses={dates[0]:True,dates[1]:True,dates[2]:True}
        gate,diag=build_gate_map(panel,crosses)
        self.assertTrue(gate[dates[0]])
        self.assertFalse(gate[dates[1]])
        self.assertFalse(gate[dates[2]])
        self.assertEqual(diag["passed_all_gates"],1)
        self.assertEqual(diag["blocked_by_breadth_after_m2_pass"],1)
        self.assertEqual(diag["blocked_by_m2"],1)


if __name__=="__main__":
    unittest.main()
