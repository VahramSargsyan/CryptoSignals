from __future__ import annotations

import unittest
import pandas as pd

from scripts.research_btc_sma_reentry_development_v1 import (
    btc_cross_series,
    ranking_tuple,
)


class BtcSmaReentryDevelopmentV1Tests(unittest.TestCase):
    def test_btc_cross_is_close_causal(self):
        panel=pd.DataFrame({
            "BTC_close":[10,9,8,7,6,7,8,9,10,11]
        })
        cross=btc_cross_series(panel,2,4)
        # There must be no crossover before both SMAs exist.
        self.assertFalse(bool(cross.iloc[0]))
        self.assertFalse(bool(cross.iloc[1]))
        self.assertFalse(bool(cross.iloc[2]))
        # At least one later bullish crossover should exist.
        self.assertTrue(bool(cross.iloc[5:].any()))

    def test_ranking_prefers_more_both_wins_before_return(self):
        a={
            "fast_sma":10,"slow_sma":30,
            "robust_both_wins":3,"robust_dd_wins":4,"robust_return_wins":5,
            "repro_median_max_drawdown":-0.5,
            "repro_median_return":0.1,
            "full_median_return":0.2,
        }
        b={
            "fast_sma":5,"slow_sma":20,
            "robust_both_wins":2,"robust_dd_wins":10,"robust_return_wins":10,
            "repro_median_max_drawdown":-0.1,
            "repro_median_return":10.0,
            "full_median_return":20.0,
        }
        self.assertLess(ranking_tuple(a),ranking_tuple(b))

    def test_ranking_uses_less_negative_repro_drawdown_after_win_ties(self):
        a={
            "fast_sma":10,"slow_sma":30,
            "robust_both_wins":2,"robust_dd_wins":4,"robust_return_wins":5,
            "repro_median_max_drawdown":-0.4,
            "repro_median_return":0.1,
            "full_median_return":0.2,
        }
        b=dict(a)
        b["fast_sma"]=12
        b["repro_median_max_drawdown"]=-0.6
        self.assertLess(ranking_tuple(a),ranking_tuple(b))


if __name__=="__main__":
    unittest.main()
