import unittest
from unittest.mock import patch

import pandas as pd

from strategies.crypto.weighted_multi_signal_v2.strategy import (
    CORE_BASE_STRENGTH,
    MA_CONFIRM_BONUS,
    MIN_BODY_STRENGTH,
    STRATEGY_ID,
    _signal_and_strength,
    evaluate_core_consensus,
    generate_weighted_multi_signal_v2_outputs,
)


class WeightedMultiSignalV2Tests(unittest.TestCase):
    def test_frozen_identity_and_strengths(self):
        self.assertEqual(CORE_BASE_STRENGTH, 75.0)
        self.assertEqual(MA_CONFIRM_BONUS, 25.0)
        self.assertEqual(MIN_BODY_STRENGTH, 0.50)

    def test_all_three_bullish_core_blocks_are_required(self):
        row = pd.Series({
            "open":100.0,"close":110.0,"volume":150.0,"volume_ma":100.0,
            "candle_body_strength":0.7,"macd":2.0,"macd_signal":1.0,
            "stoch_rsi_k":70.0,"stoch_rsi_d":50.0,
            "sma_200":90.0,"ma_bull_stack":False,"ma_bear_stack":False,
        })
        c=evaluate_core_consensus(row)
        self.assertTrue(c.bullish_core)
        self.assertEqual(_signal_and_strength(c),("BUY",75.0))

        missing_volume=row.copy()
        missing_volume["volume"]=80.0
        c=evaluate_core_consensus(missing_volume)
        self.assertFalse(c.bullish_core)
        self.assertEqual(_signal_and_strength(c),(None,0.0))

    def test_ma_confirms_strength_but_cannot_create_trade(self):
        row = pd.Series({
            "open":100.0,"close":110.0,"volume":80.0,"volume_ma":100.0,
            "candle_body_strength":0.7,"macd":2.0,"macd_signal":1.0,
            "stoch_rsi_k":70.0,"stoch_rsi_d":50.0,
            "sma_200":90.0,"ma_bull_stack":True,"ma_bear_stack":False,
        })
        c=evaluate_core_consensus(row)
        self.assertFalse(c.bullish_core)
        self.assertEqual(_signal_and_strength(c),(None,0.0))

        row["volume"]=150.0
        c=evaluate_core_consensus(row)
        self.assertTrue(c.bullish_core)
        self.assertTrue(c.bullish_ma_confirmed)
        self.assertEqual(_signal_and_strength(c),("BUY",100.0))

    def test_bearish_core_is_exact_mirror(self):
        row = pd.Series({
            "open":110.0,"close":90.0,"volume":160.0,"volume_ma":100.0,
            "candle_body_strength":0.8,"macd":-2.0,"macd_signal":-1.0,
            "stoch_rsi_k":30.0,"stoch_rsi_d":50.0,
            "sma_200":100.0,"ma_bull_stack":False,"ma_bear_stack":True,
        })
        c=evaluate_core_consensus(row)
        self.assertTrue(c.bearish_core)
        self.assertTrue(c.bearish_ma_confirmed)
        self.assertEqual(_signal_and_strength(c),("SELL",100.0))

    @patch("strategies.crypto.weighted_multi_signal_v2.strategy.build_standard_features")
    def test_generator_emits_v2_contract_identity(self,mocked_features):
        candles=pd.DataFrame([
            {"timestamp":"2025-01-01T00:00:00Z","open":100,"high":112,"low":99,"close":110,"volume":150},
            {"timestamp":"2025-01-02T00:00:00Z","open":110,"high":111,"low":88,"close":90,"volume":160},
        ])
        mocked_features.return_value=pd.DataFrame([
            {"macd":2,"macd_signal":1,"stoch_rsi_k":70,"stoch_rsi_d":50,
             "volume_ma":100,"candle_body_strength":0.8,"sma_200":90,
             "ma_bull_stack":False,"ma_bear_stack":False},
            {"macd":-2,"macd_signal":-1,"stoch_rsi_k":30,"stoch_rsi_d":50,
             "volume_ma":100,"candle_body_strength":0.8,"sma_200":100,
             "ma_bull_stack":False,"ma_bear_stack":True},
        ])
        outputs=generate_weighted_multi_signal_v2_outputs(
            candles,symbol="BTCUSDT",timeframe="1D",source_commit_sha="test",run_id="RUN"
        )
        self.assertEqual([x.signal for x in outputs],["BUY","SELL"])
        self.assertEqual([x.strength for x in outputs],[75.0,100.0])
        self.assertTrue(all(x.strategy_id==STRATEGY_ID for x in outputs))
        self.assertTrue(all(x.strategy_version=="2.0.0" for x in outputs))


if __name__=="__main__":
    unittest.main()
