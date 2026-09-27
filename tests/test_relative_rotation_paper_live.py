from __future__ import annotations

import unittest

import pandas as pd

from strategies.crypto.relative_rotation.paper_live import (
    _defensive_state_machine,
    build_pair_monitor,
    choose_held_events,
)


class RelativeRotationPaperLiveTests(unittest.TestCase):
    def test_high_dislocation_arms_then_confirms_from_right_to_left(self):
        dates = pd.date_range("2026-01-01", periods=183, freq="D", tz="UTC")
        atom = [1.0] * 183
        twt = [1.0] * 180 + [1.20, 1.25, 1.20]
        panel = pd.DataFrame(
            {
                "timestamp": dates,
                "ATOM_close": atom,
                "TWT_close": twt,
            }
        )

        events, states = build_pair_monitor(panel, assets=("ATOM", "TWT"))
        armed = [row for row in events if row["event"] == "ARMED"]
        confirmed = [row for row in events if row["event"] == "CONFIRMED"]

        self.assertEqual(len(armed), 1)
        self.assertEqual(armed[0]["from_asset"], "TWT")
        self.assertEqual(armed[0]["to_asset"], "ATOM")
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(confirmed[0]["from_asset"], "TWT")
        self.assertEqual(confirmed[0]["to_asset"], "ATOM")
        self.assertGreaterEqual(confirmed[0]["reversal_from_extreme"], 0.03)
        self.assertEqual(states[0]["mode"], "NONE")

    def test_low_dislocation_arms_from_left_to_right(self):
        dates = pd.date_range("2026-01-01", periods=180, freq="D", tz="UTC")
        atom = [1.0] * 180
        twt = [1.0] * 179 + [0.80]
        panel = pd.DataFrame(
            {
                "timestamp": dates,
                "ATOM_close": atom,
                "TWT_close": twt,
            }
        )

        events, states = build_pair_monitor(panel, assets=("ATOM", "TWT"))
        self.assertEqual(events[-1]["event"], "ARMED")
        self.assertEqual(events[-1]["from_asset"], "ATOM")
        self.assertEqual(events[-1]["to_asset"], "TWT")
        self.assertEqual(states[0]["mode"], "LOW")
        self.assertEqual(states[0]["from_asset"], "ATOM")
        self.assertEqual(states[0]["to_asset"], "TWT")

    def test_router_chooses_strongest_confirmed_outbound_candidate(self):
        latest = pd.Timestamp("2026-09-27", tz="UTC")
        events = [
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "ATOM/TWT",
                "from_asset": "ATOM",
                "to_asset": "TWT",
                "max_dislocation": 0.21,
            },
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "ATOM/SOL",
                "from_asset": "ATOM",
                "to_asset": "SOL",
                "max_dislocation": 0.34,
            },
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "TWT/LINK",
                "from_asset": "TWT",
                "to_asset": "LINK",
                "max_dislocation": 0.99,
            },
        ]

        selected = choose_held_events(events, held_asset="ATOM", latest_date=latest)
        self.assertEqual(len(selected["confirmed"]), 2)
        self.assertEqual(selected["primary_confirmed"]["to_asset"], "SOL")

    def test_defensive_state_machine_enters_and_exits_after_three_closes(self):
        dates = list(pd.date_range("2026-01-01", periods=8, freq="D", tz="UTC"))
        breadth = [6, 3, 3, 3, 4, 5, 5, 5]
        low_vol = ["TRX"] * 8

        events, state = _defensive_state_machine(dates, breadth, low_vol)
        self.assertEqual([row["event"] for row in events], ["DEFENSIVE_ENTER", "DEFENSIVE_EXIT"])
        self.assertEqual(events[0]["defensive_asset"], "TRX")
        self.assertFalse(state["active"])
        self.assertIsNone(state["defensive_asset"])

    def test_defensive_asset_is_frozen_until_exit(self):
        dates = list(pd.date_range("2026-01-01", periods=5, freq="D", tz="UTC"))
        breadth = [3, 3, 3, 2, 2]
        low_vol = ["TRX", "TRX", "TRX", "BNB", "BNB"]

        events, state = _defensive_state_machine(dates, breadth, low_vol)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["defensive_asset"], "TRX")
        self.assertTrue(state["active"])
        self.assertEqual(state["defensive_asset"], "TRX")


if __name__ == "__main__":
    unittest.main()
