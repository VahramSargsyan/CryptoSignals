from __future__ import annotations

import unittest

import pandas as pd

from scripts.run_relative_rotation_paper_live import build_notification_ru
from strategies.crypto.relative_rotation.paper_live import (
    ASSETS,
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

    def test_default_live_universe_is_u10_and_has_45_pair_states(self):
        self.assertEqual(
            ASSETS,
            ("ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK", "FIL", "HBAR"),
        )
        dates = pd.date_range("2026-01-01", periods=180, freq="D", tz="UTC")
        payload = {"timestamp": dates}
        for index, asset in enumerate(ASSETS, start=1):
            payload[f"{asset}_close"] = [float(index)] * len(dates)
        panel = pd.DataFrame(payload)

        events, states = build_pair_monitor(panel)
        self.assertEqual(events, [])
        self.assertEqual(len(states), 45)

    def test_atom_watch_can_be_evaluated_independently_of_held_asset(self):
        latest = pd.Timestamp("2026-09-27", tz="UTC")
        events = [
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "ATOM/FIL",
                "from_asset": "ATOM",
                "to_asset": "FIL",
                "max_dislocation": 0.31,
            },
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "TWT/HBAR",
                "from_asset": "TWT",
                "to_asset": "HBAR",
                "max_dislocation": 0.42,
            },
        ]

        held = choose_held_events(events, held_asset="TWT", latest_date=latest)
        atom_watch = choose_held_events(events, held_asset="ATOM", latest_date=latest)

        self.assertEqual(held["primary_confirmed"]["to_asset"], "HBAR")
        self.assertEqual(atom_watch["primary_confirmed"]["to_asset"], "FIL")


    def test_russian_telegram_notification_for_initialization(self):
        payload = {
            "held_asset": "ATOM",
            "latest_closed_candle": "2026-09-26T00:00:00+00:00",
            "held_events": {
                "primary_confirmed": None,
                "confirmed": [],
                "armed": [],
            },
            "watch_events": {},
            "defensive": {
                "active": False,
                "defensive_asset": None,
                "breadth": 8,
            },
            "latest_defensive_events": [],
            "force_notify": True,
        }

        text = build_notification_ru(payload)

        self.assertIn("Закрытая свеча: 2026-09-26T00:00:00+00:00", text)
        self.assertIn("Текущий актив: ATOM", text)
        self.assertIn("нет подтверждённой ротации", text)
        self.assertIn("Защитный режим: ВЫКЛ; ширина рынка=8/10", text)
        self.assertNotIn("Closed candle:", text)
        self.assertNotIn("Held asset:", text)

    def test_russian_telegram_notification_for_confirmed_rotation(self):
        event = {
            "from_asset": "ATOM",
            "to_asset": "TWT",
            "pair": "ATOM/TWT",
            "max_dislocation": 0.21,
            "reversal_from_extreme": 0.04,
        }
        payload = {
            "held_asset": "ATOM",
            "latest_closed_candle": "2026-09-27T00:00:00+00:00",
            "held_events": {
                "primary_confirmed": event,
                "confirmed": [event],
                "armed": [],
            },
            "watch_events": {},
            "defensive": {
                "active": False,
                "defensive_asset": None,
                "breadth": 7,
            },
            "latest_defensive_events": [],
            "force_notify": False,
        }

        text = build_notification_ru(payload)

        self.assertIn("РОТАЦИЯ ПОДТВЕРЖДЕНА", text)
        self.assertIn("ATOM -> TWT", text)
        self.assertIn("отклонение +21.00%", text)
        self.assertIn("разворот от экстремума +4.00%", text)
        self.assertIn("требуется ручное подтверждение", text)

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
