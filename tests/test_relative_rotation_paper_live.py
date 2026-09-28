from __future__ import annotations

import unittest

import pandas as pd

from scripts.run_relative_rotation_paper_live import build_notification_ru
from strategies.crypto.relative_rotation.paper_live import (
    ASSETS,
    TARGET_ASSETS,
    SUNSET_ASSETS,
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

    def test_transition_monitor_has_frozen_target_u10_plus_three_sunset_assets(self):
        self.assertEqual(
            TARGET_ASSETS,
            ("TWT", "PEPE", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "XRP", "HBAR"),
        )
        self.assertEqual(SUNSET_ASSETS, ("ATOM", "SOL", "LINK"))
        self.assertEqual(len(ASSETS), 13)
        self.assertEqual(set(ASSETS), set(TARGET_ASSETS) | set(SUNSET_ASSETS))
        dates = pd.date_range("2026-01-01", periods=180, freq="D", tz="UTC")
        payload = {"timestamp": dates}
        for index, asset in enumerate(ASSETS, start=1):
            payload[f"{asset}_close"] = [float(index)] * len(dates)
        panel = pd.DataFrame(payload)

        events, states = build_pair_monitor(panel)
        self.assertEqual(events, [])
        self.assertEqual(len(states), 78)

    def test_migration_guard_blocks_sunset_reentry_and_keeps_target_candidate(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        events = [
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "ATOM/SOL",
                "from_asset": "ATOM",
                "to_asset": "SOL",
                "max_dislocation": 0.60,
            },
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "ATOM/AVAX",
                "from_asset": "ATOM",
                "to_asset": "AVAX",
                "max_dislocation": 0.35,
            },
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "TWT/HBAR",
                "from_asset": "TWT",
                "to_asset": "HBAR",
                "max_dislocation": 0.70,
            },
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "TWT/XRP",
                "from_asset": "TWT",
                "to_asset": "XRP",
                "max_dislocation": 0.31,
            },
        ]

        atom = choose_held_events(
            events,
            held_asset="ATOM",
            latest_date=latest,
            allowed_to_assets=TARGET_ASSETS,
        )
        twt = choose_held_events(
            events,
            held_asset="TWT",
            latest_date=latest,
            allowed_to_assets=TARGET_ASSETS,
        )

        self.assertEqual(atom["primary_confirmed"]["to_asset"], "AVAX")
        self.assertEqual([e["to_asset"] for e in atom["confirmed"]], ["AVAX"])
        self.assertEqual(twt["primary_confirmed"]["to_asset"], "HBAR")
        self.assertEqual(
            [e["to_asset"] for e in twt["confirmed"]],
            ["HBAR", "XRP"],
        )

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


    def test_russian_telegram_notification_is_short_when_no_signal(self):
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
            "latest_pair_states": [],
            "force_notify": True,
        }

        text = build_notification_ru(payload)

        self.assertIn("Relative Rotation: сигналов нет.", text)
        self.assertIn("Закрытая свеча: 2026-09-26T00:00:00+00:00", text)
        self.assertIn("Текущий актив: ATOM", text)
        self.assertIn("Наблюдение 10%+: нет.", text)
        self.assertNotIn("Защитный режим:", text)
        self.assertNotIn("Closed candle:", text)
        self.assertLess(len(text), 250)

    def test_russian_telegram_starts_observation_at_ten_percent(self):
        payload = {
            "held_asset": "ATOM",
            "latest_closed_candle": "2026-09-27T00:00:00+00:00",
            "held_events": {
                "primary_confirmed": None,
                "confirmed": [],
                "armed": [],
            },
            "watch_events": {},
            "defensive": {
                "active": False,
                "defensive_asset": None,
                "breadth": 7,
            },
            "latest_defensive_events": [],
            "latest_pair_states": [
                {
                    "pair": "ATOM/TWT",
                    "mode": "NONE",
                    "deviation": -0.11,
                    "reversal_from_extreme": None,
                },
                {
                    "pair": "ATOM/SOL",
                    "mode": "NONE",
                    "deviation": 0.12,
                    "reversal_from_extreme": None,
                },
            ],
            "force_notify": False,
        }

        text = build_notification_ru(payload)

        self.assertIn("НАБЛЮДЕНИЕ 10%+", text)
        self.assertIn("Расхождение между ATOM и TWT составляет 11.00%.", text)
        self.assertIn("ATOM -> TWT", text)
        self.assertIn("ARM включается с 15%", text)
        self.assertNotIn("ATOM и SOL", text)

    def test_russian_telegram_keeps_existing_arm_visible_below_ten_percent(self):
        payload = {
            "held_asset": "ATOM",
            "latest_closed_candle": "2026-09-28T00:00:00+00:00",
            "held_events": {
                "primary_confirmed": None,
                "confirmed": [],
                "armed": [],
            },
            "watch_events": {},
            "defensive": {
                "active": False,
                "defensive_asset": None,
                "breadth": 7,
            },
            "latest_defensive_events": [],
            "latest_pair_states": [
                {
                    "pair": "ATOM/TWT",
                    "mode": "LOW",
                    "deviation": -0.09,
                    "max_dislocation": 0.17,
                    "reversal_from_extreme": 0.02,
                }
            ],
            "force_notify": False,
        }

        text = build_notification_ru(payload)

        self.assertIn("ARM 15% АКТИВЕН", text)
        self.assertIn("текущее расхождение 9.00%", text)
        self.assertIn("максимум после ARM 17.00%", text)
        self.assertIn("Ждём разворот от экстремума минимум 3%.", text)
        self.assertNotIn("сигналов нет", text)

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

    def test_russian_watch_arm_is_explicitly_not_current_held_signal(self):
        watch_event = {
            "from_asset": "LINK",
            "to_asset": "FIL",
            "pair": "FIL/LINK",
            "max_dislocation": 0.1968,
            "reversal_from_extreme": 0.0,
        }
        payload = {
            "held_asset": "ATOM",
            "latest_closed_candle": "2026-09-27T00:00:00+00:00",
            "target_assets": list(TARGET_ASSETS),
            "sunset_assets": list(SUNSET_ASSETS),
            "held_events": {
                "primary_confirmed": None,
                "confirmed": [],
                "armed": [],
            },
            "watch_events": {
                "LINK": {
                    "primary_confirmed": None,
                    "confirmed": [],
                    "armed": [watch_event],
                }
            },
            "defensive": {
                "active": False,
                "defensive_asset": None,
                "breadth": None,
            },
            "latest_defensive_events": [],
            "latest_pair_states": [],
            "defensive_overlay_enabled": False,
            "force_notify": False,
        }

        text = build_notification_ru(payload)

        self.assertIn("WATCH LINK — ARM / PREWATCH, ЭТО НЕ СИГНАЛ НА ОБМЕН", text)
        self.assertIn("LINK -> FIL", text)
        self.assertIn("Текущий актив ATOM не меняется", text)
        self.assertIn("ждём разворот от экстремума минимум 3%", text)
        self.assertNotIn("РОТАЦИЯ ПОДТВЕРЖДЕНА", text)


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
