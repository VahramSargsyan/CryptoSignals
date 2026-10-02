from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.run_relative_rotation_paper_live import _read_config, build_notification_candidates, build_notification_ru
from strategies.crypto.relative_rotation.paper_live import (
    ASSETS,
    TARGET_ASSETS,
    SUNSET_ASSETS,
    _defensive_state_machine,
    build_pair_monitor,
    choose_held_events,
    choose_destination_dominance_override,
    find_route_conflicts,
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

    def test_route_conflict_detects_stronger_candidate_and_destination_arm(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        primary = {
            "date": latest.isoformat(),
            "event": "CONFIRMED",
            "pair": "ALGO/LINK",
            "from_asset": "LINK",
            "to_asset": "ALGO",
            "max_dislocation": 0.3401,
            "reversal_from_extreme": 0.0506,
        }
        events = [primary]
        states = [
            {
                "pair": "TRX/LINK",
                "mode": "HIGH",
                "from_asset": "LINK",
                "to_asset": "TRX",
                "armed_at": "2026-09-24T00:00:00+00:00",
                "deviation": 0.7065,
                "max_dislocation": 0.7065,
                "reversal_from_extreme": 0.0,
            },
            {
                "pair": "TRX/ALGO",
                "mode": "HIGH",
                "from_asset": "ALGO",
                "to_asset": "TRX",
                "armed_at": "2026-09-24T00:00:00+00:00",
                "deviation": 0.4195,
                "max_dislocation": 0.4195,
                "reversal_from_extreme": 0.0,
            },
        ]

        conflicts = find_route_conflicts(
            events,
            states,
            primary_confirmed=primary,
            latest_date=latest,
            allowed_to_assets=TARGET_ASSETS,
        )

        self.assertEqual(len(conflicts), 1)
        conflict = conflicts[0]
        self.assertEqual(conflict["severity"], "ROUTE_CONFLICT_WARNING")
        self.assertEqual(conflict["competing_candidate"]["to_asset"], "TRX")
        self.assertEqual(conflict["destination_relation"]["from_asset"], "ALGO")
        self.assertEqual(conflict["destination_relation"]["to_asset"], "TRX")
        self.assertEqual(
            conflict["possible_intermediate_path"],
            ["LINK", "ALGO", "TRX"],
        )
        self.assertFalse(conflict["blocks_one_click_execution"])
        self.assertTrue(conflict["router_override"])

    def test_destination_dominance_override_selects_stronger_trx_route(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        primary = {
            "date": latest.isoformat(),
            "event": "CONFIRMED",
            "pair": "ALGO/LINK",
            "from_asset": "LINK",
            "to_asset": "ALGO",
            "max_dislocation": 0.3401,
            "reversal_from_extreme": 0.0506,
        }
        conflicts = [
            {
                "severity": "ROUTE_CONFLICT_WARNING",
                "source_asset": "LINK",
                "primary": primary,
                "competing_candidate": {
                    "date": latest.isoformat(),
                    "event": "ARMED",
                    "pair": "TRX/LINK",
                    "from_asset": "LINK",
                    "to_asset": "TRX",
                    "deviation": 0.619,
                    "max_dislocation": 0.7065,
                    "reversal_from_extreme": 0.051,
                },
                "destination_relation": {
                    "date": latest.isoformat(),
                    "event": "ARMED",
                    "pair": "TRX/ALGO",
                    "from_asset": "ALGO",
                    "to_asset": "TRX",
                    "max_dislocation": 0.4195,
                    "reversal_from_extreme": 0.0,
                },
            }
        ]

        effective, chosen = choose_destination_dominance_override(primary, conflicts)

        self.assertIsNotNone(chosen)
        self.assertTrue(effective["route_override"])
        self.assertEqual(effective["route_override_rule"], "DESTINATION_DOMINANCE_MIN_1_5X_V2")
        self.assertEqual(effective["from_asset"], "LINK")
        self.assertEqual(effective["to_asset"], "TRX")
        self.assertEqual(effective["pair"], "TRX/LINK")
        self.assertEqual(effective["competing_original_state"], "ARMED")
        self.assertEqual(effective["route_override_trigger"]["to_asset"], "ALGO")
        self.assertEqual(effective["destination_relation"]["from_asset"], "ALGO")
        self.assertEqual(effective["destination_relation"]["to_asset"], "TRX")

    def test_destination_dominance_requires_at_least_1_5x_strength(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        primary = {
            "date": latest.isoformat(),
            "event": "CONFIRMED",
            "pair": "ALGO/LINK",
            "from_asset": "LINK",
            "to_asset": "ALGO",
            "max_dislocation": 0.20,
            "reversal_from_extreme": 0.04,
        }
        states = [
            {
                "pair": "TRX/LINK",
                "mode": "HIGH",
                "from_asset": "LINK",
                "to_asset": "TRX",
                "max_dislocation": 0.299,
                "reversal_from_extreme": 0.0,
            },
            {
                "pair": "TRX/ALGO",
                "mode": "HIGH",
                "from_asset": "ALGO",
                "to_asset": "TRX",
                "max_dislocation": 0.25,
                "reversal_from_extreme": 0.0,
            },
        ]
        conflicts = find_route_conflicts(
            [primary],
            states,
            primary_confirmed=primary,
            latest_date=latest,
            allowed_to_assets=TARGET_ASSETS,
        )
        self.assertEqual(conflicts, [])

        states[0]["max_dislocation"] = 0.30
        conflicts = find_route_conflicts(
            [primary],
            states,
            primary_confirmed=primary,
            latest_date=latest,
            allowed_to_assets=TARGET_ASSETS,
        )
        self.assertEqual(len(conflicts), 1)
        self.assertAlmostEqual(conflicts[0]["strength_ratio"], 1.5)
        self.assertEqual(conflicts[0]["minimum_strength_ratio"], 1.5)

    def test_route_conflict_ignores_weaker_competing_candidate(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        primary = {
            "date": latest.isoformat(),
            "event": "CONFIRMED",
            "pair": "ALGO/LINK",
            "from_asset": "LINK",
            "to_asset": "ALGO",
            "max_dislocation": 0.50,
        }
        states = [
            {
                "pair": "TRX/LINK",
                "mode": "HIGH",
                "from_asset": "LINK",
                "to_asset": "TRX",
                "max_dislocation": 0.40,
                "reversal_from_extreme": 0.0,
            },
            {
                "pair": "TRX/ALGO",
                "mode": "HIGH",
                "from_asset": "ALGO",
                "to_asset": "TRX",
                "max_dislocation": 0.30,
                "reversal_from_extreme": 0.0,
            },
        ]
        self.assertEqual(
            find_route_conflicts(
                [primary],
                states,
                primary_confirmed=primary,
                latest_date=latest,
                allowed_to_assets=TARGET_ASSETS,
            ),
            [],
        )

    def test_russian_notification_shows_destination_dominance_auto_route(self):
        primary = {
            "date": "2026-09-28T00:00:00+00:00",
            "event": "CONFIRMED",
            "pair": "ALGO/LINK",
            "from_asset": "LINK",
            "to_asset": "ALGO",
            "max_dislocation": 0.3401,
            "reversal_from_extreme": 0.0506,
        }
        conflict = {
            "severity": "ROUTE_CONFLICT_WARNING",
            "primary": primary,
            "competing_candidate": {
                "event": "ARMED",
                "pair": "TRX/LINK",
                "from_asset": "LINK",
                "to_asset": "TRX",
                "max_dislocation": 0.7065,
                "reversal_from_extreme": 0.0,
            },
            "destination_relation": {
                "event": "ARMED",
                "pair": "TRX/ALGO",
                "from_asset": "ALGO",
                "to_asset": "TRX",
                "max_dislocation": 0.4195,
                "reversal_from_extreme": 0.0,
            },
            "possible_intermediate_path": ["LINK", "ALGO", "TRX"],
            "direct_alternative": ["LINK", "TRX"],
        }
        effective, chosen = choose_destination_dominance_override(primary, [conflict])
        self.assertIsNotNone(chosen)

        payload = {
            "held_asset": "ATOM",
            "latest_closed_candle": "2026-09-28T00:00:00+00:00",
            "target_assets": list(TARGET_ASSETS),
            "sunset_assets": list(SUNSET_ASSETS),
            "position_books": [
                {"book_id": "BOOK_1", "held_asset": "ATOM", "quantity": None},
                {"book_id": "BOOK_2", "held_asset": "LINK", "quantity": 100.0},
            ],
            "book_events": {
                "BOOK_1": {
                    "book": {"book_id": "BOOK_1", "held_asset": "ATOM", "quantity": None},
                    "events": {"primary_confirmed": None, "confirmed": [], "armed": []},
                    "route_conflicts": [],
                },
                "BOOK_2": {
                    "book": {"book_id": "BOOK_2", "held_asset": "LINK", "quantity": 100.0},
                    "events": {
                        "baseline_primary_confirmed": primary,
                        "primary_confirmed": effective,
                        "confirmed": [primary],
                        "armed": [],
                        "route_override": chosen,
                    },
                    "route_conflicts": [conflict],
                    "route_override": chosen,
                },
            },
            "route_conflicts": {"BOOK_1": [], "BOOK_2": [conflict]},
            "held_events": {"primary_confirmed": None, "confirmed": [], "armed": []},
            "watch_events": {},
            "defensive": {"active": False, "defensive_asset": None, "breadth": None},
            "latest_defensive_events": [],
            "latest_pair_states": [],
            "defensive_overlay_enabled": False,
            "force_notify": False,
            "latest_close_prices_usdt": {"LINK": 15.0, "ALGO": 0.15, "TRX": 0.3},
        }

        text = build_notification_ru(payload)
        self.assertIn("DESTINATION DOMINANCE", text)
        self.assertIn("основной слот 10:30", text)
        self.assertIn("резервное окно 22:30", text)
        self.assertIn("AUTO ROUTE: LINK -> TRX", text)
        self.assertIn("основной CONFIRMED LINK -> ALGO", text)
        self.assertIn("ALGO -> TRX", text)
        self.assertIn("Исполняемый маршрут стратегии: LINK -> TRX", text)
        self.assertIn("FORWARD WATCH", text)

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


    def test_russian_telegram_quiet_market_shows_rotation_percentages(self):
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
            "latest_close_prices_usdt": {
                "ATOM": 15.0,
                "TWT": 0.15,
                "AVAX": 30.0,
                "BNB": 600.0,
            },
            "latest_pair_states": [
                {
                    "pair": "ATOM/TWT",
                    "mode": "NONE",
                    "deviation": -0.086,
                    "reversal_from_extreme": None,
                },
                {
                    "pair": "ATOM/AVAX",
                    "mode": "NONE",
                    "deviation": -0.063,
                    "reversal_from_extreme": None,
                },
                {
                    "pair": "ATOM/BNB",
                    "mode": "NONE",
                    "deviation": 0.091,
                    "reversal_from_extreme": None,
                },
            ],
            "force_notify": True,
        }

        text = build_notification_ru(payload)

        self.assertIn("Relative Rotation — утренний снимок", text)
        self.assertIn("Закрытая свеча: 2026-09-26T00:00:00+00:00", text)
        self.assertIn("Текущий актив: ATOM", text)
        self.assertIn("ATOM -> TWT: 8.60% отклонение", text)
        self.assertIn("до ARM 15%: 6.40 п.п.", text)
        self.assertIn("ATOM -> AVAX: 6.30% отклонение", text)
        self.assertIn("Цена закрытия: ATOM $15; TWT $0.15; 1 ATOM = 100 TWT.", text)
        self.assertIn("Цена закрытия: ATOM $15; AVAX $30; 1 ATOM = 0.5 AVAX.", text)
        self.assertNotIn("ATOM -> BNB", text)
        self.assertIn("это ещё не CONFIRMED", text)
        self.assertNotIn("сигналов нет", text)
        self.assertNotIn("Closed candle:", text)

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
            "latest_close_prices_usdt": {
                "ATOM": 15.0,
                "TWT": 0.15,
            },
            "force_notify": False,
        }

        text = build_notification_ru(payload)

        self.assertIn("РОТАЦИЯ ПОДТВЕРЖДЕНА", text)
        self.assertIn("ATOM -> TWT", text)
        self.assertIn("отклонение +21.00%", text)
        self.assertIn("разворот от экстремума +4.00%", text)
        self.assertIn("Цена закрытия: ATOM $15; TWT $0.15; 1 ATOM = 100 TWT.", text)
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


    def test_russian_watch_confirmed_keeps_other_prealerts_visible(self):
        confirmed = {
            "from_asset": "LINK",
            "to_asset": "HBAR",
            "pair": "HBAR/LINK",
            "max_dislocation": 0.3840,
            "reversal_from_extreme": 0.0330,
        }
        armed = {
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
                    "primary_confirmed": confirmed,
                    "confirmed": [confirmed],
                    "armed": [armed],
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

        self.assertIn("LINK -> HBAR", text)
        self.assertIn("Другие LINK ARM / PREWATCH (ещё НЕ подтверждены)", text)
        self.assertIn("LINK -> FIL", text)
        self.assertIn("Это НЕ сигнал для текущего актива ATOM", text)


    def test_multibook_config_parses_atom_and_100_link(self):
        payload = {
            "schema_version": 4,
            "held_asset": "ATOM",
            "monitor_start": "2026-09-27T00:00:00Z",
            "target_assets": list(TARGET_ASSETS),
            "sunset_assets": list(SUNSET_ASSETS),
            "watch_assets": list(SUNSET_ASSETS),
            "position_books": [
                {
                    "book_id": "BOOK_1",
                    "held_asset": "ATOM",
                    "tracking_start": "2026-09-29T00:00:00Z",
                },
                {
                    "book_id": "BOOK_2",
                    "held_asset": "LINK",
                    "quantity": 100,
                    "initial_quantity": 100,
                    "tracking_start": "2026-09-29T00:00:00Z",
                },
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            config = _read_config(path)

        self.assertEqual(config["position_books"][0]["book_id"], "BOOK_1")
        self.assertEqual(config["position_books"][0]["held_asset"], "ATOM")
        self.assertEqual(config["position_books"][1]["book_id"], "BOOK_2")
        self.assertEqual(config["position_books"][1]["held_asset"], "LINK")
        self.assertEqual(config["position_books"][1]["quantity"], 100.0)
        self.assertEqual(config["position_books"][1]["initial_quantity"], 100.0)

    def test_multibook_notification_treats_link_as_book2_not_watch(self):
        link_confirmed = {
            "from_asset": "LINK",
            "to_asset": "HBAR",
            "pair": "HBAR/LINK",
            "max_dislocation": 0.3840,
            "reversal_from_extreme": 0.0330,
        }
        link_armed = {
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
            "position_books": [
                {
                    "book_id": "BOOK_1",
                    "held_asset": "ATOM",
                    "quantity": None,
                },
                {
                    "book_id": "BOOK_2",
                    "held_asset": "LINK",
                    "quantity": 100.0,
                },
            ],
            "book_events": {
                "BOOK_1": {
                    "book": {
                        "book_id": "BOOK_1",
                        "held_asset": "ATOM",
                        "quantity": None,
                    },
                    "events": {
                        "primary_confirmed": None,
                        "confirmed": [],
                        "armed": [],
                    },
                },
                "BOOK_2": {
                    "book": {
                        "book_id": "BOOK_2",
                        "held_asset": "LINK",
                        "quantity": 100.0,
                    },
                    "events": {
                        "primary_confirmed": link_confirmed,
                        "confirmed": [link_confirmed],
                        "armed": [link_armed],
                    },
                },
            },
            "held_events": {
                "primary_confirmed": None,
                "confirmed": [],
                "armed": [],
            },
            "watch_events": {},
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

        self.assertIn("Реальные ветки: BOOK_1=ATOM; BOOK_2=100 LINK", text)
        self.assertIn("BOOK_2 — текущая позиция: 100 LINK", text)
        self.assertIn("BOOK_2 — РОТАЦИЯ ПОДТВЕРЖДЕНА", text)
        self.assertIn("LINK -> HBAR", text)
        self.assertIn("Другие ARM / PREWATCH по этой ветке", text)
        self.assertIn("LINK -> FIL", text)
        self.assertNotIn("WATCH LINK", text)

    def test_multibook_legacy_alias_must_match_book1(self):
        payload = {
            "held_asset": "ATOM",
            "monitor_start": "2026-09-27T00:00:00Z",
            "target_assets": list(TARGET_ASSETS),
            "sunset_assets": list(SUNSET_ASSETS),
            "position_books": [
                {
                    "book_id": "BOOK_1",
                    "held_asset": "LINK",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ValueError):
                _read_config(path)


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


    def test_notification_candidates_replay_recent_link_signal_for_book2(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        events = [
            {
                "date": pd.Timestamp("2026-09-27", tz="UTC").isoformat(),
                "event": "CONFIRMED",
                "pair": "HBAR/LINK",
                "from_asset": "LINK",
                "to_asset": "HBAR",
                "max_dislocation": 0.3840,
                "reversal_from_extreme": 0.0330,
            },
            {
                "date": pd.Timestamp("2026-09-28", tz="UTC").isoformat(),
                "event": "ARMED",
                "pair": "FIL/LINK",
                "from_asset": "LINK",
                "to_asset": "FIL",
                "max_dislocation": 0.1968,
                "reversal_from_extreme": 0.0,
            },
        ]
        books = [
            {
                "book_id": "BOOK_1",
                "held_asset": "ATOM",
                "quantity": None,
                "tracking_start": pd.Timestamp("2026-09-29", tz="UTC"),
            },
            {
                "book_id": "BOOK_2",
                "held_asset": "LINK",
                "quantity": 100.0,
                "tracking_start": pd.Timestamp("2026-09-29", tz="UTC"),
            },
        ]

        candidates = build_notification_candidates(
            events,
            position_books=books,
            target_assets=TARGET_ASSETS,
            latest=latest,
            monitor_start=pd.Timestamp("2026-09-27", tz="UTC"),
            replay_days=7,
        )

        self.assertEqual(len(candidates), 2)
        self.assertTrue(all(row["book_id"] == "BOOK_2" for row in candidates))
        self.assertEqual(
            {row["event"]["event"] for row in candidates},
            {"ARMED", "CONFIRMED"},
        )
        self.assertEqual(
            {row["event"]["to_asset"] for row in candidates},
            {"HBAR", "FIL"},
        )
        self.assertEqual(len({row["event_id"] for row in candidates}), 2)

    def test_notification_candidates_respect_per_book_event_floor(self):
        latest = pd.Timestamp("2026-10-01", tz="UTC")
        events = [
            {
                "date": pd.Timestamp("2026-09-28", tz="UTC").isoformat(),
                "event": "CONFIRMED",
                "pair": "TRX/PEPE",
                "from_asset": "PEPE",
                "to_asset": "TRX",
                "max_dislocation": 0.42,
                "reversal_from_extreme": 0.04,
            },
            {
                "date": pd.Timestamp("2026-10-01", tz="UTC").isoformat(),
                "event": "ARMED",
                "pair": "BNB/PEPE",
                "from_asset": "PEPE",
                "to_asset": "BNB",
                "max_dislocation": 0.20,
                "reversal_from_extreme": 0.0,
            },
        ]
        books = [
            {
                "book_id": "BOOK_3",
                "held_asset": "PEPE",
                "quantity": 69341307.95379811,
                "tracking_start": pd.Timestamp("2026-10-01T18:00:00Z"),
                "notification_event_floor": "2026-10-01T00:00:00Z",
            }
        ]

        candidates = build_notification_candidates(
            events,
            position_books=books,
            target_assets=TARGET_ASSETS,
            latest=latest,
            monitor_start=pd.Timestamp("2026-09-27", tz="UTC"),
            replay_days=7,
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["book_id"], "BOOK_3")
        self.assertEqual(candidates[0]["event"]["date"], pd.Timestamp("2026-10-01", tz="UTC").isoformat())
        self.assertEqual(candidates[0]["event"]["to_asset"], "BNB")

    def test_notification_candidates_ignore_untracked_sol_watch(self):
        latest = pd.Timestamp("2026-09-28", tz="UTC")
        events = [
            {
                "date": latest.isoformat(),
                "event": "CONFIRMED",
                "pair": "SOL/TWT",
                "from_asset": "SOL",
                "to_asset": "TWT",
                "max_dislocation": 0.25,
                "reversal_from_extreme": 0.04,
            }
        ]
        books = [
            {
                "book_id": "BOOK_1",
                "held_asset": "ATOM",
                "quantity": None,
                "tracking_start": pd.Timestamp("2026-09-29", tz="UTC"),
            },
            {
                "book_id": "BOOK_2",
                "held_asset": "LINK",
                "quantity": 100.0,
                "tracking_start": pd.Timestamp("2026-09-29", tz="UTC"),
            },
        ]

        candidates = build_notification_candidates(
            events,
            position_books=books,
            target_assets=TARGET_ASSETS,
            latest=latest,
            monitor_start=pd.Timestamp("2026-09-27", tz="UTC"),
            replay_days=7,
        )

        self.assertEqual(candidates, [])


if __name__ == "__main__":
    unittest.main()
