"""Relative-rotation research and paper-live helpers."""

from .paper_live import (
    ARM_THRESHOLD,
    ASSETS,
    DEFENSIVE_CONFIRM_DAYS,
    DEFENSIVE_ENTER_BREADTH,
    DEFENSIVE_EXIT_BREADTH,
    DEFENSIVE_SMA_LOOKBACK,
    DEFENSIVE_VOL_LOOKBACK,
    LOOKBACK,
    REVERSAL,
    build_pair_monitor,
    choose_held_events,
    evaluate_defensive_mode,
)

__all__ = [
    "ARM_THRESHOLD",
    "ASSETS",
    "DEFENSIVE_CONFIRM_DAYS",
    "DEFENSIVE_ENTER_BREADTH",
    "DEFENSIVE_EXIT_BREADTH",
    "DEFENSIVE_SMA_LOOKBACK",
    "DEFENSIVE_VOL_LOOKBACK",
    "LOOKBACK",
    "REVERSAL",
    "build_pair_monitor",
    "choose_held_events",
    "evaluate_defensive_mode",
]
