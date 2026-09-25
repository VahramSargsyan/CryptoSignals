from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd

STRATEGY_ID = "VAHRAM_ORIGINAL_V1"
STRATEGY_NAME = "VAHRAM_ORIGINAL"
STRATEGY_VERSION = "1.0.0"
LEGACY_SOURCE_COMMIT_SHA = "e363ffd8a4d29492bb6e685181770c39ca3047d7"


def apply_legacy_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    """Exact frozen formulas from scripts/utils.py at LEGACY_SOURCE_COMMIT_SHA."""
    df = frame.copy()
    df["MA20"] = df["Close"].rolling(window=20).mean()
    df["STD20"] = df["Close"].rolling(window=20).std()
    df["Upper"] = df["MA20"] + 2 * df["STD20"]
    df["Lower"] = df["MA20"] - 2 * df["STD20"]

    min_val = df["Close"].rolling(window=14).min()
    max_val = df["Close"].rolling(window=14).max()
    stoch = (df["Close"] - min_val) / (max_val - min_val)
    df["StochRSI"] = stoch.rolling(3).mean() * 100
    df["MA_Volume"] = df["Volume"].rolling(20).mean()
    return df


def calculate_basic_signal(row: pd.Series) -> Optional[str]:
    if row["Close"] <= row["Lower"] and row["StochRSI"] < 20 and row["Volume"] > row["MA_Volume"]:
        return "BUY"
    if row["Close"] >= row["Upper"] and row["StochRSI"] > 80 and row["Volume"] > row["MA_Volume"]:
        return "SELL"
    return None


def calculate_signal_strength(row: pd.Series) -> Tuple[Optional[str], float]:
    strength = 0.0

    if row["High"] != row["Low"]:
        body_strength = abs(row["Close"] - row["Open"]) / (row["High"] - row["Low"])
    else:
        body_strength = 0.0

    if body_strength < 0.2:
        return None, 0.0

    if row["Close"] <= row["Lower"] and row["StochRSI"] < 20 and row["Volume"] > row["MA_Volume"]:
        strength += (20 - row["StochRSI"]) * 2
        bb_diff = (row["Lower"] - row["Close"]) / row["Lower"]
        strength += min(bb_diff * 100, 30)
        vol_boost = (row["Volume"] - row["MA_Volume"]) / row["MA_Volume"]
        strength += min(vol_boost * 100, 30)
        strength += body_strength * 10
        return "BUY", round(min(strength, 100), 1)

    if row["Close"] >= row["Upper"] and row["StochRSI"] > 80 and row["Volume"] > row["MA_Volume"]:
        strength += (row["StochRSI"] - 80) * 2
        bb_diff = (row["Close"] - row["Upper"]) / row["Upper"]
        strength += min(bb_diff * 100, 30)
        vol_boost = (row["Volume"] - row["MA_Volume"]) / row["MA_Volume"]
        strength += min(vol_boost * 100, 30)
        strength += body_strength * 10
        return "SELL", round(min(strength, 100), 1)

    return None, 0.0


def get_action_and_change(signal: str, strength: float) -> tuple[str, int]:
    if strength < 40:
        return "Ignore", 0
    if strength < 60:
        return ("Enter" if signal == "BUY" else "Reduce"), 25
    if strength < 80:
        return ("Enter" if signal == "BUY" else "Reduce"), 50
    return ("Enter" if signal == "BUY" else "Exit"), 100


def scan_strength_signals(frame: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    """Return every historical signal using the frozen strength-version behavior."""
    df = apply_legacy_indicators(frame)
    results = []
    for _, row in df.iterrows():
        signal, strength = calculate_signal_strength(row)
        if not signal:
            continue
        action, change = get_action_and_change(signal, strength)
        results.append(
            {
                "Symbol": symbol,
                "Signal": signal,
                "Action": action,
                "Date": pd.Timestamp(row["Date"]).strftime("%Y-%m-%d"),
                "Price": round(float(row["Close"]), 6),
                "Strength": strength,
                "Change %": change,
            }
        )
    return pd.DataFrame(
        results,
        columns=["Symbol", "Signal", "Action", "Date", "Price", "Strength", "Change %"],
    )
