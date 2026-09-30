from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd

TARGET_ASSETS = ("TWT", "PEPE", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "XRP", "HBAR")
SUNSET_ASSETS = ("ATOM", "SOL", "LINK")
ASSETS = TARGET_ASSETS + SUNSET_ASSETS
LOOKBACK = 180
ARM_THRESHOLD = 0.15
REVERSAL = 0.03
DESTINATION_DOMINANCE_MIN_STRENGTH_RATIO = 1.50

DEFENSIVE_SMA_LOOKBACK = 200
DEFENSIVE_ENTER_BREADTH = 3
DEFENSIVE_EXIT_BREADTH = 5
DEFENSIVE_CONFIRM_DAYS = 3
DEFENSIVE_VOL_LOOKBACK = 30


@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0
    armed_at: pd.Timestamp | None = None


def _iso(ts: pd.Timestamp | None) -> str | None:
    return None if ts is None else pd.Timestamp(ts).isoformat()


def _validate_panel(panel: pd.DataFrame, assets: Sequence[str]) -> pd.DataFrame:
    required = {"timestamp", *(f"{asset}_close" for asset in assets)}
    missing = required.difference(panel.columns)
    if missing:
        raise ValueError(f"Panel is missing columns: {sorted(missing)}")

    frame = panel[list(required)].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if frame["timestamp"].duplicated().any():
        raise ValueError("Panel contains duplicate timestamps")
    if frame.empty:
        raise ValueError("Panel is empty")
    if frame[[f"{asset}_close" for asset in assets]].isna().any().any():
        raise ValueError("Panel contains missing close values")
    if (frame[[f"{asset}_close" for asset in assets]] <= 0).any().any():
        raise ValueError("Panel contains non-positive close values")
    return frame


def build_pair_monitor(
    panel: pd.DataFrame,
    *,
    assets: Sequence[str] = ASSETS,
    lookback: int = LOOKBACK,
    arm_threshold: float = ARM_THRESHOLD,
    reversal: float = REVERSAL,
) -> tuple[list[dict], list[dict]]:
    """Recompute the frozen ARM -> extreme -> reversal engine from closed candles.

    Returns all transition events plus a latest-state snapshot for every pair.
    No orders are created and no forward prices are used.
    """
    if lookback < 2:
        raise ValueError("lookback must be >= 2")
    if not 0 < arm_threshold < 1:
        raise ValueError("arm_threshold must be between 0 and 1")
    if not 0 < reversal < 1:
        raise ValueError("reversal must be between 0 and 1")

    frame = _validate_panel(panel, assets)
    events: list[dict] = []
    latest_states: list[dict] = []

    for left, right in itertools.combinations(assets, 2):
        ratio_series = frame[f"{right}_close"] / frame[f"{left}_close"]
        median_series = ratio_series.rolling(lookback, min_periods=lookback).median()
        deviation_series = ratio_series / median_series - 1.0
        state = PairState()

        for idx, ts in enumerate(frame["timestamp"]):
            median = median_series.iloc[idx]
            deviation = deviation_series.iloc[idx]
            if pd.isna(median) or pd.isna(deviation):
                continue

            ratio = float(ratio_series.iloc[idx])
            median = float(median)
            deviation = float(deviation)

            if state.mode == "NONE":
                if deviation >= arm_threshold:
                    state = PairState(
                        mode="HIGH",
                        extreme=ratio,
                        max_dislocation=abs(deviation),
                        armed_at=ts,
                    )
                    events.append(
                        {
                            "date": ts.isoformat(),
                            "event": "ARMED",
                            "pair": f"{left}/{right}",
                            "from_asset": right,
                            "to_asset": left,
                            "ratio": ratio,
                            "median": median,
                            "deviation": deviation,
                            "max_dislocation": abs(deviation),
                            "reversal_from_extreme": 0.0,
                        }
                    )
                elif deviation <= -arm_threshold:
                    state = PairState(
                        mode="LOW",
                        extreme=ratio,
                        max_dislocation=abs(deviation),
                        armed_at=ts,
                    )
                    events.append(
                        {
                            "date": ts.isoformat(),
                            "event": "ARMED",
                            "pair": f"{left}/{right}",
                            "from_asset": left,
                            "to_asset": right,
                            "ratio": ratio,
                            "median": median,
                            "deviation": deviation,
                            "max_dislocation": abs(deviation),
                            "reversal_from_extreme": 0.0,
                        }
                    )
                continue

            if state.mode == "HIGH":
                assert state.extreme is not None
                if ratio > state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(deviation))
                retracement = 1.0 - ratio / state.extreme
                if retracement >= reversal:
                    events.append(
                        {
                            "date": ts.isoformat(),
                            "event": "CONFIRMED",
                            "pair": f"{left}/{right}",
                            "from_asset": right,
                            "to_asset": left,
                            "ratio": ratio,
                            "median": median,
                            "deviation": deviation,
                            "max_dislocation": state.max_dislocation,
                            "reversal_from_extreme": retracement,
                        }
                    )
                    state = PairState()
                continue

            if state.mode == "LOW":
                assert state.extreme is not None
                if ratio < state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(deviation))
                retracement = ratio / state.extreme - 1.0
                if retracement >= reversal:
                    events.append(
                        {
                            "date": ts.isoformat(),
                            "event": "CONFIRMED",
                            "pair": f"{left}/{right}",
                            "from_asset": left,
                            "to_asset": right,
                            "ratio": ratio,
                            "median": median,
                            "deviation": deviation,
                            "max_dislocation": state.max_dislocation,
                            "reversal_from_extreme": retracement,
                        }
                    )
                    state = PairState()

        latest_idx = len(frame) - 1
        latest_ratio = float(ratio_series.iloc[latest_idx])
        latest_median_raw = median_series.iloc[latest_idx]
        latest_deviation_raw = deviation_series.iloc[latest_idx]
        latest_median = None if pd.isna(latest_median_raw) else float(latest_median_raw)
        latest_deviation = None if pd.isna(latest_deviation_raw) else float(latest_deviation_raw)

        prospective_from: str | None = None
        prospective_to: str | None = None
        retracement: float | None = None
        if state.mode == "HIGH" and state.extreme is not None:
            prospective_from, prospective_to = right, left
            retracement = max(0.0, 1.0 - latest_ratio / state.extreme)
        elif state.mode == "LOW" and state.extreme is not None:
            prospective_from, prospective_to = left, right
            retracement = max(0.0, latest_ratio / state.extreme - 1.0)

        latest_states.append(
            {
                "pair": f"{left}/{right}",
                "mode": state.mode,
                "from_asset": prospective_from,
                "to_asset": prospective_to,
                "armed_at": _iso(state.armed_at),
                "ratio": latest_ratio,
                "median": latest_median,
                "deviation": latest_deviation,
                "extreme": state.extreme,
                "max_dislocation": state.max_dislocation,
                "reversal_from_extreme": retracement,
            }
        )

    return events, latest_states


def choose_held_events(
    events: Iterable[dict],
    *,
    held_asset: str,
    latest_date: pd.Timestamp | str,
    allowed_to_assets: Sequence[str] | None = None,
) -> dict:
    """Filter latest-candle events for the currently held asset.

    Optional allowed_to_assets enforces transition routing. In migration mode this
    prevents re-entry into sunset assets while allowing a legacy held asset to
    exit into the target universe.

    When several CONFIRMED transitions exist, preserve the frozen baseline router:
    strongest max dislocation first, then target asset, then pair name.
    """
    held_asset = held_asset.upper()
    allowed_to = None if allowed_to_assets is None else {str(asset).upper() for asset in allowed_to_assets}
    latest_iso = pd.Timestamp(latest_date).tz_convert("UTC").isoformat() if pd.Timestamp(latest_date).tzinfo else pd.Timestamp(latest_date).tz_localize("UTC").isoformat()
    latest = [
        dict(event)
        for event in events
        if event.get("date") == latest_iso
        and event.get("from_asset") == held_asset
        and (allowed_to is None or str(event.get("to_asset", "")).upper() in allowed_to)
    ]
    armed = sorted(
        (event for event in latest if event.get("event") == "ARMED"),
        key=lambda x: (-float(x["max_dislocation"]), str(x["to_asset"]), str(x["pair"])),
    )
    confirmed = sorted(
        (event for event in latest if event.get("event") == "CONFIRMED"),
        key=lambda x: (-float(x["max_dislocation"]), str(x["to_asset"]), str(x["pair"])),
    )
    return {
        "armed": armed,
        "confirmed": confirmed,
        "primary_confirmed": confirmed[0] if confirmed else None,
    }



def find_route_conflicts(
    events: Iterable[dict],
    latest_states: Iterable[dict],
    *,
    primary_confirmed: dict | None,
    latest_date: pd.Timestamp | str,
    allowed_to_assets: Sequence[str] | None = None,
) -> list[dict]:
    """Find destination-dominance route-override candidates.

    A competing destination B qualifies when:
      1) SOURCE -> B is currently ARMED or CONFIRMED;
      2) its max dislocation is at least 1.50x the primary SOURCE -> A
         max dislocation; and
      3) the direct A -> B pair is itself ARMED or CONFIRMED.

    CONFIRMED relationships are read from latest-candle events because the pair
    state resets after confirmation. Ongoing ARMED relationships are read from
    latest_states so older arms remain visible until their 3% reversal fires.
    """
    if not primary_confirmed:
        return []
    if str(primary_confirmed.get("event") or "CONFIRMED").upper() != "CONFIRMED":
        return []

    source = str(primary_confirmed.get("from_asset") or "").upper()
    primary_to = str(primary_confirmed.get("to_asset") or "").upper()
    if not source or not primary_to:
        return []

    primary_strength = float(primary_confirmed.get("max_dislocation") or 0.0)
    allowed_to = (
        None
        if allowed_to_assets is None
        else {str(asset).upper() for asset in allowed_to_assets}
    )

    ts = pd.Timestamp(latest_date)
    latest_iso = (
        ts.tz_localize("UTC").isoformat()
        if ts.tzinfo is None
        else ts.tz_convert("UTC").isoformat()
    )

    latest_events = [
        dict(event)
        for event in events
        if str(event.get("date") or "") == latest_iso
        and str(event.get("event") or "").upper() in {"ARMED", "CONFIRMED"}
    ]
    state_rows = [dict(row) for row in latest_states]

    def candidate_from_state(row: dict) -> dict | None:
        mode = str(row.get("mode") or "NONE").upper()
        from_asset = str(row.get("from_asset") or "").upper()
        to_asset = str(row.get("to_asset") or "").upper()
        if mode not in {"HIGH", "LOW"} or not from_asset or not to_asset:
            return None
        return {
            "date": latest_iso,
            "event": "ARMED",
            "pair": row.get("pair"),
            "from_asset": from_asset,
            "to_asset": to_asset,
            "deviation": row.get("deviation"),
            "max_dislocation": float(row.get("max_dislocation") or 0.0),
            "reversal_from_extreme": row.get("reversal_from_extreme"),
            "armed_at": row.get("armed_at"),
            "mode": mode,
        }

    def better(existing: dict | None, candidate: dict) -> dict:
        if existing is None:
            return candidate
        existing_confirmed = str(existing.get("event") or "").upper() == "CONFIRMED"
        candidate_confirmed = str(candidate.get("event") or "").upper() == "CONFIRMED"
        if candidate_confirmed != existing_confirmed:
            return candidate if candidate_confirmed else existing
        if float(candidate.get("max_dislocation") or 0.0) > float(
            existing.get("max_dislocation") or 0.0
        ):
            return candidate
        return existing

    outbound: dict[str, dict] = {}
    for event in latest_events:
        if str(event.get("from_asset") or "").upper() != source:
            continue
        to_asset = str(event.get("to_asset") or "").upper()
        if not to_asset or to_asset == primary_to:
            continue
        if allowed_to is not None and to_asset not in allowed_to:
            continue
        outbound[to_asset] = better(outbound.get(to_asset), event)

    for row in state_rows:
        candidate = candidate_from_state(row)
        if candidate is None or candidate["from_asset"] != source:
            continue
        to_asset = candidate["to_asset"]
        if to_asset == primary_to:
            continue
        if allowed_to is not None and to_asset not in allowed_to:
            continue
        outbound[to_asset] = better(outbound.get(to_asset), candidate)

    def destination_relation(to_asset: str) -> dict | None:
        relation: dict | None = None
        for event in latest_events:
            if (
                str(event.get("from_asset") or "").upper() == primary_to
                and str(event.get("to_asset") or "").upper() == to_asset
            ):
                relation = better(relation, event)
        for row in state_rows:
            candidate = candidate_from_state(row)
            if candidate is None:
                continue
            if (
                candidate["from_asset"] == primary_to
                and candidate["to_asset"] == to_asset
            ):
                relation = better(relation, candidate)
        return relation

    conflicts: list[dict] = []
    for competing_to, competing in outbound.items():
        competing_strength = float(competing.get("max_dislocation") or 0.0)
        required_strength = (
            primary_strength * DESTINATION_DOMINANCE_MIN_STRENGTH_RATIO
        )
        if competing_strength < required_strength:
            continue

        relation = destination_relation(competing_to)
        if relation is None:
            continue

        relation_status = str(relation.get("event") or "ARMED").upper()
        severity = (
            "ROUTE_CONFLICT_HIGH"
            if relation_status == "CONFIRMED"
            else "ROUTE_CONFLICT_WARNING"
        )
        conflicts.append(
            {
                "severity": severity,
                "source_asset": source,
                "primary": dict(primary_confirmed),
                "competing_candidate": dict(competing),
                "destination_relation": dict(relation),
                "possible_intermediate_path": [source, primary_to, competing_to],
                "direct_alternative": [source, competing_to],
                "execution_policy": "AUTO_ROUTE_OVERRIDE_ELIGIBLE",
                "blocks_one_click_execution": False,
                "router_override": True,
                "strength_ratio": (
                    competing_strength / primary_strength
                    if primary_strength > 0
                    else None
                ),
                "minimum_strength_ratio": DESTINATION_DOMINANCE_MIN_STRENGTH_RATIO,
            }
        )

    severity_rank = {"ROUTE_CONFLICT_HIGH": 0, "ROUTE_CONFLICT_WARNING": 1}
    conflicts.sort(
        key=lambda row: (
            -float(
                row.get("competing_candidate", {}).get("max_dislocation") or 0.0
            ),
            severity_rank.get(str(row.get("severity") or ""), 9),
            str(row.get("competing_candidate", {}).get("to_asset") or ""),
        )
    )
    return conflicts


def choose_destination_dominance_override(
    primary_confirmed: dict | None,
    conflicts: Sequence[dict],
) -> tuple[dict | None, dict | None]:
    """Return the effective actionable route under the accepted DDG override.

    Rule:
    - baseline route SOURCE -> A is CONFIRMED;
    - SOURCE -> B is currently ARMED or CONFIRMED and at least 1.50x
      the baseline SOURCE -> A max dislocation;
    - actual A -> B pair is currently ARMED or CONFIRMED toward B;
    - choose the strongest qualifying B by max_dislocation immediately.

    The returned event remains event=CONFIRMED because execution authority comes
    from the baseline confirmed trigger plus the accepted topology override.
    Metadata preserves the baseline trigger and the competing pair's own state.
    """
    if primary_confirmed is None:
        return None, None
    if not conflicts:
        return primary_confirmed, None

    chosen = max(
        (dict(conflict) for conflict in conflicts),
        key=lambda row: (
            float(row.get("competing_candidate", {}).get("max_dislocation") or 0.0),
            str(row.get("competing_candidate", {}).get("to_asset") or ""),
        ),
    )
    competing = dict(chosen.get("competing_candidate") or {})
    relation = dict(chosen.get("destination_relation") or {})
    source = str(primary_confirmed.get("from_asset") or "").upper()
    target = str(competing.get("to_asset") or "").upper()
    if not source or not target:
        return primary_confirmed, None

    effective = dict(primary_confirmed)
    effective.update(
        {
            "event": "CONFIRMED",
            "pair": competing.get("pair"),
            "from_asset": source,
            "to_asset": target,
            "deviation": competing.get("deviation"),
            "max_dislocation": competing.get("max_dislocation"),
            "reversal_from_extreme": competing.get("reversal_from_extreme"),
            "route_override": True,
            "route_override_rule": "DESTINATION_DOMINANCE_MIN_1_5X_V2",
            "route_override_trigger": dict(primary_confirmed),
            "competing_original_state": competing.get("event"),
            "destination_relation": relation,
            "confirmation_basis": (
                "BASELINE_CONFIRMED_PLUS_1_5X_SAME_SOURCE_AND_DESTINATION_DOMINANCE"
            ),
        }
    )
    chosen["selected_for_override"] = True
    chosen["effective_route"] = {
        "from_asset": source,
        "to_asset": target,
        "pair": competing.get("pair"),
    }
    return effective, chosen

def _defensive_state_machine(
    dates: Sequence[pd.Timestamp],
    breadth_values: Sequence[int | None],
    low_vol_assets: Sequence[str | None],
    *,
    enter_breadth: int = DEFENSIVE_ENTER_BREADTH,
    exit_breadth: int = DEFENSIVE_EXIT_BREADTH,
    confirm_days: int = DEFENSIVE_CONFIRM_DAYS,
) -> tuple[list[dict], dict]:
    defensive = False
    defensive_asset: str | None = None
    low_streak = 0
    high_streak = 0
    events: list[dict] = []

    for ts, breadth, low_vol_asset in zip(dates, breadth_values, low_vol_assets):
        if breadth is None:
            continue

        low_streak = low_streak + 1 if breadth <= enter_breadth else 0
        high_streak = high_streak + 1 if breadth >= exit_breadth else 0

        if not defensive and low_streak >= confirm_days:
            if low_vol_asset is None:
                continue
            defensive = True
            defensive_asset = low_vol_asset
            events.append(
                {
                    "date": pd.Timestamp(ts).isoformat(),
                    "event": "DEFENSIVE_ENTER",
                    "breadth": int(breadth),
                    "defensive_asset": defensive_asset,
                }
            )
            high_streak = 0
            continue

        if defensive and high_streak >= confirm_days:
            events.append(
                {
                    "date": pd.Timestamp(ts).isoformat(),
                    "event": "DEFENSIVE_EXIT",
                    "breadth": int(breadth),
                    "defensive_asset": defensive_asset,
                }
            )
            defensive = False
            defensive_asset = None
            low_streak = 0

    snapshot = {
        "active": defensive,
        "defensive_asset": defensive_asset,
        "low_streak": low_streak,
        "high_streak": high_streak,
    }
    return events, snapshot


def evaluate_defensive_mode(
    panel: pd.DataFrame,
    *,
    assets: Sequence[str] = ASSETS,
    sma_lookback: int = DEFENSIVE_SMA_LOOKBACK,
    enter_breadth: int = DEFENSIVE_ENTER_BREADTH,
    exit_breadth: int = DEFENSIVE_EXIT_BREADTH,
    confirm_days: int = DEFENSIVE_CONFIRM_DAYS,
    vol_lookback: int = DEFENSIVE_VOL_LOOKBACK,
) -> tuple[list[dict], dict, pd.DataFrame]:
    """Evaluate the documented low-vol crypto defensive candidate.

    This is an alert-only research overlay. It does not place orders and does not
    change the frozen relative-rotation rules.
    """
    frame = _validate_panel(panel, assets)
    closes = frame.set_index("timestamp")[[f"{asset}_close" for asset in assets]].copy()
    closes.columns = list(assets)
    sma = closes.rolling(sma_lookback, min_periods=sma_lookback).mean()
    valid = sma.notna().all(axis=1)
    breadth = (closes > sma).sum(axis=1).where(valid)

    returns = closes.pct_change(fill_method=None)
    volatility = returns.rolling(vol_lookback, min_periods=vol_lookback).std()
    low_vol_asset = volatility.idxmin(axis=1).where(volatility.notna().all(axis=1))

    breadth_values: list[int | None] = [
        None if pd.isna(value) else int(value) for value in breadth.tolist()
    ]
    low_vol_values: list[str | None] = [
        None if pd.isna(value) else str(value) for value in low_vol_asset.tolist()
    ]

    events, state = _defensive_state_machine(
        list(closes.index),
        breadth_values,
        low_vol_values,
        enter_breadth=enter_breadth,
        exit_breadth=exit_breadth,
        confirm_days=confirm_days,
    )

    latest_breadth_raw = breadth.iloc[-1]
    latest_low_vol_raw = low_vol_asset.iloc[-1]
    state.update(
        {
            "breadth": None if pd.isna(latest_breadth_raw) else int(latest_breadth_raw),
            "lowest_vol_asset_now": None if pd.isna(latest_low_vol_raw) else str(latest_low_vol_raw),
            "sma_lookback": sma_lookback,
            "enter_breadth": enter_breadth,
            "exit_breadth": exit_breadth,
            "confirm_days": confirm_days,
            "vol_lookback": vol_lookback,
            "status": "PROMISING_RESEARCH_CANDIDATE / NOT_PRODUCTION_APPROVED",
        }
    )

    diagnostics = pd.DataFrame(
        {
            "timestamp": closes.index,
            "breadth": breadth.values,
            "lowest_vol_asset": low_vol_asset.values,
        }
    )
    return events, state, diagnostics
