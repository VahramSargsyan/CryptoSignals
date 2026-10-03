from __future__ import annotations

import itertools
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import (
    ARM_THRESHOLD,
    ASSETS,
    LOOKBACK,
    REVERSAL,
    TARGET_ASSETS,
    choose_destination_dominance_override,
    choose_held_events,
    find_route_conflicts,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_d1_anchor_h1_confirm_v1"

AS_OF = pd.Timestamp("2026-10-03T20:30:00Z")
DOWNLOAD_START = pd.Timestamp("2023-01-01T00:00:00Z")
MATURE_START = pd.Timestamp("2023-10-31T00:00:00Z")
WINDOW_STARTS = {
    "MATURE": MATURE_START,
    "LAST_2Y": pd.Timestamp("2024-10-04T00:00:00Z"),
    "LAST_1Y": pd.Timestamp("2025-10-04T00:00:00Z"),
}
COSTS = (0.001, 0.005, 0.01, 0.03)
PRIMARY_COST = 0.001

ENGINES = ("D1_CORE", "D1_ARM_H1_CONFIRM", "D1_ANCHOR_FULL_H1")
HYBRID_ENGINES = ("D1_ARM_H1_CONFIRM", "D1_ANCHOR_FULL_H1")

CASE_START = pd.Timestamp("2026-09-27T00:00:00Z")
AAVE_TRACE_START = pd.Timestamp("2026-09-28T00:00:00Z")
PAIR_CASE = "TRX/AAVE"


@dataclass
class RuntimeState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0
    armed_at: pd.Timestamp | None = None


def source_sha() -> str:
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def _missing_grid_times(frame: pd.DataFrame, timeframe: str) -> list[pd.Timestamp]:
    if frame.empty:
        return []
    freq = pd.Timedelta(hours=1) if timeframe == "1H" else pd.Timedelta(days=1)
    ts = pd.DatetimeIndex(pd.to_datetime(frame["timestamp"], utc=True)).sort_values()
    missing: list[pd.Timestamp] = []
    for left, right in zip(ts[:-1], ts[1:]):
        cursor = left + freq
        while cursor < right:
            missing.append(pd.Timestamp(cursor))
            cursor += freq
    return missing


def _quality_is_only_missing_candles(quality) -> bool:
    return (
        int(getattr(quality, "missing_candles", 0) or 0) > 0
        and int(getattr(quality, "duplicate_timestamps", 0) or 0) == 0
        and int(getattr(quality, "out_of_order_rows", 0) or 0) == 0
        and int(getattr(quality, "off_grid_timestamps", 0) or 0) == 0
        and int(getattr(quality, "invalid_ohlc_rows", 0) or 0) == 0
        and int(getattr(quality, "null_cells", 0) or 0) == 0
        and int(getattr(quality, "negative_volume_rows", 0) or 0) == 0
    )


def download_panel(timeframe: str) -> tuple[pd.DataFrame, dict]:
    client = BinanceSpotRestClient()
    pieces = []
    meta: dict[str, dict] = {}

    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=f"{asset}USDT",
            start=DOWNLOAD_START,
            end=AS_OF,
            timeframe=timeframe,
            as_of=AS_OF,
        )
        if result.dataset is None:
            raise RuntimeError(
                f"{asset}: no {timeframe} dataset ({result.metadata.status})"
            )
        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

        missing_times = _missing_grid_times(frame, timeframe)
        if result.dataset.quality.has_critical_issues:
            allow_warmup_only_gap = (
                timeframe == "1H"
                and _quality_is_only_missing_candles(result.dataset.quality)
                and len(missing_times)
                == int(getattr(result.dataset.quality, "missing_candles", 0) or 0)
                and all(ts < MATURE_START for ts in missing_times)
            )
            if not allow_warmup_only_gap:
                raise RuntimeError(
                    f"{asset}: critical {timeframe} quality issue: "
                    f"{result.dataset.quality}; missing_times="
                    f"{[x.isoformat() for x in missing_times[:20]]}"
                )

        frame = frame.rename(
            columns={
                "open": f"{asset}_open",
                "close": f"{asset}_close",
            }
        )
        pieces.append(frame)
        meta[asset] = {
            "dataset_id": result.dataset.dataset_id,
            "timeframe": timeframe,
            "rows": int(len(frame)),
            "actual_start": result.metadata.actual_start,
            "actual_end": result.metadata.actual_end,
            "status": result.metadata.status,
            "missing_grid_times": [x.isoformat() for x in missing_times],
            "warmup_only_gap_allowed": bool(
                timeframe == "1H"
                and missing_times
                and all(ts < MATURE_START for ts in missing_times)
            ),
        }

    panel = pieces[0]
    for frame in pieces[1:]:
        panel = panel.merge(
            frame, on="timestamp", how="inner", validate="one_to_one"
        )
    panel["timestamp"] = pd.to_datetime(panel["timestamp"], utc=True)
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)

    if panel.empty:
        raise RuntimeError(f"Common {timeframe} panel is empty")

    meta["panel"] = {
        "rows": int(len(panel)),
        "start": pd.Timestamp(panel.iloc[0]["timestamp"]).isoformat(),
        "end": pd.Timestamp(panel.iloc[-1]["timestamp"]).isoformat(),
    }
    return panel, meta


def validate_d1_h1_close_consistency(
    d1: pd.DataFrame,
    h1: pd.DataFrame,
) -> pd.DataFrame:
    h1_lookup = {
        pd.Timestamp(ts) + pd.Timedelta(hours=1): i
        for i, ts in enumerate(h1["timestamp"])
    }
    rows = []

    for asset in ASSETS:
        max_rel = 0.0
        checked = 0
        missing = 0
        for _, row in d1.iterrows():
            close_time = pd.Timestamp(row["timestamp"]) + pd.Timedelta(days=1)
            hidx = h1_lookup.get(close_time)
            if hidx is None:
                missing += 1
                continue
            dclose = float(row[f"{asset}_close"])
            hclose = float(h1.iloc[hidx][f"{asset}_close"])
            rel = abs(hclose / dclose - 1.0)
            max_rel = max(max_rel, rel)
            checked += 1

        rows.append(
            {
                "asset": asset,
                "checked_days": checked,
                "missing_h1_matches": missing,
                "max_relative_close_error": max_rel,
            }
        )

    out = pd.DataFrame(rows)
    if (out["checked_days"] <= 0).any():
        raise RuntimeError("D1/H1 close consistency has an asset with zero matches")
    if (out["max_relative_close_error"] > 1e-8).any():
        bad = out[out["max_relative_close_error"] > 1e-8]
        raise RuntimeError(
            "D1/H1 close mismatch exceeds tolerance: "
            + bad.to_json(orient="records")
        )
    return out


def pair_specs() -> list[tuple[str, str, str]]:
    return [
        (left, right, f"{left}/{right}")
        for left, right in itertools.combinations(ASSETS, 2)
    ]


def prepare_pair_arrays(
    d1: pd.DataFrame,
    h1: pd.DataFrame,
) -> dict[str, dict]:
    d1_close_times = pd.DatetimeIndex(
        pd.to_datetime(d1["timestamp"], utc=True) + pd.Timedelta(days=1)
    )
    h1_close_times = pd.DatetimeIndex(
        pd.to_datetime(h1["timestamp"], utc=True) + pd.Timedelta(hours=1)
    )

    out: dict[str, dict] = {}
    for left, right, pair in pair_specs():
        d1_ratio = (
            d1[f"{right}_close"].astype(float)
            / d1[f"{left}_close"].astype(float)
        )
        d1_median = d1_ratio.rolling(
            LOOKBACK, min_periods=LOOKBACK
        ).median()

        anchor_series = pd.Series(
            d1_median.to_numpy(dtype=float),
            index=d1_close_times,
        )
        h1_anchor = anchor_series.reindex(
            h1_close_times, method="ffill"
        ).to_numpy(dtype=float)

        h1_ratio = (
            h1[f"{right}_close"].astype(float)
            / h1[f"{left}_close"].astype(float)
        ).to_numpy(dtype=float)

        out[pair] = {
            "left": left,
            "right": right,
            "d1_ratio": d1_ratio.to_numpy(dtype=float),
            "d1_median": d1_median.to_numpy(dtype=float),
            "h1_ratio": h1_ratio,
            "h1_anchor": h1_anchor,
        }
    return out


def event_row(
    *,
    event_time: pd.Timestamp,
    event: str,
    pair: str,
    from_asset: str,
    to_asset: str,
    ratio: float,
    median: float,
    deviation: float,
    max_dislocation: float,
    reversal_from_extreme: float,
    engine: str,
) -> dict:
    return {
        "date": pd.Timestamp(event_time).isoformat(),
        "event_time": pd.Timestamp(event_time),
        "event": event,
        "pair": pair,
        "from_asset": from_asset,
        "to_asset": to_asset,
        "ratio": float(ratio),
        "median": float(median),
        "deviation": float(deviation),
        "max_dislocation": float(max_dislocation),
        "reversal_from_extreme": float(reversal_from_extreme),
        "engine": engine,
    }


def state_row(
    *,
    pair: str,
    left: str,
    right: str,
    state: RuntimeState,
    ratio: float,
    median: float | None,
) -> dict:
    deviation = (
        None
        if median is None or pd.isna(median)
        else float(ratio / float(median) - 1.0)
    )
    from_asset = None
    to_asset = None
    reversal = None

    if state.mode == "HIGH" and state.extreme is not None:
        from_asset, to_asset = right, left
        reversal = max(0.0, 1.0 - ratio / state.extreme)
    elif state.mode == "LOW" and state.extreme is not None:
        from_asset, to_asset = left, right
        reversal = max(0.0, ratio / state.extreme - 1.0)

    return {
        "pair": pair,
        "mode": state.mode,
        "from_asset": from_asset,
        "to_asset": to_asset,
        "armed_at": (
            None
            if state.armed_at is None
            else pd.Timestamp(state.armed_at).isoformat()
        ),
        "ratio": float(ratio),
        "median": None if median is None or pd.isna(median) else float(median),
        "deviation": deviation,
        "extreme": state.extreme,
        "max_dislocation": float(state.max_dislocation),
        "reversal_from_extreme": reversal,
    }


def route_rows_at(
    *,
    event_time: pd.Timestamp,
    events_now: list[dict],
    states_now: list[dict],
    engine: str,
) -> list[dict]:
    rows = []
    if not any(str(x.get("event")).upper() == "CONFIRMED" for x in events_now):
        return rows

    for source in ASSETS:
        picked = choose_held_events(
            events_now,
            held_asset=source,
            latest_date=event_time,
            allowed_to_assets=TARGET_ASSETS,
        )
        primary = picked.get("primary_confirmed")
        if primary is None:
            continue

        conflicts = find_route_conflicts(
            events_now,
            states_now,
            primary_confirmed=primary,
            latest_date=event_time,
            allowed_to_assets=TARGET_ASSETS,
        )
        effective, chosen = choose_destination_dominance_override(
            primary, conflicts
        )
        if effective is None:
            continue

        rows.append(
            {
                "engine": engine,
                "action_time": pd.Timestamp(event_time),
                "source": source,
                "baseline_to": str(primary["to_asset"]).upper(),
                "effective_to": str(effective["to_asset"]).upper(),
                "ddg_override": chosen is not None,
                "baseline_pair": str(primary.get("pair") or ""),
                "effective_pair": str(effective.get("pair") or ""),
                "baseline_max_dislocation": float(
                    primary.get("max_dislocation") or 0.0
                ),
                "effective_max_dislocation": float(
                    effective.get("max_dislocation") or 0.0
                ),
                "strength_ratio": (
                    np.nan
                    if chosen is None
                    else float(chosen.get("strength_ratio") or np.nan)
                ),
            }
        )
    return rows


def build_d1_core(
    d1: pd.DataFrame,
    pair_data: dict[str, dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    states = {pair: RuntimeState() for pair in pair_data}
    events_all = []
    routes_all = []

    for i, ts in enumerate(d1["timestamp"]):
        event_time = pd.Timestamp(ts) + pd.Timedelta(days=1)
        events_now = []

        for pair, data in pair_data.items():
            ratio = float(data["d1_ratio"][i])
            median = float(data["d1_median"][i])
            if pd.isna(median):
                continue
            deviation = ratio / median - 1.0
            state = states[pair]
            left, right = data["left"], data["right"]

            if state.mode == "NONE":
                if deviation >= ARM_THRESHOLD:
                    state.mode = "HIGH"
                    state.extreme = ratio
                    state.max_dislocation = abs(deviation)
                    state.armed_at = event_time
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="ARMED",
                            pair=pair,
                            from_asset=right,
                            to_asset=left,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=abs(deviation),
                            reversal_from_extreme=0.0,
                            engine="D1_CORE",
                        )
                    )
                elif deviation <= -ARM_THRESHOLD:
                    state.mode = "LOW"
                    state.extreme = ratio
                    state.max_dislocation = abs(deviation)
                    state.armed_at = event_time
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="ARMED",
                            pair=pair,
                            from_asset=left,
                            to_asset=right,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=abs(deviation),
                            reversal_from_extreme=0.0,
                            engine="D1_CORE",
                        )
                    )
            elif state.mode == "HIGH":
                assert state.extreme is not None
                if ratio > state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(
                        state.max_dislocation, abs(deviation)
                    )
                reversal = 1.0 - ratio / state.extreme
                if reversal >= REVERSAL:
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="CONFIRMED",
                            pair=pair,
                            from_asset=right,
                            to_asset=left,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=state.max_dislocation,
                            reversal_from_extreme=reversal,
                            engine="D1_CORE",
                        )
                    )
                    states[pair] = RuntimeState()
            elif state.mode == "LOW":
                assert state.extreme is not None
                if ratio < state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(
                        state.max_dislocation, abs(deviation)
                    )
                reversal = ratio / state.extreme - 1.0
                if reversal >= REVERSAL:
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="CONFIRMED",
                            pair=pair,
                            from_asset=left,
                            to_asset=right,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=state.max_dislocation,
                            reversal_from_extreme=reversal,
                            engine="D1_CORE",
                        )
                    )
                    states[pair] = RuntimeState()

        states_now = []
        for pair, data in pair_data.items():
            median = data["d1_median"][i]
            ratio = float(data["d1_ratio"][i])
            states_now.append(
                state_row(
                    pair=pair,
                    left=data["left"],
                    right=data["right"],
                    state=states[pair],
                    ratio=ratio,
                    median=None if pd.isna(median) else float(median),
                )
            )

        events_all.extend(events_now)
        routes_all.extend(
            route_rows_at(
                event_time=event_time,
                events_now=events_now,
                states_now=states_now,
                engine="D1_CORE",
            )
        )

    return pd.DataFrame(events_all), pd.DataFrame(routes_all)


def build_hybrid_engine(
    h1: pd.DataFrame,
    d1: pd.DataFrame,
    pair_data: dict[str, dict],
    *,
    engine: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if engine not in HYBRID_ENGINES:
        raise ValueError(engine)

    states = {pair: RuntimeState() for pair in pair_data}
    events_all = []
    routes_all = []
    case_trace = []

    d1_close_set = {
        pd.Timestamp(ts) + pd.Timedelta(days=1)
        for ts in d1["timestamp"]
    }
    h1_close_times = [
        pd.Timestamp(ts) + pd.Timedelta(hours=1)
        for ts in h1["timestamp"]
    ]

    for i, event_time in enumerate(h1_close_times):
        events_now = []

        for pair, data in pair_data.items():
            ratio = float(data["h1_ratio"][i])
            median_raw = data["h1_anchor"][i]
            if pd.isna(median_raw):
                continue
            median = float(median_raw)
            deviation = ratio / median - 1.0

            state = states[pair]
            left, right = data["left"], data["right"]
            was_armed = state.mode != "NONE"
            confirmed_this_bar = False

            if state.mode == "HIGH":
                assert state.extreme is not None
                if ratio > state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(
                        state.max_dislocation, abs(deviation)
                    )
                reversal = 1.0 - ratio / state.extreme
                if reversal >= REVERSAL:
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="CONFIRMED",
                            pair=pair,
                            from_asset=right,
                            to_asset=left,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=state.max_dislocation,
                            reversal_from_extreme=reversal,
                            engine=engine,
                        )
                    )
                    states[pair] = RuntimeState()
                    state = states[pair]
                    confirmed_this_bar = True

            elif state.mode == "LOW":
                assert state.extreme is not None
                if ratio < state.extreme:
                    state.extreme = ratio
                    state.max_dislocation = max(
                        state.max_dislocation, abs(deviation)
                    )
                reversal = ratio / state.extreme - 1.0
                if reversal >= REVERSAL:
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="CONFIRMED",
                            pair=pair,
                            from_asset=left,
                            to_asset=right,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=state.max_dislocation,
                            reversal_from_extreme=reversal,
                            engine=engine,
                        )
                    )
                    states[pair] = RuntimeState()
                    state = states[pair]
                    confirmed_this_bar = True

            arm_allowed = (
                engine == "D1_ANCHOR_FULL_H1"
                or event_time in d1_close_set
            )

            if (
                not was_armed
                and not confirmed_this_bar
                and state.mode == "NONE"
                and arm_allowed
            ):
                if deviation >= ARM_THRESHOLD:
                    state.mode = "HIGH"
                    state.extreme = ratio
                    state.max_dislocation = abs(deviation)
                    state.armed_at = event_time
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="ARMED",
                            pair=pair,
                            from_asset=right,
                            to_asset=left,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=abs(deviation),
                            reversal_from_extreme=0.0,
                            engine=engine,
                        )
                    )
                elif deviation <= -ARM_THRESHOLD:
                    state.mode = "LOW"
                    state.extreme = ratio
                    state.max_dislocation = abs(deviation)
                    state.armed_at = event_time
                    events_now.append(
                        event_row(
                            event_time=event_time,
                            event="ARMED",
                            pair=pair,
                            from_asset=left,
                            to_asset=right,
                            ratio=ratio,
                            median=median,
                            deviation=deviation,
                            max_dislocation=abs(deviation),
                            reversal_from_extreme=0.0,
                            engine=engine,
                        )
                    )

            if pair == PAIR_CASE and event_time >= AAVE_TRACE_START:
                current = states[pair]
                reversal = None
                if current.mode == "HIGH" and current.extreme is not None:
                    reversal = max(0.0, 1.0 - ratio / current.extreme)
                elif current.mode == "LOW" and current.extreme is not None:
                    reversal = max(0.0, ratio / current.extreme - 1.0)
                case_trace.append(
                    {
                        "engine": engine,
                        "event_time": event_time,
                        "ratio_aave_trx": ratio,
                        "d1_median_anchor": median,
                        "deviation": deviation,
                        "mode_after_close": current.mode,
                        "extreme_after_close": current.extreme,
                        "max_dislocation_after_close": current.max_dislocation,
                        "reversal_after_close": reversal,
                        "events_this_close": ";".join(
                            f"{x['event']}:{x['from_asset']}->{x['to_asset']}"
                            for x in events_now
                            if x["pair"] == pair
                        ),
                    }
                )

        if events_now:
            states_now = []
            for pair, data in pair_data.items():
                median_raw = data["h1_anchor"][i]
                ratio = float(data["h1_ratio"][i])
                states_now.append(
                    state_row(
                        pair=pair,
                        left=data["left"],
                        right=data["right"],
                        state=states[pair],
                        ratio=ratio,
                        median=(
                            None
                            if pd.isna(median_raw)
                            else float(median_raw)
                        ),
                    )
                )
            routes_all.extend(
                route_rows_at(
                    event_time=event_time,
                    events_now=events_now,
                    states_now=states_now,
                    engine=engine,
                )
            )
            events_all.extend(events_now)

    return (
        pd.DataFrame(events_all),
        pd.DataFrame(routes_all),
        pd.DataFrame(case_trace),
    )


def route_map(routes: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], dict]:
    if routes.empty:
        return {}
    return {
        (pd.Timestamp(row["action_time"]), str(row["source"])): row.to_dict()
        for _, row in routes.iterrows()
    }


def first_index_on_or_after(panel: pd.DataFrame, ts: pd.Timestamp) -> int:
    times = pd.to_datetime(panel["timestamp"], utc=True)
    hits = np.flatnonzero(times.ge(ts).to_numpy())
    if not len(hits):
        raise RuntimeError(f"No H1 timestamp on/after {ts}")
    return int(hits[0])


def simulate(
    h1: pd.DataFrame,
    routes: pd.DataFrame,
    *,
    start_i: int,
    end_i: int,
    start_asset: str,
    cost_rate: float,
) -> dict:
    rmap = route_map(routes)
    asset = start_asset
    qty = 100.0 / float(h1.iloc[start_i][f"{asset}_open"])
    last_exec_i = start_i
    holding_hours = []
    transitions = 0
    cost_paid = 0.0
    equity = [100.0]

    for i in range(start_i, end_i + 1):
        t = pd.Timestamp(h1.iloc[i]["timestamp"])
        action = rmap.get((t, asset))
        if action is not None:
            target = str(action["effective_to"])
            if target != asset:
                before = qty * float(h1.iloc[i][f"{asset}_open"])
                fee = before * cost_rate
                after = before - fee
                qty = after / float(h1.iloc[i][f"{target}_open"])
                holding_hours.append(max(1, i - last_exec_i))
                last_exec_i = i
                transitions += 1
                cost_paid += fee
                asset = target

        close_value = qty * float(h1.iloc[i][f"{asset}_close"])
        equity.append(float(close_value))

    holding_hours.append(max(1, end_i - last_exec_i + 1))
    arr = np.asarray(equity, dtype=float)
    peaks = np.maximum.accumulate(arr)

    return {
        "start_asset": start_asset,
        "final_asset": asset,
        "final_capital": float(arr[-1]),
        "return": float(arr[-1] / 100.0 - 1.0),
        "max_dd": float(np.min(arr / peaks - 1.0)),
        "transitions": int(transitions),
        "median_holding_hours": float(np.median(holding_hours)),
        "modeled_cost_paid": float(cost_paid),
    }


def run_path_tests(
    h1: pd.DataFrame,
    routes_by_engine: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    end_i = len(h1) - 1
    detail = []

    for window, start_ts in WINDOW_STARTS.items():
        start_i = first_index_on_or_after(h1, start_ts)
        for cost in COSTS:
            for engine in ENGINES:
                for asset in ASSETS:
                    row = simulate(
                        h1,
                        routes_by_engine[engine],
                        start_i=start_i,
                        end_i=end_i,
                        start_asset=asset,
                        cost_rate=cost,
                    )
                    row.update(
                        {
                            "window": window,
                            "cost_rate": cost,
                            "engine": engine,
                            "start_date": pd.Timestamp(
                                h1.iloc[start_i]["timestamp"]
                            ),
                            "end_date": pd.Timestamp(
                                h1.iloc[end_i]["timestamp"]
                            ),
                        }
                    )
                    detail.append(row)

    detail_df = pd.DataFrame(detail)
    summaries = []
    comparisons = []

    for window in WINDOW_STARTS:
        for cost in COSTS:
            sub_base = detail_df[
                (detail_df["window"] == window)
                & np.isclose(detail_df["cost_rate"], cost)
                & (detail_df["engine"] == "D1_CORE")
            ].copy()

            for engine in ENGINES:
                sub = detail_df[
                    (detail_df["window"] == window)
                    & np.isclose(detail_df["cost_rate"], cost)
                    & (detail_df["engine"] == engine)
                ].copy()

                modal = sub["final_asset"].value_counts()
                modal_asset = str(modal.index[0])
                modal_share = float(modal.iloc[0] / len(sub))

                summaries.append(
                    {
                        "window": window,
                        "cost_rate": cost,
                        "engine": engine,
                        "start_count": int(len(sub)),
                        "median_final_capital": float(
                            sub["final_capital"].median()
                        ),
                        "median_return": float(sub["return"].median()),
                        "worst_return": float(sub["return"].min()),
                        "best_return": float(sub["return"].max()),
                        "positive_start_rate": float(
                            (sub["return"] > 0).mean()
                        ),
                        "median_max_dd": float(sub["max_dd"].median()),
                        "worst_max_dd": float(sub["max_dd"].min()),
                        "median_transitions": float(
                            sub["transitions"].median()
                        ),
                        "median_holding_hours": float(
                            sub["median_holding_hours"].median()
                        ),
                        "median_modeled_cost_paid": float(
                            sub["modeled_cost_paid"].median()
                        ),
                        "modal_final_asset": modal_asset,
                        "modal_final_asset_share": modal_share,
                    }
                )

                if engine != "D1_CORE":
                    merged = sub_base.merge(
                        sub,
                        on=["start_asset", "window", "cost_rate"],
                        suffixes=("_d1", "_hybrid"),
                        validate="one_to_one",
                    )
                    ratio = (
                        merged["final_capital_hybrid"]
                        / merged["final_capital_d1"]
                    )
                    comparisons.append(
                        {
                            "window": window,
                            "cost_rate": cost,
                            "engine": engine,
                            "n": int(len(merged)),
                            "improved_count": int((ratio > 1.0).sum()),
                            "improved_share": float((ratio > 1.0).mean()),
                            "median_final_capital_ratio": float(
                                ratio.median()
                            ),
                            "median_return_delta": float(
                                (
                                    merged["return_hybrid"]
                                    - merged["return_d1"]
                                ).median()
                            ),
                            "median_max_dd_delta": float(
                                (
                                    merged["max_dd_hybrid"]
                                    - merged["max_dd_d1"]
                                ).median()
                            ),
                            "median_transition_delta": float(
                                (
                                    merged["transitions_hybrid"]
                                    - merged["transitions_d1"]
                                ).median()
                            ),
                            "transition_ratio": float(
                                merged["transitions_hybrid"].median()
                                / max(1.0, merged["transitions_d1"].median())
                            ),
                        }
                    )

    return (
        detail_df,
        pd.DataFrame(summaries),
        pd.DataFrame(comparisons),
    )


def confirmed_only(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    return events[events["event"] == "CONFIRMED"].copy()


def build_pair_lead(
    d1_events: pd.DataFrame,
    hybrid_events: pd.DataFrame,
    engine: str,
) -> pd.DataFrame:
    d1c = confirmed_only(d1_events)
    h1c = confirmed_only(hybrid_events)
    d1c = d1c[d1c["event_time"] >= MATURE_START].copy()
    h1c = h1c[h1c["event_time"] >= MATURE_START].copy()
    rows = []

    for _, d in d1c.iterrows():
        t = pd.Timestamp(d["event_time"])
        matches = h1c[
            (h1c["pair"] == d["pair"])
            & (h1c["from_asset"] == d["from_asset"])
            & (h1c["to_asset"] == d["to_asset"])
            & (h1c["event_time"] <= t)
            & (h1c["event_time"] >= t - pd.Timedelta(hours=24))
        ].sort_values("event_time")
        if matches.empty:
            rows.append(
                {
                    "engine": engine,
                    "d1_event_time": t,
                    "pair": d["pair"],
                    "from_asset": d["from_asset"],
                    "to_asset": d["to_asset"],
                    "matched": False,
                    "hybrid_event_time": pd.NaT,
                    "lead_hours": np.nan,
                }
            )
            continue
        h = matches.iloc[0]
        ht = pd.Timestamp(h["event_time"])
        rows.append(
            {
                "engine": engine,
                "d1_event_time": t,
                "pair": d["pair"],
                "from_asset": d["from_asset"],
                "to_asset": d["to_asset"],
                "matched": True,
                "hybrid_event_time": ht,
                "lead_hours": float((t - ht) / pd.Timedelta(hours=1)),
            }
        )
    return pd.DataFrame(rows)


def build_route_lead(
    d1_routes: pd.DataFrame,
    hybrid_routes: pd.DataFrame,
    h1: pd.DataFrame,
    engine: str,
) -> pd.DataFrame:
    d1r = d1_routes[d1_routes["action_time"] >= MATURE_START].copy()
    h1r = hybrid_routes[
        hybrid_routes["action_time"] >= MATURE_START
    ].copy()
    price_idx = {
        pd.Timestamp(t): i for i, t in enumerate(h1["timestamp"])
    }
    rows = []

    for _, d in d1r.iterrows():
        t = pd.Timestamp(d["action_time"])
        matches = h1r[
            (h1r["source"] == d["source"])
            & (h1r["effective_to"] == d["effective_to"])
            & (h1r["action_time"] <= t)
            & (h1r["action_time"] >= t - pd.Timedelta(hours=24))
        ].sort_values("action_time")

        row = {
            "engine": engine,
            "d1_action_time": t,
            "source": d["source"],
            "effective_to": d["effective_to"],
            "matched": not matches.empty,
            "hybrid_action_time": pd.NaT,
            "lead_hours": np.nan,
            "execution_units_improvement": np.nan,
        }

        if not matches.empty:
            h = matches.iloc[0]
            ht = pd.Timestamp(h["action_time"])
            row["hybrid_action_time"] = ht
            row["lead_hours"] = float(
                (t - ht) / pd.Timedelta(hours=1)
            )
            di = price_idx.get(t)
            hi = price_idx.get(ht)
            if di is not None and hi is not None:
                source = str(d["source"])
                target = str(d["effective_to"])
                d_units = (
                    float(h1.iloc[di][f"{source}_open"])
                    / float(h1.iloc[di][f"{target}_open"])
                )
                h_units = (
                    float(h1.iloc[hi][f"{source}_open"])
                    / float(h1.iloc[hi][f"{target}_open"])
                )
                row["execution_units_improvement"] = (
                    h_units / d_units - 1.0
                )
        rows.append(row)
    return pd.DataFrame(rows)


def false_confirm_audit(
    d1: pd.DataFrame,
    d1_events: pd.DataFrame,
    hybrid_events: pd.DataFrame,
    engine: str,
) -> pd.DataFrame:
    d1_close_times = pd.DatetimeIndex(
        pd.to_datetime(d1["timestamp"], utc=True) + pd.Timedelta(days=1)
    )
    d1_conf = confirmed_only(d1_events)
    d1_set = {
        (
            pd.Timestamp(r["event_time"]),
            str(r["pair"]),
            str(r["from_asset"]),
            str(r["to_asset"]),
        )
        for _, r in d1_conf.iterrows()
    }

    h1_conf = confirmed_only(hybrid_events)
    h1_conf = h1_conf[h1_conf["event_time"] >= MATURE_START].copy()
    rows = []

    for _, r in h1_conf.iterrows():
        t = pd.Timestamp(r["event_time"])
        pos = int(d1_close_times.searchsorted(t, side="left"))
        next_times = list(d1_close_times[pos : pos + 3])
        same_next = False
        within3 = False

        for j, dt in enumerate(next_times):
            key = (
                pd.Timestamp(dt),
                str(r["pair"]),
                str(r["from_asset"]),
                str(r["to_asset"]),
            )
            if key in d1_set:
                within3 = True
                if j == 0:
                    same_next = True
                break

        rows.append(
            {
                "engine": engine,
                "hybrid_event_time": t,
                "pair": r["pair"],
                "from_asset": r["from_asset"],
                "to_asset": r["to_asset"],
                "preserved_next_d1": same_next,
                "preserved_within_3_d1": within3,
                "h1_only_within_3d": not within3,
            }
        )
    return pd.DataFrame(rows)


def summarize_leads(
    pair_leads: dict[str, pd.DataFrame],
    route_leads: dict[str, pd.DataFrame],
    false_audits: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows = []
    for engine in HYBRID_ENGINES:
        p = pair_leads[engine]
        pm = p[p["matched"]].copy()
        r = route_leads[engine]
        rm = r[r["matched"]].copy()
        f = false_audits[engine]

        rows.append(
            {
                "engine": engine,
                "d1_pair_confirm_count": int(len(p)),
                "pair_match_count": int(len(pm)),
                "pair_match_rate": float(len(pm) / len(p)) if len(p) else np.nan,
                "pair_earlier_count": int((pm["lead_hours"] > 0).sum()),
                "pair_median_lead_hours": (
                    float(pm["lead_hours"].median())
                    if len(pm) else np.nan
                ),
                "pair_p25_lead_hours": (
                    float(pm["lead_hours"].quantile(0.25))
                    if len(pm) else np.nan
                ),
                "pair_p75_lead_hours": (
                    float(pm["lead_hours"].quantile(0.75))
                    if len(pm) else np.nan
                ),
                "d1_route_count": int(len(r)),
                "route_match_count": int(len(rm)),
                "route_match_rate": float(len(rm) / len(r)) if len(r) else np.nan,
                "route_earlier_count": int((rm["lead_hours"] > 0).sum()),
                "route_median_lead_hours": (
                    float(rm["lead_hours"].median())
                    if len(rm) else np.nan
                ),
                "median_execution_units_improvement": (
                    float(rm["execution_units_improvement"].dropna().median())
                    if rm["execution_units_improvement"].notna().any()
                    else np.nan
                ),
                "hybrid_confirm_count": int(len(f)),
                "preserved_next_d1_rate": (
                    float(f["preserved_next_d1"].mean())
                    if len(f) else np.nan
                ),
                "preserved_within_3_d1_rate": (
                    float(f["preserved_within_3_d1"].mean())
                    if len(f) else np.nan
                ),
                "h1_only_within_3d_rate": (
                    float(f["h1_only_within_3d"].mean())
                    if len(f) else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def classify(
    comparisons: pd.DataFrame,
    path_summary: pd.DataFrame,
    lead_summary: pd.DataFrame,
) -> tuple[str, dict]:
    cmp = comparisons[
        (comparisons["window"] == "MATURE")
        & np.isclose(comparisons["cost_rate"], PRIMARY_COST)
        & (comparisons["engine"] == "D1_ARM_H1_CONFIRM")
    ].iloc[0]
    base = path_summary[
        (path_summary["window"] == "MATURE")
        & np.isclose(path_summary["cost_rate"], PRIMARY_COST)
        & (path_summary["engine"] == "D1_CORE")
    ].iloc[0]
    hyb = path_summary[
        (path_summary["window"] == "MATURE")
        & np.isclose(path_summary["cost_rate"], PRIMARY_COST)
        & (path_summary["engine"] == "D1_ARM_H1_CONFIRM")
    ].iloc[0]
    lead = lead_summary[
        lead_summary["engine"] == "D1_ARM_H1_CONFIRM"
    ].iloc[0]

    gates = {
        "mature_median_capital_improves": bool(
            hyb["median_final_capital"] > base["median_final_capital"]
        ),
        "mature_improved_share_ge_50pct": bool(
            cmp["improved_share"] >= 0.50
        ),
        "median_dd_delta_ge_minus_10pp": bool(
            cmp["median_max_dd_delta"] >= -0.10
        ),
        "transition_ratio_le_1p5x": bool(
            cmp["transition_ratio"] <= 1.50
        ),
        "matched_route_median_lead_gt_0h": bool(
            lead["route_median_lead_hours"] > 0
        ),
        "h1_only_within_3d_lt_50pct": bool(
            lead["h1_only_within_3d_rate"] < 0.50
        ),
    }

    if all(gates.values()):
        label = "H1_CONFIRM_PROMISING"
    elif (
        not gates["mature_median_capital_improves"]
        and not gates["mature_improved_share_ge_50pct"]
    ):
        label = "H1_CONFIRM_REJECTED"
    else:
        label = "H1_CONFIRM_MIXED"

    return label, gates


def build_case_routes(
    routes_by_engine: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    pieces = []
    for engine, routes in routes_by_engine.items():
        if routes.empty:
            continue
        sub = routes[
            (routes["action_time"] >= CASE_START)
            & (
                routes["source"].isin(
                    ["LINK", "ALGO", "TRX", "AAVE"]
                )
            )
        ].copy()
        pieces.append(sub)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


def build_case_pair_events(
    events_by_engine: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    pieces = []
    for engine, events in events_by_engine.items():
        if events.empty:
            continue
        sub = events[
            (events["event_time"] >= AAVE_TRACE_START)
            & (events["pair"] == PAIR_CASE)
        ].copy()
        pieces.append(sub)
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()


def fmt_pct(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):+.2f}%"


def fmt_rate(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{100*float(v):.1f}%"


def fmt_hours(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{float(v):.1f}h"


def main() -> None:
    d1, d1_meta = download_panel("1D")
    h1, h1_meta = download_panel("1H")
    consistency = validate_d1_h1_close_consistency(d1, h1)
    pair_data = prepare_pair_arrays(d1, h1)

    d1_events, d1_routes = build_d1_core(d1, pair_data)

    primary_events, primary_routes, primary_case_trace = build_hybrid_engine(
        h1, d1, pair_data, engine="D1_ARM_H1_CONFIRM"
    )
    full_events, full_routes, full_case_trace = build_hybrid_engine(
        h1, d1, pair_data, engine="D1_ANCHOR_FULL_H1"
    )

    events_by_engine = {
        "D1_CORE": d1_events,
        "D1_ARM_H1_CONFIRM": primary_events,
        "D1_ANCHOR_FULL_H1": full_events,
    }
    routes_by_engine = {
        "D1_CORE": d1_routes,
        "D1_ARM_H1_CONFIRM": primary_routes,
        "D1_ANCHOR_FULL_H1": full_routes,
    }

    path_detail, path_summary, comparisons = run_path_tests(
        h1, routes_by_engine
    )

    pair_leads = {}
    route_leads = {}
    false_audits = {}
    for engine in HYBRID_ENGINES:
        pair_leads[engine] = build_pair_lead(
            d1_events, events_by_engine[engine], engine
        )
        route_leads[engine] = build_route_lead(
            d1_routes, routes_by_engine[engine], h1, engine
        )
        false_audits[engine] = false_confirm_audit(
            d1, d1_events, events_by_engine[engine], engine
        )

    lead_summary = summarize_leads(
        pair_leads, route_leads, false_audits
    )
    classification, gates = classify(
        comparisons, path_summary, lead_summary
    )

    case_routes = build_case_routes(routes_by_engine)
    case_pair_events = build_case_pair_events(events_by_engine)
    case_h1_trace = pd.concat(
        [primary_case_trace, full_case_trace],
        ignore_index=True,
    )

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    consistency.to_csv(run_dir / "d1_h1_close_consistency.csv", index=False)
    path_detail.to_csv(run_dir / "path_detail.csv", index=False)
    path_summary.to_csv(run_dir / "path_summary.csv", index=False)
    comparisons.to_csv(run_dir / "core_vs_hybrid.csv", index=False)
    lead_summary.to_csv(run_dir / "lead_summary.csv", index=False)
    d1_events.to_csv(run_dir / "d1_pair_events.csv", index=False)
    d1_routes.to_csv(run_dir / "d1_routes.csv", index=False)
    primary_events.to_csv(run_dir / "primary_h1_pair_events.csv", index=False)
    primary_routes.to_csv(run_dir / "primary_h1_routes.csv", index=False)
    full_events.to_csv(run_dir / "full_h1_pair_events.csv", index=False)
    full_routes.to_csv(run_dir / "full_h1_routes.csv", index=False)
    case_routes.to_csv(run_dir / "current_case_routes.csv", index=False)
    case_pair_events.to_csv(
        run_dir / "aave_trx_case_pair_events.csv", index=False
    )
    case_h1_trace.to_csv(
        run_dir / "aave_trx_h1_state_trace.csv", index=False
    )

    for engine in HYBRID_ENGINES:
        pair_leads[engine].to_csv(
            run_dir / f"{engine.lower()}_pair_leads.csv", index=False
        )
        route_leads[engine].to_csv(
            run_dir / f"{engine.lower()}_route_leads.csv", index=False
        )
        false_audits[engine].to_csv(
            run_dir / f"{engine.lower()}_false_confirm_audit.csv",
            index=False,
        )

    summary_json = {
        "experiment": "RR_D1_ANCHOR_H1_CONFIRM_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "as_of": AS_OF.isoformat(),
        "latest_closed_d1_timestamp": pd.Timestamp(
            d1.iloc[-1]["timestamp"]
        ).isoformat(),
        "latest_closed_h1_timestamp": pd.Timestamp(
            h1.iloc[-1]["timestamp"]
        ).isoformat(),
        "classification": classification,
        "classification_gates": gates,
        "arm_threshold": ARM_THRESHOLD,
        "reversal": REVERSAL,
        "lookback_d1": LOOKBACK,
        "costs": list(COSTS),
        "d1_data": d1_meta,
        "h1_data": h1_meta,
        "production_changes": "NONE",
        "test_level": (
            "GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_H1_"
            "PATH_DEPENDENT_STRESS_TEST"
        ),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    lines = [
        "# RR D1-ANCHOR + H1 CONFIRMATION V1 — RESULTS",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        f"Classification: {classification}",
        f"As-of: {AS_OF.isoformat()}",
        f"Latest D1 candle timestamp: {pd.Timestamp(d1.iloc[-1]['timestamp']).isoformat()}",
        f"Latest H1 candle timestamp: {pd.Timestamp(h1.iloc[-1]['timestamp']).isoformat()}",
        "",
        "## Full-path absolute results — 0.10% cost",
        "",
        "|Window|Engine|Median return|Median capital|Median DD|Worst DD|Transitions|Median hold|",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]

    prim = path_summary[np.isclose(path_summary["cost_rate"], PRIMARY_COST)]
    for _, r in prim.iterrows():
        lines.append(
            f"|{r['window']}|{r['engine']}|"
            f"{fmt_pct(r['median_return'])}|"
            f"{r['median_final_capital']:.2f}|"
            f"{fmt_pct(r['median_max_dd'])}|"
            f"{fmt_pct(r['worst_max_dd'])}|"
            f"{r['median_transitions']:.1f}|"
            f"{fmt_hours(r['median_holding_hours'])}|"
        )

    lines += [
        "",
        "## Head-to-head versus D1_CORE — 0.10% cost",
        "",
        "|Window|Engine|Starts improved|Median capital ratio|Median return delta|Median DD delta|Extra transitions|Transition ratio|",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    cmp_primary = comparisons[
        np.isclose(comparisons["cost_rate"], PRIMARY_COST)
    ]
    for _, r in cmp_primary.iterrows():
        lines.append(
            f"|{r['window']}|{r['engine']}|"
            f"{int(r['improved_count'])}/{int(r['n'])} ({fmt_rate(r['improved_share'])})|"
            f"{r['median_final_capital_ratio']:.4f}x|"
            f"{fmt_pct(r['median_return_delta'])}|"
            f"{fmt_pct(r['median_max_dd_delta'])}|"
            f"{r['median_transition_delta']:+.1f}|"
            f"{r['transition_ratio']:.2f}x|"
        )

    lines += [
        "",
        "## Timing / preservation",
        "",
        "|Engine|Pair match|Pair median lead|Route match|Route median lead|Execution-unit improvement|Preserved next D1|Preserved within 3 D1|H1-only within 3D|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in lead_summary.iterrows():
        lines.append(
            f"|{r['engine']}|{fmt_rate(r['pair_match_rate'])}|"
            f"{fmt_hours(r['pair_median_lead_hours'])}|"
            f"{fmt_rate(r['route_match_rate'])}|"
            f"{fmt_hours(r['route_median_lead_hours'])}|"
            f"{fmt_pct(r['median_execution_units_improvement'])}|"
            f"{fmt_rate(r['preserved_next_d1_rate'])}|"
            f"{fmt_rate(r['preserved_within_3_d1_rate'])}|"
            f"{fmt_rate(r['h1_only_within_3d_rate'])}|"
        )

    lines += [
        "",
        "## Cost robustness — MATURE",
        "",
        "|Cost|Engine|Median return|Median capital|Transitions|",
        "|---:|---|---:|---:|---:|",
    ]
    mature = path_summary[path_summary["window"] == "MATURE"]
    for _, r in mature.iterrows():
        lines.append(
            f"|{100*r['cost_rate']:.2f}%|{r['engine']}|"
            f"{fmt_pct(r['median_return'])}|"
            f"{r['median_final_capital']:.2f}|"
            f"{r['median_transitions']:.1f}|"
        )

    lines += [
        "",
        "## Current-case routes from 2026-09-27",
        "",
        "|Engine|Action time|Source|Baseline|Effective|DDG|",
        "|---|---|---|---|---|---|",
    ]
    if case_routes.empty:
        lines.append("|—|—|—|—|—|—|")
    else:
        for _, r in case_routes.sort_values(
            ["action_time", "engine", "source"]
        ).iterrows():
            lines.append(
                f"|{r['engine']}|{pd.Timestamp(r['action_time']).isoformat()}|"
                f"{r['source']}|{r['baseline_to']}|{r['effective_to']}|"
                f"{'YES' if r['ddg_override'] else 'NO'}|"
            )

    lines += [
        "",
        "## AAVE/TRX pair events from 2026-09-28",
        "",
        "|Engine|Time|Event|From|To|Max dislocation|Reversal|",
        "|---|---|---|---|---|---:|---:|",
    ]
    if case_pair_events.empty:
        lines.append("|—|—|—|—|—|—|—|")
    else:
        for _, r in case_pair_events.sort_values(
            ["event_time", "engine"]
        ).iterrows():
            lines.append(
                f"|{r['engine']}|{pd.Timestamp(r['event_time']).isoformat()}|"
                f"{r['event']}|{r['from_asset']}|{r['to_asset']}|"
                f"{fmt_pct(r['max_dislocation'])}|"
                f"{fmt_pct(r['reversal_from_extreme'])}|"
            )

    lines += [
        "",
        "## Frozen decision gates",
        "",
    ]
    for k, v in gates.items():
        lines.append(f"- {k}: {v}")

    lines += [
        "",
        "## Boundary",
        "",
        "- Canonical D1 production strategy was not modified.",
        "- Existing Telegram H1 snapshot was not modified.",
        "- H1 engines exist only in this research branch.",
        "- No exchange execution is authorized.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_H1_PATH_DEPENDENT_STRESS_TEST",
        "",
    ]

    report = "\n".join(lines)
    (run_dir / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
