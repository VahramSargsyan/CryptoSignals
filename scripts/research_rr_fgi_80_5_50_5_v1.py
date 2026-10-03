from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_fgi_80_5_50_5_v1"

ASSETS = ("TWT", "PEPE", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "XRP", "HBAR")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
DD_RATIO = 1.50
COST = 0.001
DATA_START = pd.Timestamp("2023-01-01", tz="UTC")
FGI_URL = "https://api.alternative.me/fng/?limit=0&format=json"

GREED_ARM = 80.0
GREED_RETRACE = 5.0
FEAR_ARM = 50.0
FEAR_REBOUND = 5.0


@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0
    armed_at: pd.Timestamp | None = None


def utc(value):
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def download_panel(cutoff: pd.Timestamp):
    client = BinanceSpotRestClient()
    panel = None
    metadata = {}
    for asset in ASSETS:
        result = download_historical_dataset(
            client,
            symbol=asset + "USDT",
            start=DATA_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: no dataset ({result.metadata.status})")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality")
        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.rename(columns={"open": asset + "_open", "close": asset + "_close"})
        metadata[asset] = {
            "rows": int(len(frame)),
            "start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = frame if panel is None else panel.merge(
            frame, on="timestamp", how="inner", validate="one_to_one"
        )
    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if len(panel) < LOOKBACK + 2:
        raise RuntimeError(f"common panel too short: {len(panel)} rows")
    return panel, metadata


def download_fgi() -> pd.DataFrame:
    req = Request(FGI_URL, headers={"User-Agent": "CryptoSignals-RR-FGI-Research/1.0"})
    with urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("metadata", {}).get("error"):
        raise RuntimeError(f"FGI API error: {payload['metadata']['error']}")
    rows = []
    for item in payload.get("data", []):
        ts = pd.to_datetime(int(item["timestamp"]), unit="s", utc=True).floor("D")
        rows.append({"timestamp": ts, "fgi": float(item["value"])})
    frame = pd.DataFrame(rows).drop_duplicates("timestamp", keep="last")
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if frame.empty:
        raise RuntimeError("FGI API returned no data")
    return frame


def pair_key(a: str, b: str):
    return tuple(sorted((a, b)))


def build_rr_history(panel: pd.DataFrame):
    timestamps = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    states = {pair_key(a, b): PairState() for a, b in itertools.combinations(ASSETS, 2)}
    ratio_history = {
        pair_key(a, b): [] for a, b in itertools.combinations(ASSETS, 2)
    }
    events_by_day: list[list[dict]] = [[] for _ in range(len(panel))]
    state_snapshots: list[dict[tuple[str, str], dict]] = []

    for i, ts in enumerate(timestamps):
        day_events = events_by_day[i]
        snapshot = {}
        for left, right in itertools.combinations(ASSETS, 2):
            key = pair_key(left, right)
            # canonical pair orientation follows tuple order from ASSETS combination:
            # ratio = RIGHT / LEFT
            ratio = float(panel.iloc[i][right + "_close"]) / float(panel.iloc[i][left + "_close"])
            ratios = ratio_history[key]
            ratios.append(ratio)
            if len(ratios) < LOOKBACK:
                snapshot[key] = {
                    "pair": f"{left}/{right}",
                    "mode": states[key].mode,
                    "from_asset": None,
                    "to_asset": None,
                    "deviation": None,
                    "max_dislocation": states[key].max_dislocation,
                    "reversal_from_extreme": None,
                }
                continue

            median = float(np.median(ratios[-LOOKBACK:]))
            deviation = ratio / median - 1.0
            state = states[key]

            if state.mode == "NONE":
                if deviation >= ARM:
                    state = PairState("HIGH", ratio, abs(deviation), ts)
                    day_events.append({
                        "date": ts.isoformat(), "event": "ARMED", "pair": f"{left}/{right}",
                        "from_asset": right, "to_asset": left,
                        "deviation": deviation, "max_dislocation": abs(deviation),
                        "reversal_from_extreme": 0.0,
                    })
                elif deviation <= -ARM:
                    state = PairState("LOW", ratio, abs(deviation), ts)
                    day_events.append({
                        "date": ts.isoformat(), "event": "ARMED", "pair": f"{left}/{right}",
                        "from_asset": left, "to_asset": right,
                        "deviation": deviation, "max_dislocation": abs(deviation),
                        "reversal_from_extreme": 0.0,
                    })
            elif state.mode == "HIGH":
                if ratio > float(state.extreme):
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(deviation))
                retr = 1.0 - ratio / float(state.extreme)
                if retr >= REVERSAL:
                    day_events.append({
                        "date": ts.isoformat(), "event": "CONFIRMED", "pair": f"{left}/{right}",
                        "from_asset": right, "to_asset": left,
                        "deviation": deviation, "max_dislocation": state.max_dislocation,
                        "reversal_from_extreme": retr,
                    })
                    state = PairState()
            elif state.mode == "LOW":
                if ratio < float(state.extreme):
                    state.extreme = ratio
                    state.max_dislocation = max(state.max_dislocation, abs(deviation))
                retr = ratio / float(state.extreme) - 1.0
                if retr >= REVERSAL:
                    day_events.append({
                        "date": ts.isoformat(), "event": "CONFIRMED", "pair": f"{left}/{right}",
                        "from_asset": left, "to_asset": right,
                        "deviation": deviation, "max_dislocation": state.max_dislocation,
                        "reversal_from_extreme": retr,
                    })
                    state = PairState()

            states[key] = state
            prospective_from = None
            prospective_to = None
            reversal_now = None
            if state.mode == "HIGH" and state.extreme is not None:
                prospective_from, prospective_to = right, left
                reversal_now = max(0.0, 1.0 - ratio / float(state.extreme))
            elif state.mode == "LOW" and state.extreme is not None:
                prospective_from, prospective_to = left, right
                reversal_now = max(0.0, ratio / float(state.extreme) - 1.0)

            snapshot[key] = {
                "pair": f"{left}/{right}",
                "mode": state.mode,
                "from_asset": prospective_from,
                "to_asset": prospective_to,
                "deviation": deviation,
                "max_dislocation": state.max_dislocation,
                "reversal_from_extreme": reversal_now,
            }
        state_snapshots.append(snapshot)
    return events_by_day, state_snapshots


def better(existing: dict | None, candidate: dict):
    if existing is None:
        return candidate
    ec = str(existing.get("event") or "").upper() == "CONFIRMED"
    cc = str(candidate.get("event") or "").upper() == "CONFIRMED"
    if cc != ec:
        return candidate if cc else existing
    if float(candidate.get("max_dislocation") or 0.0) > float(existing.get("max_dislocation") or 0.0):
        return candidate
    return existing


def state_candidate(row: dict, date_iso: str):
    if str(row.get("mode") or "NONE").upper() not in {"HIGH", "LOW"}:
        return None
    fa = str(row.get("from_asset") or "")
    ta = str(row.get("to_asset") or "")
    if not fa or not ta:
        return None
    return {
        "date": date_iso,
        "event": "ARMED",
        "pair": row.get("pair"),
        "from_asset": fa,
        "to_asset": ta,
        "deviation": row.get("deviation"),
        "max_dislocation": float(row.get("max_dislocation") or 0.0),
        "reversal_from_extreme": row.get("reversal_from_extreme"),
    }


def effective_route_for_day(current: str, day_i: int, timestamps, events_by_day, state_snapshots):
    latest_events = events_by_day[day_i]
    confirmed = [
        e for e in latest_events
        if e["event"] == "CONFIRMED" and e["from_asset"] == current and e["to_asset"] in ASSETS
    ]
    confirmed.sort(key=lambda e: (-float(e["max_dislocation"]), e["to_asset"], e["pair"]))
    if not confirmed:
        return None
    primary = dict(confirmed[0])
    primary_to = primary["to_asset"]
    primary_strength = float(primary["max_dislocation"])
    date_iso = timestamps[day_i].isoformat()

    outbound = {}
    for e in latest_events:
        if e["from_asset"] == current and e["to_asset"] != primary_to:
            outbound[e["to_asset"]] = better(outbound.get(e["to_asset"]), dict(e))
    for row in state_snapshots[day_i].values():
        cand = state_candidate(row, date_iso)
        if cand and cand["from_asset"] == current and cand["to_asset"] != primary_to:
            outbound[cand["to_asset"]] = better(outbound.get(cand["to_asset"]), cand)

    def destination_relation(target):
        rel = None
        for e in latest_events:
            if e["from_asset"] == primary_to and e["to_asset"] == target:
                rel = better(rel, dict(e))
        for row in state_snapshots[day_i].values():
            cand = state_candidate(row, date_iso)
            if cand and cand["from_asset"] == primary_to and cand["to_asset"] == target:
                rel = better(rel, cand)
        return rel

    qualifying = []
    for target, cand in outbound.items():
        strength = float(cand.get("max_dislocation") or 0.0)
        if strength + 1e-12 < primary_strength * DD_RATIO:
            continue
        rel = destination_relation(target)
        if rel is None:
            continue
        qualifying.append((strength, target, cand, rel))

    if not qualifying:
        return primary

    qualifying.sort(key=lambda x: (x[0], x[1]), reverse=True)
    strength, target, cand, rel = qualifying[0]
    out = dict(primary)
    out.update({
        "to_asset": target,
        "pair": cand.get("pair"),
        "deviation": cand.get("deviation"),
        "max_dislocation": cand.get("max_dislocation"),
        "reversal_from_extreme": cand.get("reversal_from_extreme"),
        "route_override": True,
        "route_override_trigger": primary,
        "destination_relation": rel,
    })
    return out


def build_shadow_path(panel, start_i, end_i, start_asset, events_by_day, state_snapshots):
    timestamps = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    # Asset held immediately after each day's open. A close-T signal executes at T+1 open.
    asset_at_open = [None] * (end_i - start_i + 1)
    transitions = {}
    current = start_asset
    pending = None

    for i in range(start_i, end_i + 1):
        if pending is not None:
            current = pending["to_asset"]
            transitions[i] = pending
            pending = None
        asset_at_open[i - start_i] = current
        if i < end_i:
            route = effective_route_for_day(current, i, timestamps, events_by_day, state_snapshots)
            if route is not None:
                pending = route
    return asset_at_open, transitions


def fgi_signals(frame: pd.DataFrame):
    # signal maps keyed by date; execution is next available panel open
    exits = {}
    entries = {}
    mode = "INVESTED"
    greed_armed = False
    greed_peak = None
    fear_armed = False
    fear_floor = None
    cycles = []
    active = None

    for _, row in frame.iterrows():
        ts = utc(row["timestamp"])
        value = float(row["fgi"])

        if mode == "INVESTED":
            if not greed_armed and value >= GREED_ARM:
                greed_armed = True
                greed_peak = value
                active = {"greed_arm_date": ts.isoformat(), "greed_arm_value": value}
            elif greed_armed:
                greed_peak = max(float(greed_peak), value)
                if value <= float(greed_peak) - GREED_RETRACE:
                    exits[ts] = {
                        "signal": "EXIT_TO_CASH",
                        "fgi": value,
                        "greed_peak": float(greed_peak),
                    }
                    active.update({
                        "greed_peak": float(greed_peak),
                        "exit_signal_date": ts.isoformat(),
                        "exit_signal_fgi": value,
                    })
                    mode = "CASH"
                    greed_armed = False
                    greed_peak = None
                    fear_armed = False
                    fear_floor = None
        else:
            if not fear_armed and value <= FEAR_ARM:
                fear_armed = True
                fear_floor = value
                active["fear_arm_date"] = ts.isoformat()
                active["fear_arm_value"] = value
            elif fear_armed:
                fear_floor = min(float(fear_floor), value)
                if value >= float(fear_floor) + FEAR_REBOUND:
                    entries[ts] = {
                        "signal": "REENTER_RR",
                        "fgi": value,
                        "fear_floor": float(fear_floor),
                    }
                    active.update({
                        "fear_floor": float(fear_floor),
                        "entry_signal_date": ts.isoformat(),
                        "entry_signal_fgi": value,
                    })
                    cycles.append(active)
                    active = None
                    mode = "INVESTED"
                    fear_armed = False
                    fear_floor = None
    return exits, entries, cycles


def max_drawdown(equity):
    arr = np.asarray(equity, dtype=float)
    peak = np.maximum.accumulate(arr)
    return float(np.min(arr / peak - 1.0))


def simulate_baseline(panel, start_i, end_i, start_asset, shadow_assets, shadow_transitions):
    opens = {a: panel[a + "_open"].astype(float).to_numpy() for a in ASSETS}
    closes = {a: panel[a + "_close"].astype(float).to_numpy() for a in ASSETS}
    current = start_asset
    qty = 1.0 / float(opens[current][start_i])
    initial = qty * float(closes[current][start_i])
    equity = []
    open_equity = []

    for i in range(start_i, end_i + 1):
        if i in shadow_transitions:
            route = shadow_transitions[i]
            value = qty * float(opens[current][i])
            current = route["to_asset"]
            qty = value * (1.0 - COST) / float(opens[current][i])
        open_equity.append(qty * float(opens[current][i]))
        equity.append(qty * float(closes[current][i]))

    return {
        "return": float(equity[-1] / initial - 1.0),
        "max_dd": max_drawdown(equity),
        "transitions": int(len(shadow_transitions)),
        "equity": equity,
        "open_equity": open_equity,
    }


def simulate_overlay(panel, start_i, end_i, start_asset, shadow_assets, shadow_transitions, exit_dates, entry_dates):
    timestamps = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    opens = {a: panel[a + "_open"].astype(float).to_numpy() for a in ASSETS}
    closes = {a: panel[a + "_close"].astype(float).to_numpy() for a in ASSETS}

    current = start_asset
    qty = 1.0 / float(opens[current][start_i])
    cash = 0.0
    invested = True
    initial = qty * float(closes[current][start_i])
    equity = []
    cash_days = 0
    rr_trades = 0
    exits = 0
    entries = 0

    pending_exit = False
    pending_entry = False

    for i in range(start_i, end_i + 1):
        ts = timestamps[i]
        # Execute FGI decision from prior close at this open. Exit/re-entry has
        # precedence over an RR transition scheduled for the same open.
        if pending_exit and invested:
            cash = qty * float(opens[current][i]) * (1.0 - COST)
            qty = 0.0
            invested = False
            exits += 1
            pending_exit = False
        elif pending_entry and not invested:
            current = shadow_assets[i - start_i]
            qty = cash * (1.0 - COST) / float(opens[current][i])
            cash = 0.0
            invested = True
            entries += 1
            pending_entry = False
        elif invested and i in shadow_transitions:
            route = shadow_transitions[i]
            value = qty * float(opens[current][i])
            current = route["to_asset"]
            qty = value * (1.0 - COST) / float(opens[current][i])
            rr_trades += 1

        if invested:
            equity.append(qty * float(closes[current][i]))
        else:
            equity.append(cash)
            cash_days += 1

        # Signals use only today's known FGI and execute on next panel open.
        if i < end_i:
            if invested and ts in exit_dates:
                pending_exit = True
            elif (not invested) and ts in entry_dates:
                pending_entry = True

    return {
        "return": float(equity[-1] / initial - 1.0),
        "max_dd": max_drawdown(equity),
        "rr_trades": int(rr_trades),
        "cash_exits": int(exits),
        "cash_entries": int(entries),
        "cash_days": int(cash_days),
        "cash_fraction": float(cash_days / len(equity)),
        "equity": equity,
    }


def pct(x):
    return f"{100*x:+.2f}%"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default=None)
    args = parser.parse_args()
    cutoff = utc(args.cutoff) if args.cutoff else pd.Timestamp.now(tz="UTC").floor("D")

    panel, data_meta = download_panel(cutoff)
    fgi = download_fgi()
    panel_dates = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))

    # Restrict to common FGI/price history, with 180 observations already available.
    first_mature = panel_dates[LOOKBACK - 1]
    common_start = max(first_mature, utc(fgi["timestamp"].min()))
    common_end = min(panel_dates[-1], utc(fgi["timestamp"].max()))
    start_i = int(panel_dates.searchsorted(common_start, side="left"))
    end_i = int(panel_dates.searchsorted(common_end, side="right")) - 1
    if end_i - start_i < 90:
        raise RuntimeError("common FGI/RR evaluation window is too short")

    fgi = fgi[(fgi["timestamp"] >= panel_dates[start_i]) & (fgi["timestamp"] <= panel_dates[end_i])].copy()
    exits, entries, cycles = fgi_signals(fgi)
    exit_dates = set(exits)
    entry_dates = set(entries)

    events_by_day, state_snapshots = build_rr_history(panel)

    rows = []
    cycle_by_start = []
    for start_asset in ASSETS:
        shadow_assets, shadow_transitions = build_shadow_path(
            panel, start_i, end_i, start_asset, events_by_day, state_snapshots
        )
        base = simulate_baseline(
            panel, start_i, end_i, start_asset, shadow_assets, shadow_transitions
        )
        overlay = simulate_overlay(
            panel, start_i, end_i, start_asset, shadow_assets, shadow_transitions,
            exit_dates, entry_dates
        )
        rows.append({
            "start_asset": start_asset,
            "baseline_return": base["return"],
            "overlay_return": overlay["return"],
            "return_delta_pp": 100.0 * (overlay["return"] - base["return"]),
            "baseline_max_dd": base["max_dd"],
            "overlay_max_dd": overlay["max_dd"],
            "dd_improvement_pp": 100.0 * (overlay["max_dd"] - base["max_dd"]),
            "baseline_transitions": base["transitions"],
            "overlay_rr_trades": overlay["rr_trades"],
            "cash_exits": overlay["cash_exits"],
            "cash_entries": overlay["cash_entries"],
            "cash_days": overlay["cash_days"],
            "cash_fraction": overlay["cash_fraction"],
        })

        ts_to_local = {
            panel_dates[i]: i - start_i for i in range(start_i, end_i + 1)
        }
        for cycle_no, cycle in enumerate(cycles, start=1):
            exit_signal = utc(cycle["exit_signal_date"])
            entry_signal = utc(cycle["entry_signal_date"])
            exit_exec = exit_signal + pd.Timedelta(days=1)
            entry_exec = entry_signal + pd.Timedelta(days=1)
            if exit_exec not in ts_to_local or entry_exec not in ts_to_local:
                continue
            lo = ts_to_local[exit_exec]
            hi = ts_to_local[entry_exec]
            factor = float(base["open_equity"][hi] / base["open_equity"][lo])
            cycle_by_start.append({
                "cycle": cycle_no,
                "start_asset": start_asset,
                "exit_execution_date": exit_exec.isoformat(),
                "entry_execution_date": entry_exec.isoformat(),
                "cash_calendar_days": int((entry_exec - exit_exec).days),
                "baseline_shadow_return_during_cash": factor - 1.0,
            })

    result = pd.DataFrame(rows)
    cycle_detail = pd.DataFrame(cycle_by_start)
    cycle_summary_rows = []
    if not cycle_detail.empty:
        for cycle_no, sub in cycle_detail.groupby("cycle", sort=True):
            cycle_meta = dict(cycles[int(cycle_no) - 1])
            cycle_summary_rows.append({
                "cycle": int(cycle_no),
                **cycle_meta,
                "exit_execution_date": sub["exit_execution_date"].iloc[0],
                "entry_execution_date": sub["entry_execution_date"].iloc[0],
                "cash_calendar_days": int(sub["cash_calendar_days"].iloc[0]),
                "median_rr_return_missed": float(sub["baseline_shadow_return_during_cash"].median()),
                "worst_rr_return_missed": float(sub["baseline_shadow_return_during_cash"].min()),
                "best_rr_return_missed": float(sub["baseline_shadow_return_during_cash"].max()),
                "positive_rr_starts_during_cash": int((sub["baseline_shadow_return_during_cash"] > 0).sum()),
            })
    cycle_summary = pd.DataFrame(cycle_summary_rows)

    summary = {
        "experiment": "RR_FGI_80_MINUS5_50_PLUS5_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "production_changes": "NONE",
        "parameters": {
            "rr_assets": list(ASSETS),
            "rr_lookback_days": LOOKBACK,
            "rr_arm_threshold": ARM,
            "rr_reversal": REVERSAL,
            "destination_dominance_ratio": DD_RATIO,
            "transition_cost": COST,
            "execution": "signal on close T -> execute next daily open",
            "fgi_greed_arm": GREED_ARM,
            "fgi_greed_retrace": GREED_RETRACE,
            "fgi_fear_arm": FEAR_ARM,
            "fgi_fear_rebound": FEAR_REBOUND,
            "cash_yield": 0.0,
            "shadow_rr_continues_while_cash": True,
        },
        "data": {
            "price_metadata": data_meta,
            "panel_start": panel_dates[0].isoformat(),
            "panel_end": panel_dates[-1].isoformat(),
            "fgi_start": utc(fgi["timestamp"].min()).isoformat(),
            "fgi_end": utc(fgi["timestamp"].max()).isoformat(),
            "evaluation_start": panel_dates[start_i].isoformat(),
            "evaluation_end": panel_dates[end_i].isoformat(),
            "evaluation_days": int(end_i - start_i + 1),
            "fgi_source": "Alternative.me Fear & Greed Index API",
        },
        "fgi_cycles_completed": int(len(cycles)),
        "fgi_exit_signals": int(len(exits)),
        "fgi_entry_signals": int(len(entries)),
        "aggregate": {
            "median_baseline_return": float(result["baseline_return"].median()),
            "median_overlay_return": float(result["overlay_return"].median()),
            "median_return_delta_pp": float(result["return_delta_pp"].median()),
            "worst_baseline_return": float(result["baseline_return"].min()),
            "worst_overlay_return": float(result["overlay_return"].min()),
            "best_baseline_return": float(result["baseline_return"].max()),
            "best_overlay_return": float(result["overlay_return"].max()),
            "overlay_beats_baseline_starts": int((result["overlay_return"] > result["baseline_return"]).sum()),
            "start_count": int(len(result)),
            "median_baseline_max_dd": float(result["baseline_max_dd"].median()),
            "median_overlay_max_dd": float(result["overlay_max_dd"].median()),
            "median_dd_improvement_pp": float(result["dd_improvement_pp"].median()),
            "median_cash_fraction": float(result["cash_fraction"].median()),
            "median_cash_days": float(result["cash_days"].median()),
        },
        "cycles": cycles,
        "cycle_attribution": cycle_summary_rows,
    }

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    result.to_csv(run_dir / "start_asset_comparison.csv", index=False)
    pd.DataFrame(cycles).to_csv(run_dir / "fgi_cycles.csv", index=False)
    cycle_detail.to_csv(run_dir / "cycle_attribution_by_start.csv", index=False)
    cycle_summary.to_csv(run_dir / "cycle_attribution_summary.csv", index=False)
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )

    agg = summary["aggregate"]
    lines = [
        "# RR + FGI 80/-5 -> 50/+5 V1",
        "",
        "Mode: STRESS_TEST_ONLY — production/live unchanged.",
        "",
        "## Frozen rule",
        "",
        "- RR baseline: U10, 180D median, ARM 15%, reversal 3%, destination dominance 1.5x.",
        "- FGI >=80 arms greed; track peak; exit to cash after -5 points from peak.",
        "- While cash, shadow RR continues virtually.",
        "- FGI <=50 arms fear; track trough; re-enter shadow RR after +5 points from trough.",
        "- All FGI and RR close signals execute at next daily open.",
        f"- Cost: {100*COST:.2f}% per actual transition; cash yield 0%.",
        "",
        "## Common history",
        "",
        f"- Evaluation: {summary['data']['evaluation_start']} -> {summary['data']['evaluation_end']}",
        f"- Days: {summary['data']['evaluation_days']}",
        f"- Completed FGI cash cycles: {summary['fgi_cycles_completed']}",
        "",
        "## Aggregate across all 10 RR start assets",
        "",
        f"- Baseline median return: {pct(agg['median_baseline_return'])}",
        f"- Overlay median return: {pct(agg['median_overlay_return'])}",
        f"- Median return delta: {agg['median_return_delta_pp']:+.2f} pp",
        f"- Overlay beats baseline: {agg['overlay_beats_baseline_starts']} / {agg['start_count']} starts",
        f"- Baseline median max DD: {pct(agg['median_baseline_max_dd'])}",
        f"- Overlay median max DD: {pct(agg['median_overlay_max_dd'])}",
        f"- Median DD improvement: {agg['median_dd_improvement_pp']:+.2f} pp",
        f"- Median time in cash: {100*agg['median_cash_fraction']:.1f}%",
        "",
        "## Cash-cycle attribution",
        "",
    ]
    for row in cycle_summary_rows:
        lines.append(
            f"- Cycle {row['cycle']}: {row['exit_execution_date'][:10]} -> "
            f"{row['entry_execution_date'][:10]} ({row['cash_calendar_days']}d), "
            f"median RR move while in cash {pct(row['median_rr_return_missed'])}; "
            f"RR positive in {row['positive_rr_starts_during_cash']}/10 starts."
        )
    lines += [
        "",
        "## Start-asset detail",
        "",
        "|start|baseline|FGI overlay|delta pp|base DD|overlay DD|cash %|",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in result.sort_values("start_asset").iterrows():
        lines.append(
            f"|{row['start_asset']}|{pct(row['baseline_return'])}|{pct(row['overlay_return'])}|"
            f"{row['return_delta_pp']:+.2f}|{pct(row['baseline_max_dd'])}|"
            f"{pct(row['overlay_max_dd'])}|{100*row['cash_fraction']:.1f}%|"
        )
    lines += [
        "",
        "## Interpretation boundary",
        "",
        "- This is historical stress-test evidence, not production approval.",
        "- Thresholds 80/-5/50/+5 are frozen for this first pass; no tuning was performed.",
        "- The FGI source describes broad crypto sentiment and is not an asset-specific signal.",
        "- Production RR, Telegram and execution behavior are unchanged.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    (run_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"run_dir={run_dir}")
    print(f"evaluation={summary['data']['evaluation_start']}->{summary['data']['evaluation_end']}")
    print(f"baseline_median={agg['median_baseline_return']:.6f}")
    print(f"overlay_median={agg['median_overlay_return']:.6f}")
    print(f"delta_pp={agg['median_return_delta_pp']:.4f}")
    print(f"beats={agg['overlay_beats_baseline_starts']}/{agg['start_count']}")


if __name__ == "__main__":
    raise SystemExit(main())
