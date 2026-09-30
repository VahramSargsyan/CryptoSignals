from __future__ import annotations

import argparse
import bisect
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

OUT = Path("research_artifacts/sol_xrp_holdout_v1")

COMMON8 = ("TWT", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "HBAR")
SOL9 = COMMON8 + ("SOL",)
XRP9 = COMMON8 + ("XRP",)
BOTH10 = COMMON8 + ("SOL", "XRP")
POOL = BOTH10

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2021-01-27", tz="UTC")
CUTOFF = pd.Timestamp("2023-05-05", tz="UTC")
WINDOWS = {
    "FULL_HOLDOUT": (pd.Timestamp("2021-07-26", tz="UTC"), pd.Timestamp("2023-05-04", tz="UTC")),
    "H2_2021": (pd.Timestamp("2021-07-26", tz="UTC"), pd.Timestamp("2021-12-31", tz="UTC")),
    "YEAR_2022": (pd.Timestamp("2022-01-01", tz="UTC"), pd.Timestamp("2022-12-31", tz="UTC")),
    "PRE_PEPE_2023": (pd.Timestamp("2023-01-01", tz="UTC"), pd.Timestamp("2023-05-04", tz="UTC")),
}

def utc(v):
    t = pd.Timestamp(v)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

def download_panel():
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    for asset in POOL:
        result = download_historical_dataset(
            client,
            symbol=asset + "USDT",
            start=DATA_START,
            end=CUTOFF,
            timeframe="1D",
            as_of=CUTOFF,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: no dataset ({result.metadata.status})")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")
        f = result.dataset.candles[["timestamp", "open", "close"]].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.rename(columns={"open": asset + "_open", "close": asset + "_close"})
        meta[asset] = {
            "rows": int(len(f)),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, meta

def build_signal_index(panel):
    cols = ["timestamp"] + [a + "_close" for a in POOL]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    ts_to_i = {utc(ts): i for i, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))}
    grouped = defaultdict(list)
    for e in events:
        if e["event"] != "CONFIRMED":
            continue
        i = ts_to_i.get(utc(e["date"]))
        if i is None:
            continue
        grouped[(e["from_asset"], i)].append({
            "to_asset": e["to_asset"],
            "pair": e["pair"],
            "max_dislocation": float(e["max_dislocation"]),
        })
    positions = {a: [] for a in POOL}
    candidates = {a: [] for a in POOL}
    for (frm, i), items in grouped.items():
        items.sort(key=lambda x: (-x["max_dislocation"], x["to_asset"], x["pair"]))
        positions[frm].append(i)
        candidates[frm].append(items)
    for a in POOL:
        if positions[a]:
            order = np.argsort(positions[a]).tolist()
            positions[a] = [positions[a][j] for j in order]
            candidates[a] = [candidates[a][j] for j in order]
    return positions, candidates

def arrays(panel):
    ts = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    opens = {a: panel[a + "_open"].astype(float).to_numpy() for a in POOL}
    closes = {a: panel[a + "_close"].astype(float).to_numpy() for a in POOL}
    return ts, opens, closes

def bounds(ts, start, end):
    si = int(ts.searchsorted(utc(start), side="left"))
    ei = int(ts.searchsorted(utc(end), side="right")) - 1
    if ei < si:
        raise RuntimeError(f"empty window {start} -> {end}")
    return si, ei

def simulate(opens, closes, positions, candidates, assets, si, ei, start_asset):
    aset = set(assets)
    cur = start_asset
    qty = 1.0 / float(opens[cur][si])
    initial = qty * float(closes[cur][si])
    peak = initial
    min_dd = 0.0
    transitions = 0
    segment_start = si
    search_from = si

    while segment_start <= ei:
        pl = positions[cur]
        ca = candidates[cur]
        k = bisect.bisect_left(pl, search_from)
        found = False
        while k < len(pl):
            signal_i = pl[k]
            if signal_i >= ei:
                break
            eligible = [e for e in ca[k] if e["to_asset"] in aset]
            if not eligible:
                k += 1
                continue
            chosen = eligible[0]
            vals = qty * closes[cur][segment_start:signal_i + 1]
            running = np.maximum.accumulate(vals)
            running = np.maximum(running, peak)
            min_dd = min(min_dd, float(np.min(vals / running - 1.0)))
            peak = max(peak, float(running[-1]))
            execute_i = signal_i + 1
            value = qty * float(opens[cur][execute_i])
            cur = chosen["to_asset"]
            qty = value * (1.0 - COST) / float(opens[cur][execute_i])
            transitions += 1
            segment_start = execute_i
            search_from = execute_i
            found = True
            break
        if found:
            continue
        vals = qty * closes[cur][segment_start:ei + 1]
        running = np.maximum.accumulate(vals)
        running = np.maximum(running, peak)
        min_dd = min(min_dd, float(np.min(vals / running - 1.0)))
        break

    final = qty * float(closes[cur][ei])
    return {
        "return": float(final / initial - 1.0),
        "max_dd": float(min_dd),
        "transitions": int(transitions),
        "final_asset": cur,
    }

def evaluate(opens, closes, positions, candidates, ts, assets, start, end):
    si, ei = bounds(ts, start, end)
    runs = [
        simulate(opens, closes, positions, candidates, assets, si, ei, a)
        for a in assets
    ]
    rets = np.array([r["return"] for r in runs], dtype=float)
    return {
        "median_return": float(np.median(rets)),
        "worst_return": float(np.min(rets)),
        "best_return": float(np.max(rets)),
        "positive_starts": int(np.sum(rets > 0)),
        "start_count": int(len(rets)),
        "median_max_dd": float(np.median([r["max_dd"] for r in runs])),
        "median_transitions": float(np.median([r["transitions"] for r in runs])),
    }

def rolling(panel, opens, closes, positions, candidates, ts, months):
    rows = []
    first = pd.Timestamp("2021-08-01", tz="UTC")
    end_limit = WINDOWS["FULL_HOLDOUT"][1]
    for start in pd.date_range(first, end_limit, freq="MS"):
        end = start + pd.DateOffset(months=months) - pd.Timedelta(days=1)
        if end > end_limit:
            continue
        row = {"start": start.date().isoformat(), "end": end.date().isoformat(), "months": months}
        for label, assets in (("BASE8", COMMON8), ("SOL9", SOL9), ("XRP9", XRP9), ("BOTH10", BOTH10)):
            m = evaluate(opens, closes, positions, candidates, ts, assets, start, end)
            row[label + "_return"] = m["median_return"]
        row["SOL_minus_XRP"] = row["SOL9_return"] - row["XRP9_return"]
        row["SOL_minus_BASE"] = row["SOL9_return"] - row["BASE8_return"]
        row["XRP_minus_BASE"] = row["XRP9_return"] - row["BASE8_return"]
        rows.append(row)
    return pd.DataFrame(rows)

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    panel, data_meta = download_panel()
    ts, opens, closes = arrays(panel)
    positions, candidates = build_signal_index(panel)

    rows = []
    for window, (start, end) in WINDOWS.items():
        metrics = {}
        for label, assets in (("BASE8", COMMON8), ("SOL9", SOL9), ("XRP9", XRP9), ("BOTH10", BOTH10)):
            m = evaluate(opens, closes, positions, candidates, ts, assets, start, end)
            metrics[label] = m
            rows.append({
                "window": window,
                "start": start.date().isoformat(),
                "end": end.date().isoformat(),
                "universe": label,
                "assets": "|".join(assets),
                **m,
            })
        rows.append({
            "window": window,
            "start": start.date().isoformat(),
            "end": end.date().isoformat(),
            "universe": "COMPARISON",
            "assets": "",
            "median_return": np.nan,
            "worst_return": np.nan,
            "best_return": np.nan,
            "positive_starts": np.nan,
            "start_count": np.nan,
            "median_max_dd": np.nan,
            "median_transitions": np.nan,
            "sol_minus_xrp": metrics["SOL9"]["median_return"] - metrics["XRP9"]["median_return"],
            "sol_minus_base": metrics["SOL9"]["median_return"] - metrics["BASE8"]["median_return"],
            "xrp_minus_base": metrics["XRP9"]["median_return"] - metrics["BASE8"]["median_return"],
        })

    window_df = pd.DataFrame(rows)
    window_df.to_csv(OUT / "holdout_windows.csv", index=False)

    roll180 = rolling(panel, opens, closes, positions, candidates, ts, 6)
    roll365 = rolling(panel, opens, closes, positions, candidates, ts, 12)
    roll180.to_csv(OUT / "rolling_6m.csv", index=False)
    roll365.to_csv(OUT / "rolling_12m.csv", index=False)

    full = {}
    for label, assets in (("BASE8", COMMON8), ("SOL9", SOL9), ("XRP9", XRP9), ("BOTH10", BOTH10)):
        full[label] = evaluate(
            opens, closes, positions, candidates, ts, assets,
            WINDOWS["FULL_HOLDOUT"][0], WINDOWS["FULL_HOLDOUT"][1]
        )

    summary = {
        "experiment": "SOL_XRP_PRE_PEPE_HOLDOUT_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes": "NONE",
        "data_source": "BINANCE_SPOT_D1",
        "data_start": DATA_START.isoformat(),
        "common_panel_start": ts[0].isoformat(),
        "common_panel_end": ts[-1].isoformat(),
        "parameters": {
            "lookback": LOOKBACK,
            "arm": ARM,
            "reversal": REVERSAL,
            "transition_cost": COST,
            "execution": "confirmed close T -> next available daily open",
        },
        "universes": {
            "BASE8": list(COMMON8),
            "SOL9": list(SOL9),
            "XRP9": list(XRP9),
            "BOTH10": list(BOTH10),
        },
        "full_holdout": full,
        "full_deltas": {
            "SOL_minus_XRP": full["SOL9"]["median_return"] - full["XRP9"]["median_return"],
            "SOL_minus_BASE": full["SOL9"]["median_return"] - full["BASE8"]["median_return"],
            "XRP_minus_BASE": full["XRP9"]["median_return"] - full["BASE8"]["median_return"],
        },
        "rolling_6m": {
            "window_count": int(len(roll180)),
            "SOL_beats_XRP_rate": float((roll180["SOL_minus_XRP"] > 0).mean()) if len(roll180) else None,
            "SOL_beats_BASE_rate": float((roll180["SOL_minus_BASE"] > 0).mean()) if len(roll180) else None,
            "XRP_beats_BASE_rate": float((roll180["XRP_minus_BASE"] > 0).mean()) if len(roll180) else None,
            "median_SOL_minus_XRP": float(roll180["SOL_minus_XRP"].median()) if len(roll180) else None,
        },
        "rolling_12m": {
            "window_count": int(len(roll365)),
            "SOL_beats_XRP_rate": float((roll365["SOL_minus_XRP"] > 0).mean()) if len(roll365) else None,
            "SOL_beats_BASE_rate": float((roll365["SOL_minus_BASE"] > 0).mean()) if len(roll365) else None,
            "XRP_beats_BASE_rate": float((roll365["XRP_minus_BASE"] > 0).mean()) if len(roll365) else None,
            "median_SOL_minus_XRP": float(roll365["SOL_minus_XRP"].median()) if len(roll365) else None,
        },
        "data_metadata": data_meta,
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        "# SOL vs XRP PRE-PEPE HOLDOUT V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "This test uses a pre-PEPE historical holdout so the 2023-2026 U10 selection cannot directly see these dates.",
        "Universe is reduced symmetrically by removing PEPE from the current common set.",
        "",
        f"Full holdout: {WINDOWS['FULL_HOLDOUT'][0].date()} -> {WINDOWS['FULL_HOLDOUT'][1].date()}",
        "",
        "|Universe|Median return|Median DD|Median transitions|",
        "|---|---:|---:|---:|",
    ]
    for label in ("BASE8", "SOL9", "XRP9", "BOTH10"):
        m = full[label]
        lines.append(f"|{label}|{100*m['median_return']:+.1f}%|{100*m['median_max_dd']:+.1f}%|{m['median_transitions']:.1f}|")
    lines += [
        "",
        f"SOL-XRP delta: {100*summary['full_deltas']['SOL_minus_XRP']:+.1f} pp",
        f"SOL-BASE delta: {100*summary['full_deltas']['SOL_minus_BASE']:+.1f} pp",
        f"XRP-BASE delta: {100*summary['full_deltas']['XRP_minus_BASE']:+.1f} pp",
        "",
        f"Rolling 6m SOL>XRP rate: {100*summary['rolling_6m']['SOL_beats_XRP_rate']:.1f}%",
        f"Rolling 12m SOL>XRP rate: {100*summary['rolling_12m']['SOL_beats_XRP_rate']:.1f}%",
        "",
        "No production/live configuration was changed.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (OUT / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))

if __name__ == "__main__":
    main()
