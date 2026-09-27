from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u8_drawdown_entry_stress_v1"

ASSETS = ("ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001
DATA_START = pd.Timestamp("2023-01-01", tz="UTC")
START_ASSET = "ATOM"

SCENARIOS = (
    ("ONE_YEAR_BEFORE_PEAK", "2024-09-20"),
    ("SAME_YEAR_START", "2025-01-01"),
    ("AT_PEAK_DATE", "2025-09-20"),
    ("TROUGH_YEAR_START", "2026-01-01"),
)


def utc(value):
    t = pd.Timestamp(value)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    return os.environ.get("SOURCE_COMMIT_SHA") or subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
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
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")

        frame = result.dataset.candles[["timestamp", "open", "close"]].copy()
        frame = frame.rename(columns={"open": asset + "_open", "close": asset + "_close"})
        meta[asset] = {
            "rows": len(frame),
            "start": utc(frame.iloc[0]["timestamp"]).isoformat(),
            "end": utc(frame.iloc[-1]["timestamp"]).isoformat(),
            "listing_truncated": bool(result.metadata.listing_truncated),
        }
        panel = frame if panel is None else panel.merge(
            frame, on="timestamp", how="inner", validate="one_to_one"
        )

    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if len(panel) < LOOKBACK:
        raise RuntimeError("insufficient common panel")
    return panel, meta


def build_event_map(panel):
    close_cols = ["timestamp"] + [asset + "_close" for asset in ASSETS]
    events, _ = build_pair_monitor(
        panel[close_cols].copy(),
        assets=ASSETS,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    out = defaultdict(list)
    for event in events:
        if event["event"] == "CONFIRMED":
            out[utc(event["date"])].append(event)
    return out


def choose(emap, ts, current):
    candidates = [
        e for e in emap.get(ts, [])
        if e["from_asset"] == current and e["to_asset"] in ASSETS
    ]
    if not candidates:
        return None, 0
    selected = sorted(
        candidates,
        key=lambda e: (-float(e["max_dislocation"]), e["to_asset"], e["pair"]),
    )[0]
    return selected, len(candidates)


def run_scenario(panel, emap, name, start_date, initial_usdt):
    start = utc(start_date)
    w = panel[panel["timestamp"] >= start].copy().reset_index(drop=True)
    if w.empty:
        raise RuntimeError(f"{name}: empty scenario")

    current = START_ASSET
    first_open = float(w.iloc[0][current + "_open"])
    qty = float(initial_usdt) / first_open
    pending = None
    transitions = []
    equity_rows = []
    route = [current]
    conflicts = 0

    for pos, row in w.iterrows():
        ts = utc(row["timestamp"])

        if pending is not None:
            from_asset = current
            to_asset = pending["to_asset"]
            from_open = float(row[from_asset + "_open"])
            to_open = float(row[to_asset + "_open"])
            from_qty = qty
            gross = from_qty * from_open
            fee = gross * COST
            net = gross - fee
            qty = net / to_open
            current = to_asset
            route.append(current)

            transitions.append({
                "transition_no": len(transitions) + 1,
                "signal_date": pending["signal_date"].isoformat(),
                "execution_date": ts.isoformat(),
                "pair": pending["pair"],
                "from_asset": from_asset,
                "from_qty": from_qty,
                "from_open_usdt": from_open,
                "gross_usdt": gross,
                "fee_usdt": fee,
                "net_usdt": net,
                "to_asset": to_asset,
                "to_open_usdt": to_open,
                "to_qty": qty,
                "max_dislocation": float(pending["max_dislocation"]),
                "candidate_count": int(pending["candidate_count"]),
            })
            pending = None

        close_price = float(row[current + "_close"])
        equity_rows.append({
            "timestamp": ts.isoformat(),
            "asset": current,
            "qty": qty,
            "close": close_price,
            "equity_usdt": qty * close_price,
        })

        if pos == len(w) - 1:
            continue

        selected, candidate_count = choose(emap, ts, current)
        if selected is not None:
            if candidate_count > 1:
                conflicts += 1
            pending = {
                **selected,
                "signal_date": ts,
                "candidate_count": candidate_count,
            }

    eq = pd.DataFrame(equity_rows)
    tr = pd.DataFrame(transitions)
    values = eq["equity_usdt"].astype(float)
    running_peak = values.cummax()
    drawdown = values / running_peak - 1.0

    max_dd_idx = int(drawdown.idxmin())
    peak_idx = int(values.loc[:max_dd_idx].idxmax())
    min_idx = int(values.idxmin())

    min_equity = float(values.loc[min_idx])
    below = values < float(initial_usdt)
    below_initial_days = int(below.sum())

    first_recovery_date = None
    recovered_after_below = False
    if below.any():
        first_below_idx = int(below[below].index[0])
        recovered = values.loc[first_below_idx + 1 :]
        recovered = recovered[recovered >= float(initial_usdt)]
        if not recovered.empty:
            recovered_after_below = True
            recovery_idx = int(recovered.index[0])
            first_recovery_date = str(eq.loc[recovery_idx, "timestamp"])

    final_equity = float(values.iloc[-1])
    peak_equity = float(values.loc[peak_idx])
    trough_equity = float(values.loc[max_dd_idx])

    summary = {
        "scenario": name,
        "requested_start": start.isoformat(),
        "actual_start": str(eq.iloc[0]["timestamp"]),
        "end": str(eq.iloc[-1]["timestamp"]),
        "initial_usdt": float(initial_usdt),
        "initial_asset": START_ASSET,
        "initial_open_usdt": first_open,
        "initial_qty": float(initial_usdt) / first_open,
        "transitions": int(len(tr)),
        "conflicts": int(conflicts),
        "route": route,
        "final_asset": current,
        "final_qty": float(qty),
        "final_equity_usdt": final_equity,
        "total_return": final_equity / float(initial_usdt) - 1.0,
        "min_equity_usdt": min_equity,
        "min_equity_date": str(eq.loc[min_idx, "timestamp"]),
        "min_vs_initial_return": min_equity / float(initial_usdt) - 1.0,
        "below_initial_days": below_initial_days,
        "recovered_after_first_below": recovered_after_below,
        "first_recovery_date": first_recovery_date,
        "max_drawdown": float(drawdown.loc[max_dd_idx]),
        "max_drawdown_peak_equity_usdt": peak_equity,
        "max_drawdown_peak_date": str(eq.loc[peak_idx, "timestamp"]),
        "max_drawdown_trough_equity_usdt": trough_equity,
        "max_drawdown_trough_date": str(eq.loc[max_dd_idx, "timestamp"]),
        "max_drawdown_trough_vs_initial": trough_equity / float(initial_usdt) - 1.0,
    }
    return summary, tr, eq


def pct(x):
    return f"{100*x:+.2f}%"


def write_report(payload, run_dir):
    lines = [
        "# U8 Drawdown Entry Stress v1",
        "",
        "Mode: PATCH_FIX / research reporting only",
        "Live U8 logic: UNCHANGED",
        "",
        "Fresh 10,000 USDT portfolios are started at several dates around the strongest historical continuation-path drawdown.",
        "Market history before each start remains available for the frozen U8 signal state, but capital starts only on the scenario date.",
        "",
        "## Scenario summary",
        "",
        "| Scenario | Start | Final equity | Return | Minimum equity | Min vs initial | Days below 10k | Max DD from peak | DD trough vs initial | Transitions |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for s in payload["scenarios"]:
        lines.append(
            f"| {s['scenario']} | {pd.Timestamp(s['actual_start']).date()} | "
            f"{s['final_equity_usdt']:,.2f} USDT | {pct(s['total_return'])} | "
            f"{s['min_equity_usdt']:,.2f} USDT | {pct(s['min_vs_initial_return'])} | "
            f"{s['below_initial_days']} | {pct(s['max_drawdown'])} | "
            f"{pct(s['max_drawdown_trough_vs_initial'])} | {s['transitions']} |"
        )

    for s in payload["scenarios"]:
        lines += [
            "",
            f"## {s['scenario']}",
            "",
            f"- Start: {pd.Timestamp(s['actual_start']).date()}",
            f"- Initial: 10,000.00 USDT -> {s['initial_qty']:,.8f} ATOM at {s['initial_open_usdt']:.8f} USDT",
            f"- Final: {s['final_equity_usdt']:,.2f} USDT ({pct(s['total_return'])})",
            f"- Lowest equity: {s['min_equity_usdt']:,.2f} USDT on {pd.Timestamp(s['min_equity_date']).date()} = {pct(s['min_vs_initial_return'])} vs initial",
            f"- Daily closes below initial: {s['below_initial_days']}",
            f"- Recovered after first below-initial close: {s['recovered_after_first_below']}",
            f"- First recovery date: {pd.Timestamp(s['first_recovery_date']).date() if s['first_recovery_date'] else 'N/A'}",
            f"- Max drawdown from prior peak: {pct(s['max_drawdown'])}",
            f"- Drawdown peak: {s['max_drawdown_peak_equity_usdt']:,.2f} USDT on {pd.Timestamp(s['max_drawdown_peak_date']).date()}",
            f"- Drawdown trough: {s['max_drawdown_trough_equity_usdt']:,.2f} USDT on {pd.Timestamp(s['max_drawdown_trough_date']).date()}",
            f"- Drawdown trough vs initial: {pct(s['max_drawdown_trough_vs_initial'])}",
            f"- Route: {' -> '.join(s['route'])}",
        ]

    lines += [
        "",
        "## Interpretation boundary",
        "",
        "- MAX DRAWDOWN FROM PRIOR PEAK and MINIMUM EQUITY VS INITIAL CAPITAL are separate metrics.",
        "- Each scenario is a fresh ATOM-start portfolio, not continuation equity from another scenario.",
        "- Historical warm-up/state remains available before the chosen capital start date.",
        "- Transition cost is 0.1% on each executed rotation.",
        "- Slippage beyond that frozen cost model is not separately modeled.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    parser.add_argument("--initial-usdt", type=float, default=10000.0)
    args = parser.parse_args()

    panel, meta = download_panel(utc(args.cutoff))
    emap = build_event_map(panel)
    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    scenarios = []
    all_transitions = []

    for name, start in SCENARIOS:
        summary, transitions, equity = run_scenario(
            panel, emap, name, start, args.initial_usdt
        )
        scenarios.append(summary)
        safe = name.lower()
        transitions.to_csv(run_dir / f"{safe}_transitions.csv", index=False)
        equity.to_csv(run_dir / f"{safe}_equity.csv", index=False)
        if not transitions.empty:
            t = transitions.copy()
            t.insert(0, "scenario", name)
            all_transitions.append(t)

    summary_df = pd.DataFrame([
        {
            "scenario": s["scenario"],
            "start": s["actual_start"],
            "end": s["end"],
            "final_equity_usdt": s["final_equity_usdt"],
            "total_return": s["total_return"],
            "min_equity_usdt": s["min_equity_usdt"],
            "min_equity_date": s["min_equity_date"],
            "min_vs_initial_return": s["min_vs_initial_return"],
            "below_initial_days": s["below_initial_days"],
            "max_drawdown": s["max_drawdown"],
            "max_drawdown_peak_equity_usdt": s["max_drawdown_peak_equity_usdt"],
            "max_drawdown_peak_date": s["max_drawdown_peak_date"],
            "max_drawdown_trough_equity_usdt": s["max_drawdown_trough_equity_usdt"],
            "max_drawdown_trough_date": s["max_drawdown_trough_date"],
            "max_drawdown_trough_vs_initial": s["max_drawdown_trough_vs_initial"],
            "transitions": s["transitions"],
        }
        for s in scenarios
    ])
    summary_df.to_csv(run_dir / "scenario_summary.csv", index=False)

    if all_transitions:
        pd.concat(all_transitions, ignore_index=True).to_csv(
            run_dir / "all_transitions.csv", index=False
        )

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data_metadata": meta,
        "parameters": {
            "assets": list(ASSETS),
            "lookback": LOOKBACK,
            "arm": ARM,
            "reversal": REVERSAL,
            "transition_cost": COST,
            "execution": "next_open",
            "router": "strongest_confirmed_max_dislocation",
            "initial_usdt": float(args.initial_usdt),
        },
        "scenarios": scenarios,
    }
    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    for s in scenarios:
        print(
            f"{s['scenario']}: start={s['actual_start']} "
            f"final={s['final_equity_usdt']:.2f} "
            f"min={s['min_equity_usdt']:.2f} "
            f"min_vs_initial={s['min_vs_initial_return']:.6f} "
            f"max_dd={s['max_drawdown']:.6f} "
            f"dd_trough_vs_initial={s['max_drawdown_trough_vs_initial']:.6f} "
            f"transitions={s['transitions']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
