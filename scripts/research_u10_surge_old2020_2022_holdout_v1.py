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
from scripts.research_u10_monthly_surge_pullback_v1 import (
    INITIAL_USDT,
    analyze_values,
    event_study,
    monthly_state,
    run_reference,
    utc,
)
from scripts.research_u10_monthly_surge_trailing_reentry_peak_v1 import (
    run_overlay_trailing_peak,
)
from scripts.research_u10_last_year_vs_u8_universes_v1 import (
    ARM,
    COST,
    LOOKBACK,
    REVERSAL,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u10_surge_old2020_2022_holdout_v1"

OLD10 = (
    "ATOM", "BTC", "ETH", "BNB", "XRP",
    "TRX", "ADA", "LINK", "XLM", "LTC",
)

DOWNLOAD_START = pd.Timestamp("2018-01-01", tz="UTC")
EVAL_START = pd.Timestamp("2020-01-01", tz="UTC")
EVAL_END = pd.Timestamp("2022-12-31", tz="UTC")

P1 = {"name": "P1_TRAILING", "surge": 1.00, "pullback": 0.05, "reentry": 0.25}
P2 = {"name": "P2_TRAILING", "surge": 1.00, "pullback": 0.10, "reentry": 0.25}
CASH_FRACTION = 0.30


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def download_old_panel(cutoff: pd.Timestamp):
    client = BinanceSpotRestClient()
    frames = {}
    meta = {}

    for asset in OLD10:
        result = download_historical_dataset(
            client,
            symbol=asset + "USDT",
            start=DOWNLOAD_START,
            end=cutoff,
            timeframe="1D",
            as_of=cutoff,
        )
        if result.dataset is None:
            raise RuntimeError(f"{asset}: {result.metadata.status}")
        if result.dataset.quality.has_critical_issues:
            raise RuntimeError(f"{asset}: critical data quality issues")

        f = result.dataset.candles[
            ["timestamp", "open", "close"]
        ].copy()
        f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
        f = f.sort_values("timestamp", kind="stable").reset_index(drop=True)

        if f.empty:
            raise RuntimeError(f"{asset}: empty historical dataset")

        last_ts = utc(f.iloc[-1]["timestamp"])
        if last_ts < pd.Timestamp("2026-09-26", tz="UTC"):
            raise RuntimeError(
                f"{asset}: survival/current-data gate failed; last candle {last_ts}"
            )

        meta[asset] = {
            "rows": int(len(f)),
            "start": utc(f.iloc[0]["timestamp"]).isoformat(),
            "end": last_ts.isoformat(),
            "listing_truncated": bool(result.metadata.listing_truncated),
        }

        f = f.rename(
            columns={
                "open": asset + "_open",
                "close": asset + "_close",
            }
        )
        frames[asset] = f

    panel = None
    for asset in OLD10:
        f = frames[asset]
        panel = (
            f
            if panel is None
            else panel.merge(
                f, on="timestamp", how="inner", validate="one_to_one"
            )
        )

    panel = panel.sort_values("timestamp", kind="stable").reset_index(drop=True)

    prehistory_rows = int((panel["timestamp"] < EVAL_START).sum())
    if prehistory_rows < LOOKBACK:
        raise RuntimeError(
            f"OLD10 common panel has only {prehistory_rows} rows before "
            f"{EVAL_START.date()}, need >= {LOOKBACK}"
        )

    eval_panel = panel[panel["timestamp"] <= EVAL_END].copy().reset_index(drop=True)
    if eval_panel.empty:
        raise RuntimeError("empty OLD10 panel through evaluation end")

    if utc(eval_panel.iloc[-1]["timestamp"]) != EVAL_END:
        raise RuntimeError(
            f"expected evaluation end {EVAL_END}, got "
            f"{utc(eval_panel.iloc[-1]['timestamp'])}"
        )

    return eval_panel, meta, prehistory_rows


def build_old_events(panel: pd.DataFrame):
    cols = ["timestamp"] + [asset + "_close" for asset in OLD10]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=OLD10,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    for event in events:
        if event["event"] == "CONFIRMED":
            by_date[utc(event["date"])].append(event)
    return by_date


def summarize_event_study(events: pd.DataFrame) -> dict:
    if events.empty:
        return {
            "event_count": 0,
            "hit_rate_31d": None,
            "hit_rate_62d": None,
            "median_dd_31d": None,
            "median_dd_62d": None,
        }
    return {
        "event_count": int(len(events)),
        "hit_rate_31d": float(events["hit_minus25_31d"].mean()),
        "hit_rate_62d": float(events["hit_minus25_62d"].mean()),
        "median_dd_31d": float(events["worst_running_dd_31d"].median()),
        "median_dd_62d": float(events["worst_running_dd_62d"].median()),
    }


def overlay_result(reference, anchors, baseline, config):
    result, cycles = run_overlay_trailing_peak(
        reference,
        anchors,
        surge_threshold=config["surge"],
        pullback_threshold=config["pullback"],
        reentry_threshold=config["reentry"],
        cash_fraction=CASH_FRACTION,
        initial_usdt=INITIAL_USDT,
    )
    result["name"] = config["name"]
    result["delta_vs_baseline_usdt"] = (
        result["final_equity_usdt"] - baseline["final_equity_usdt"]
    )
    result["delta_vs_baseline_pct"] = (
        result["final_equity_usdt"] / baseline["final_equity_usdt"] - 1.0
    )
    result["max_dd_improvement_pp"] = (
        result["max_drawdown"] - baseline["max_drawdown"]
    ) * 100.0
    return result, cycles


def pct(value):
    if value is None:
        return "n/a"
    return f"{100*value:+.2f}%"


def write_report(payload: dict, run_dir: Path) -> None:
    baseline = payload["baseline"]
    event = payload["event_study"]
    p1 = payload["P1_TRAILING"]
    p2 = payload["P2_TRAILING"]

    lines = [
        "# OLD10 2020-2022 Temporal Holdout — Monthly Surge Pullback",
        "",
        "Mode: STRESS_TEST_ONLY",
        "Purpose: validation-only temporal holdout",
        "Live/paper logic: UNCHANGED",
        "",
        "## Fixed OLD10 universe",
        "",
        ", ".join(OLD10),
        "",
        f"Evaluation: {EVAL_START.date()} -> {EVAL_END.date()}",
        f"Common prehistory rows before 2020-01-01: {payload['prehistory_rows']}",
        "",
        "## Direct +100% surge event study",
        "",
        f"Event count: {event['event_count']}",
        f"-25% hit within 31d: {pct(event['hit_rate_31d'])}",
        f"-25% hit within 62d: {pct(event['hit_rate_62d'])}",
        f"Median worst 31d drawdown: {pct(event['median_dd_31d'])}",
        f"Median worst 62d drawdown: {pct(event['median_dd_62d'])}",
        "",
        "## Portfolio results",
        "",
        "| Scenario | Final equity | Return | Delta vs baseline | Max DD | DD change | Cash-outs | Re-entries | Unfinished | Cash days |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| Baseline OLD10 | {baseline['final_equity_usdt']:,.2f} | "
        f"{pct(baseline['total_return'])} | — | {pct(baseline['max_drawdown'])} | — | — | — | — | — |",
    ]

    for row in (p1, p2):
        lines.append(
            f"| {row['name']} | {row['final_equity_usdt']:,.2f} | "
            f"{pct(row['total_return'])} | {pct(row['delta_vs_baseline_pct'])} | "
            f"{pct(row['max_drawdown'])} | {row['max_dd_improvement_pp']:+.2f} pp | "
            f"{row['cashouts']} | {row['reentries']} | "
            f"{row['unfinished_cycles']} | {row['days_in_cash']} |"
        )

    lines += [
        "",
        "## Validation boundary",
        "",
        "- No parameter was selected from 2020-2022 results.",
        "- OLD10 differs from current U10 because several modern assets did not exist in 2020.",
        "- The universe has survivorship bias by explicit design: only assets that existed then and survive today were selected.",
        "- This test is evidence about temporal portability of the overlay mechanics, not an unbiased reconstruction of all investable 2020 crypto.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_TEMPORAL_HOLDOUT",
    ]
    (run_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    cutoff = utc(args.cutoff)
    panel, metadata, prehistory_rows = download_old_panel(cutoff)
    event_map = build_old_events(panel)

    baseline, reference = run_reference(
        panel,
        event_map,
        OLD10,
        EVAL_START,
        INITIAL_USDT,
    )

    # run_reference continues to the end of panel, which is frozen at 2022-12-31.
    if utc(reference.iloc[0]["timestamp"]) != EVAL_START:
        raise RuntimeError(
            f"reference starts at {reference.iloc[0]['timestamp']} not {EVAL_START}"
        )
    if utc(reference.iloc[-1]["timestamp"]) != EVAL_END:
        raise RuntimeError(
            f"reference ends at {reference.iloc[-1]['timestamp']} not {EVAL_END}"
        )

    monthly, anchors = monthly_state(reference, INITIAL_USDT)
    events = event_study(reference, monthly, surge_threshold=1.0)
    event_summary = summarize_event_study(events)

    p1, p1_cycles = overlay_result(reference, anchors, baseline, P1)
    p2, p2_cycles = overlay_result(reference, anchors, baseline, P2)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    reference.to_csv(run_dir / "old10_reference_equity.csv", index=False)
    monthly.to_csv(run_dir / "old10_monthly_selected_extremes.csv", index=False)
    if not events.empty:
        events.to_csv(run_dir / "old10_surge_events.csv", index=False)
    if not p1_cycles.empty:
        p1_cycles.to_csv(run_dir / "p1_trailing_cycles.csv", index=False)
    if not p2_cycles.empty:
        p2_cycles.to_csv(run_dir / "p2_trailing_cycles.csv", index=False)

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "old10": list(OLD10),
        "evaluation_start": EVAL_START.isoformat(),
        "evaluation_end": EVAL_END.isoformat(),
        "prehistory_rows": prehistory_rows,
        "data_metadata": metadata,
        "baseline": baseline,
        "event_study": event_summary,
        "event_rows": events.to_dict(orient="records"),
        "P1_TRAILING": p1,
        "P2_TRAILING": p2,
    }

    (run_dir / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    write_report(payload, run_dir)

    print("run_dir=" + str(run_dir))
    print("prehistory_rows=%d" % prehistory_rows)
    print(
        "baseline final=%.8f return=%.8f dd=%.8f transitions=%d"
        % (
            baseline["final_equity_usdt"],
            baseline["total_return"],
            baseline["max_drawdown"],
            baseline["transitions"],
        )
    )
    print(
        "events count=%d hit31=%s hit62=%s"
        % (
            event_summary["event_count"],
            str(event_summary["hit_rate_31d"]),
            str(event_summary["hit_rate_62d"]),
        )
    )
    for row in (p1, p2):
        print(
            "%s final=%.8f delta=%.8f dd=%.8f ddpp=%.8f cashouts=%d "
            "reentries=%d unfinished=%d cashdays=%d"
            % (
                row["name"],
                row["final_equity_usdt"],
                row["delta_vs_baseline_pct"],
                row["max_drawdown"],
                row["max_dd_improvement_pp"],
                row["cashouts"],
                row["reentries"],
                row["unfinished_cycles"],
                row["days_in_cash"],
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
