from __future__ import annotations

import argparse
import bisect
import itertools
import json
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "robust_exhaustive_u9_u10_atom_replacement_v1"

POOL = (
    "ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK",
    "AVAX", "FIL", "ETH", "ALGO", "ADA", "XRP", "HBAR",
)

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
EVAL_END = pd.Timestamp("2026-09-26", tz="UTC")
WINDOWS = {
    "last_1y": (pd.Timestamp("2025-09-27", tz="UTC"), EVAL_END),
    "last_2y": (pd.Timestamp("2024-09-27", tz="UTC"), EVAL_END),
    "mature": (pd.Timestamp("2023-10-31", tz="UTC"), EVAL_END),
}
ENDPOINT_OFFSETS = (0, 30, 60, 90, 120, 180)
FINALIST_N = 30

PREVIOUS_BASE_U9 = (
    "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK", "FIL", "HBAR",
)
PR54_TARGET_U9 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE", "AVAX", "FIL", "ALGO", "XRP",
)
OLD_U10_WITH_ATOM = (
    "ATOM", "TWT", "PEPE", "BNB", "SOL", "TRX", "AAVE", "LINK", "FIL", "HBAR",
)


def utc(value):
    t = pd.Timestamp(value)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def sorted_key(assets):
    return "|".join(sorted(assets))


def download_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    metadata = {}
    for asset in POOL:
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
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.rename(
            columns={"open": asset + "_open", "close": asset + "_close"}
        )
        metadata[asset] = {
            "rows": int(len(frame)),
            "start": pd.Timestamp(frame.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(frame.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = (
            frame
            if panel is None
            else panel.merge(frame, on="timestamp", how="inner", validate="one_to_one")
        )
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, metadata


def build_signal_index(panel):
    cols = ["timestamp"] + [asset + "_close" for asset in POOL]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=POOL,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )

    ts_to_index = {
        utc(ts): idx for idx, ts in enumerate(pd.to_datetime(panel["timestamp"], utc=True))
    }
    grouped = defaultdict(list)
    for event in events:
        if event["event"] != "CONFIRMED":
            continue
        ts = utc(event["date"])
        idx = ts_to_index.get(ts)
        if idx is None:
            continue
        grouped[(event["from_asset"], idx)].append(
            {
                "to_asset": event["to_asset"],
                "pair": event["pair"],
                "max_dislocation": float(event["max_dislocation"]),
            }
        )

    positions = {asset: [] for asset in POOL}
    candidates = {asset: [] for asset in POOL}
    for (from_asset, idx), items in grouped.items():
        items.sort(
            key=lambda e: (-e["max_dislocation"], e["to_asset"], e["pair"])
        )
        positions[from_asset].append(idx)
        candidates[from_asset].append(items)

    for asset in POOL:
        order = np.argsort(positions[asset]).tolist() if positions[asset] else []
        positions[asset] = [positions[asset][i] for i in order]
        candidates[asset] = [candidates[asset][i] for i in order]

    return positions, candidates


def prepare_arrays(panel):
    timestamps = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    opens = {
        asset: panel[asset + "_open"].astype(float).to_numpy()
        for asset in POOL
    }
    closes = {
        asset: panel[asset + "_close"].astype(float).to_numpy()
        for asset in POOL
    }
    return timestamps, opens, closes


def bounds(timestamps, start, end):
    start_i = int(timestamps.searchsorted(utc(start), side="left"))
    end_i = int(timestamps.searchsorted(utc(end), side="right")) - 1
    if start_i < 0 or end_i < start_i or end_i >= len(timestamps):
        raise RuntimeError(f"invalid/empty bounds {start} -> {end}")
    return start_i, end_i


def segment_dd(values, prior_peak):
    if len(values) == 0:
        return prior_peak, 0.0
    running = np.maximum.accumulate(values)
    if prior_peak is not None:
        running = np.maximum(running, prior_peak)
    dd = float(np.min(values / running - 1.0))
    peak = float(max(float(running[-1]), prior_peak or float("-inf")))
    return peak, dd


def simulate_one(
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    start_i,
    end_i,
    start_asset,
    collect_equity=False,
):
    aset = set(assets)
    current = start_asset
    qty = 1.0 / float(opens[current][start_i])
    initial_equity = qty * float(closes[current][start_i])

    global_peak = initial_equity
    min_dd = 0.0
    transitions = 0
    conflicts = 0
    segment_start = start_i
    search_from = start_i

    equity = (
        np.empty(end_i - start_i + 1, dtype=float) if collect_equity else None
    )

    while segment_start <= end_i:
        pos_list = signal_positions[current]
        cand_list = signal_candidates[current]
        p = bisect.bisect_left(pos_list, search_from)
        transition_found = False

        while p < len(pos_list):
            signal_i = pos_list[p]
            if signal_i >= end_i:
                break

            eligible = [
                event for event in cand_list[p]
                if event["to_asset"] in aset
            ]
            if not eligible:
                p += 1
                continue

            chosen = eligible[0]
            if len(eligible) > 1:
                conflicts += 1

            segment_values = qty * closes[current][segment_start:signal_i + 1]
            global_peak, seg_dd = segment_dd(segment_values, global_peak)
            min_dd = min(min_dd, seg_dd)
            if collect_equity:
                lo = segment_start - start_i
                hi = signal_i - start_i + 1
                equity[lo:hi] = segment_values

            execute_i = signal_i + 1
            value_before = qty * float(opens[current][execute_i])
            next_asset = chosen["to_asset"]
            qty = value_before * (1.0 - COST) / float(opens[next_asset][execute_i])
            current = next_asset
            transitions += 1
            segment_start = execute_i
            search_from = execute_i
            transition_found = True
            break

        if transition_found:
            continue

        segment_values = qty * closes[current][segment_start:end_i + 1]
        global_peak, seg_dd = segment_dd(segment_values, global_peak)
        min_dd = min(min_dd, seg_dd)
        if collect_equity:
            lo = segment_start - start_i
            equity[lo:] = segment_values
        break

    final_equity = qty * float(closes[current][end_i])
    result = {
        "return": float(final_equity / initial_equity - 1.0),
        "max_dd": float(min_dd),
        "transitions": int(transitions),
        "conflicts": int(conflicts),
    }
    if collect_equity:
        result["equity"] = equity
    return result


def evaluate_universe(
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    start_i,
    end_i,
    collect_equity=False,
):
    runs = []
    equities = []
    for start_asset in assets:
        result = simulate_one(
            opens,
            closes,
            signal_positions,
            signal_candidates,
            assets,
            start_i,
            end_i,
            start_asset,
            collect_equity=collect_equity,
        )
        runs.append(result)
        if collect_equity:
            equities.append(result["equity"])

    returns = np.asarray([r["return"] for r in runs], dtype=float)
    dds = np.asarray([r["max_dd"] for r in runs], dtype=float)
    transitions = np.asarray([r["transitions"] for r in runs], dtype=float)
    conflicts = np.asarray([r["conflicts"] for r in runs], dtype=float)
    out = {
        "median_return": float(np.median(returns)),
        "worst_return": float(np.min(returns)),
        "best_return": float(np.max(returns)),
        "positive_starts": int(np.sum(returns > 0)),
        "start_count": int(len(returns)),
        "median_max_dd": float(np.median(dds)),
        "worst_max_dd": float(np.min(dds)),
        "median_transitions": float(np.median(transitions)),
        "median_conflicts": float(np.median(conflicts)),
    }
    if collect_equity:
        out["_equities"] = equities
    return out


def enumerate_primary(
    size,
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
):
    window_bounds = {
        name: bounds(timestamps, start, end)
        for name, (start, end) in WINDOWS.items()
    }
    rows = []
    total = 0
    for assets in itertools.combinations(POOL, size):
        total += 1
        row = {
            "key": sorted_key(assets),
            "assets": "|".join(assets),
            "size": size,
        }
        for name, (start_i, end_i) in window_bounds.items():
            metrics = evaluate_universe(
                opens,
                closes,
                signal_positions,
                signal_candidates,
                assets,
                start_i,
                end_i,
            )
            for metric, value in metrics.items():
                row[f"{name}_{metric}"] = value
        rows.append(row)
    return pd.DataFrame(rows), total


def add_robust_ranking(df):
    df = df.copy()
    return_pct_cols = []
    worst_pct_cols = []
    for name in WINDOWS:
        ret_col = f"{name}_median_return"
        worst_col = f"{name}_worst_return"
        ret_pct = f"{name}_return_pct"
        worst_pct = f"{name}_worst_return_pct"
        df[ret_pct] = df[ret_col].rank(
            method="average", pct=True, ascending=True
        )
        df[worst_pct] = df[worst_col].rank(
            method="average", pct=True, ascending=True
        )
        return_pct_cols.append(ret_pct)
        worst_pct_cols.append(worst_pct)

    df["primary_robust_score"] = df[return_pct_cols].min(axis=1)
    df["secondary_robust_score"] = df[return_pct_cols].median(axis=1)
    df["worst_start_score"] = df[worst_pct_cols].min(axis=1)

    df = df.sort_values(
        [
            "primary_robust_score",
            "secondary_robust_score",
            "worst_start_score",
            "mature_median_max_dd",
            "key",
        ],
        ascending=[False, False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    df["robust_rank"] = np.arange(1, len(df) + 1)
    return df


def token_frequency(df, n):
    counter = Counter()
    rows = df.head(n)
    for assets in rows["assets"]:
        counter.update(assets.split("|"))
    return dict(sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])))


def top_core(df, n):
    sets = [set(x.split("|")) for x in df.head(n)["assets"]]
    if not sets:
        return []
    return sorted(set.intersection(*sets))


def row_rank(df, assets):
    key = sorted_key(assets)
    hit = df[df["key"] == key]
    if hit.empty:
        return None
    return int(hit.iloc[0]["robust_rank"])


def build_marginal(u9, u10):
    u9_map = {row["key"]: row for _, row in u9.iterrows()}
    details = []

    for _, row10 in u10.iterrows():
        assets10 = tuple(row10["assets"].split("|"))
        for token in assets10:
            reduced = tuple(asset for asset in assets10 if asset != token)
            row9 = u9_map.get(sorted_key(reduced))
            if row9 is None:
                raise RuntimeError(f"matched U9 missing for {row10['key']} minus {token}")
            item = {
                "token": token,
                "u10_key": row10["key"],
                "matched_u9_key": row9["key"],
            }
            for name in WINDOWS:
                item[f"{name}_return_delta"] = (
                    float(row10[f"{name}_median_return"])
                    - float(row9[f"{name}_median_return"])
                )
                item[f"{name}_dd_delta"] = (
                    float(row10[f"{name}_median_max_dd"])
                    - float(row9[f"{name}_median_max_dd"])
                )
            details.append(item)

    detail_df = pd.DataFrame(details)
    summary = []
    for token in POOL:
        sub = detail_df[detail_df["token"] == token]
        row = {"token": token, "context_count": int(len(sub))}
        for name in WINDOWS:
            ret = sub[f"{name}_return_delta"]
            dd = sub[f"{name}_dd_delta"]
            row.update(
                {
                    f"{name}_median_return_delta": float(ret.median()),
                    f"{name}_positive_delta_rate": float((ret > 0).mean()),
                    f"{name}_q25_return_delta": float(ret.quantile(0.25)),
                    f"{name}_q75_return_delta": float(ret.quantile(0.75)),
                    f"{name}_worst_return_delta": float(ret.min()),
                    f"{name}_best_return_delta": float(ret.max()),
                    f"{name}_median_dd_delta": float(dd.median()),
                }
            )
        summary.append(row)
    summary_df = pd.DataFrame(summary)
    summary_df = summary_df.sort_values(
        [
            "mature_median_return_delta",
            "last_2y_median_return_delta",
            "last_1y_median_return_delta",
            "token",
        ],
        ascending=[False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    return detail_df, summary_df


def rolling_windows(months):
    first = pd.Timestamp("2023-11-01", tz="UTC")
    rows = []
    for start in pd.date_range(first, EVAL_END, freq="MS", tz="UTC"):
        end = utc(start + pd.DateOffset(months=months) - pd.Timedelta(days=1))
        if end <= EVAL_END:
            rows.append((utc(start), end))
    return rows


def rolling_summary(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    months,
):
    rows = []
    for start, end in rolling_windows(months):
        start_i, end_i = bounds(timestamps, start, end)
        metrics = evaluate_universe(
            opens,
            closes,
            signal_positions,
            signal_candidates,
            assets,
            start_i,
            end_i,
        )
        rows.append(metrics)
    df = pd.DataFrame(rows)
    return {
        "window_count": int(len(df)),
        "median_window_return": float(df["median_return"].median()),
        "worst_window_return": float(df["median_return"].min()),
        "positive_window_rate": float((df["median_return"] > 0).mean()),
        "median_max_dd": float(df["median_max_dd"].median()),
        "worst_max_dd": float(df["median_max_dd"].min()),
    }


def endpoint_summary(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
):
    rows = []
    for offset in ENDPOINT_OFFSETS:
        end = EVAL_END - pd.Timedelta(days=offset)
        start = utc(end - pd.DateOffset(years=1) + pd.Timedelta(days=1))
        start_i, end_i = bounds(timestamps, start, end)
        metrics = evaluate_universe(
            opens,
            closes,
            signal_positions,
            signal_candidates,
            assets,
            start_i,
            end_i,
        )
        rows.append(metrics)
    df = pd.DataFrame(rows)
    return {
        "endpoint_count": int(len(df)),
        "median_endpoint_return": float(df["median_return"].median()),
        "worst_endpoint_return": float(df["median_return"].min()),
        "positive_endpoint_rate": float((df["median_return"] > 0).mean()),
        "median_endpoint_dd": float(df["median_max_dd"].median()),
        "worst_endpoint_dd": float(df["median_max_dd"].min()),
    }


def broad_bull_mask(closes, length):
    frame = pd.DataFrame(
        {asset: pd.Series(closes[asset], dtype=float) for asset in POOL}
    )
    ret30 = frame / frame.shift(30) - 1.0
    broad = ret30.median(axis=1, skipna=True)
    mask = (broad >= 0.25).to_numpy(dtype=bool)
    if len(mask) != length:
        raise RuntimeError("bull mask length mismatch")
    return mask


def bull_neutral_summary(
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
    assets,
    bull_mask,
):
    start_i, end_i = bounds(
        timestamps, WINDOWS["mature"][0], WINDOWS["mature"][1]
    )
    result = evaluate_universe(
        opens,
        closes,
        signal_positions,
        signal_candidates,
        assets,
        start_i,
        end_i,
        collect_equity=True,
    )
    neutral_returns = []
    for equity in result["_equities"]:
        daily = np.zeros(len(equity), dtype=float)
        if len(equity) > 1:
            daily[1:] = equity[1:] / equity[:-1] - 1.0
        local_bull = bull_mask[start_i:end_i + 1]
        adjusted = daily.copy()
        adjusted[(local_bull) & (adjusted > 0)] = 0.0
        neutral_returns.append(float(np.prod(1.0 + adjusted) - 1.0))
    arr = np.asarray(neutral_returns, dtype=float)
    return {
        "bull_neutral_median_return": float(np.median(arr)),
        "bull_neutral_worst_return": float(np.min(arr)),
        "bull_neutral_best_return": float(np.max(arr)),
        "bull_neutral_positive_start_rate": float(np.mean(arr > 0)),
    }


def finalist_diagnostics(
    ranked_u9,
    ranked_u10,
    timestamps,
    opens,
    closes,
    signal_positions,
    signal_candidates,
):
    bull_mask = broad_bull_mask(closes, len(timestamps))
    finalists = []
    seen = set()
    for size, df in ((9, ranked_u9), (10, ranked_u10)):
        for _, row in df.head(FINALIST_N).iterrows():
            key = (size, row["key"])
            if key in seen:
                continue
            seen.add(key)
            finalists.append(
                {
                    "size": size,
                    "robust_rank": int(row["robust_rank"]),
                    "key": row["key"],
                    "assets": tuple(row["assets"].split("|")),
                    "primary_robust_score": float(row["primary_robust_score"]),
                    "secondary_robust_score": float(row["secondary_robust_score"]),
                }
            )

    output = []
    for item in finalists:
        assets = item["assets"]
        row = {
            "size": item["size"],
            "robust_rank": item["robust_rank"],
            "key": item["key"],
            "assets": "|".join(assets),
            "primary_robust_score": item["primary_robust_score"],
            "secondary_robust_score": item["secondary_robust_score"],
        }
        for months in (12, 24):
            summary = rolling_summary(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                assets,
                months,
            )
            for metric, value in summary.items():
                row[f"rolling_{months}m_{metric}"] = value
        endpoint = endpoint_summary(
            timestamps,
            opens,
            closes,
            signal_positions,
            signal_candidates,
            assets,
        )
        row.update(endpoint)
        row.update(
            bull_neutral_summary(
                timestamps,
                opens,
                closes,
                signal_positions,
                signal_candidates,
                assets,
                bull_mask,
            )
        )
        output.append(row)

    return pd.DataFrame(output)


def compact_top(df, n=10):
    cols = [
        "robust_rank",
        "assets",
        "primary_robust_score",
        "secondary_robust_score",
        "last_1y_median_return",
        "last_2y_median_return",
        "mature_median_return",
        "last_1y_median_max_dd",
        "mature_median_max_dd",
    ]
    return json.loads(df.head(n)[cols].to_json(orient="records"))


def pct(value):
    return f"{100.0 * value:+.1f}%"


def report_table_top(df, n=10):
    lines = [
        "|rank|assets|robust floor|1Y|2Y|mature|mature DD|",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for _, row in df.head(n).iterrows():
        lines.append(
            f"|{int(row['robust_rank'])}|{row['assets']}|"
            f"{row['primary_robust_score']:.3f}|"
            f"{pct(row['last_1y_median_return'])}|"
            f"{pct(row['last_2y_median_return'])}|"
            f"{pct(row['mature_median_return'])}|"
            f"{pct(row['mature_median_max_dd'])}|"
        )
    return "\n".join(lines)


def report_table_marginal(df):
    lines = [
        "|token|contexts|1Y median delta|1Y +rate|2Y median delta|2Y +rate|mature median delta|mature +rate|",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in df.iterrows():
        lines.append(
            f"|{row['token']}|{int(row['context_count'])}|"
            f"{pct(row['last_1y_median_return_delta'])}|"
            f"{100*row['last_1y_positive_delta_rate']:.1f}%|"
            f"{pct(row['last_2y_median_return_delta'])}|"
            f"{100*row['last_2y_positive_delta_rate']:.1f}%|"
            f"{pct(row['mature_median_return_delta'])}|"
            f"{100*row['mature_positive_delta_rate']:.1f}%|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = parser.parse_args()

    panel, data_meta = download_panel(utc(args.cutoff))
    timestamps, opens, closes = prepare_arrays(panel)
    signal_positions, signal_candidates = build_signal_index(panel)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_u9, count9 = enumerate_primary(
        9, timestamps, opens, closes, signal_positions, signal_candidates
    )
    raw_u10, count10 = enumerate_primary(
        10, timestamps, opens, closes, signal_positions, signal_candidates
    )

    ranked_u9 = add_robust_ranking(raw_u9)
    ranked_u10 = add_robust_ranking(raw_u10)

    ranked_u9.to_csv(run_dir / "all_u9_primary_windows.csv", index=False)
    ranked_u10.to_csv(run_dir / "all_u10_primary_windows.csv", index=False)
    ranked_u9.head(200).to_csv(run_dir / "top_u9_robust.csv", index=False)
    ranked_u10.head(200).to_csv(run_dir / "top_u10_robust.csv", index=False)

    marginal_detail, marginal_summary = build_marginal(ranked_u9, ranked_u10)
    marginal_detail.to_csv(
        run_dir / "matched_u10_minus_u9_detail.csv", index=False
    )
    marginal_summary.to_csv(
        run_dir / "token_marginal_contribution.csv", index=False
    )

    finalist_df = finalist_diagnostics(
        ranked_u9,
        ranked_u10,
        timestamps,
        opens,
        closes,
        signal_positions,
        signal_candidates,
    )
    finalist_df.to_csv(run_dir / "finalists_rolling_endpoint.csv", index=False)

    summary = {
        "experiment": "ROBUST_EXHAUSTIVE_U9_U10_ATOM_REPLACEMENT_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": utc(args.cutoff).isoformat(),
        "pool": list(POOL),
        "parameters": {
            "lookback": LOOKBACK,
            "arm_threshold": ARM,
            "reversal": REVERSAL,
            "transition_cost": COST,
            "execution": "confirmed signal T -> next day open",
            "ranking": "maximise minimum cross-window median-return percentile",
        },
        "windows": {
            name: {"start": start.isoformat(), "end": end.isoformat()}
            for name, (start, end) in WINDOWS.items()
        },
        "enumeration": {"u9": count9, "u10": count10, "total": count9 + count10},
        "data_metadata": data_meta,
        "common_panel": {
            "rows": int(len(panel)),
            "start": timestamps[0].isoformat(),
            "end": timestamps[-1].isoformat(),
        },
        "u9": {
            "top10": compact_top(ranked_u9),
            "top10_frequency": token_frequency(ranked_u9, 10),
            "top50_frequency": token_frequency(ranked_u9, 50),
            "top100_frequency": token_frequency(ranked_u9, 100),
            "top10_core": top_core(ranked_u9, 10),
            "previous_base_u9_rank": row_rank(ranked_u9, PREVIOUS_BASE_U9),
            "pr54_target_u9_rank": row_rank(ranked_u9, PR54_TARGET_U9),
        },
        "u10": {
            "top10": compact_top(ranked_u10),
            "top10_frequency": token_frequency(ranked_u10, 10),
            "top50_frequency": token_frequency(ranked_u10, 50),
            "top100_frequency": token_frequency(ranked_u10, 100),
            "top10_core": top_core(ranked_u10, 10),
            "old_u10_with_atom_rank": row_rank(ranked_u10, OLD_U10_WITH_ATOM),
        },
        "marginal_contribution": json.loads(
            marginal_summary.to_json(orient="records")
        ),
        "finalist_count": int(len(finalist_df)),
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes": "NONE",
    }

    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )

    report = []
    report.append("# ROBUST EXHAUSTIVE U9/U10 ATOM REPLACEMENT V1")
    report.append("")
    report.append("Mode: STRESS_TEST_ONLY")
    report.append("")
    report.append(
        f"Enumerated U9={count9:,}, U10={count10:,}, total={count9+count10:,}."
    )
    report.append("")
    report.append("## U9 robust top 10")
    report.append("")
    report.append(report_table_top(ranked_u9))
    report.append("")
    report.append("## U10 robust top 10")
    report.append("")
    report.append(report_table_top(ranked_u10))
    report.append("")
    report.append("## Token matched marginal contribution")
    report.append("")
    report.append(report_table_marginal(marginal_summary))
    report.append("")
    report.append("## Reference composition ranks")
    report.append("")
    report.append(
        f"- Previous ATOM-out base U9 rank: {summary['u9']['previous_base_u9_rank']} / {count9}"
    )
    report.append(
        f"- PR54 superseding target U9 rank: {summary['u9']['pr54_target_u9_rank']} / {count9}"
    )
    report.append(
        f"- Old U10 with ATOM rank: {summary['u10']['old_u10_with_atom_rank']} / {count10}"
    )
    report.append("")
    report.append("## Frequency")
    report.append("")
    report.append(
        "- U9 top50: " + json.dumps(summary["u9"]["top50_frequency"], sort_keys=True)
    )
    report.append(
        "- U10 top50: " + json.dumps(summary["u10"]["top50_frequency"], sort_keys=True)
    )
    report.append("")
    report.append("## Finalist diagnostics")
    report.append("")
    report.append(
        f"Top {FINALIST_N} per size received rolling 12m/24m, endpoint sensitivity, "
        "and bull-run-neutralization diagnostics."
    )
    report.append("")
    report.append("## Safety")
    report.append("")
    report.append("- Production/live strategy: unchanged.")
    report.append("- Telegram/live notifications: unchanged.")
    report.append("- Historical results are hindsight evidence, not a promotion decision.")
    report.append("")
    report.append("TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST")
    report.append("")
    (run_dir / "report.md").write_text("\n".join(report), encoding="utf-8")

    print("\n".join(report))


if __name__ == "__main__":
    main()
