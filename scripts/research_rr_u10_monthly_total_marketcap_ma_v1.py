from __future__ import annotations

import argparse
import calendar
import json
import math
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core
from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "rr_u10_monthly_total_marketcap_ma_v1"

U10 = (
    "TWT", "PEPE", "BNB", "TRX", "AAVE",
    "AVAX", "FIL", "ALGO", "XRP", "HBAR",
)
MATURE_START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json"[^>]*>(.*?)</script>',
    flags=re.S,
)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def source_sha() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def requested_month_ends() -> list[pd.Timestamp]:
    rows = []
    cursor = pd.Timestamp("2023-10-01", tz="UTC")
    while cursor <= CUTOFF:
        if cursor.year == CUTOFF.year and cursor.month == CUTOFF.month:
            rows.append(CUTOFF)
            break
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        rows.append(
            pd.Timestamp(
                year=cursor.year,
                month=cursor.month,
                day=last_day,
                tz="UTC",
            )
        )
        cursor = cursor + pd.offsets.MonthBegin(1)
    return rows


def fetch_cmc_market_cap(requested: pd.Timestamp) -> dict:
    last_error = None
    for backward in range(0, 4):
        actual = requested - pd.Timedelta(days=backward)
        key = actual.strftime("%Y%m%d")
        url = f"https://coinmarketcap.com/historical/{key}/"
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", "replace")
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            continue

        match = NEXT_DATA_RE.search(raw)
        if not match:
            last_error = "missing __NEXT_DATA__"
            continue
        try:
            payload = json.loads(match.group(1))
            page_props = payload["props"]["pageProps"]
            market_cap = float(page_props["globalMetrics"]["marketCap"])
            display_date = page_props.get("displayDate")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            last_error = f"payload parse error: {exc}"
            continue

        if not math.isfinite(market_cap) or market_cap <= 0:
            last_error = f"invalid market cap: {market_cap}"
            continue

        return {
            "requested_date": requested.isoformat(),
            "snapshot_date": actual.isoformat(),
            "display_date": display_date,
            "market_cap_usd": market_cap,
            "source_url": url,
            "backward_days": backward,
        }

    raise RuntimeError(
        f"Could not retrieve CoinMarketCap snapshot for "
        f"{requested.date()} after 0..3 day fallback; last_error={last_error}"
    )


def download_btc(cutoff: pd.Timestamp) -> pd.DataFrame:
    client = BinanceSpotRestClient()
    result = download_historical_dataset(
        client,
        symbol="BTCUSDT",
        start=pd.Timestamp("2023-10-01", tz="UTC"),
        end=cutoff + pd.Timedelta(days=1),
        timeframe="1D",
        as_of=cutoff + pd.Timedelta(days=1),
    )
    if result.dataset is None:
        raise RuntimeError(f"BTCUSDT: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError("BTCUSDT: critical quality issues")
    frame = result.dataset.candles[["timestamp", "close"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["btc_close"] = frame["close"].astype(float)
    return (
        frame.drop(columns=["close"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def last_on_or_before(
    timestamps: pd.DatetimeIndex,
    values: np.ndarray,
    when: pd.Timestamp,
) -> float:
    idx = int(timestamps.searchsorted(when, side="right") - 1)
    if idx < 0:
        return np.nan
    return float(values[idx])


def build_u10_monthly_samples(
    panel: pd.DataFrame,
    month_dates: list[pd.Timestamp],
) -> tuple[pd.DataFrame, dict]:
    timestamps, opens, closes = core.prepare_arrays(panel)
    signal_positions, signal_candidates = core.build_signal_index(panel)
    start_i, end_i = core.bounds(timestamps, MATURE_START, CUTOFF)
    eval_ts = timestamps[start_i : end_i + 1]

    equity_by_start: dict[str, np.ndarray] = {}
    for start_asset in U10:
        result = core.simulate_one(
            opens,
            closes,
            signal_positions,
            signal_candidates,
            U10,
            start_i,
            end_i,
            start_asset,
            collect_equity=True,
        )
        equity = np.asarray(result["equity"], dtype=float)
        if len(equity) != len(eval_ts):
            raise RuntimeError(
                f"Equity length mismatch for {start_asset}: "
                f"{len(equity)} != {len(eval_ts)}"
            )
        equity_by_start[start_asset] = equity

    sampled_rows = []
    for when in month_dates:
        row = {"requested_date": when}
        for start_asset, equity in equity_by_start.items():
            row[start_asset] = last_on_or_before(eval_ts, equity, when)
        sampled_rows.append(row)
    sampled = pd.DataFrame(sampled_rows)

    monthly_returns = []
    for i in range(len(sampled)):
        if i == 0:
            monthly_returns.append(
                {
                    "u10_monthly_return_median": np.nan,
                    "u10_monthly_return_min": np.nan,
                    "u10_monthly_return_max": np.nan,
                }
            )
            continue
        vals = []
        for asset in U10:
            prev = float(sampled.iloc[i - 1][asset])
            cur = float(sampled.iloc[i][asset])
            vals.append(cur / prev - 1.0)
        arr = np.asarray(vals, dtype=float)
        monthly_returns.append(
            {
                "u10_monthly_return_median": float(np.median(arr)),
                "u10_monthly_return_min": float(np.min(arr)),
                "u10_monthly_return_max": float(np.max(arr)),
            }
        )

    return pd.DataFrame(monthly_returns), {
        "start_assets": list(U10),
        "aggregation": "median of monthly returns across 10 frozen start states",
        "evaluation_rows": int(len(eval_ts)),
        "evaluation_start": eval_ts[0].isoformat(),
        "evaluation_end": eval_ts[-1].isoformat(),
    }


def add_mas(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for n in (2, 3, 7, 10):
        out[f"mc_ma{n}_usd"] = (
            out["market_cap_usd"].rolling(n, min_periods=n).mean()
        )
        out[f"mc_above_ma{n}"] = (
            out["market_cap_usd"] > out[f"mc_ma{n}_usd"]
        ).where(out[f"mc_ma{n}_usd"].notna())
        out[f"mc_ma{n}_slope_positive"] = (
            out[f"mc_ma{n}_usd"].diff() > 0
        ).where(out[f"mc_ma{n}_usd"].notna())

    states = []
    for _, row in out.iterrows():
        vals = [row.get(f"mc_ma{n}_usd") for n in (2, 3, 7, 10)]
        cap = float(row["market_cap_usd"])
        if any(pd.isna(x) for x in vals):
            states.append("INSUFFICIENT_HISTORY")
            continue
        ma2, ma3, ma7, ma10 = map(float, vals)
        if cap > ma2 > ma3 > ma7 > ma10:
            states.append("FULL_BULL_ALIGNMENT")
        elif cap < ma2 < ma3 < ma7 < ma10:
            states.append("FULL_BEAR_ALIGNMENT")
        elif cap > ma2 and ma2 > ma3:
            states.append("BULL_BUILDING")
        elif cap < ma2 and ma2 < ma3:
            states.append("BEAR_BUILDING")
        else:
            states.append("MIXED")
    out["mc_ma_state"] = states
    return out


def forward_compound(series: pd.Series, start_i: int, months: int) -> float:
    end = min(len(series), start_i + 1 + months)
    vals = series.iloc[start_i + 1 : end].dropna().astype(float).to_numpy()
    if len(vals) < months:
        return np.nan
    return float(np.prod(1.0 + vals) - 1.0)


def transition_events(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    prev_state = None
    for i, row in frame.iterrows():
        state = str(row["mc_ma_state"])
        if state == "INSUFFICIENT_HISTORY":
            prev_state = state
            continue
        if state != prev_state:
            rows.append(
                {
                    "month": row["month"],
                    "from_state": prev_state,
                    "to_state": state,
                    "market_cap_usd": float(row["market_cap_usd"]),
                    "market_cap_mom": row["market_cap_mom"],
                    "btc_monthly_return": row["btc_monthly_return"],
                    "u10_monthly_return_median": row[
                        "u10_monthly_return_median"
                    ],
                    "btc_next_1m": forward_compound(
                        frame["btc_monthly_return"], i, 1
                    ),
                    "btc_next_3m": forward_compound(
                        frame["btc_monthly_return"], i, 3
                    ),
                    "u10_next_1m": forward_compound(
                        frame["u10_monthly_return_median"], i, 1
                    ),
                    "u10_next_3m": forward_compound(
                        frame["u10_monthly_return_median"], i, 3
                    ),
                }
            )
        prev_state = state
    return pd.DataFrame(rows)


def correlation(a: pd.Series, b: pd.Series) -> float:
    valid = pd.concat([a, b], axis=1).dropna()
    if len(valid) < 3:
        return np.nan
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1]))


def direction_agreement(a: pd.Series, b: pd.Series) -> float:
    valid = pd.concat([a, b], axis=1).dropna()
    if not len(valid):
        return np.nan
    aa = np.sign(valid.iloc[:, 0].to_numpy(float))
    bb = np.sign(valid.iloc[:, 1].to_numpy(float))
    return float(np.mean(aa == bb))


def fmt_pct(x) -> str:
    return "—" if pd.isna(x) else f"{100.0 * float(x):+.1f}%"


def fmt_trillion(x) -> str:
    return "—" if pd.isna(x) else "$" + f"{float(x)/1e12:.2f}T"


def report_table(frame: pd.DataFrame) -> str:
    lines = [
        "|Month|Total cap|Cap MoM|BTC MoM|U10 MoM|MA2|MA3|MA7|MA10|MA state|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for _, r in frame.iterrows():
        lines.append(
            f"|{r['month']}|{fmt_trillion(r['market_cap_usd'])}|"
            f"{fmt_pct(r['market_cap_mom'])}|"
            f"{fmt_pct(r['btc_monthly_return'])}|"
            f"{fmt_pct(r['u10_monthly_return_median'])}|"
            f"{fmt_trillion(r['mc_ma2_usd'])}|"
            f"{fmt_trillion(r['mc_ma3_usd'])}|"
            f"{fmt_trillion(r['mc_ma7_usd'])}|"
            f"{fmt_trillion(r['mc_ma10_usd'])}|"
            f"{r['mc_ma_state']}|"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cutoff", default="2026-09-26T00:00:00Z")
    parser.add_argument("--sleep-seconds", type=float, default=0.35)
    args = parser.parse_args()

    cutoff = core.utc(args.cutoff)
    if cutoff.normalize() != CUTOFF:
        raise RuntimeError(
            f"This preregistered run expects cutoff {CUTOFF.date()}, got {cutoff.date()}"
        )

    requested_dates = requested_month_ends()
    cap_rows = []
    for i, when in enumerate(requested_dates, start=1):
        print(
            f"[market-cap] {i}/{len(requested_dates)} "
            f"fetching {when.date()}",
            flush=True,
        )
        cap_rows.append(fetch_cmc_market_cap(when))
        time.sleep(args.sleep_seconds)

    caps = pd.DataFrame(cap_rows)
    caps["requested_date"] = pd.to_datetime(caps["requested_date"], utc=True)
    caps["snapshot_date"] = pd.to_datetime(caps["snapshot_date"], utc=True)
    caps = caps.sort_values("requested_date").reset_index(drop=True)

    core.POOL = U10
    panel, data_meta = core.download_panel(cutoff + pd.Timedelta(days=1))
    month_dates = list(caps["requested_date"])

    u10_returns, u10_meta = build_u10_monthly_samples(panel, month_dates)

    btc = download_btc(cutoff)
    btc_ts = pd.DatetimeIndex(btc["timestamp"])
    btc_values = btc["btc_close"].to_numpy(float)
    btc_closes = [
        last_on_or_before(btc_ts, btc_values, when) for when in month_dates
    ]

    frame = caps[
        [
            "requested_date",
            "snapshot_date",
            "market_cap_usd",
            "source_url",
            "backward_days",
        ]
    ].copy()
    frame["month"] = frame["requested_date"].dt.strftime("%Y-%m")
    frame["market_cap_mom"] = frame["market_cap_usd"].pct_change(
        fill_method=None
    )
    frame["btc_close_usd"] = btc_closes
    frame["btc_monthly_return"] = frame["btc_close_usd"].pct_change(
        fill_method=None
    )
    frame = pd.concat(
        [frame.reset_index(drop=True), u10_returns.reset_index(drop=True)],
        axis=1,
    )
    frame = add_mas(frame)

    transitions = transition_events(frame)

    corr_cap_btc = correlation(
        frame["market_cap_mom"], frame["btc_monthly_return"]
    )
    corr_cap_u10 = correlation(
        frame["market_cap_mom"], frame["u10_monthly_return_median"]
    )
    agree_cap_btc = direction_agreement(
        frame["market_cap_mom"], frame["btc_monthly_return"]
    )
    agree_cap_u10 = direction_agreement(
        frame["market_cap_mom"], frame["u10_monthly_return_median"]
    )

    state_summary_rows = []
    valid_return_rows = frame.dropna(
        subset=["btc_monthly_return", "u10_monthly_return_median"]
    )
    for state, group in valid_return_rows.groupby("mc_ma_state", sort=False):
        if state == "INSUFFICIENT_HISTORY":
            continue
        state_summary_rows.append(
            {
                "mc_ma_state": state,
                "month_count": int(len(group)),
                "median_cap_mom": float(group["market_cap_mom"].median()),
                "median_btc_return": float(group["btc_monthly_return"].median()),
                "median_u10_return": float(
                    group["u10_monthly_return_median"].median()
                ),
                "positive_btc_month_rate": float(
                    (group["btc_monthly_return"] > 0).mean()
                ),
                "positive_u10_month_rate": float(
                    (group["u10_monthly_return_median"] > 0).mean()
                ),
            }
        )
    state_summary = pd.DataFrame(state_summary_rows)

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)

    frame.to_csv(run_dir / "monthly_table.csv", index=False)
    transitions.to_csv(run_dir / "transition_events.csv", index=False)
    state_summary.to_csv(run_dir / "ma_state_summary.csv", index=False)

    summary = {
        "experiment": "RR_U10_MONTHLY_TOTAL_MARKETCAP_MA_V1",
        "workflow_mode": "STRESS_TEST_ONLY",
        "source_commit_sha": source_sha(),
        "cutoff": cutoff.isoformat(),
        "frozen_universe_id": "RR_TARGET_U10_CANDIDATE_HBAR_V1",
        "assets": list(U10),
        "market_cap_source": (
            "CoinMarketCap public historical snapshot "
            "__NEXT_DATA__.props.pageProps.globalMetrics.marketCap"
        ),
        "monthly_row_count": int(len(frame)),
        "u10_sampling": u10_meta,
        "data_metadata": data_meta,
        "ma_definition": {
            "MC_MA2": "2 monthly snapshots, ~60d coarse analogue",
            "MC_MA3": "3 monthly snapshots, ~90d coarse analogue",
            "MC_MA7": "7 monthly snapshots, ~210d coarse analogue",
            "MC_MA10": "10 monthly snapshots, ~300d coarse analogue",
            "warning": "These are monthly snapshot MAs, not exact daily SMA50/100/200/300.",
        },
        "relationship": {
            "pearson_cap_mom_vs_btc_monthly": corr_cap_btc,
            "pearson_cap_mom_vs_u10_monthly": corr_cap_u10,
            "direction_agreement_cap_vs_btc": agree_cap_btc,
            "direction_agreement_cap_vs_u10": agree_cap_u10,
        },
        "states": json.loads(state_summary.to_json(orient="records")),
        "transition_events": json.loads(
            transitions.to_json(orient="records", date_format="iso")
        ),
        "production_changes": "NONE",
        "test_level": "GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    report = [
        "# RR U10 MONTHLY TOTAL MARKET CAP + MA V1",
        "",
        "Mode: STRESS_TEST_ONLY",
        "",
        "## Monthly table",
        "",
        report_table(frame),
        "",
        "## Relationship",
        "",
        f"- Pearson cap MoM vs BTC monthly: {corr_cap_btc:.3f}",
        f"- Pearson cap MoM vs U10 monthly: {corr_cap_u10:.3f}",
        f"- Direction agreement cap vs BTC: {100*agree_cap_btc:.1f}%",
        f"- Direction agreement cap vs U10: {100*agree_cap_u10:.1f}%",
        "",
        "## MA-state summary",
        "",
        "|State|Months|Median cap MoM|Median BTC|Median U10|BTC positive months|U10 positive months|",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in state_summary.iterrows():
        report.append(
            f"|{r['mc_ma_state']}|{int(r['month_count'])}|"
            f"{fmt_pct(r['median_cap_mom'])}|"
            f"{fmt_pct(r['median_btc_return'])}|"
            f"{fmt_pct(r['median_u10_return'])}|"
            f"{100*r['positive_btc_month_rate']:.1f}%|"
            f"{100*r['positive_u10_month_rate']:.1f}%|"
        )

    report += [
        "",
        "## State transitions",
        "",
        "|Month|From|To|Cap|Cap MoM|BTC|U10|BTC next 3m|U10 next 3m|",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, r in transitions.iterrows():
        report.append(
            f"|{r['month']}|{r['from_state']}|{r['to_state']}|"
            f"{fmt_trillion(r['market_cap_usd'])}|"
            f"{fmt_pct(r['market_cap_mom'])}|"
            f"{fmt_pct(r['btc_monthly_return'])}|"
            f"{fmt_pct(r['u10_monthly_return_median'])}|"
            f"{fmt_pct(r['btc_next_3m'])}|"
            f"{fmt_pct(r['u10_next_3m'])}|"
        )

    report += [
        "",
        "## Interpretation boundary",
        "",
        "- Market cap is mechanically related to asset prices; correlation does not establish causality.",
        "- MA2/3/7/10 are monthly-snapshot approximations, not exact daily SMA50/100/200/300.",
        "- No MA period or state rule was optimized against U10 returns after inspection.",
        "- Production/paper-live/Telegram/exchange behavior is unchanged.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "",
    ]
    text_report = "\n".join(report)
    (run_dir / "report.md").write_text(text_report, encoding="utf-8")
    print(text_report)


if __name__ == "__main__":
    main()
