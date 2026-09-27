from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "u9_meme_node_substitution_v1"

CORE = ("ATOM","TWT","BNB","TRX","AAVE","LINK","FIL","HBAR")
MEMES = ("PEPE","DOGE","SHIB","BONK")
ALL = CORE + MEMES
UNIVERSES = {
    "U8_NO_MEME": CORE,
    "U9_PEPE": CORE + ("PEPE",),
    "U9_DOGE": CORE + ("DOGE",),
    "U9_SHIB": CORE + ("SHIB",),
    "U9_BONK": CORE + ("BONK",),
}
COMMON_STARTERS = CORE

LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
COST = 0.001

STRATEGY_DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
BULL_DATA_START = pd.Timestamp("2019-01-01", tz="UTC")
END = pd.Timestamp("2026-09-26", tz="UTC")
YEAR_START = pd.Timestamp("2025-09-27", tz="UTC")
TWO_YEAR_START = pd.Timestamp("2024-09-27", tz="UTC")
WINDOW_CACHE = {}


def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def source_sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"], cwd=ROOT, text=True).strip()


def download_single(client, asset, start, end):
    result = download_historical_dataset(
        client,
        symbol=asset+"USDT",
        start=start,
        end=end,
        timeframe="1D",
        as_of=end,
    )
    if result.dataset is None:
        raise RuntimeError(f"{asset}: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"{asset}: critical data quality issues")
    return result.dataset.candles[["timestamp","open","close"]].copy()


def download_strategy_panel(cutoff):
    client = BinanceSpotRestClient()
    panel = None
    meta = {}
    raw = {}
    for a in ALL:
        f = download_single(client, a, STRATEGY_DATA_START, cutoff)
        raw[a] = f.copy()
        meta[a] = {
            "rows": len(f),
            "start": pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end": pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        f = f.rename(columns={"open":a+"_open","close":a+"_close"})
        panel = f if panel is None else panel.merge(f, on="timestamp", how="inner", validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, meta, raw, client


def download_bull_series(client, cutoff):
    out = {}
    for a in MEMES:
        out[a] = download_single(client, a, BULL_DATA_START, cutoff).sort_values("timestamp").reset_index(drop=True)
    return out


def build_events(panel, assets):
    cols = ["timestamp"] + [a+"_close" for a in assets]
    events, _ = build_pair_monitor(
        panel[cols].copy(),
        assets=assets,
        lookback=LOOKBACK,
        arm_threshold=ARM,
        reversal=REVERSAL,
    )
    by_date = defaultdict(list)
    for e in events:
        if e["event"] == "CONFIRMED":
            by_date[utc(e["date"])].append(e)
    return by_date


def rows_for(panel, start, end):
    k = (start.isoformat(), end.isoformat())
    if k not in WINDOW_CACHE:
        w = panel[(panel["timestamp"] >= start) & (panel["timestamp"] <= end)]
        if w.empty:
            raise ValueError(f"empty window {start} -> {end}")
        WINDOW_CACHE[k] = list(w.itertuples(index=False, name="MarketRow"))
    return WINDOW_CACHE[k]


def run_one(panel, events, assets, start, end, start_asset, collect=False):
    aset = set(assets)
    rows = rows_for(panel, start, end)
    current = start_asset
    qty = 1.0 / float(getattr(rows[0], current+"_open"))
    pending = None
    equity = []
    dates = []
    holdings = []
    route = [current]
    transitions = 0
    conflicts = 0
    edges = Counter()

    for pos, row in enumerate(rows):
        ts = utc(row.timestamp)

        if pending is not None:
            value = qty * float(getattr(row, current+"_open"))
            old = current
            current = pending["to_asset"]
            qty = value * (1.0-COST) / float(getattr(row, current+"_open"))
            transitions += 1
            edges[(old,current)] += 1
            route.append(current)
            pending = None

        holdings.append(current)
        dates.append(ts)
        equity.append(qty * float(getattr(row, current+"_close")))

        if pos == len(rows)-1:
            continue
        cands = [
            e for e in events.get(ts, [])
            if e["from_asset"] == current and e["to_asset"] in aset
        ]
        if not cands:
            continue
        if len(cands) > 1:
            conflicts += 1
        pending = dict(sorted(
            cands,
            key=lambda e:(-float(e["max_dislocation"]), e["to_asset"], e["pair"])
        )[0])

    peak = equity[0]
    worst_dd = 0.0
    for v in equity:
        peak = max(peak, v)
        worst_dd = min(worst_dd, v/peak - 1.0)

    out = {
        "start_asset": start_asset,
        "return": float(equity[-1]/equity[0]-1.0),
        "max_dd": float(worst_dd),
        "transitions": transitions,
        "conflicts": conflicts,
    }
    if collect:
        out.update({
            "equity": equity,
            "dates": dates,
            "holdings": holdings,
            "route": route,
            "edges": {f"{a}->{b}":n for (a,b),n in edges.items()},
        })
    return out


def summarize(panel, events, assets, start, end, collect_atom=False):
    runs = []
    atom_detail = None
    for a in COMMON_STARTERS:
        r = run_one(panel, events, assets, start, end, a, collect=(collect_atom and a=="ATOM"))
        runs.append(r)
        if collect_atom and a=="ATOM":
            atom_detail = r
    df = pd.DataFrame(runs)
    atom = df[df["start_asset"]=="ATOM"].iloc[0]
    out = {
        "median_return": float(df["return"].median()),
        "worst_return": float(df["return"].min()),
        "best_return": float(df["return"].max()),
        "positive_starts": int((df["return"]>0).sum()),
        "median_max_dd": float(df["max_dd"].median()),
        "worst_max_dd": float(df["max_dd"].min()),
        "median_transitions": float(df["transitions"].median()),
        "median_conflicts": float(df["conflicts"].median()),
        "atom_return": float(atom["return"]),
        "atom_max_dd": float(atom["max_dd"]),
    }
    if atom_detail is not None:
        out["atom_route"] = atom_detail["route"]
        out["atom_holdings"] = dict(Counter(atom_detail["holdings"]))
        out["atom_edges"] = atom_detail["edges"]
    return out


def meme_start_result(panel, events, assets, meme, start, end):
    if meme is None:
        return None
    return run_one(panel, events, assets, start, end, meme)


def max_drawup_interval(frame, days):
    dates = [utc(x) for x in frame["timestamp"]]
    prices = frame["close"].astype(float).to_numpy()
    from collections import deque
    q = deque()
    best = -math.inf
    bi = bj = 0
    for j, p in enumerate(prices):
        while q and q[0] < j-days:
            q.popleft()
        while q and prices[q[-1]] >= p:
            q.pop()
        q.append(j)
        i = q[0]
        if i < j:
            gain = p/prices[i]-1.0
            if gain > best:
                best = gain
                bi, bj = i, j
    return {
        "drawup": float(best),
        "start": dates[bi],
        "end": dates[bj],
    }


def bull_metrics(frame):
    x90 = max_drawup_interval(frame, 90)
    x180 = max_drawup_interval(frame, 180)
    return {
        "max_90d_drawup": x90["drawup"],
        "max_90d_start": x90["start"].isoformat(),
        "max_90d_end": x90["end"].isoformat(),
        "max_180d_drawup": x180["drawup"],
        "max_180d_start": x180["start"].isoformat(),
        "max_180d_end": x180["end"].isoformat(),
        "primary_explosive": bool(x90["drawup"] >= 2.0 or x180["drawup"] >= 4.0),
    }


def neutralized_return(run, held_asset, start, end):
    value = 1.0
    for i in range(1, len(run["equity"])):
        ratio = run["equity"][i] / run["equity"][i-1]
        if run["holdings"][i] == held_asset and start <= run["dates"][i] <= end and ratio > 1.0:
            ratio = 1.0
        value *= ratio
    return float(value-1.0)


def bull_neutralized_summary(panel, events, assets, meme, mature_start, bull):
    if meme is None:
        return None
    bs = utc(bull["max_90d_start"])
    be = utc(bull["max_90d_end"])
    vals = []
    atom = None
    for a in COMMON_STARTERS:
        run = run_one(panel, events, assets, mature_start, END, a, collect=True)
        ret = neutralized_return(run, meme, bs, be)
        vals.append(ret)
        if a == "ATOM":
            atom = ret
    return {
        "median_return": float(pd.Series(vals).median()),
        "atom_return": float(atom),
    }


def monthly_windows(panel, mature_start, months):
    cursor = pd.Timestamp(mature_start.year, mature_start.month, 1, tz="UTC")
    if cursor < mature_start:
        cursor += pd.offsets.MonthBegin(1)
    out = []
    while True:
        starts = panel[panel["timestamp"] >= cursor]
        if starts.empty:
            break
        start = utc(starts.iloc[0]["timestamp"])
        target = start + pd.DateOffset(months=months) - pd.Timedelta("1D")
        if target > END:
            break
        ends = panel[panel["timestamp"] <= target]
        if ends.empty:
            break
        end = utc(ends.iloc[-1]["timestamp"])
        out.append((start,end))
        cursor += pd.offsets.MonthBegin(1)
    return out


def rolling_summary(panel, events, assets, mature_start, months):
    rows = []
    for start,end in monthly_windows(panel,mature_start,months):
        s = summarize(panel,events,assets,start,end)
        rows.append({"start":start.isoformat(),"end":end.isoformat(),**s})
    if not rows:
        return {"count":0}
    df = pd.DataFrame(rows)
    return {
        "count": len(df),
        "median_window_return": float(df["median_return"].median()),
        "worst_window_return": float(df["median_return"].min()),
        "positive_window_rate": float((df["median_return"]>0).mean()),
        "worst_atom_return": float(df["atom_return"].min()),
        "atom_positive_window_rate": float((df["atom_return"]>0).mean()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff", default="2026-09-27T00:00:00Z")
    args = ap.parse_args()

    cutoff = utc(args.cutoff)
    panel, meta, raw, client = download_strategy_panel(cutoff)
    bull_series = download_bull_series(client, cutoff)
    bull = {a:bull_metrics(bull_series[a]) for a in MEMES}

    common_start = utc(panel.iloc[0]["timestamp"])
    if len(panel) < LOOKBACK:
        raise RuntimeError("not enough common rows for warmup")
    mature_start = utc(panel.iloc[LOOKBACK-1]["timestamp"])

    run_dir = OUT / pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True,exist_ok=True)

    result = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "source_commit": source_sha(),
        "data": meta,
        "common_start": common_start.isoformat(),
        "common_mature_start": mature_start.isoformat(),
        "end": END.isoformat(),
        "bull_metrics": bull,
        "universes": {},
    }

    for name, assets in UNIVERSES.items():
        WINDOW_CACHE.clear()
        events = build_events(panel, assets)
        meme = None if name=="U8_NO_MEME" else name.replace("U9_","")
        mature = summarize(panel, events, assets, mature_start, END, collect_atom=True)
        year = summarize(panel, events, assets, YEAR_START, END, collect_atom=True)
        two = summarize(panel, events, assets, TWO_YEAR_START, END, collect_atom=True)
        meme_start = meme_start_result(panel,events,assets,meme,mature_start,END)
        neutral = bull_neutralized_summary(panel,events,assets,meme,mature_start,bull[meme]) if meme else None
        occ = 0.0
        entries = exits = 0
        if meme:
            h = mature["atom_holdings"]
            total = sum(h.values())
            occ = h.get(meme,0)/total if total else 0.0
            entries = sum(n for edge,n in mature["atom_edges"].items() if edge.endswith("->"+meme))
            exits = sum(n for edge,n in mature["atom_edges"].items() if edge.startswith(meme+"->"))

        result["universes"][name] = {
            "assets": list(assets),
            "meme": meme,
            "common_mature": mature,
            "latest_1y": year,
            "latest_2y": two,
            "rolling_12m": rolling_summary(panel,events,assets,mature_start,12),
            "rolling_24m": rolling_summary(panel,events,assets,mature_start,24),
            "meme_start": meme_start,
            "own_bull_neutralized_common_mature": neutral,
            "atom_meme_occupancy_share": occ,
            "atom_meme_entries": entries,
            "atom_meme_exits": exits,
        }

    (run_dir/"results.json").write_text(
        json.dumps(result,indent=2,sort_keys=True,allow_nan=False),
        encoding="utf-8"
    )

    lines = [
        "# U9 Meme-Node Substitution v1","",
        "Mode: STRESS_TEST_ONLY",
        "Live unchanged.","",
        f"All-candidate common history begins: {common_start.date()}",
        f"Fair mature start after 180 common rows: {mature_start.date()}",
        "",
        "| Universe | Common mature | ATOM | Latest 1Y | Latest 2Y | DD | Own-bull-neutralized |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name,u in result["universes"].items():
        neu = u["own_bull_neutralized_common_mature"]
        neu_txt = "n/a" if neu is None else f"{100*neu['median_return']:+.2f}%"
        lines.append(
            f"| {name} | {100*u['common_mature']['median_return']:+.2f}% | "
            f"{100*u['common_mature']['atom_return']:+.2f}% | "
            f"{100*u['latest_1y']['median_return']:+.2f}% | "
            f"{100*u['latest_2y']['median_return']:+.2f}% | "
            f"{100*u['common_mature']['median_max_dd']:+.2f}% | {neu_txt} |"
        )
    (run_dir/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

    print("run_dir="+str(run_dir))
    print("common_start="+common_start.isoformat())
    print("mature_start="+mature_start.isoformat())
    for meme in MEMES:
        b=bull[meme]
        print(meme+"_bull90=%.6f" % b["max_90d_drawup"])
        print(meme+"_bull90_interval="+b["max_90d_start"]+".."+b["max_90d_end"])
        print(meme+"_primary="+str(b["primary_explosive"]))
    for name,u in result["universes"].items():
        print(name+"_mature=%.6f" % u["common_mature"]["median_return"])
        print(name+"_atom=%.6f" % u["common_mature"]["atom_return"])
        print(name+"_1y=%.6f" % u["latest_1y"]["median_return"])
        print(name+"_2y=%.6f" % u["latest_2y"]["median_return"])
        print(name+"_dd=%.6f" % u["common_mature"]["median_max_dd"])
        if u["own_bull_neutralized_common_mature"] is not None:
            print(name+"_bullneutral=%.6f" % u["own_bull_neutralized_common_mature"]["median_return"])
        print(name+"_24m_worst="+(
            "NA" if u["rolling_24m"].get("count",0)==0 else "%.6f" % u["rolling_24m"]["worst_window_return"]
        ))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
