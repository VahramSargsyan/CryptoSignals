from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from collections import defaultdict
from pathlib import Path

import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient
from strategies.crypto.relative_rotation.paper_live import build_pair_monitor

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "relative_rotation_universe_scale_v1"
COST = 0.001
DATA_START = pd.Timestamp("2023-04-01", tz="UTC")

BASE8 = ("ATOM","TWT","PEPE","BNB","SOL","TRX","AAVE","LINK")
U20 = BASE8 + ("BTC","ETH","XRP","DOGE","ADA","AVAX","DOT","LTC","BCH","NEAR","UNI","FIL")
U30 = U20 + ("ICP","XLM","ETC","RUNE","CRV","SAND","MANA","OP","ARB","APE")
U40 = U30 + ("GALA","AXS","THETA","VET","ALGO","XTZ","CHZ","ENJ","COMP","EOS")
TIERS = {"U8":BASE8, "U20":U20, "U30":U30, "U40":U40}

WINDOWS = (
    ("W1","2023-10-31","2024-04-27"),
    ("W2","2024-04-28","2024-10-24"),
    ("W3","2024-10-25","2025-04-22"),
    ("W4","2025-04-23","2025-10-19"),
    ("W5","2025-10-20","2026-03-28"),
    ("OPENED_2026","2026-03-29","2026-09-26"),
    ("HIGHLIGHT_1Y","2025-03-29","2026-03-28"),
)

def utc(x):
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

def sha():
    v = os.environ.get("SOURCE_COMMIT_SHA")
    if v:
        return v
    return subprocess.check_output(["git","rev-parse","HEAD"], cwd=ROOT, text=True).strip()

def cutoff_now():
    return pd.Timestamp.now(tz="UTC").floor("D")

def dd(values):
    peak = -math.inf
    worst = 0.0
    for v in values:
        peak = max(peak, float(v))
        if peak > 0:
            worst = min(worst, float(v) / peak - 1.0)
    return worst

def download_all(cutoff):
    client = BinanceSpotRestClient()
    data, meta = {}, {}
    expected = cutoff - pd.Timedelta(days=1)
    for asset in U40:
        try:
            r = download_historical_dataset(
                client, symbol=asset+"USDT", start=DATA_START, end=cutoff,
                timeframe="1D", as_of=cutoff,
            )
            if r.dataset is None:
                meta[asset] = {"ok":False,"reason":r.metadata.status}
                continue
            f = r.dataset.candles[["timestamp","open","close"]].copy()
            last = pd.Timestamp(f.iloc[-1]["timestamp"])
            bad = r.dataset.quality.has_critical_issues
            stale = last < expected - pd.Timedelta(days=1)
            ok = not bad and not stale
            meta[asset] = {
                "ok":ok,
                "rows":len(f),
                "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
                "end":last.isoformat(),
                "reason":"CRITICAL_DATA" if bad else (("STALE_"+last.isoformat()) if stale else None),
            }
            if ok:
                data[asset] = f
        except Exception as e:
            meta[asset] = {"ok":False,"reason":type(e).__name__+":"+str(e)}
    return data, meta

def panel_for(data, assets):
    p = None
    for a in assets:
        f = data[a].rename(columns={"open":a+"_open","close":a+"_close"})
        p = f if p is None else p.merge(f, on="timestamp", how="inner", validate="one_to_one")
    return p.sort_values("timestamp").reset_index(drop=True)

def confirmed_map(panel, assets):
    close = panel[["timestamp"] + [a+"_close" for a in assets]].copy()
    events, _ = build_pair_monitor(close, assets=assets)
    m = defaultdict(list)
    for e in events:
        if e["event"] == "CONFIRMED":
            m[utc(e["date"])].append(e)
    return m, len(events)

def run_one(panel, emap, start, end, start_asset):
    w = panel[(panel.timestamp >= start) & (panel.timestamp <= end)].copy()
    if w.empty:
        raise ValueError("empty window")
    cur = start_asset
    qty = 1.0 / float(w.iloc[0][cur+"_open"])
    pending = None
    equity, route = [], [cur]
    transitions = conflicts = 0
    for pos, (_, row) in enumerate(w.iterrows()):
        ts = utc(row["timestamp"])
        if pending is not None:
            value = qty * float(row[cur+"_open"])
            cur = pending["to_asset"]
            qty = value * (1.0-COST) / float(row[cur+"_open"])
            route.append(cur)
            transitions += 1
            pending = None
        equity.append(qty * float(row[cur+"_close"]))
        if pos == len(w)-1:
            continue
        c = [e for e in emap.get(ts, []) if e["from_asset"] == cur]
        if not c:
            continue
        if len(c) > 1:
            conflicts += 1
        pending = sorted(c, key=lambda e:(-float(e["max_dislocation"]), e["to_asset"], e["pair"]))[0]
    return {
        "start_asset":start_asset,
        "return":float(equity[-1]-1.0),
        "max_dd":dd(equity),
        "transitions":transitions,
        "conflicts":conflicts,
        "route":route,
    }

def summarize(runs):
    r = pd.Series([x["return"] for x in runs], dtype=float)
    d = pd.Series([x["max_dd"] for x in runs], dtype=float)
    t = pd.Series([x["transitions"] for x in runs], dtype=float)
    c = pd.Series([x["conflicts"] for x in runs], dtype=float)
    atom = next(x for x in runs if x["start_asset"]=="ATOM")
    return {
        "starts":len(runs),
        "median_return":float(r.median()),
        "worst_return":float(r.min()),
        "best_return":float(r.max()),
        "positive_starts":int((r>0).sum()),
        "median_max_dd":float(d.median()),
        "worst_max_dd":float(d.min()),
        "median_transitions":float(t.median()),
        "median_conflicts":float(c.median()),
        "atom_return":atom["return"],
        "atom_max_dd":atom["max_dd"],
        "atom_route":atom["route"],
    }

def evaluate(data, assets):
    p = panel_for(data, assets)
    emap, event_count = confirmed_map(p, assets)
    first, last = utc(p.iloc[0]["timestamp"]), utc(p.iloc[-1]["timestamp"])
    out = {
        "assets":list(assets),
        "asset_count":len(assets),
        "pair_count":len(assets)*(len(assets)-1)//2,
        "common_start":first.isoformat(),
        "common_end":last.isoformat(),
        "event_count":event_count,
        "windows":{},
    }
    for name, s, e in WINDOWS:
        start, end = utc(s), utc(e)
        if start < first or end > last:
            out["windows"][name] = {"status":"UNAVAILABLE_COMMON_HISTORY"}
            continue
        runs = [run_one(p, emap, start, end, a) for a in assets]
        out["windows"][name] = {"status":"OK", **summarize(runs)}
    return out

def report(payload):
    lines = [
        "# Relative Rotation Universe Scale Stress Test v1","",
        "Mode: STRESS_TEST_ONLY",
        "Canonical live graph remains unchanged: 8 assets / 28 pairs.","",
        "| Tier | Assets | Pairs |","|---|---:|---:|",
    ]
    for k,a in TIERS.items():
        lines.append("| %s | %d | %d |" % (k,len(a),len(a)*(len(a)-1)//2))
    lines += ["","## Results",""]
    for k,res in payload["universes"].items():
        lines += ["### "+k,""]
        if res.get("status")=="SKIPPED":
            lines += ["SKIPPED: "+res["reason"],""]
            continue
        lines += [
            "Common history: %s -> %s" % (res["common_start"],res["common_end"]),"",
            "| Window | Median | Worst | Positive starts | Median DD | ATOM | Transitions | Conflicts |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for w,row in res["windows"].items():
            if row["status"]!="OK":
                lines.append("| %s | - | - | - | - | - | - | - |" % w)
                continue
            lines.append("| %s | %+.2f%% | %+.2f%% | %d/%d | %+.2f%% | %+.2f%% | %.1f | %.1f |" % (
                w,100*row["median_return"],100*row["worst_return"],row["positive_starts"],row["starts"],
                100*row["median_max_dd"],100*row["atom_return"],row["median_transitions"],row["median_conflicts"]
            ))
        lines.append("")
    lines += [
        "## Guardrails","",
        "- Fixed tiers were declared before outcome review.",
        "- No live-monitor asset list is changed by this test.",
        "- A higher return is not enough to promote a larger universe.",
        "- OPENED_2026 is diagnostic, not untouched validation.",
        "- NEW_NODE_PROBATION is evaluated separately after unrestricted topology behavior is known.",
        "- No automatic trading.",
    ]
    return "\n".join(lines)+"\n"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cutoff")
    args = ap.parse_args()
    cutoff = utc(args.cutoff) if args.cutoff else cutoff_now()
    data, meta = download_all(cutoff)
    now = pd.Timestamp.now(tz="UTC")
    run_dir = OUT / now.strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    universes = {}
    for k,assets in TIERS.items():
        missing = [a for a in assets if a not in data]
        universes[k] = {"status":"SKIPPED","reason":"Unavailable/stale: "+",".join(missing)} if missing else evaluate(data,assets)
    payload = {
        "generated_at":now.isoformat(),
        "source_commit":sha(),
        "cutoff":cutoff.isoformat(),
        "cost":COST,
        "tiers":{k:list(v) for k,v in TIERS.items()},
        "data":meta,
        "universes":universes,
    }
    (run_dir/"results.json").write_text(json.dumps(payload,indent=2,sort_keys=True,allow_nan=False),encoding="utf-8")
    (run_dir/"report.md").write_text(report(payload),encoding="utf-8")
    print("run_dir="+str(run_dir))
    for k,r in universes.items():
        print(k+"_status="+r.get("status","OK"))
        if r.get("status")!="SKIPPED":
            h=r["windows"]["HIGHLIGHT_1Y"]
            if h["status"]=="OK":
                print(k+"_highlight_median=%.6f" % h["median_return"])
                print(k+"_highlight_atom=%.6f" % h["atom_return"])
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
