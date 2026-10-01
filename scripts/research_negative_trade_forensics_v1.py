from __future__ import annotations

import itertools
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research_artifacts" / "negative_trade_forensics_v1"
TOTAL_CACHE = ROOT / "data" / "market" / "cryptocap_total_d1.csv"

U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
LOOKBACK = 180
ARM = 0.15
REVERSAL = 0.03
DDG_RATIO = 1.50
COST = 0.001
START = pd.Timestamp("2023-10-31", tz="UTC")
CUTOFF = pd.Timestamp("2026-09-26", tz="UTC")
DATA_START = pd.Timestamp("2023-05-05", tz="UTC")
MARKET_WARMUP = pd.Timestamp("2022-01-01", tz="UTC")
EXPECTED_DDG_MATURE_MEDIAN = 35.8836

VARIANTS = {
    "REV4": {"reversal":0.04,"confirm_closes":1},
    "REV5": {"reversal":0.05,"confirm_closes":1},
    "PERSIST2": {"reversal":0.03,"confirm_closes":2},
    "PERSIST3": {"reversal":0.03,"confirm_closes":3},
}

@dataclass
class PairState:
    mode: str = "NONE"
    extreme: float | None = None
    max_dislocation: float = 0.0
    confirm_streak: int = 0


def utc(v):
    t = pd.Timestamp(v)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def download_symbol(symbol, start, end, cols=("open","close")):
    result = download_historical_dataset(
        BinanceSpotRestClient(),
        symbol=symbol,
        start=start,
        end=end + pd.Timedelta(days=1),
        timeframe="1D",
        as_of=end + pd.Timedelta(days=1),
    )
    if result.dataset is None:
        raise RuntimeError(f"{symbol}: {result.metadata.status}")
    if result.dataset.quality.has_critical_issues:
        raise RuntimeError(f"{symbol}: critical data quality")
    use = ["timestamp", *cols]
    f = result.dataset.candles[use].copy()
    f["timestamp"] = pd.to_datetime(f["timestamp"], utc=True)
    return f.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def download_panel():
    panel = None
    meta = {}
    for a in U10:
        f = download_symbol(a+"USDT", DATA_START, CUTOFF, ("open","close"))
        f = f.rename(columns={"open":a+"_open","close":a+"_close"})
        meta[a] = {
            "rows":int(len(f)),
            "start":pd.Timestamp(f.iloc[0]["timestamp"]).isoformat(),
            "end":pd.Timestamp(f.iloc[-1]["timestamp"]).isoformat(),
        }
        panel = f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    panel = panel.sort_values("timestamp").reset_index(drop=True)
    return panel, meta


def pair_daily_tape(panel, reversal=REVERSAL, confirm_closes=1):
    n = len(panel)
    daily_events = [[] for _ in range(n)]
    daily_active = [[] for _ in range(n)]

    for left,right in itertools.combinations(U10,2):
        ratio = panel[right+"_close"].astype(float) / panel[left+"_close"].astype(float)
        median = ratio.rolling(LOOKBACK,min_periods=LOOKBACK).median()
        dev = ratio/median - 1.0
        st = PairState()

        for i in range(n):
            if pd.isna(median.iloc[i]) or pd.isna(dev.iloc[i]):
                continue
            r=float(ratio.iloc[i]); d=float(dev.iloc[i])

            if st.mode=="NONE":
                if d>=ARM:
                    st=PairState("HIGH",r,abs(d),0)
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":right,"to_asset":left,
                        "max_dislocation":abs(d),"deviation":d,
                        "reversal_from_extreme":0.0,
                    })
                elif d<=-ARM:
                    st=PairState("LOW",r,abs(d),0)
                    daily_events[i].append({
                        "event":"ARMED","pair":f"{left}/{right}",
                        "from_asset":left,"to_asset":right,
                        "max_dislocation":abs(d),"deviation":d,
                        "reversal_from_extreme":0.0,
                    })

            elif st.mode=="HIGH":
                if r>float(st.extreme):
                    st.extreme=r
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                    st.confirm_streak=0
                retr=1.0-r/float(st.extreme)
                if retr>=reversal:
                    st.confirm_streak += 1
                    if st.confirm_streak>=confirm_closes:
                        daily_events[i].append({
                            "event":"CONFIRMED","pair":f"{left}/{right}",
                            "from_asset":right,"to_asset":left,
                            "max_dislocation":st.max_dislocation,"deviation":d,
                            "reversal_from_extreme":retr,
                        })
                        st=PairState()
                else:
                    st.confirm_streak=0

            elif st.mode=="LOW":
                if r<float(st.extreme):
                    st.extreme=r
                    st.max_dislocation=max(st.max_dislocation,abs(d))
                    st.confirm_streak=0
                retr=r/float(st.extreme)-1.0
                if retr>=reversal:
                    st.confirm_streak += 1
                    if st.confirm_streak>=confirm_closes:
                        daily_events[i].append({
                            "event":"CONFIRMED","pair":f"{left}/{right}",
                            "from_asset":left,"to_asset":right,
                            "max_dislocation":st.max_dislocation,"deviation":d,
                            "reversal_from_extreme":retr,
                        })
                        st=PairState()
                else:
                    st.confirm_streak=0

            if st.mode=="HIGH":
                retr=max(0.0,1.0-r/float(st.extreme))
                daily_active[i].append({
                    "event":"ARMED","pair":f"{left}/{right}",
                    "from_asset":right,"to_asset":left,
                    "max_dislocation":st.max_dislocation,
                    "deviation":d,"reversal_from_extreme":retr,
                })
            elif st.mode=="LOW":
                retr=max(0.0,r/float(st.extreme)-1.0)
                daily_active[i].append({
                    "event":"ARMED","pair":f"{left}/{right}",
                    "from_asset":left,"to_asset":right,
                    "max_dislocation":st.max_dislocation,
                    "deviation":d,"reversal_from_extreme":retr,
                })

    return daily_events,daily_active


def better(existing,candidate):
    if existing is None:
        return dict(candidate)
    ec=str(existing.get("event")).upper()=="CONFIRMED"
    cc=str(candidate.get("event")).upper()=="CONFIRMED"
    if ec != cc:
        return dict(candidate) if cc else existing
    if float(candidate.get("max_dislocation",0)) > float(existing.get("max_dislocation",0)):
        return dict(candidate)
    return existing


def merged_relations(events,active):
    rel={}
    for row in list(events)+list(active):
        if row["from_asset"] not in U10 or row["to_asset"] not in U10:
            continue
        key=(row["from_asset"],row["to_asset"])
        rel[key]=better(rel.get(key),row)
    return rel


def effective_route(day_events,day_active,source):
    confirmed=[
        dict(e) for e in day_events
        if e.get("event")=="CONFIRMED"
        and e.get("from_asset")==source
        and e.get("to_asset") in U10
    ]
    confirmed.sort(key=lambda e:(-float(e["max_dislocation"]),str(e["to_asset"]),str(e["pair"])))
    if not confirmed:
        return None

    primary=confirmed[0]
    rel=merged_relations(day_events,day_active)
    pto=primary["to_asset"]; pstr=float(primary["max_dislocation"])
    candidates=[]
    for (frm,to),row in rel.items():
        if frm!=source or to==pto:
            continue
        cstr=float(row.get("max_dislocation",0))
        if cstr+1e-12 < pstr*DDG_RATIO:
            continue
        relation=rel.get((pto,to))
        if relation is None:
            continue
        candidates.append((cstr,str(to),row,relation))
    if not candidates:
        return {
            "source":source,"primary":primary,"effective":dict(primary),
            "override":False,"override_detail":None,
        }
    candidates.sort(key=lambda x:(-x[0],x[1]))
    cstr,to,row,relation=candidates[0]
    eff=dict(primary)
    eff.update({
        "to_asset":to,"pair":row.get("pair"),
        "max_dislocation":cstr,
        "deviation":row.get("deviation"),
        "reversal_from_extreme":row.get("reversal_from_extreme"),
        "route_override":True,
    })
    return {
        "source":source,"primary":primary,"effective":eff,
        "override":True,
        "override_detail":{
            "competing":dict(row),
            "destination_relation":dict(relation),
            "strength_ratio":cstr/pstr if pstr else None,
        },
    }


def returns_and_ranks(panel,i,lookback):
    if i<lookback:
        return {},{}
    vals={}
    for a in U10:
        vals[a]=float(panel.loc[i,a+"_close"])/float(panel.loc[i-lookback,a+"_close"])-1.0
    ordered=sorted(vals,key=lambda a:(-vals[a],a))
    return vals,{a:j+1 for j,a in enumerate(ordered)}


def route_features(panel,i,events,active,route):
    source=route["source"]; dest=route["effective"]["to_asset"]
    rel=merged_relations(events,active)
    inbound_active={frm for (frm,to),r in rel.items() if to==dest and frm!=source}
    inbound_confirmed={
        e["from_asset"] for e in events
        if e.get("event")=="CONFIRMED" and e.get("to_asset")==dest and e.get("from_asset")!=source
    }
    outbound=[r for (frm,to),r in rel.items() if frm==source and to!=dest]
    best_other=max((float(r.get("max_dislocation",0)) for r in outbound),default=0.0)

    out={
        "source":source,
        "destination":dest,
        "primary_destination":route["primary"]["to_asset"],
        "ddg_override":bool(route["override"]),
        "effective_strength":float(route["effective"]["max_dislocation"]),
        "primary_strength":float(route["primary"]["max_dislocation"]),
        "primary_reversal":float(route["primary"].get("reversal_from_extreme") or 0.0),
        "effective_reversal":float(route["effective"].get("reversal_from_extreme") or 0.0),
        "best_other_outbound_strength":best_other,
        "inbound_active_ex_source":len(inbound_active),
        "inbound_confirmed_ex_source":len(inbound_confirmed),
        "ddg_strength_ratio":(
            None if not route["override"]
            else float(route["override_detail"]["strength_ratio"])
        ),
        "ddg_destination_relation_state":(
            None if not route["override"]
            else str(route["override_detail"]["destination_relation"].get("event"))
        ),
    }
    for lb in (30,60,90):
        vals,ranks=returns_and_ranks(panel,i,lb)
        out[f"source_ret{lb}"]=vals.get(source)
        out[f"dest_ret{lb}"]=vals.get(dest)
        out[f"source_rank{lb}"]=ranks.get(source)
        out[f"dest_rank{lb}"]=ranks.get(dest)
        out[f"rel{lb}"]=None if source not in vals or dest not in vals else vals[dest]-vals[source]
    return out


def add_hold_rule_flags(f):
    def iv(k,default=0): return int(f.get(k) if f.get(k) is not None else default)
    def fv(k,default=0.0): return float(f.get(k) if f.get(k) is not None else default)
    flags={
        "C1_NO_INDEPENDENT_SUPPORT": iv("inbound_active_ex_source")==0,
        "C2_LOW_SUPPORT": iv("inbound_active_ex_source")<=1,
        "C3_NO_SUPPORT_DEST_BOTTOM_HALF_30": iv("inbound_active_ex_source")==0 and iv("dest_rank30",99)>5,
        "C4_NO_SUPPORT_DEST_NOT_BEATING_SOURCE_30": iv("inbound_active_ex_source")==0 and fv("rel30")<=0,
        "C5_NO_SUPPORT_SIGNAL_LT20": iv("inbound_active_ex_source")==0 and fv("effective_strength")<0.20,
        "C6_LOW_SUPPORT_DEST_NOT_BEATING_SOURCE_30": iv("inbound_active_ex_source")<=1 and fv("rel30")<=0,
        "C7_NO_CONFIRMED_SUPPORT_DEST_NOT_BEATING_SOURCE_60": iv("inbound_confirmed_ex_source")==0 and fv("rel60")<=0,
        "M1_DEST_WORSE_SOURCE_30_60_90": fv("rel30")<=0 and fv("rel60")<=0 and fv("rel90")<=0,
        "M2_DEST_BOTTOM_HALF_30_60_90": iv("dest_rank30",99)>5 and iv("dest_rank60",99)>5 and iv("dest_rank90",99)>5,
        "M3_DEST_RANK_WORSE_SOURCE_ALL": (
            iv("dest_rank30",99)>iv("source_rank30",0)
            and iv("dest_rank60",99)>iv("source_rank60",0)
            and iv("dest_rank90",99)>iv("source_rank90",0)
        ),
        "S1_SIGNAL_LT18": fv("effective_strength")<0.18,
        "S2_SIGNAL_LT20": fv("effective_strength")<0.20,
        "S3_SIGNAL_LT25": fv("effective_strength")<0.25,
        "MS1_LT20_DEST_WORSE_30_60": fv("effective_strength")<0.20 and fv("rel30")<=0 and fv("rel60")<=0,
    }
    return flags


def load_total():
    f=pd.read_csv(TOTAL_CACHE)
    f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
    f=f.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    f=f[f["timestamp"]<=CUTOFF].copy()
    for n in (25,50,100,200,300):
        f[f"sma{n}_calc"]=f["close"].astype(float).rolling(n,min_periods=n).mean()
    def state(row):
        vals=[row[f"sma{n}_calc"] for n in (50,100,200,300)]
        if any(pd.isna(x) for x in vals):
            return "UNKNOWN"
        c=float(row["close"]); s50,s100,s200,s300=map(float,vals)
        if c>s50>s100>s200>s300: return "FULL_BULL_ALIGNMENT"
        if c<s50<s100<s200<s300: return "FULL_BEAR_ALIGNMENT"
        if c>s50>s100: return "BULL_BUILDING"
        if c<s50<s100: return "BEAR_BUILDING"
        return "MIXED"
    f["total_state"]=[state(r) for _,r in f.iterrows()]
    f["total_bull"]=f["total_state"].isin(["BULL_BUILDING","FULL_BULL_ALIGNMENT"])
    f["total_bear"]=f["total_state"].isin(["BEAR_BUILDING","FULL_BEAR_ALIGNMENT"])
    f["date"]=f["timestamp"].dt.normalize()
    return f


def market_state(panel):
    btc=download_symbol("BTCUSDT",MARKET_WARMUP,CUTOFF,("close",)).rename(columns={"close":"btc_close"})
    eth=download_symbol("ETHUSDT",MARKET_WARMUP,CUTOFF,("close",)).rename(columns={"close":"eth_close"})
    b=btc.set_index("timestamp").copy()
    e=eth.set_index("timestamp").copy()
    idx=b.index.intersection(e.index)
    m=pd.DataFrame(index=idx)
    m["btc_close"]=b.loc[idx,"btc_close"].astype(float)
    m["eth_close"]=e.loc[idx,"eth_close"].astype(float)

    for n in (50,200):
        m[f"btc_sma{n}"]=m["btc_close"].rolling(n,min_periods=n).mean()
    m["btc_above_sma200"]=m["btc_close"]>=m["btc_sma200"]
    ready=m[["btc_close","btc_sma50","btc_sma200"]].notna().all(axis=1)
    bull=ready&(m["btc_close"]>m["btc_sma200"])&(m["btc_sma50"]>m["btc_sma200"])
    bear=ready&(m["btc_close"]<m["btc_sma200"])&(m["btc_sma50"]<m["btc_sma200"])
    m["btc_trend"]="UNKNOWN"
    m.loc[bull,"btc_trend"]="BTC_BULL"
    m.loc[bear,"btc_trend"]="BTC_BEAR"
    m.loc[ready&~(bull|bear),"btc_trend"]="BTC_TRANSITION"

    m["btc_eth"]=m["btc_close"]/m["eth_close"]
    m["eth_btc"]=m["eth_close"]/m["btc_close"]
    for ratio in ("btc_eth","eth_btc"):
        for n in (25,50,100,200):
            m[f"{ratio}_sma{n}"]=m[ratio].rolling(n,min_periods=n).mean()
            m[f"{ratio}_vs_sma{n}"]=m[ratio]/m[f"{ratio}_sma{n}"]-1.0

    above200=m["btc_eth"]>m["btc_eth_sma200"]
    below25=m["btc_eth"]<m["btc_eth_sma25"]
    below50=m["btc_eth"]<m["btc_eth_sma50"]
    m["btceth_above200_persist3"]=above200&above200.shift(1,fill_value=False)&above200.shift(2,fill_value=False)
    m["btceth_below25_persist3"]=below25&below25.shift(1,fill_value=False)&below25.shift(2,fill_value=False)
    m["btceth_below50_persist3"]=below50&below50.shift(1,fill_value=False)&below50.shift(2,fill_value=False)

    # U10 breadth / volatility on strategy panel.
    p=panel.set_index("timestamp")
    closes=pd.DataFrame({a:p[a+"_close"].astype(float) for a in U10})
    sma50=closes.rolling(50,min_periods=50).mean()
    sma200=closes.rolling(200,min_periods=200).mean()
    breadth50=(closes>sma50).mean(axis=1).where(sma50.notna().all(axis=1))
    breadth200_count=(closes>sma200).sum(axis=1).where(sma200.notna().all(axis=1))
    vol30=closes.pct_change(fill_method=None).rolling(30,min_periods=30).std()
    lowvol=vol30.idxmin(axis=1).where(vol30.notna().all(axis=1))

    for ts in closes.index:
        if ts in m.index:
            m.loc[ts,"u10_breadth50"]=breadth50.loc[ts]
            m.loc[ts,"u10_breadth200_count"]=breadth200_count.loc[ts]
            m.loc[ts,"lowest_vol_asset"]=lowvol.loc[ts] if pd.notna(lowvol.loc[ts]) else None
            if pd.notna(breadth50.loc[ts]):
                v=float(breadth50.loc[ts])
                m.loc[ts,"broad_crypto_trend"]="BROAD_BULL" if v>=0.60 else ("BROAD_BEAR" if v<=0.40 else "BROAD_SIDEWAYS")

    low=(m["u10_breadth200_count"]<=3).fillna(False)
    m["defensive_low_breadth_persist3"]=low&low.shift(1,fill_value=False)&low.shift(2,fill_value=False)

    total=load_total().set_index("timestamp")
    common=m.index.intersection(total.index)
    for col in ["close","total_state","total_bull","total_bear"]+[f"sma{n}_calc" for n in (25,50,100,200,300)]:
        name="total_close" if col=="close" else col
        m.loc[common,name]=total.loc[common,col].values
    for n in (25,50,100,200,300):
        m[f"total_vs_sma{n}"]=m["total_close"].astype(float)/m[f"sma{n}_calc"].astype(float)-1.0

    m["risk_total_notbull_ethbtc_below100"]=(~m["total_bull"].fillna(False))&(m["eth_btc"]<m["eth_btc_sma100"])
    m["combo_risk_200"]=m["total_bear"].fillna(False)&m["btceth_above200_persist3"]
    return m,vol30


def simulate_runs(panel,events,active):
    ts=pd.DatetimeIndex(panel["timestamp"])
    si=int(ts.searchsorted(START,"left")); ei=int(ts.searchsorted(CUTOFF,"right"))-1
    runs=[]
    for start_asset in U10:
        cur=start_asset
        qty=1.0/float(panel.loc[si,cur+"_open"])
        initial=qty*float(panel.loc[si,cur+"_close"])
        pending=None
        ledger=[]
        equity=np.empty(ei-si+1,float)
        for i in range(si,ei+1):
            if pending is not None:
                value=qty*float(panel.loc[i,cur+"_open"])
                nxt=pending["destination"]
                qty=value*(1.0-COST)/float(panel.loc[i,nxt+"_open"])
                ledger.append({
                    "signal_index":pending["signal_index"],
                    "signal_date":ts[pending["signal_index"]].isoformat(),
                    "execute_index":i,
                    "execute_date":ts[i].isoformat(),
                    "from_asset":cur,"to_asset":nxt,
                    **pending["features"],
                })
                cur=nxt; pending=None

            equity[i-si]=qty*float(panel.loc[i,cur+"_close"])
            if i>=ei: continue
            route=effective_route(events[i],active[i],cur)
            if route is not None:
                f=route_features(panel,i,events[i],active[i],route)
                pending={"signal_index":i,"destination":route["effective"]["to_asset"],"features":f}
        runs.append({"start_asset":start_asset,"initial":initial,"final":float(equity[-1]),"return":float(equity[-1]/initial-1),"ledger":ledger})
    med=float(np.median([r["return"] for r in runs]))
    if abs(med-EXPECTED_DDG_MATURE_MEDIAN)>0.03:
        raise RuntimeError(f"DDG baseline mismatch {med:.6f}")
    return runs,si,ei,med


def completed_trade_rows(panel,runs,market,vol30,variant_tapes):
    ts=pd.DatetimeIndex(panel["timestamp"])
    rows=[]
    for run in runs:
        led=run["ledger"]
        for j,e in enumerate(led[:-1]):
            nxt=led[j+1]
            entry_i=int(e["execute_index"])
            exit_i=int(nxt["execute_index"])
            dest=e["to_asset"]
            entry_open=float(panel.loc[entry_i,dest+"_open"])
            exit_open=float(panel.loc[exit_i,dest+"_open"])
            gross=exit_open/entry_open-1.0
            net=(1.0-COST)*(1.0-COST)*(exit_open/entry_open)-1.0
            signal_i=int(e["signal_index"])
            signal_ts=ts[signal_i]
            row={
                "start_asset":run["start_asset"],
                "signal_date":signal_ts.date().isoformat(),
                "execute_date":ts[entry_i].date().isoformat(),
                "exit_signal_date":str(nxt["signal_date"])[:10],
                "exit_execute_date":ts[exit_i].date().isoformat(),
                "holding_days":int((ts[exit_i]-ts[entry_i]).days),
                "gross_trade_return":gross,
                "net_roundtrip_return":net,
                "is_loss":bool(net<0),
                **{k:v for k,v in e.items() if k not in {"signal_index","signal_date","execute_index","execute_date","from_asset","to_asset"}},
                "source":e["from_asset"],"destination":dest,
            }

            # Destination/source vol rank at signal date.
            if signal_ts in vol30.index and signal_ts in panel.set_index("timestamp").index:
                vv=vol30.loc[signal_ts].dropna().sort_values()
                ranks={a:i+1 for i,a in enumerate(vv.index)}
                row["dest_vol30"]=float(vol30.loc[signal_ts,dest]) if pd.notna(vol30.loc[signal_ts,dest]) else np.nan
                row["source_vol30"]=float(vol30.loc[signal_ts,e["from_asset"]]) if pd.notna(vol30.loc[signal_ts,e["from_asset"]]) else np.nan
                row["dest_vol_rank_low_to_high"]=ranks.get(dest)

            if signal_ts in market.index:
                mr=market.loc[signal_ts]
                for col in [
                    "btc_close","btc_sma50","btc_sma200","btc_above_sma200","btc_trend",
                    "btc_eth","eth_btc","btceth_above200_persist3","btceth_below25_persist3","btceth_below50_persist3",
                    "u10_breadth50","u10_breadth200_count","lowest_vol_asset","broad_crypto_trend",
                    "defensive_low_breadth_persist3","total_close","total_state","total_bull","total_bear",
                    "risk_total_notbull_ethbtc_below100","combo_risk_200"
                ]+[f"btc_eth_vs_sma{n}" for n in (25,50,100,200)]+[f"eth_btc_vs_sma{n}" for n in (25,50,100,200)]+[f"total_vs_sma{n}" for n in (25,50,100,200,300)]:
                    row[col]=mr.get(col)

            row.update({f"rule_{k}":bool(v) for k,v in add_hold_rule_flags(row).items()})
            row["flag_reversal_lt4"]=float(row.get("primary_reversal",0))<0.04
            row["flag_reversal_lt5"]=float(row.get("primary_reversal",0))<0.05
            row["flag_btc_below_sma200"]=not bool(row.get("btc_above_sma200",False))
            row["flag_btc_bear"]=str(row.get("btc_trend"))=="BTC_BEAR"
            row["flag_broad_bear"]=str(row.get("broad_crypto_trend"))=="BROAD_BEAR"
            row["flag_defensive_low_breadth"]=bool(row.get("defensive_low_breadth_persist3",False))
            row["flag_total_not_bull"]=not bool(row.get("total_bull",False))
            row["flag_total_bear"]=bool(row.get("total_bear",False))
            row["flag_btceth_long200"]=bool(row.get("btceth_above200_persist3",False))
            row["flag_ethbtc_below_sma100"]=float(row.get("eth_btc_vs_sma100",np.nan))<0 if pd.notna(row.get("eth_btc_vs_sma100",np.nan)) else False
            row["flag_high_attention"]=bool(row.get("risk_total_notbull_ethbtc_below100",False))
            row["flag_combo_risk200"]=bool(row.get("combo_risk_200",False))
            row["flag_ddg_override"]=bool(row.get("ddg_override",False))
            row["flag_dest_high_vol_half"]=(int(row.get("dest_vol_rank_low_to_high",0) or 0)>5)

            # Local delayed-rule survival. This is NOT available at original T.
            for name,(ve,va) in variant_tapes.items():
                first=None
                for d in range(0,6):
                    ii=signal_i+d
                    if ii>=len(ve): break
                    rr=effective_route(ve[ii],va[ii],e["from_asset"])
                    if rr is not None:
                        first=(d,rr["effective"]["to_asset"])
                        break
                row[f"delay_{name}_first_route_days"]=None if first is None else first[0]
                row[f"delay_{name}_same_destination"]=False if first is None else first[1]==dest

            row["fundamental_point_in_time"]="NOT_AVAILABLE_POINT_IN_TIME"
            rows.append(row)

    df=pd.DataFrame(rows)

    # Deduplicate identical realized episodes repeated after path convergence.
    key=["signal_date","execute_date","exit_execute_date","source","destination"]
    grouped=[]
    for _,g in df.groupby(key,dropna=False,sort=True):
        r=g.iloc[0].to_dict()
        r["start_count"]=int(len(g))
        r["start_assets"]="|".join(sorted(g["start_asset"].astype(str).unique()))
        grouped.append(r)
    out=pd.DataFrame(grouped).sort_values(["signal_date","source","destination"]).reset_index(drop=True)
    return out


def flag_diagnostics(df,flags):
    base_loss=float(df["is_loss"].mean())
    rows=[]
    for flag in flags:
        mask=df[flag].fillna(False).astype(bool)
        support=int(mask.sum())
        neg=int((mask&df["is_loss"]).sum())
        pos=int((mask&~df["is_loss"]).sum())
        precision=neg/support if support else np.nan
        recall=neg/int(df["is_loss"].sum()) if int(df["is_loss"].sum()) else np.nan
        rows.append({
            "flag":flag,"support":support,"negative":neg,"positive":pos,
            "loss_rate_when_true":precision,
            "loss_rate_when_false":float(df.loc[~mask,"is_loss"].mean()) if (~mask).any() else np.nan,
            "loss_rate_lift_vs_base":precision/base_loss if support and base_loss>0 else np.nan,
            "negative_recall":recall,
            "median_trade_return_when_true":float(df.loc[mask,"net_roundtrip_return"].median()) if support else np.nan,
        })
    return pd.DataFrame(rows).sort_values(["loss_rate_when_true","support"],ascending=[False,False])


def combo_diagnostics(df,flags):
    rows=[]
    years=pd.to_datetime(df["signal_date"]).dt.year
    for size in (2,3):
        for combo in itertools.combinations(flags,size):
            mask=np.ones(len(df),dtype=bool)
            for f in combo:
                mask &= df[f].fillna(False).astype(bool).to_numpy()
            support=int(mask.sum())
            if support<3:
                continue
            neg=int(np.sum(mask & df["is_loss"].to_numpy(bool)))
            if neg<2:
                continue
            present_years=sorted(set(years[mask].tolist()))
            year_rates={}
            for y in present_years:
                ym=mask&(years.to_numpy()==y)
                year_rates[str(y)]={"support":int(ym.sum()),"loss_rate":float(df.loc[ym,"is_loss"].mean())}
            discovery=mask&(pd.to_datetime(df["signal_date"]).dt.date<=pd.Timestamp("2025-09-26").date())
            validation=mask&(pd.to_datetime(df["signal_date"]).dt.date>=pd.Timestamp("2025-09-27").date())
            rows.append({
                "combo":" + ".join(combo),"size":size,"support":support,
                "negative":neg,"positive":support-neg,
                "loss_rate":neg/support,
                "median_return":float(df.loc[mask,"net_roundtrip_return"].median()),
                "years_present":len(present_years),
                "year_rates_json":json.dumps(year_rates,sort_keys=True),
                "discovery_support":int(discovery.sum()),
                "discovery_loss_rate":float(df.loc[discovery,"is_loss"].mean()) if discovery.any() else np.nan,
                "validation_support":int(validation.sum()),
                "validation_loss_rate":float(df.loc[validation,"is_loss"].mean()) if validation.any() else np.nan,
            })
    return pd.DataFrame(rows).sort_values(["loss_rate","support","years_present"],ascending=[False,False,False])


def domain_score(df):
    x=df.copy()
    x["domain_rr_caution"]=x["flag_reversal_lt5"]|x["rule_S2_SIGNAL_LT20"]|x["rule_S3_SIGNAL_LT25"]
    x["domain_network_caution"]=(
        x["rule_C1_NO_INDEPENDENT_SUPPORT"]
        |x["rule_M1_DEST_WORSE_SOURCE_30_60_90"]
        |x["rule_M2_DEST_BOTTOM_HALF_30_60_90"]
        |x["rule_M3_DEST_RANK_WORSE_SOURCE_ALL"]
    )
    x["domain_market_caution"]=x["flag_btc_below_sma200"]|x["flag_broad_bear"]|x["flag_defensive_low_breadth"]
    x["domain_total_ratio_caution"]=x["flag_high_attention"]|x["flag_combo_risk200"]|x["flag_btceth_long200"]
    domains=["domain_rr_caution","domain_network_caution","domain_market_caution","domain_total_ratio_caution"]
    x["warning_domain_count"]=x[domains].astype(int).sum(axis=1)
    rows=[]
    for score,g in x.groupby("warning_domain_count"):
        rows.append({
            "warning_domain_count":int(score),"trades":int(len(g)),
            "negative":int(g["is_loss"].sum()),"loss_rate":float(g["is_loss"].mean()),
            "median_return":float(g["net_roundtrip_return"].median()),
            "worst_return":float(g["net_roundtrip_return"].min()),
        })
    return x,pd.DataFrame(rows).sort_values("warning_domain_count")


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,data_meta=download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    base_events,base_active=pair_daily_tape(panel,REVERSAL,1)
    variant_tapes={name:pair_daily_tape(panel,**cfg) for name,cfg in VARIANTS.items()}
    market,vol30=market_state(panel)
    runs,si,ei,baseline_median=simulate_runs(panel,base_events,base_active)

    trades=completed_trade_rows(panel,runs,market,vol30,variant_tapes)
    trades.to_csv(OUT/"all_unique_completed_trades.csv",index=False)
    neg=trades[trades["is_loss"]].copy().sort_values("net_roundtrip_return")
    neg.to_csv(OUT/"negative_trade_forensics.csv",index=False)

    flag_cols=[
        c for c in trades.columns
        if c.startswith("rule_") or c.startswith("flag_")
    ]
    singles=flag_diagnostics(trades,flag_cols)
    singles.to_csv(OUT/"single_flag_diagnostics.csv",index=False)

    # Use only causal-at-T warning flags in combination search.
    combo_flags=[
        "flag_reversal_lt5","rule_S2_SIGNAL_LT20","rule_S3_SIGNAL_LT25",
        "rule_C1_NO_INDEPENDENT_SUPPORT","rule_C2_LOW_SUPPORT",
        "rule_M1_DEST_WORSE_SOURCE_30_60_90","rule_M2_DEST_BOTTOM_HALF_30_60_90",
        "rule_M3_DEST_RANK_WORSE_SOURCE_ALL",
        "flag_btc_below_sma200","flag_btc_bear","flag_broad_bear","flag_defensive_low_breadth",
        "flag_total_not_bull","flag_total_bear","flag_btceth_long200",
        "flag_ethbtc_below_sma100","flag_high_attention","flag_combo_risk200",
        "flag_dest_high_vol_half","flag_ddg_override",
    ]
    combo_flags=[f for f in combo_flags if f in trades.columns]
    combos=combo_diagnostics(trades,combo_flags)
    combos.to_csv(OUT/"exploratory_pair_triple_combos.csv",index=False)

    scored,score_summary=domain_score(trades)
    scored.to_csv(OUT/"all_trades_with_domain_score.csv",index=False)
    score_summary.to_csv(OUT/"warning_domain_score_summary.csv",index=False)

    delay_rows=[]
    for name in VARIANTS:
        same=trades[f"delay_{name}_same_destination"].fillna(False).astype(bool)
        delay_rows.append({
            "variant":name,
            "all_trade_same_destination_rate":float(same.mean()),
            "negative_same_destination_rate":float(same[trades["is_loss"]].mean()) if trades["is_loss"].any() else np.nan,
            "positive_same_destination_rate":float(same[~trades["is_loss"]].mean()) if (~trades["is_loss"]).any() else np.nan,
            "negative_block_or_change_rate":float((~same[trades["is_loss"]]).mean()) if trades["is_loss"].any() else np.nan,
            "positive_block_or_change_rate":float((~same[~trades["is_loss"]]).mean()) if (~trades["is_loss"]).any() else np.nan,
        })
    pd.DataFrame(delay_rows).to_csv(OUT/"delayed_rule_local_survival.csv",index=False)

    # Compact every-negative-trade table.
    compact_cols=[
        "signal_date","execute_date","exit_execute_date","source","destination",
        "holding_days","net_roundtrip_return","effective_strength","primary_reversal","ddg_override",
        "inbound_active_ex_source","dest_rank30","dest_rank60","dest_rank90",
        "rel30","rel60","rel90","btc_trend","broad_crypto_trend","u10_breadth50",
        "total_state","btc_eth_vs_sma200","eth_btc_vs_sma100",
        "flag_high_attention","flag_combo_risk200","flag_btc_below_sma200",
        "flag_defensive_low_breadth","dest_vol_rank_low_to_high",
        "delay_REV4_same_destination","delay_REV5_same_destination",
        "delay_PERSIST2_same_destination","delay_PERSIST3_same_destination",
        "start_count"
    ]
    neg[compact_cols].to_csv(OUT/"negative_trade_compact.csv",index=False)

    summary={
        "experiment":"NEGATIVE_TRADE_FORENSICS_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "universe":list(U10),
        "baseline_mature_median_return":baseline_median,
        "unique_completed_trades":int(len(trades)),
        "negative_trades":int(trades["is_loss"].sum()),
        "positive_trades":int((~trades["is_loss"]).sum()),
        "baseline_loss_rate":float(trades["is_loss"].mean()),
        "median_trade_return":float(trades["net_roundtrip_return"].median()),
        "worst_trade_return":float(trades["net_roundtrip_return"].min()),
        "best_trade_return":float(trades["net_roundtrip_return"].max()),
        "top_single_flags":json.loads(singles.head(15).to_json(orient="records")),
        "top_exploratory_combos":json.loads(combos.head(20).to_json(orient="records")) if len(combos) else [],
        "domain_score_summary":json.loads(score_summary.to_json(orient="records")),
        "data_meta":data_meta,
        "fundamental_layer":"NOT_AVAILABLE_POINT_IN_TIME",
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# NEGATIVE TRADE FORENSICS V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Current U10+DDG MATURE baseline median: {100*baseline_median:+.1f}%",
        f"Unique completed trade episodes: {len(trades)}",
        f"Negative episodes: {int(trades['is_loss'].sum())} ({100*trades['is_loss'].mean():.1f}%)","",
        "## Every negative trade","",
        "|Signal|Route|Exit|Days|Net trade|Strength|Reversal|BTC trend|Broad|TOTAL|Warnings|",
        "|---|---|---|---:|---:|---:|---:|---|---|---|---:|"
    ]
    scored_idx=scored.set_index(["signal_date","source","destination","exit_execute_date"])
    for _,r in neg.iterrows():
        key=(r["signal_date"],r["source"],r["destination"],r["exit_execute_date"])
        warning=int(scored_idx.loc[key,"warning_domain_count"]) if key in scored_idx.index else -1
        lines.append(
            f"|{r['signal_date']}|{r['source']}→{r['destination']}|{r['exit_execute_date']}|{int(r['holding_days'])}|"
            f"{100*r['net_roundtrip_return']:+.1f}%|{100*r['effective_strength']:.1f}%|{100*r['primary_reversal']:.1f}%|"
            f"{r.get('btc_trend','')}|{r.get('broad_crypto_trend','')}|{r.get('total_state','')}|{warning}|"
        )

    lines += ["","## Warning-domain score","",
              "|Domains warning|Trades|Losses|Loss rate|Median trade|Worst trade|",
              "|---:|---:|---:|---:|---:|---:|"]
    for _,r in score_summary.iterrows():
        lines.append(
            f"|{int(r['warning_domain_count'])}|{int(r['trades'])}|{int(r['negative'])}|"
            f"{100*r['loss_rate']:.1f}%|{100*r['median_return']:+.1f}%|{100*r['worst_return']:+.1f}%|"
        )

    lines += ["","## Strongest single warning flags (descriptive)","",
              "|Flag|Support|Losses|Loss rate|Lift vs base|Recall of losses|",
              "|---|---:|---:|---:|---:|---:|"]
    for _,r in singles.head(12).iterrows():
        lines.append(
            f"|{r['flag']}|{int(r['support'])}|{int(r['negative'])}|"
            f"{100*r['loss_rate_when_true']:.1f}%|{r['loss_rate_lift_vs_base']:.2f}x|{100*r['negative_recall']:.1f}%|"
        )

    if len(combos):
        lines += ["","## Exploratory pairs/triples — NOT production rules","",
                  "|Combination|Support|Losses|Loss rate|Years|Validation support|Validation loss rate|",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for _,r in combos.head(12).iterrows():
            vl="n/a" if pd.isna(r["validation_loss_rate"]) else f"{100*r['validation_loss_rate']:.1f}%"
            lines.append(
                f"|{r['combo']}|{int(r['support'])}|{int(r['negative'])}|{100*r['loss_rate']:.1f}%|"
                f"{int(r['years_present'])}|{int(r['validation_support'])}|{vl}|"
            )

    lines += [
        "",
        "## Interpretation guardrails",
        "",
        "- All combination results are exploratory on known history.",
        "- Delayed 4/5% and 2/3-close columns use future confirmation days and are not T-time features.",
        "- Fundamental analysis is intentionally excluded because point-in-time historical fundamentals are not frozen in the repository.",
        "- No production rule is authorized by this pass.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
