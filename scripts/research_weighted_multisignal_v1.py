from __future__ import annotations

import itertools, json, math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from integrations.binance.historical import download_historical_dataset
from integrations.binance.rest_client import BinanceSpotRestClient

OUT = Path("research_artifacts/weighted_multisignal_v1")

U10 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","XRP","HBAR")
LOOKBACK=180
ARM=0.15
REVERSAL=0.03
DDG_RATIO=1.50
COST=0.001
VOL_LOOKBACK=30
MAX_BOOKS_PER_ASSET=2
CAP=0.50

DATA_START=pd.Timestamp("2023-05-05",tz="UTC")
CUTOFF=pd.Timestamp("2026-09-27",tz="UTC")
WINDOWS={
    "DISCOVERY":(pd.Timestamp("2023-10-31",tz="UTC"),pd.Timestamp("2025-09-26",tz="UTC")),
    "VALIDATION_1Y":(pd.Timestamp("2025-09-27",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
    "LAST_2Y":(pd.Timestamp("2024-09-27",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
    "MATURE":(pd.Timestamp("2023-10-31",tz="UTC"),pd.Timestamp("2026-09-26",tz="UTC")),
}
WEIGHT_MODES=("EQUAL","LINEAR","SQRT","CAPPED_LINEAR")
KS=(3,4)
EXPECTED_DDG_MATURE=35.8836

@dataclass
class State:
    mode:str="NONE"
    extreme:float|None=None
    max_dislocation:float=0.0

def utc(v):
    t=pd.Timestamp(v)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

def download_panel():
    client=BinanceSpotRestClient()
    panel=None; meta={}
    for a in U10:
        r=download_historical_dataset(client,symbol=a+"USDT",start=DATA_START,end=CUTOFF,timeframe="1D",as_of=CUTOFF)
        if r.dataset is None: raise RuntimeError(f"{a}: {r.metadata.status}")
        if r.dataset.quality.has_critical_issues: raise RuntimeError(f"{a}: critical candle quality")
        f=r.dataset.candles[["timestamp","open","close"]].copy()
        f["timestamp"]=pd.to_datetime(f["timestamp"],utc=True)
        f=f.rename(columns={"open":a+"_open","close":a+"_close"})
        meta[a]={"rows":len(f),"start":str(f.iloc[0]["timestamp"]),"end":str(f.iloc[-1]["timestamp"])}
        panel=f if panel is None else panel.merge(f,on="timestamp",how="inner",validate="one_to_one")
    return panel.sort_values("timestamp").reset_index(drop=True),meta

def build_tape(panel):
    n=len(panel); events=[[] for _ in range(n)]; active=[[] for _ in range(n)]
    for left,right in itertools.combinations(U10,2):
        ratio=panel[right+"_close"].astype(float)/panel[left+"_close"].astype(float)
        med=ratio.rolling(LOOKBACK,min_periods=LOOKBACK).median()
        dev=ratio/med-1.0
        st=State()
        for i in range(n):
            if pd.isna(med.iloc[i]) or pd.isna(dev.iloc[i]): continue
            r=float(ratio.iloc[i]); d=float(dev.iloc[i])
            if st.mode=="NONE":
                if d>=ARM:
                    st=State("HIGH",r,abs(d))
                    events[i].append({"event":"ARMED","pair":f"{left}/{right}","from_asset":right,"to_asset":left,"max_dislocation":abs(d)})
                elif d<=-ARM:
                    st=State("LOW",r,abs(d))
                    events[i].append({"event":"ARMED","pair":f"{left}/{right}","from_asset":left,"to_asset":right,"max_dislocation":abs(d)})
            elif st.mode=="HIGH":
                if r>float(st.extreme):
                    st.extreme=r; st.max_dislocation=max(st.max_dislocation,abs(d))
                retr=1-r/float(st.extreme)
                if retr>=REVERSAL:
                    events[i].append({"event":"CONFIRMED","pair":f"{left}/{right}","from_asset":right,"to_asset":left,"max_dislocation":st.max_dislocation})
                    st=State()
            elif st.mode=="LOW":
                if r<float(st.extreme):
                    st.extreme=r; st.max_dislocation=max(st.max_dislocation,abs(d))
                retr=r/float(st.extreme)-1
                if retr>=REVERSAL:
                    events[i].append({"event":"CONFIRMED","pair":f"{left}/{right}","from_asset":left,"to_asset":right,"max_dislocation":st.max_dislocation})
                    st=State()
            if st.mode=="HIGH":
                active[i].append({"event":"ARMED","pair":f"{left}/{right}","from_asset":right,"to_asset":left,"max_dislocation":st.max_dislocation})
            elif st.mode=="LOW":
                active[i].append({"event":"ARMED","pair":f"{left}/{right}","from_asset":left,"to_asset":right,"max_dislocation":st.max_dislocation})
    return events,active

def better(x,y):
    if x is None: return dict(y)
    xc=str(x.get("event")).upper()=="CONFIRMED"; yc=str(y.get("event")).upper()=="CONFIRMED"
    if xc!=yc: return dict(y) if yc else x
    return dict(y) if float(y.get("max_dislocation",0))>float(x.get("max_dislocation",0)) else x

def merged_rel(events,active):
    rel={}
    for r in list(events)+list(active):
        k=(r["from_asset"],r["to_asset"]); rel[k]=better(rel.get(k),r)
    return rel

def effective_route(day_events,day_active,source):
    conf=[dict(e) for e in day_events if e.get("event")=="CONFIRMED" and e.get("from_asset")==source]
    conf.sort(key=lambda e:(-float(e["max_dislocation"]),str(e["to_asset"]),str(e["pair"])))
    if not conf: return None
    primary=conf[0]; pto=primary["to_asset"]; pstr=float(primary["max_dislocation"])
    rel=merged_rel(day_events,day_active); cand=[]
    for (frm,to),row in rel.items():
        if frm!=source or to==pto: continue
        s=float(row.get("max_dislocation",0))
        if s+1e-12 < pstr*DDG_RATIO: continue
        if rel.get((pto,to)) is None: continue
        cand.append((s,str(to),row))
    if not cand:
        return {"to_asset":pto,"max_dislocation":pstr,"pair":primary["pair"],"ddg_override":False}
    cand.sort(key=lambda z:(-z[0],z[1]))
    s,to,row=cand[0]
    return {"to_asset":to,"max_dislocation":s,"pair":row.get("pair"),"ddg_override":True}

def bounds(ts,start,end):
    si=int(ts.searchsorted(utc(start),"left")); ei=int(ts.searchsorted(utc(end),"right"))-1
    return si,ei

def build_vol(panel):
    out={}
    for a in U10:
        ret=panel[a+"_close"].astype(float).pct_change(fill_method=None)
        out[a]=ret.rolling(VOL_LOOKBACK,min_periods=VOL_LOOKBACK).std().to_numpy(float)
    return out

def vol_at_open(vol,a,idx):
    j=idx-1
    if j<0: return math.inf
    v=float(vol[a][j])
    return v if math.isfinite(v) else math.inf

def capped_linear(scores,cap=CAP):
    scores=np.asarray(scores,float)
    if np.all(scores<=0): return np.ones(len(scores))/len(scores)
    w=scores/scores.sum()
    fixed=np.zeros(len(w),bool)
    for _ in range(len(w)+2):
        over=(w>cap+1e-12)&(~fixed)
        if not over.any(): break
        fixed|=over; w[over]=cap
        rem=1-w[fixed].sum()
        idx=np.where(~fixed)[0]
        if len(idx)==0: break
        base=scores[idx]
        w[idx]=rem*(base/base.sum() if base.sum()>0 else np.ones(len(idx))/len(idx))
    return w/w.sum()

def weights(scores,mode):
    s=np.asarray(scores,float)
    if mode=="EQUAL": return np.ones(len(s))/len(s)
    if mode=="LINEAR": return s/s.sum() if s.sum()>0 else np.ones(len(s))/len(s)
    if mode=="SQRT":
        x=np.sqrt(np.maximum(s,0)); return x/x.sum() if x.sum()>0 else np.ones(len(s))/len(s)
    if mode=="CAPPED_LINEAR": return capped_linear(s)
    raise ValueError(mode)

def assign_max2(books,vol,idx):
    desired=defaultdict(list)
    for j,b in enumerate(books): desired[b["shadow"]].append(j)
    assignments=[None]*len(books); occ=Counter(); losers=[]
    for target,members in desired.items():
        def pr(j):
            b=books[j]
            return (int(b["actual"]==target),float(b["fresh_score"] if b["fresh"] else 0.0),-j)
        order=sorted(members,key=pr,reverse=True)
        for j in order[:MAX_BOOKS_PER_ASSET]:
            assignments[j]=target; occ[target]+=1
        losers.extend(order[MAX_BOOKS_PER_ASSET:])
    unresolved=[]
    for j in losers:
        b=books[j]
        if b["actual"]!=b["shadow"] and occ[b["actual"]]<MAX_BOOKS_PER_ASSET:
            assignments[j]=b["actual"]; occ[b["actual"]]+=1
        else: unresolved.append(j)
    ranked=sorted(U10,key=lambda a:(vol_at_open(vol,a,idx),U10.index(a)))
    for j in unresolved:
        a=next(x for x in ranked if occ[x]<MAX_BOOKS_PER_ASSET)
        assignments[j]=a; occ[a]+=1
    return assignments

def rebalance(qty,panel,idx,target_weights):
    current={a:float(qty.get(a,0.0))*float(panel.loc[idx,a+"_open"]) for a in U10}
    total=sum(current.values())
    target0={a:total*float(target_weights.get(a,0.0)) for a in U10}
    moved=0.5*sum(abs(current[a]-target0[a]) for a in U10)
    fee=COST*moved
    after=total-fee
    newqty={}
    for a in U10:
        tv=after*float(target_weights.get(a,0.0))
        newqty[a]=tv/float(panel.loc[idx,a+"_open"]) if tv>0 else 0.0
    return newqty,fee,moved

def simulate_weighted(panel,ts,events,active,vol,start_i,end_i,start_assets,mode):
    k=len(start_assets)
    books=[{"shadow":a,"actual":a,"score":ARM,"fresh":False,"fresh_score":0.0} for a in start_assets]
    qty={a:0.0 for a in U10}
    for a in start_assets: qty[a]=(1.0/k)/float(panel.loc[start_i,a+"_open"])
    pending=[None]*k
    eq=[]; largest=[]; costs=0.0; turnover=0.0; rebalances=0; shadow_trans=0; phys_reassign=0
    jan_weight=None

    for idx in range(start_i,end_i+1):
        changed=False
        old_actual=[b["actual"] for b in books]
        for j,b in enumerate(books):
            b["fresh"]=False; b["fresh_score"]=0.0
            ev=pending[j]
            if ev is not None:
                b["shadow"]=ev["to_asset"]; b["score"]=float(ev["max_dislocation"])
                b["fresh"]=True; b["fresh_score"]=float(ev["max_dislocation"])
                shadow_trans+=1; changed=True

        if idx!=start_i:
            assigns=assign_max2(books,vol,idx)
            for j,a in enumerate(assigns):
                if a!=books[j]["actual"]: phys_reassign+=1; changed=True
                books[j]["actual"]=a

        if changed:
            bw=weights([b["score"] for b in books],mode)
            aw=Counter()
            for j,b in enumerate(books): aw[b["actual"]]+=float(bw[j])
            qty,fee,moved=rebalance(qty,panel,idx,aw)
            costs+=fee; turnover+=moved; rebalances+=1

            if ts[idx].date().isoformat()=="2024-01-15":
                jan_weight=float(sum(bw[j] for j,b in enumerate(books) if b["shadow"]=="XRP"))

        vals=Counter()
        for a in U10:
            vals[a]+=float(qty.get(a,0.0))*float(panel.loc[idx,a+"_close"])
        total=sum(vals.values()); eq.append(total)
        largest.append(max(vals.values())/total if total>0 else 0)

        if idx==end_i: continue
        nxt=[]
        for b in books:
            r=effective_route(events[idx],active[idx],b["shadow"])
            nxt.append(None if r is None else r)
        pending=nxt

    arr=np.asarray(eq,float); peak=np.maximum.accumulate(arr); shares=np.asarray(largest,float)
    return {
        "return":float(arr[-1]/arr[0]-1),
        "max_dd":float(np.min(arr/peak-1)),
        "max_largest_share":float(shares.max()),
        "median_largest_share":float(np.median(shares)),
        "days_gt_50":float(np.mean(shares>0.50)),
        "days_gt_80":float(np.mean(shares>0.80)),
        "rebalances":rebalances,
        "shadow_transitions":shadow_trans,
        "physical_reassignments":phys_reassign,
        "cost_paid":costs,
        "turnover_notional":turnover,
        "jan2024_xrp_weight":jan_weight,
        "end_actual":"|".join(b["actual"] for b in books),
        "end_shadow":"|".join(b["shadow"] for b in books),
    }

def simulate_single(panel,ts,events,active,start_i,end_i,start_asset):
    cur=start_asset; qty=1.0/float(panel.loc[start_i,cur+"_open"]); eq=[]; pending=None; n=0
    for idx in range(start_i,end_i+1):
        if pending is not None:
            value=qty*float(panel.loc[idx,cur+"_open"])
            cur=pending["to_asset"]; qty=value*(1-COST)/float(panel.loc[idx,cur+"_open"]); n+=1; pending=None
        eq.append(qty*float(panel.loc[idx,cur+"_close"]))
        if idx<end_i:
            r=effective_route(events[idx],active[idx],cur)
            if r is not None: pending=r
    arr=np.asarray(eq); peak=np.maximum.accumulate(arr)
    return {"return":float(arr[-1]/arr[0]-1),"dd":float(np.min(arr/peak-1)),"transitions":n}

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,meta=download_panel(); ts=pd.DatetimeIndex(pd.to_datetime(panel["timestamp"],utc=True))
    events,active=build_tape(panel); vol=build_vol(panel)

    si,ei=bounds(ts,*WINDOWS["MATURE"])
    base=[simulate_single(panel,ts,events,active,si,ei,a) for a in U10]
    base_med=float(np.median([x["return"] for x in base]))
    if abs(base_med-EXPECTED_DDG_MATURE)>0.02:
        raise RuntimeError(f"DDG baseline mismatch {base_med} vs {EXPECTED_DDG_MATURE}")

    rows=[]
    for k in KS:
        combos=list(itertools.combinations(U10,k))
        for mode in WEIGHT_MODES:
            for w,(start,end) in WINDOWS.items():
                si,ei=bounds(ts,start,end)
                runs=[simulate_weighted(panel,ts,events,active,vol,si,ei,c,mode) for c in combos]
                rets=np.array([r["return"] for r in runs]); dds=np.array([r["max_dd"] for r in runs])
                jan=[r["jan2024_xrp_weight"] for r in runs if r["jan2024_xrp_weight"] is not None]
                rows.append({
                    "k":k,"weight_mode":mode,"window":w,"start_combos":len(combos),
                    "median_return":float(np.median(rets)),"worst_return":float(rets.min()),"best_return":float(rets.max()),
                    "median_dd":float(np.median(dds)),"worst_dd":float(dds.min()),
                    "median_largest_share":float(np.median([r["median_largest_share"] for r in runs])),
                    "median_peak_share":float(np.median([r["max_largest_share"] for r in runs])),
                    "median_days_gt_50":float(np.median([r["days_gt_50"] for r in runs])),
                    "median_days_gt_80":float(np.median([r["days_gt_80"] for r in runs])),
                    "median_rebalances":float(np.median([r["rebalances"] for r in runs])),
                    "median_cost_paid":float(np.median([r["cost_paid"] for r in runs])),
                    "median_jan2024_xrp_weight":None if not jan else float(np.median(jan)),
                    "max_jan2024_xrp_weight":None if not jan else float(np.max(jan)),
                })

    df=pd.DataFrame(rows); df.to_csv(OUT/"weighted_multisignal_results.csv",index=False)

    summary={
        "experiment":"WEIGHTED_MULTISIGNAL_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "baseline_ddg_mature_median_return":base_med,
        "ks":list(KS),"weight_modes":list(WEIGHT_MODES),
        "capital_rebalance":"ONLY_ON_NEW_SHADOW_ROTATION_OR_PHYSICAL_REASSIGNMENT",
        "max_books_per_asset":MAX_BOOKS_PER_ASSET,
        "capped_linear_cap":CAP,
        "results":json.loads(df.to_json(orient="records")),
        "data_metadata":meta,
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=["# WEIGHTED MULTISIGNAL V1","",f"DDG baseline Mature validation: {100*base_med:+.1f}%","",
           "|K|Weights|Discovery|Validation 1Y|Mature|Mature DD|Median peak share|Jan-2024 XRP weight|",
           "|---:|---|---:|---:|---:|---:|---:|---:|"]
    for k in KS:
        for mode in WEIGHT_MODES:
            sub=df[(df.k==k)&(df.weight_mode==mode)].set_index("window")
            d=sub.loc["DISCOVERY"]; v=sub.loc["VALIDATION_1Y"]; m=sub.loc["MATURE"]
            jw=m["median_jan2024_xrp_weight"]
            lines.append(f"|{k}|{mode}|{100*d.median_return:+.1f}%|{100*v.median_return:+.1f}%|{100*m.median_return:+.1f}%|{100*m.median_dd:+.1f}%|{100*m.median_peak_share:.1f}%|{'' if pd.isna(jw) else f'{100*jw:.1f}%'}|")
    lines+=["","No production/live/Telegram/execution configuration changed.","","TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))

if __name__=="__main__":
    from collections import defaultdict
    main()
