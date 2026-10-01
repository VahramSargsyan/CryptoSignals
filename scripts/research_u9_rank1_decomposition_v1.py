from __future__ import annotations

import bisect
import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_robust_exhaustive_u9_u10_atom_replacement_v1 as core

OUT = Path("research_artifacts/u9_rank1_decomposition_v1")

RANK1 = ("TWT","PEPE","SOL","AAVE","LINK","AVAX","FIL","ALGO","HBAR")
RANK4 = ("TWT","PEPE","BNB","TRX","AAVE","AVAX","FIL","ALGO","HBAR")
COMMON = tuple(a for a in RANK1 if a in RANK4)
SWAP_POOL = ("SOL","LINK","BNB","TRX")

MATURE_START = pd.Timestamp("2023-10-31",tz="UTC")
EVAL_END = pd.Timestamp("2026-09-26",tz="UTC")
WINDOWS = {
    "LAST_1Y":(pd.Timestamp("2025-09-27",tz="UTC"),EVAL_END),
    "LAST_2Y":(pd.Timestamp("2024-09-27",tz="UTC"),EVAL_END),
    "MATURE":(MATURE_START,EVAL_END),
}
COST=0.001


def trace_one(opens,closes,positions,candidates,assets,start_i,end_i,start_asset,initial_equity=None):
    aset=set(assets)
    current=start_asset
    if initial_equity is None:
        qty=1.0/float(opens[current][start_i])
        initial=qty*float(closes[current][start_i])
    else:
        initial=float(initial_equity)
        qty=initial/float(closes[current][start_i])

    n=end_i-start_i+1
    equity=np.empty(n,float)
    holdings=np.empty(n,object)
    ledger=[]
    segment_start=start_i
    search_from=start_i

    while segment_start<=end_i:
        pos_list=positions[current]
        cand_list=candidates[current]
        p=bisect.bisect_left(pos_list,search_from)
        found=False
        while p<len(pos_list):
            signal_i=pos_list[p]
            if signal_i>=end_i:
                break
            eligible=[e for e in cand_list[p] if e["to_asset"] in aset]
            if not eligible:
                p+=1
                continue
            chosen=eligible[0]
            equity[segment_start-start_i:signal_i-start_i+1]=qty*closes[current][segment_start:signal_i+1]
            holdings[segment_start-start_i:signal_i-start_i+1]=current
            execute_i=signal_i+1
            value=qty*float(opens[current][execute_i])
            nxt=chosen["to_asset"]
            qty=value*(1.0-COST)/float(opens[nxt][execute_i])
            ledger.append({
                "signal_i":int(signal_i),
                "execute_i":int(execute_i),
                "from_asset":current,
                "to_asset":nxt,
                "pair":chosen["pair"],
                "max_dislocation":float(chosen["max_dislocation"]),
            })
            current=nxt
            segment_start=execute_i
            search_from=execute_i
            found=True
            break
        if found:
            continue
        equity[segment_start-start_i:]=qty*closes[current][segment_start:end_i+1]
        holdings[segment_start-start_i:]=current
        break

    peak=np.maximum.accumulate(equity)
    return {
        "initial_equity":float(initial),
        "final_equity":float(equity[-1]),
        "return":float(equity[-1]/initial-1.0),
        "max_dd":float(np.min(equity/peak-1.0)),
        "equity":equity,
        "holdings":holdings,
        "ledger":ledger,
    }


def evaluate(opens,closes,positions,candidates,timestamps,assets,start,end):
    si,ei=core.bounds(timestamps,start,end)
    runs=[trace_one(opens,closes,positions,candidates,assets,si,ei,a) for a in assets]
    return {
        "median_return":float(np.median([r["return"] for r in runs])),
        "worst_return":float(np.min([r["return"] for r in runs])),
        "best_return":float(np.max([r["return"] for r in runs])),
        "median_dd":float(np.median([r["max_dd"] for r in runs])),
        "worst_dd":float(np.min([r["max_dd"] for r in runs])),
        "median_transitions":float(np.median([len(r["ledger"]) for r in runs])),
    }


def divergence_segments(timestamps,start_i,ha,hb):
    diff=np.asarray(ha)!=np.asarray(hb)
    seg=[]
    i=0
    while i<len(diff):
        if not diff[i]:
            i+=1
            continue
        j=i
        while j+1<len(diff) and diff[j+1]:
            j+=1
        seg.append({
            "start_i":i,"end_i":j,
            "start_date":timestamps[start_i+i].date().isoformat(),
            "end_date":timestamps[start_i+j].date().isoformat(),
            "days":int(j-i+1),
            "a_start":str(ha[i]),"b_start":str(hb[i]),
            "a_end":str(ha[j]),"b_end":str(hb[j]),
        })
        i=j+1
    return seg


def ledger_event_on_date(run,timestamps,date):
    out=[]
    for e in run["ledger"]:
        if timestamps[e["signal_i"]].date().isoformat()==date:
            out.append({
                "signal_date":date,
                "execute_date":timestamps[e["execute_i"]].date().isoformat(),
                "from_asset":e["from_asset"],"to_asset":e["to_asset"],
                "max_dislocation":e["max_dislocation"],"pair":e["pair"],
            })
    return out


def first_reconvergence_after_diff(ha,hb,first_diff):
    for i in range(first_diff+1,len(ha)):
        if ha[i]==hb[i]:
            return i
    return None


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    panel,meta=core.download_panel(EVAL_END)
    positions,candidates=core.build_signal_index(panel)
    timestamps,opens,closes=core.prepare_arrays(panel)

    summary_rows=[]
    for label,assets in (("RANK1",RANK1),("RANK4",RANK4)):
        for w,(start,end) in WINDOWS.items():
            m=evaluate(opens,closes,positions,candidates,timestamps,assets,start,end)
            summary_rows.append({"universe":label,"window":w,**m})
    summary_df=pd.DataFrame(summary_rows)
    summary_df.to_csv(OUT/"primary_summary.csv",index=False)

    # Six exact U9 combinations of two replacement nodes added to the shared core-7.
    combo_rows=[]
    for pair in itertools.combinations(SWAP_POOL,2):
        assets=COMMON+pair
        row={"pair":"+".join(pair),"assets":"|".join(assets)}
        for w,(start,end) in WINDOWS.items():
            m=evaluate(opens,closes,positions,candidates,timestamps,assets,start,end)
            for k,v in m.items():
                row[f"{w}_{k}"]=v
        combo_rows.append(row)
    combo_df=pd.DataFrame(combo_rows).sort_values("MATURE_median_return",ascending=False)
    combo_df.to_csv(OUT/"swap_pair_matrix.csv",index=False)

    si,ei=core.bounds(timestamps,MATURE_START,EVAL_END)
    path_rows=[]
    segment_rows=[]
    fork_rows=[]
    all_first_dates=[]

    for start_asset in COMMON:
        a=trace_one(opens,closes,positions,candidates,RANK1,si,ei,start_asset)
        b=trace_one(opens,closes,positions,candidates,RANK4,si,ei,start_asset)
        diff=np.asarray(a["holdings"])!=np.asarray(b["holdings"])
        diff_days=int(diff.sum())
        first_idx=int(np.argmax(diff)) if diff.any() else None
        first_date=None if first_idx is None else timestamps[si+first_idx].date().isoformat()
        recon=None if first_idx is None else first_reconvergence_after_diff(a["holdings"],b["holdings"],first_idx)
        recon_date=None if recon is None else timestamps[si+recon].date().isoformat()
        ratio=float(a["final_equity"]/b["final_equity"])
        recon_cap_ratio=None if recon is None else float(a["equity"][recon]/b["equity"][recon])
        post_recon_equal_ratio=None
        if recon is not None:
            abs_recon=si+recon
            hold=str(a["holdings"][recon])
            pa=trace_one(opens,closes,positions,candidates,RANK1,abs_recon,ei,hold,initial_equity=1.0)
            pb=trace_one(opens,closes,positions,candidates,RANK4,abs_recon,ei,hold,initial_equity=1.0)
            post_recon_equal_ratio=float(pa["final_equity"]/pb["final_equity"])
        path_rows.append({
            "start_asset":start_asset,
            "rank1_return":a["return"],"rank4_return":b["return"],
            "rank1_to_rank4_final_capital_ratio":ratio,
            "divergent_days":diff_days,
            "first_divergence_date":first_date,
            "first_reconvergence_date":recon_date,
            "rank1_to_rank4_capital_ratio_at_reconvergence":recon_cap_ratio,
            "equal_cap_rank1_to_rank4_ratio_from_reconvergence_to_end":post_recon_equal_ratio,
            "rank1_transitions":len(a["ledger"]),"rank4_transitions":len(b["ledger"]),
            "rank1_end":str(a["holdings"][-1]),"rank4_end":str(b["holdings"][-1]),
        })
        if first_date:
            all_first_dates.append(first_date)
            for s in divergence_segments(timestamps,si,a["holdings"],b["holdings"]):
                segment_rows.append({"start_asset":start_asset,**s})

            # Equal-capital counterfactual from the first divergence day onward.
            abs_i=si+first_idx
            eq_a=trace_one(opens,closes,positions,candidates,RANK1,abs_i,ei,str(a["holdings"][first_idx]),initial_equity=1.0)
            eq_b=trace_one(opens,closes,positions,candidates,RANK4,abs_i,ei,str(b["holdings"][first_idx]),initial_equity=1.0)
            fork_rows.append({
                "start_asset":start_asset,
                "fork_date":first_date,
                "rank1_holding_at_fork":str(a["holdings"][first_idx]),
                "rank4_holding_at_fork":str(b["holdings"][first_idx]),
                "equal_cap_rank1_final":eq_a["final_equity"],
                "equal_cap_rank4_final":eq_b["final_equity"],
                "equal_cap_rank1_to_rank4_ratio":eq_a["final_equity"]/eq_b["final_equity"],
                "natural_rank1_to_rank4_ratio":ratio,
            })

    pd.DataFrame(path_rows).to_csv(OUT/"common_start_path_comparison.csv",index=False)

    ledger_rows=[]
    for start_asset in COMMON:
        for label,assets in (("RANK1",RANK1),("RANK4",RANK4)):
            r=trace_one(opens,closes,positions,candidates,assets,si,ei,start_asset)
            for e in r["ledger"]:
                sd=timestamps[e["signal_i"]].date().isoformat()
                ed=timestamps[e["execute_i"]].date().isoformat()
                if "2023-10-31" <= sd <= "2024-02-15":
                    ledger_rows.append({
                        "start_asset":start_asset,"universe":label,
                        "signal_date":sd,"execute_date":ed,
                        "from_asset":e["from_asset"],"to_asset":e["to_asset"],
                        "max_dislocation":e["max_dislocation"],"pair":e["pair"],
                    })
    pd.DataFrame(ledger_rows).to_csv(OUT/"early_fork_transition_ledger.csv",index=False)
    pd.DataFrame(segment_rows).to_csv(OUT/"divergence_segments.csv",index=False)
    pd.DataFrame(fork_rows).to_csv(OUT/"first_fork_counterfactuals.csv",index=False)

    # Frequency of first divergence dates + exact outgoing transitions near each dominant first fork.
    first_counts=pd.Series(all_first_dates).value_counts().rename_axis("first_divergence_date").reset_index(name="common_starts")
    first_counts.to_csv(OUT/"first_divergence_frequency.csv",index=False)

    focus=[]
    for execute_date in first_counts.head(10)["first_divergence_date"].tolist():
        execute_idx=int(timestamps.searchsorted(pd.Timestamp(execute_date,tz="UTC"),side="left"))
        abs_signal_idx=execute_idx-1
        signal_date=timestamps[abs_signal_idx].date().isoformat()
        for universe_label,assets in (("RANK1",RANK1),("RANK4",RANK4)):
            for source in set(COMMON)|set(SWAP_POOL):
                pos=positions[source]
                p=bisect.bisect_left(pos,abs_signal_idx)
                if p<len(pos) and pos[p]==abs_signal_idx:
                    eligible=[e for e in candidates[source][p] if e["to_asset"] in set(assets)]
                    if eligible:
                        ch=eligible[0]
                        focus.append({
                            "signal_date":signal_date,"execute_date":execute_date,
                            "universe":universe_label,"source":source,
                            "chosen_to":ch["to_asset"],"chosen_strength":ch["max_dislocation"],
                            "eligible_count":len(eligible),
                            "eligible_routes":";".join(f"{e['to_asset']}:{e['max_dislocation']:.6f}" for e in eligible[:5]),
                        })
    pd.DataFrame(focus).to_csv(OUT/"focus_divergence_signal_audit.csv",index=False)

    # Token usage: days held and transition touches across all common starts.
    usage=[]
    for label,assets in (("RANK1",RANK1),("RANK4",RANK4)):
        held=Counter(); entered=Counter(); exited=Counter()
        for start_asset in COMMON:
            r=trace_one(opens,closes,positions,candidates,assets,si,ei,start_asset)
            held.update(r["holdings"].tolist())
            for e in r["ledger"]:
                entered[e["to_asset"]]+=1; exited[e["from_asset"]]+=1
        total=sum(held.values())
        for a in assets:
            usage.append({
                "universe":label,"asset":a,
                "held_days":int(held[a]),"held_day_share":float(held[a]/total),
                "entries":int(entered[a]),"exits":int(exited[a]),
            })
    pd.DataFrame(usage).to_csv(OUT/"token_path_usage.csv",index=False)

    result={
        "experiment":"U9_RANK1_DECOMPOSITION_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "rank1_assets":RANK1,
        "rank4_assets":RANK4,
        "common_assets":COMMON,
        "rank1_only":sorted(set(RANK1)-set(RANK4)),
        "rank4_only":sorted(set(RANK4)-set(RANK1)),
        "primary":json.loads(summary_df.to_json(orient="records")),
        "swap_pair_matrix":json.loads(combo_df.to_json(orient="records")),
        "first_divergence_frequency":json.loads(first_counts.to_json(orient="records")),
        "data_meta":meta,
    }
    (OUT/"summary.json").write_text(json.dumps(result,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    r1=summary_df[(summary_df.universe=="RANK1")&(summary_df.window=="MATURE")].iloc[0]
    r4=summary_df[(summary_df.universe=="RANK4")&(summary_df.window=="MATURE")].iloc[0]
    lines=[
        "# U9 ROBUST RANK #1 DECOMPOSITION V1","",
        "Mode: STRESS_TEST_ONLY","",
        "Rank #1: "+", ".join(RANK1),
        "Rank #4: "+", ".join(RANK4),"",
        f"Rank1 Mature median: {100*r1.median_return:+.1f}%",
        f"Rank4 Mature median: {100*r4.median_return:+.1f}%","",
        "## Exact 2-of-4 replacement matrix","",
        "|Pair|1Y|2Y|Mature|Mature DD|",
        "|---|---:|---:|---:|---:|",
    ]
    for _,row in combo_df.iterrows():
        lines.append(f"|{row['pair']}|{100*row['LAST_1Y_median_return']:+.1f}%|{100*row['LAST_2Y_median_return']:+.1f}%|{100*row['MATURE_median_return']:+.1f}%|{100*row['MATURE_median_dd']:+.1f}%|")
    lines += ["","## First divergence dates",""]
    for _,row in first_counts.head(10).iterrows():
        lines.append(f"- {row['first_divergence_date']}: {int(row['common_starts'])} common starts")
    lines += ["","No production/live/Telegram changes.","","TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    from collections import Counter
    main()
