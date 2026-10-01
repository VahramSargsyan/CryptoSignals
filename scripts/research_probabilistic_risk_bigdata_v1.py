from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_probabilistic_risk_model_v1 as pr

OUT = Path("research_artifacts/probabilistic_risk_bigdata_v1")

DISCOVERY_END = pd.Timestamp("2025-09-26", tz="UTC")
VALIDATION_START = pd.Timestamp("2025-09-27", tz="UTC")
CUTOFF = fx.CUTOFF
MATURE_START = fx.START

MODEL_NAMES = (
    "COUNT_PRIMARY",
    "HYBRID_EXPLORATORY",
    "STATE_HYBRID_EXPLORATORY",
)


def enrich_warning_domains(row):
    flags = fx.add_hold_rule_flags(row)
    row.update({f"rule_{k}": bool(v) for k,v in flags.items()})

    row["flag_reversal_lt5"] = float(row.get("primary_reversal",0) or 0) < 0.05
    row["flag_btc_below_sma200"] = not bool(row.get("btc_above_sma200",False))
    row["flag_broad_bear"] = str(row.get("broad_crypto_trend")) == "BROAD_BEAR"
    row["flag_defensive_low_breadth"] = bool(row.get("defensive_low_breadth_persist3",False))
    row["flag_total_not_bull"] = not bool(row.get("total_bull",False))
    row["flag_btceth_long200"] = bool(row.get("btceth_above200_persist3",False))
    row["flag_high_attention"] = bool(row.get("risk_total_notbull_ethbtc_below100",False))
    row["flag_combo_risk200"] = bool(row.get("combo_risk_200",False))

    row["domain_rr_caution"] = bool(
        row["flag_reversal_lt5"]
        or row["rule_S2_SIGNAL_LT20"]
        or row["rule_S3_SIGNAL_LT25"]
    )
    row["domain_network_caution"] = bool(
        row["rule_C1_NO_INDEPENDENT_SUPPORT"]
        or row["rule_M1_DEST_WORSE_SOURCE_30_60_90"]
        or row["rule_M2_DEST_BOTTOM_HALF_30_60_90"]
        or row["rule_M3_DEST_RANK_WORSE_SOURCE_ALL"]
    )
    row["domain_market_caution"] = bool(
        row["flag_btc_below_sma200"]
        or row["flag_broad_bear"]
        or row["flag_defensive_low_breadth"]
    )
    row["domain_total_ratio_caution"] = bool(
        row["flag_high_attention"]
        or row["flag_combo_risk200"]
        or row["flag_btceth_long200"]
    )
    row["warning_domain_count"] = int(sum([
        row["domain_rr_caution"],
        row["domain_network_caution"],
        row["domain_market_caution"],
        row["domain_total_ratio_caution"],
    ]))
    return row


def market_features(market, ts):
    if ts not in market.index:
        return {}
    mr = market.loc[ts]
    cols = [
        "btc_close","btc_sma50","btc_sma200","btc_above_sma200","btc_trend",
        "btc_eth","eth_btc","btceth_above200_persist3","btceth_below25_persist3",
        "btceth_below50_persist3","u10_breadth50","u10_breadth200_count",
        "lowest_vol_asset","broad_crypto_trend","defensive_low_breadth_persist3",
        "total_close","total_state","total_bull","total_bear",
        "risk_total_notbull_ethbtc_below100","combo_risk_200",
        "eth_btc_vs_sma100",
    ]
    return {c: mr.get(c) for c in cols}


def next_exit_for_destination(panel, events, active, destination, search_from, end_i):
    for j in range(search_from, end_i):
        rr = fx.effective_route(events[j], active[j], destination)
        if rr is not None:
            return {
                "exit_signal_i": j,
                "exit_execute_i": j+1,
                "next_destination": rr["effective"]["to_asset"],
            }
    return None


def build_candidate_panel(panel, events, active, market, vol30):
    ts = pd.DatetimeIndex(pd.to_datetime(panel["timestamp"], utc=True))
    start_i = int(ts.searchsorted(MATURE_START, side="left"))
    end_i = int(ts.searchsorted(CUTOFF, side="right")) - 1
    rows = []

    for i in range(start_i, end_i):
        signal_ts = ts[i]
        for source in fx.U10:
            route = fx.effective_route(events[i], active[i], source)
            if route is None:
                continue
            destination = route["effective"]["to_asset"]
            exit_info = next_exit_for_destination(
                panel, events, active, destination, i+1, end_i
            )
            if exit_info is None:
                continue

            entry_i = i+1
            exit_i = int(exit_info["exit_execute_i"])
            if exit_i > end_i:
                continue

            entry_open = float(panel.loc[entry_i, destination+"_open"])
            exit_open = float(panel.loc[exit_i, destination+"_open"])
            net = (1.0-fx.COST)*(1.0-fx.COST)*(exit_open/entry_open)-1.0

            f = fx.route_features(panel, i, events[i], active[i], route)
            row = {
                "signal_date": signal_ts.date().isoformat(),
                "signal_date_ts": signal_ts,
                "execute_date": ts[entry_i].date().isoformat(),
                "exit_signal_date": ts[int(exit_info["exit_signal_i"])].date().isoformat(),
                "exit_execute_date": ts[exit_i].date().isoformat(),
                "exit_execute_ts": ts[exit_i],
                "source": source,
                "destination": destination,
                "next_destination": exit_info["next_destination"],
                "holding_days": int((ts[exit_i]-ts[entry_i]).days),
                "net_roundtrip_return": float(net),
                "y_loss": int(net < 0),
                "y_material_loss": int(net <= -0.10),
                **f,
                **market_features(market, signal_ts),
            }

            if signal_ts in vol30.index:
                vv = vol30.loc[signal_ts].dropna().sort_values()
                ranks = {a:j+1 for j,a in enumerate(vv.index)}
                row["dest_vol_rank_low_to_high"] = ranks.get(destination)

            rows.append(enrich_warning_domains(row))

    d = pd.DataFrame(rows)
    d = d.sort_values(["signal_date_ts","source","destination"]).reset_index(drop=True)
    d["warning_domain_count_centered"] = d["warning_domain_count"].astype(float)-2.0
    d = pr.add_causal_strategy_state(d)
    return d


def metric_bundle(y,p):
    return pr.metrics(np.asarray(y,int), np.asarray(p,float))


def fit_predict(train, test, model_name, target, draws=4000, seed=20261001):
    spec = pr.MODEL_SPECS[model_name]
    Xtr,y,Xte,_ = pr.prepare_xy(train,test,spec,target)
    fit = pr.fit_bayes_logit(
        Xtr,y,spec["coef_prior_sd"],draws=draws,seed=seed
    )
    pred = pr.posterior_predict(fit,Xte)
    out = test[[
        "signal_date","exit_execute_date","source","destination",
        "warning_domain_count","net_roundtrip_return",target
    ]].copy()
    out["p_mean"] = pred["mean"]
    out["p_lo90"] = pred["lo90"]
    out["p_hi90"] = pred["hi90"]
    out["model"] = model_name
    out["target"] = target
    return out, fit


def discovery_validation(df):
    # Critical anti-leak rule:
    # a training outcome must have been fully known before validation starts.
    train = df[
        (df["signal_date_ts"] <= DISCOVERY_END)
        & (df["exit_execute_ts"] < VALIDATION_START)
    ].copy()
    test = df[
        (df["signal_date_ts"] >= VALIDATION_START)
        & (df["signal_date_ts"] <= CUTOFF)
    ].copy()

    rows=[]
    preds=[]
    for target in ("y_loss","y_material_loss"):
        base_p = (float(train[target].sum())+1.0)/(len(train)+2.0)
        base_metrics = metric_bundle(test[target], np.repeat(base_p,len(test)))
        for mi,model in enumerate(MODEL_NAMES):
            pred,_ = fit_predict(train,test,model,target,seed=20261001+mi)
            m = metric_bundle(pred[target],pred["p_mean"])
            preds.append(pred)
            rows.append({
                "split":"DISCOVERY_TO_VALIDATION_1Y",
                "target":target,"model":model,
                "train_n":int(len(train)),"train_events":int(train[target].sum()),
                "test_n":int(len(test)),"test_events":int(test[target].sum()),
                "brier":m["brier"],"logloss":m["logloss"],"auc":m["auc"],
                "baseline_p":base_p,
                "baseline_brier":base_metrics["brier"],
                "baseline_logloss":base_metrics["logloss"],
            })
    return train,test,pd.DataFrame(rows),pd.concat(preds,ignore_index=True)


def monthly_walk_forward(df, min_history=120):
    months = sorted(pd.to_datetime(df["signal_date_ts"]).dt.to_period("M").unique())
    pred_rows=[]
    metric_rows=[]

    for target in ("y_loss","y_material_loss"):
        for model in MODEL_NAMES:
            allp=[]
            for period in months:
                month_start = pd.Timestamp(period.start_time, tz="UTC")
                month_end = pd.Timestamp(period.end_time, tz="UTC")
                train = df[df["exit_execute_ts"] < month_start].copy()
                test = df[
                    (df["signal_date_ts"] >= month_start)
                    & (df["signal_date_ts"] <= month_end)
                ].copy()
                if len(train) < min_history or len(test)==0:
                    continue
                pred,_ = fit_predict(
                    train,test,model,target,draws=1500,
                    seed=20262000+len(allp)
                )
                base_p=(float(train[target].sum())+1.0)/(len(train)+2.0)
                pred["baseline_p"]=base_p
                pred["walk_month"]=str(period)
                allp.append(pred)

            if not allp:
                continue
            p=pd.concat(allp,ignore_index=True)
            mm=metric_bundle(p[target],p["p_mean"])
            bm=metric_bundle(p[target],p["baseline_p"])
            metric_rows.append({
                "target":target,"model":model,
                "n":int(len(p)),"events":int(p[target].sum()),
                "brier":mm["brier"],"logloss":mm["logloss"],"auc":mm["auc"],
                "baseline_brier":bm["brier"],"baseline_logloss":bm["logloss"],
            })
            pred_rows.append(p)
    return pd.DataFrame(metric_rows), pd.concat(pred_rows,ignore_index=True)


def source_destination_holdout(df):
    """Leave complete source assets out one at a time.

    This tests whether the risk mapping transports to routes from a source never
    seen during fitting. It is deliberately harsh and is reported separately.
    """
    rows=[]
    for target in ("y_loss","y_material_loss"):
        for model in MODEL_NAMES:
            pp=[]
            for si,source in enumerate(fx.U10):
                train=df[df["source"]!=source].copy()
                test=df[df["source"]==source].copy()
                if len(test)==0: continue
                pred,_=fit_predict(train,test,model,target,draws=1500,seed=20263000+si)
                pred["heldout_source"]=source
                pp.append(pred)
            p=pd.concat(pp,ignore_index=True)
            m=metric_bundle(p[target],p["p_mean"])
            base=float((df[target].sum()+1)/(len(df)+2))
            bm=metric_bundle(p[target],np.repeat(base,len(p)))
            rows.append({
                "target":target,"model":model,
                "n":len(p),"events":int(p[target].sum()),
                "brier":m["brier"],"logloss":m["logloss"],"auc":m["auc"],
                "baseline_brier":bm["brier"],"baseline_logloss":bm["logloss"],
            })
    return pd.DataFrame(rows)


def warning_curve(df):
    rows=[]
    for target in ("y_loss","y_material_loss"):
        for s in range(5):
            g=df[df["warning_domain_count"]==s]
            if len(g)==0: continue
            a=1+int(g[target].sum()); b=1+int(len(g)-g[target].sum())
            rng=np.random.default_rng(20264000+s+(100 if target=="y_material_loss" else 0))
            draw=rng.beta(a,b,size=100000)
            rows.append({
                "target":target,"warning_domain_count":s,
                "n":int(len(g)),"events":int(g[target].sum()),
                "empirical_rate":float(g[target].mean()),
                "beta_mean":float(a/(a+b)),
                "beta_lo90":float(np.quantile(draw,0.05)),
                "beta_hi90":float(np.quantile(draw,0.95)),
                "median_return":float(g["net_roundtrip_return"].median()),
            })
    return pd.DataFrame(rows)


def calibration(pred):
    bins=[-1e-9,.15,.30,.45,.60,1.000001]
    labels=["<15%","15-30%","30-45%","45-60%",">=60%"]
    rows=[]
    for (target,model),g0 in pred.groupby(["target","model"]):
        g=g0.copy()
        g["band"]=pd.cut(g["p_mean"],bins=bins,labels=labels,include_lowest=True,right=False)
        for band in labels:
            h=g[g["band"]==band]
            if len(h)==0: continue
            rows.append({
                "target":target,"model":model,"band":band,
                "n":len(h),"events":int(h[target].sum()),
                "mean_predicted":float(h["p_mean"].mean()),
                "observed_rate":float(h[target].mean()),
            })
    return pd.DataFrame(rows)


def main():
    OUT.mkdir(parents=True,exist_ok=True)

    panel,_ = fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active = fx.pair_daily_tape(panel,fx.REVERSAL,1)
    market,vol30 = fx.market_state(panel)

    d = build_candidate_panel(panel,events,active,market,vol30)
    d.to_csv(OUT/"candidate_route_panel.csv",index=False)

    train,test,holdout_metrics,holdout_pred = discovery_validation(d)
    holdout_metrics.to_csv(OUT/"discovery_validation_metrics.csv",index=False)
    holdout_pred.to_csv(OUT/"discovery_validation_predictions.csv",index=False)

    walk_metrics,walk_pred = monthly_walk_forward(d)
    walk_metrics.to_csv(OUT/"monthly_walkforward_metrics.csv",index=False)
    walk_pred.to_csv(OUT/"monthly_walkforward_predictions.csv",index=False)

    source_metrics = source_destination_holdout(d)
    source_metrics.to_csv(OUT/"leave_source_out_metrics.csv",index=False)

    curve=warning_curve(d)
    curve.to_csv(OUT/"warning_probability_curve.csv",index=False)

    cal=calibration(holdout_pred)
    cal.to_csv(OUT/"validation_calibration.csv",index=False)

    state = pr.state_summary(d,"y_loss")
    state.to_csv(OUT/"stateful_sequence_diagnostics.csv",index=False)

    summary={
        "experiment":"RR_PROBABILISTIC_RISK_BIGDATA_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "dataset_semantics":"ALL_CURRENT_U10_EXECUTABLE_SOURCE_DATE_ROUTES; COUNTERFACTUAL ROUTE_EPISODES",
        "candidate_routes":int(len(d)),
        "losses":int(d["y_loss"].sum()),
        "material_losses":int(d["y_material_loss"].sum()),
        "unique_signal_dates":int(d["signal_date"].nunique()),
        "sources":int(d["source"].nunique()),
        "destinations":int(d["destination"].nunique()),
        "discovery_train_n":int(len(train)),
        "validation_n":int(len(test)),
        "anti_leak_rule":"TRAIN LABEL INCLUDED ONLY IF EXIT COMPLETED BEFORE TEST PERIOD",
        "models":list(MODEL_NAMES),
        "holdout_metrics":json.loads(holdout_metrics.to_json(orient="records")),
        "walkforward_metrics":json.loads(walk_metrics.to_json(orient="records")),
        "source_holdout_metrics":json.loads(source_metrics.to_json(orient="records")),
        "warning_curve":json.loads(curve.to_json(orient="records")),
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# RR PROBABILISTIC RISK BIG-DATA V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Candidate route episodes: {len(d)}",
        f"Losses: {int(d['y_loss'].sum())} ({100*d['y_loss'].mean():.1f}%)",
        f"Material losses <= -10%: {int(d['y_material_loss'].sum())} ({100*d['y_material_loss'].mean():.1f}%)",
        f"Unique signal dates: {d['signal_date'].nunique()}","",
        "Dataset = every executable current-U10 RR+DDG route by source/date, not only the 30 routes physically visited by one realized path.",
        "Train labels are usable only after their exit has occurred.","",
        "## Discovery -> final 1Y validation","",
        "|Target|Model|Train N|Test N|Events|Brier|Baseline|AUC|",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _,r in holdout_metrics.iterrows():
        auc="n/a" if pd.isna(r["auc"]) else f"{r['auc']:.3f}"
        lines.append(
            f"|{r['target']}|{r['model']}|{int(r['train_n'])}|{int(r['test_n'])}|{int(r['test_events'])}|"
            f"{r['brier']:.3f}|{r['baseline_brier']:.3f}|{auc}|"
        )

    lines += ["","## Monthly causal walk-forward","",
              "|Target|Model|N|Events|Brier|Baseline|AUC|",
              "|---|---|---:|---:|---:|---:|---:|"]
    for _,r in walk_metrics.iterrows():
        auc="n/a" if pd.isna(r["auc"]) else f"{r['auc']:.3f}"
        lines.append(
            f"|{r['target']}|{r['model']}|{int(r['n'])}|{int(r['events'])}|"
            f"{r['brier']:.3f}|{r['baseline_brier']:.3f}|{auc}|"
        )

    lines += ["","## Warning count over large route panel","",
              "|Target|Warnings|N|Events|Empirical|Bayes mean 90%|Median return|",
              "|---|---:|---:|---:|---:|---|---:|"]
    for _,r in curve.iterrows():
        lines.append(
            f"|{r['target']}|{int(r['warning_domain_count'])}|{int(r['n'])}|{int(r['events'])}|"
            f"{100*r['empirical_rate']:.1f}%|{100*r['beta_mean']:.1f}% "
            f"({100*r['beta_lo90']:.1f}-{100*r['beta_hi90']:.1f}%)|"
            f"{100*r['median_return']:+.1f}%|"
        )

    lines += [
        "",
        "## Guardrails",
        "",
        "- Candidate-route episodes are counterfactual opportunities, not independent realized portfolio trades.",
        "- They are still correlated by market date and shared assets; large N does not equal large independent N.",
        "- Final-year validation and monthly walk-forward are therefore more important than in-sample fit.",
        "- No probability threshold, veto, or sizing rule is production-approved.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
