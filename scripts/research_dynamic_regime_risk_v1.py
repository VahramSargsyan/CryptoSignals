from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

import scripts.research_negative_trade_forensics_v1 as fx
import scripts.research_probabilistic_risk_bigdata_v1 as bg

OUT = Path("research_artifacts/dynamic_regime_risk_v1")
SEED = 20261001
DISCOVERY_END = pd.Timestamp("2025-09-26", tz="UTC")
VALIDATION_START = pd.Timestamp("2025-09-27", tz="UTC")
CUTOFF = fx.CUTOFF

INTERCEPT_PRIOR_MEAN = math.log(0.40 / 0.60)
INTERCEPT_PRIOR_SD = 1.0

# Frozen before results.
MODEL_SPECS = {
    "STATIC_STATE": {
        "features": [
            "warning_domain_count_centered",
            "prior_negative_batch_streak_z",
            "prior5_loss_rate_z",
        ],
        "coef_prior_sd": 0.45,
        "half_life_days": None,
        "status": "CONTROL",
    },
    "REGIME_COMPACT": {
        "features": [
            "warning_domain_count_centered",
            "prior_negative_batch_streak_z",
            "prior5_loss_rate_z",
            "market_adverse_count_centered",
            "streak_x_adverse",
            "loss5_x_adverse",
        ],
        "coef_prior_sd": 0.40,
        "half_life_days": None,
        "status": "PRIMARY_DYNAMIC",
    },
    "REGIME_DETAIL": {
        "features": [
            "warning_domain_count_centered",
            "prior_negative_batch_streak_z",
            "prior5_loss_rate_z",
            "btc_bear",
            "broad_bear",
            "total_not_bull",
            "streak_x_btc_bear",
            "streak_x_broad_bear",
            "streak_x_total_not_bull",
        ],
        "coef_prior_sd": 0.35,
        "half_life_days": None,
        "status": "DIAGNOSTIC",
    },
    "DECAY_365": {
        "features": [
            "warning_domain_count_centered",
            "prior_negative_batch_streak_z",
            "prior5_loss_rate_z",
            "market_adverse_count_centered",
            "streak_x_adverse",
            "loss5_x_adverse",
        ],
        "coef_prior_sd": 0.40,
        "half_life_days": 365.0,
        "status": "DIAGNOSTIC_RECENCY",
    },
    "DECAY_180": {
        "features": [
            "warning_domain_count_centered",
            "prior_negative_batch_streak_z",
            "prior5_loss_rate_z",
            "market_adverse_count_centered",
            "streak_x_adverse",
            "loss5_x_adverse",
        ],
        "coef_prior_sd": 0.40,
        "half_life_days": 180.0,
        "status": "DIAGNOSTIC_RECENCY",
    },
}


def sigmoid(x):
    x=np.asarray(x,float)
    out=np.empty_like(x)
    pos=x>=0
    out[pos]=1.0/(1.0+np.exp(-x[pos]))
    ex=np.exp(x[~pos])
    out[~pos]=ex/(1.0+ex)
    return out


def build_dataset():
    panel,_=fx.download_panel()
    panel["timestamp"]=pd.to_datetime(panel["timestamp"],utc=True)
    events,active=fx.pair_daily_tape(panel,fx.REVERSAL,1)
    market,vol30=fx.market_state(panel)
    d=bg.build_candidate_panel(panel,events,active,market,vol30)

    d["btc_bear"]=(d["btc_trend"].astype(str)=="BTC_BEAR").astype(float)
    d["broad_bear"]=(d["broad_crypto_trend"].astype(str)=="BROAD_BEAR").astype(float)
    d["total_not_bull"]=(~d["total_bull"].fillna(False).astype(bool)).astype(float)

    d["market_adverse_count"]=d[["btc_bear","broad_bear","total_not_bull"]].sum(axis=1)
    d["market_adverse_count_centered"]=d["market_adverse_count"]-1.5

    # Raw interaction columns; z-scored state features are produced train-only below.
    d["signal_date_ts"]=pd.to_datetime(d["signal_date_ts"],utc=True)
    d["exit_execute_ts"]=pd.to_datetime(d["exit_execute_ts"],utc=True)
    return d.sort_values(["signal_date_ts","source","destination"]).reset_index(drop=True)


def standardize_train_test(train,test):
    tr=train.copy()
    te=test.copy()

    for col in ["prior_negative_batch_streak","prior5_loss_rate"]:
        vals=pd.to_numeric(tr[col],errors="coerce")
        med=float(vals.median())
        sd=float(vals.std(ddof=0))
        if not math.isfinite(sd) or sd<1e-12:
            sd=1.0
        tr[col+"_z"]=(pd.to_numeric(tr[col],errors="coerce").fillna(med)-med)/sd
        te[col+"_z"]=(pd.to_numeric(te[col],errors="coerce").fillna(med)-med)/sd

    for frame in (tr,te):
        frame["streak_x_adverse"]=frame["prior_negative_batch_streak_z"]*frame["market_adverse_count_centered"]
        frame["loss5_x_adverse"]=frame["prior5_loss_rate_z"]*frame["market_adverse_count_centered"]
        frame["streak_x_btc_bear"]=frame["prior_negative_batch_streak_z"]*frame["btc_bear"]
        frame["streak_x_broad_bear"]=frame["prior_negative_batch_streak_z"]*frame["broad_bear"]
        frame["streak_x_total_not_bull"]=frame["prior_negative_batch_streak_z"]*frame["total_not_bull"]
    return tr,te


def recency_weights(train, asof, half_life_days):
    if half_life_days is None:
        return np.ones(len(train),float)
    age=(pd.Timestamp(asof)-train["exit_execute_ts"]).dt.total_seconds()/86400.0
    age=np.maximum(age.to_numpy(float),0.0)
    w=np.power(0.5, age/float(half_life_days))
    # Normalize mean to 1 so likelihood/prior balance is comparable.
    return w/max(float(w.mean()),1e-12)


def prepare_xy(train,test,spec,target,asof):
    tr,te=standardize_train_test(train,test)
    feats=spec["features"]
    Xtr=np.column_stack([np.ones(len(tr)),tr[feats].astype(float).to_numpy()])
    Xte=np.column_stack([np.ones(len(te)),te[feats].astype(float).to_numpy()])
    y=tr[target].astype(int).to_numpy()
    weights=recency_weights(tr,asof,spec["half_life_days"])
    return Xtr,y,Xte,weights,feats


def fit_weighted_bayes_logit(X,y,weights,coef_prior_sd,draws=2000,seed=SEED):
    p=X.shape[1]
    prior_mean=np.zeros(p,float); prior_mean[0]=INTERCEPT_PRIOR_MEAN
    prior_sd=np.full(p,float(coef_prior_sd)); prior_sd[0]=INTERCEPT_PRIOR_SD
    prior_prec=1.0/(prior_sd**2)

    def objective(beta):
        eta=X@beta
        nll=np.sum(weights*(np.logaddexp(0.0,eta)-y*eta))
        d=beta-prior_mean
        return float(nll+0.5*np.sum(prior_prec*d*d))

    def grad(beta):
        pp=sigmoid(X@beta)
        return X.T@(weights*(pp-y))+prior_prec*(beta-prior_mean)

    res=minimize(objective,prior_mean.copy(),jac=grad,method="BFGS",
                 options={"maxiter":2000,"gtol":1e-9})
    beta=np.asarray(res.x,float)
    pp=sigmoid(X@beta)
    W=weights*pp*(1-pp)
    H=X.T@(X*W[:,None])+np.diag(prior_prec)
    try:
        cov=np.linalg.inv(H)
    except np.linalg.LinAlgError:
        cov=np.linalg.pinv(H)
    cov=(cov+cov.T)/2
    eig=np.linalg.eigvalsh(cov)
    if eig.min()<=0:
        cov+=np.eye(p)*(abs(eig.min())+1e-10)
    rng=np.random.default_rng(seed)
    samples=rng.multivariate_normal(beta,cov,size=draws)
    return {"map":beta,"cov":cov,"samples":samples,"success":bool(res.success)}


def predict(fit,X):
    p=sigmoid(X@fit["samples"].T)
    return {
        "mean":p.mean(axis=1),
        "lo90":np.quantile(p,0.05,axis=1),
        "hi90":np.quantile(p,0.95,axis=1),
    }


def metrics(y,p):
    return bg.metric_bundle(np.asarray(y,int),np.asarray(p,float))


def fit_predict(train,test,model_name,target,asof,draws=2000,seed=SEED):
    spec=MODEL_SPECS[model_name]
    Xtr,y,Xte,w,features=prepare_xy(train,test,spec,target,asof)
    fit=fit_weighted_bayes_logit(Xtr,y,w,spec["coef_prior_sd"],draws=draws,seed=seed)
    pr=predict(fit,Xte)
    out=test[[
        "signal_date","signal_date_ts","exit_execute_date","source","destination",
        "warning_domain_count","market_adverse_count","prior_negative_batch_streak",
        "prior5_loss_rate","net_roundtrip_return",target
    ]].copy()
    out["p_mean"]=pr["mean"]; out["p_lo90"]=pr["lo90"]; out["p_hi90"]=pr["hi90"]
    out["model"]=model_name; out["target"]=target
    return out,fit,features


def final_holdout(df):
    train=df[
        (df["signal_date_ts"]<=DISCOVERY_END)
        & (df["exit_execute_ts"]<VALIDATION_START)
    ].copy()
    test=df[
        (df["signal_date_ts"]>=VALIDATION_START)
        & (df["signal_date_ts"]<=CUTOFF)
    ].copy()
    rows=[]; preds=[]
    for target in ("y_loss","y_material_loss"):
        pbase=(float(train[target].sum())+1)/(len(train)+2)
        bm=metrics(test[target],np.repeat(pbase,len(test)))
        for mi,name in enumerate(MODEL_SPECS):
            p,_,_=fit_predict(train,test,name,target,VALIDATION_START,draws=3000,seed=SEED+mi)
            mm=metrics(p[target],p["p_mean"])
            preds.append(p)
            rows.append({
                "target":target,"model":name,"status":MODEL_SPECS[name]["status"],
                "train_n":len(train),"test_n":len(test),"events":int(test[target].sum()),
                "brier":mm["brier"],"logloss":mm["logloss"],"auc":mm["auc"],
                "baseline_brier":bm["brier"],"baseline_logloss":bm["logloss"],
            })
    return train,test,pd.DataFrame(rows),pd.concat(preds,ignore_index=True)


def monthly_walkforward(df,min_history=120):
    periods=sorted(df["signal_date_ts"].dt.to_period("M").unique())
    pred_frames=[]
    rows=[]
    for target in ("y_loss","y_material_loss"):
        for mi,name in enumerate(MODEL_SPECS):
            pp=[]
            for pi,period in enumerate(periods):
                month_start=pd.Timestamp(period.start_time,tz="UTC")
                month_end=pd.Timestamp(period.end_time,tz="UTC")
                train=df[df["exit_execute_ts"]<month_start].copy()
                test=df[
                    (df["signal_date_ts"]>=month_start)
                    & (df["signal_date_ts"]<=month_end)
                ].copy()
                if len(train)<min_history or len(test)==0:
                    continue
                p,_,_=fit_predict(
                    train,test,name,target,month_start,
                    draws=1200,seed=SEED+10000+mi*100+pi
                )
                p["walk_month"]=str(period)
                pbase=(float(train[target].sum())+1)/(len(train)+2)
                p["baseline_p"]=pbase
                pp.append(p)
            p=pd.concat(pp,ignore_index=True)
            mm=metrics(p[target],p["p_mean"])
            bm=metrics(p[target],p["baseline_p"])
            pred_frames.append(p)
            rows.append({
                "target":target,"model":name,"status":MODEL_SPECS[name]["status"],
                "n":len(p),"events":int(p[target].sum()),
                "brier":mm["brier"],"logloss":mm["logloss"],"auc":mm["auc"],
                "baseline_brier":bm["brier"],"baseline_logloss":bm["logloss"],
            })
    return pd.DataFrame(rows),pd.concat(pred_frames,ignore_index=True)


def yearly_metrics(pred):
    rows=[]
    p=pred.copy()
    p["year"]=pd.to_datetime(p["signal_date"]).dt.year
    for (target,model,year),g in p.groupby(["target","model","year"]):
        if len(g)<5: continue
        mm=metrics(g[target],g["p_mean"])
        bm=metrics(g[target],g["baseline_p"])
        rows.append({
            "target":target,"model":model,"year":int(year),
            "n":len(g),"events":int(g[target].sum()),
            "brier":mm["brier"],"baseline_brier":bm["brier"],
            "brier_delta":mm["brier"]-bm["brier"],
            "auc":mm["auc"],
        })
    return pd.DataFrame(rows)


def monthly_cluster_bootstrap(pred, target, model_a, model_b=None, reps=3000):
    a=pred[(pred["target"]==target)&(pred["model"]==model_a)].copy()
    if model_b is None:
        a["loss_a"]=(a["p_mean"]-a[target])**2
        a["loss_b"]=(a["baseline_p"]-a[target])**2
    else:
        b=pred[(pred["target"]==target)&(pred["model"]==model_b)][
            ["signal_date","source","destination","p_mean"]
        ].rename(columns={"p_mean":"p_b"})
        a=a.merge(b,on=["signal_date","source","destination"],how="inner")
        a["loss_a"]=(a["p_mean"]-a[target])**2
        a["loss_b"]=(a["p_b"]-a[target])**2

    months=sorted(a["walk_month"].unique())
    grouped={m:a[a["walk_month"]==m] for m in months}
    rng=np.random.default_rng(SEED+70000+(0 if model_b is None else 1000))
    diffs=[]
    for _ in range(reps):
        sampled=rng.choice(months,size=len(months),replace=True)
        ga=pd.concat([grouped[m] for m in sampled],ignore_index=True)
        diffs.append(float(ga["loss_a"].mean()-ga["loss_b"].mean()))
    arr=np.asarray(diffs)
    return {
        "target":target,
        "model_a":model_a,
        "model_b":"BASELINE" if model_b is None else model_b,
        "mean_delta_brier":float(arr.mean()),
        "lo95":float(np.quantile(arr,0.025)),
        "hi95":float(np.quantile(arr,0.975)),
        "prob_a_better":float(np.mean(arr<0)),
    }


def regime_streak_table(df):
    rows=[]
    d=df.copy()
    d["streak_bucket"]=pd.cut(
        d["prior_negative_batch_streak"],
        bins=[-0.1,0.5,1.5,np.inf],
        labels=["0","1","2+"]
    )
    for adverse in range(4):
        for streak in ["0","1","2+"]:
            g=d[(d["market_adverse_count"]==adverse)&(d["streak_bucket"]==streak)]
            if len(g)==0: continue
            rows.append({
                "market_adverse_count":adverse,"streak":streak,
                "n":len(g),"losses":int(g["y_loss"].sum()),
                "loss_rate":float(g["y_loss"].mean()),
                "material_losses":int(g["y_material_loss"].sum()),
                "material_loss_rate":float(g["y_material_loss"].mean()),
                "median_return":float(g["net_roundtrip_return"].median()),
            })
    return pd.DataFrame(rows)


def calibration(pred):
    bins=[-1e-9,.20,.35,.50,.65,1.000001]
    labels=["<20%","20-35%","35-50%","50-65%",">=65%"]
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
                "mean_pred":float(h["p_mean"].mean()),
                "observed":float(h[target].mean()),
            })
    return pd.DataFrame(rows)


def robustness_verdict(walk,yearly):
    rows=[]
    for target in ("y_loss","y_material_loss"):
        static=walk[(walk.target==target)&(walk.model=="STATIC_STATE")].iloc[0]
        for name in MODEL_SPECS:
            r=walk[(walk.target==target)&(walk.model==name)].iloc[0]
            yy=yearly[(yearly.target==target)&(yearly.model==name)]
            years_better=int((yy["brier_delta"]<0).sum())
            years_auc=int((yy["auc"]>0.5).sum())
            beats_static=bool(r["brier"]<static["brier"]-1e-12) if name!="STATIC_STATE" else False
            robust=bool(
                name!="STATIC_STATE"
                and r["brier"]<r["baseline_brier"]
                and beats_static
                and years_better>=2
                and years_auc>=2
            )
            rows.append({
                "target":target,"model":name,
                "full_brier":r["brier"],"baseline_brier":r["baseline_brier"],
                "static_brier":static["brier"],
                "years_brier_better_than_baseline":years_better,
                "years_auc_gt_0_5":years_auc,
                "beats_static_full":beats_static,
                "robust_pass":robust,
            })
    return pd.DataFrame(rows)


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    d=build_dataset()
    d.to_csv(OUT/"dynamic_model_dataset.csv",index=False)

    train,test,holdout,holdout_pred=final_holdout(d)
    holdout.to_csv(OUT/"final1y_metrics.csv",index=False)
    holdout_pred.to_csv(OUT/"final1y_predictions.csv",index=False)

    walk,walk_pred=monthly_walkforward(d)
    walk.to_csv(OUT/"monthly_walkforward_metrics.csv",index=False)
    walk_pred.to_csv(OUT/"monthly_walkforward_predictions.csv",index=False)

    yearly=yearly_metrics(walk_pred)
    yearly.to_csv(OUT/"yearly_walkforward_metrics.csv",index=False)

    regime=regime_streak_table(d)
    regime.to_csv(OUT/"regime_streak_matrix.csv",index=False)

    cal=calibration(holdout_pred)
    cal.to_csv(OUT/"final1y_calibration.csv",index=False)

    verdict=robustness_verdict(walk,yearly)
    verdict.to_csv(OUT/"robustness_verdict.csv",index=False)

    boot=[]
    for target in ("y_loss","y_material_loss"):
        for name in ("REGIME_COMPACT","REGIME_DETAIL","DECAY_365","DECAY_180"):
            boot.append(monthly_cluster_bootstrap(walk_pred,target,name,None))
            boot.append(monthly_cluster_bootstrap(walk_pred,target,name,"STATIC_STATE"))
    bootdf=pd.DataFrame(boot)
    bootdf.to_csv(OUT/"monthly_cluster_bootstrap.csv",index=False)

    summary={
        "experiment":"RR_DYNAMIC_REGIME_RISK_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "production_changes":"NONE",
        "dataset_n":int(len(d)),
        "losses":int(d["y_loss"].sum()),
        "material_losses":int(d["y_material_loss"].sum()),
        "models":MODEL_SPECS,
        "pre_registered_robustness_rule":{
            "full_walkforward_brier_below_baseline":True,
            "full_walkforward_brier_below_static_state":True,
            "years_brier_better_than_baseline_min":2,
            "years_auc_gt_0_5_min":2,
        },
        "holdout":json.loads(holdout.to_json(orient="records")),
        "walkforward":json.loads(walk.to_json(orient="records")),
        "yearly":json.loads(yearly.to_json(orient="records")),
        "robustness":json.loads(verdict.to_json(orient="records")),
        "bootstrap":json.loads(bootdf.to_json(orient="records")),
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    lines=[
        "# RR DYNAMIC / REGIME-CONDITIONED RISK V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Dataset: {len(d)} executable current-U10 route episodes.",
        "Models and robustness rule were fixed before results.","",
        "## Final 1Y holdout","",
        "|Target|Model|Status|Brier|Baseline|AUC|",
        "|---|---|---|---:|---:|---:|",
    ]
    for _,r in holdout.iterrows():
        auc="n/a" if pd.isna(r["auc"]) else f"{r['auc']:.3f}"
        lines.append(f"|{r['target']}|{r['model']}|{r['status']}|{r['brier']:.3f}|{r['baseline_brier']:.3f}|{auc}|")

    lines += ["","## Full monthly causal walk-forward","",
              "|Target|Model|Brier|Baseline|AUC|",
              "|---|---|---:|---:|---:|"]
    for _,r in walk.iterrows():
        auc="n/a" if pd.isna(r["auc"]) else f"{r['auc']:.3f}"
        lines.append(f"|{r['target']}|{r['model']}|{r['brier']:.3f}|{r['baseline_brier']:.3f}|{auc}|")

    lines += ["","## Pre-registered robustness verdict","",
              "|Target|Model|Beats static|Years Brier win|Years AUC>0.5|PASS|",
              "|---|---|---|---:|---:|---|"]
    for _,r in verdict.iterrows():
        lines.append(
            f"|{r['target']}|{r['model']}|{bool(r['beats_static_full'])}|"
            f"{int(r['years_brier_better_than_baseline'])}|{int(r['years_auc_gt_0_5'])}|{bool(r['robust_pass'])}|"
        )

    lines += ["","## Guardrails","",
              "- No sizing/veto/live rule is changed.",
              "- 2527 routes are correlated counterfactual opportunities, not independent portfolio trades.",
              "- Recency half-lives 365d and 180d were fixed before results and are diagnostic.",
              "- PASS here means research robustness only, not production approval.",
              "",
              "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST"]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
