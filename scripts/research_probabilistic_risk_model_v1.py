from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

import scripts.research_negative_trade_forensics_v1 as fx

OUT = Path("research_artifacts/probabilistic_risk_model_v1")
SEED = 20261001
POSTERIOR_DRAWS = 5000

DOMAIN_COLS = [
    "domain_rr_caution",
    "domain_network_caution",
    "domain_market_caution",
    "domain_total_ratio_caution",
]

# Frozen before results.
MODEL_SPECS = {
    "COUNT_PRIMARY": {
        "features": ["warning_domain_count_centered"],
        "coef_prior_sd": 0.75,
        "status": "PRIMARY",
    },
    "DOMAINS_DIAGNOSTIC": {
        "features": DOMAIN_COLS,
        "coef_prior_sd": 0.60,
        "status": "DIAGNOSTIC",
    },
    "HYBRID_EXPLORATORY": {
        "features": [
            "warning_domain_count_centered",
            "effective_strength_z",
            "primary_reversal_z",
            "u10_breadth50_z",
        ],
        "coef_prior_sd": 0.50,
        "status": "EXPLORATORY",
    },
}
INTERCEPT_PRIOR_MEAN = math.log(0.25 / 0.75)
INTERCEPT_PRIOR_SD = 1.00


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
    runs,_,_,baseline=fx.simulate_runs(panel,events,active)
    trades=fx.completed_trade_rows(panel,runs,market,vol30,{})
    scored,score_summary=fx.domain_score(trades)

    scored["signal_date_ts"]=pd.to_datetime(scored["signal_date"],utc=True)
    scored=scored.sort_values(["signal_date_ts","source","destination"]).reset_index(drop=True)
    scored["y_loss"]=(scored["net_roundtrip_return"]<0).astype(int)
    scored["y_material_loss"]=(scored["net_roundtrip_return"]<=-0.10).astype(int)
    scored["warning_domain_count_centered"]=scored["warning_domain_count"].astype(float)-2.0

    # Standardization constants are computed on the full forensic sample only for
    # descriptive HYBRID research. LOO/prequential refits recompute z-scores inside
    # each train split to avoid target leakage from held-out feature scale.
    return panel,scored,score_summary,baseline


def prepare_xy(train,test,spec,target):
    tr=train.copy()
    te=test.copy()

    # Recompute train-only continuous standardization for HYBRID.
    for col in ["effective_strength","primary_reversal","u10_breadth50"]:
        vals=pd.to_numeric(tr[col],errors="coerce").astype(float)
        med=float(vals.median())
        sd=float(vals.std(ddof=0))
        if not math.isfinite(sd) or sd<1e-12:
            sd=1.0
        tr[col+"_z"]=(pd.to_numeric(tr[col],errors="coerce").fillna(med)-med)/sd
        te[col+"_z"]=(pd.to_numeric(te[col],errors="coerce").fillna(med)-med)/sd

    features=spec["features"]
    Xtr=tr[features].astype(float).to_numpy()
    Xte=te[features].astype(float).to_numpy()
    Xtr=np.column_stack([np.ones(len(Xtr)),Xtr])
    Xte=np.column_stack([np.ones(len(Xte)),Xte])
    y=tr[target].astype(int).to_numpy()
    return Xtr,y,Xte,features


def fit_bayes_logit(X,y,coef_prior_sd,draws=POSTERIOR_DRAWS,seed=SEED):
    p=X.shape[1]
    prior_mean=np.zeros(p,float)
    prior_mean[0]=INTERCEPT_PRIOR_MEAN
    prior_sd=np.full(p,float(coef_prior_sd))
    prior_sd[0]=INTERCEPT_PRIOR_SD
    prior_prec=1.0/(prior_sd**2)

    def objective(beta):
        eta=X@beta
        # stable logistic negative log likelihood
        nll=np.sum(np.logaddexp(0.0,eta)-y*eta)
        d=beta-prior_mean
        return float(nll+0.5*np.sum(prior_prec*d*d))

    def grad(beta):
        pr=sigmoid(X@beta)
        return X.T@(pr-y)+prior_prec*(beta-prior_mean)

    res=minimize(objective,prior_mean.copy(),jac=grad,method="BFGS",options={"maxiter":2000,"gtol":1e-9})
    beta=np.asarray(res.x,float)
    pr=sigmoid(X@beta)
    W=pr*(1-pr)
    H=X.T@(X*W[:,None])+np.diag(prior_prec)
    try:
        cov=np.linalg.inv(H)
    except np.linalg.LinAlgError:
        cov=np.linalg.pinv(H)
    cov=(cov+cov.T)/2.0
    eig=np.linalg.eigvalsh(cov)
    if eig.min()<=0:
        cov += np.eye(p)*(abs(eig.min())+1e-10)

    rng=np.random.default_rng(seed)
    samples=rng.multivariate_normal(beta,cov,size=draws)
    return {
        "map":beta,
        "cov":cov,
        "samples":samples,
        "success":bool(res.success),
        "message":str(res.message),
    }


def posterior_predict(fit,Xnew):
    eta=Xnew@fit["samples"].T
    ps=sigmoid(eta)
    return {
        "mean":ps.mean(axis=1),
        "median":np.median(ps,axis=1),
        "lo90":np.quantile(ps,0.05,axis=1),
        "hi90":np.quantile(ps,0.95,axis=1),
    }


def metrics(y,p):
    y=np.asarray(y,int); p=np.clip(np.asarray(p,float),1e-8,1-1e-8)
    brier=float(np.mean((p-y)**2))
    logloss=float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p)))
    # Mann-Whitney AUC.
    pos=np.where(y==1)[0]; neg=np.where(y==0)[0]
    if len(pos)==0 or len(neg)==0:
        auc=np.nan
    else:
        wins=0.0
        for i in pos:
            for j in neg:
                wins += 1.0 if p[i]>p[j] else (0.5 if p[i]==p[j] else 0.0)
        auc=float(wins/(len(pos)*len(neg)))
    return {"n":int(len(y)),"events":int(y.sum()),"brier":brier,"logloss":logloss,"auc":auc}


def loo_predictions(df,spec,target,seed_offset=0):
    rows=[]
    for i in range(len(df)):
        train=df.drop(index=df.index[i]).copy()
        test=df.iloc[[i]].copy()
        Xtr,y,Xte,_=prepare_xy(train,test,spec,target)
        fit=fit_bayes_logit(Xtr,y,spec["coef_prior_sd"],draws=2500,seed=SEED+seed_offset+i)
        pred=posterior_predict(fit,Xte)
        rows.append({
            "row":i,
            "signal_date":test.iloc[0]["signal_date"],
            "source":test.iloc[0]["source"],
            "destination":test.iloc[0]["destination"],
            "actual":int(test.iloc[0][target]),
            "p_mean":float(pred["mean"][0]),
            "p_median":float(pred["median"][0]),
            "p_lo90":float(pred["lo90"][0]),
            "p_hi90":float(pred["hi90"][0]),
        })
    out=pd.DataFrame(rows)
    return out,metrics(out["actual"],out["p_mean"])


def loo_beta_baseline(df,target):
    rows=[]
    for i in range(len(df)):
        y=df.drop(index=df.index[i])[target].astype(int)
        p=(float(y.sum())+1.0)/(len(y)+2.0)
        rows.append({"actual":int(df.iloc[i][target]),"p":p})
    x=pd.DataFrame(rows)
    return metrics(x["actual"],x["p"])


def prequential(df,spec,target,min_train=10,seed_offset=10000):
    rows=[]
    for i in range(min_train,len(df)):
        train=df.iloc[:i].copy()
        test=df.iloc[[i]].copy()
        Xtr,y,Xte,_=prepare_xy(train,test,spec,target)
        fit=fit_bayes_logit(Xtr,y,spec["coef_prior_sd"],draws=2500,seed=SEED+seed_offset+i)
        pred=posterior_predict(fit,Xte)
        # Bayesian constant baseline with Beta(1,1).
        pbase=(float(y.sum())+1.0)/(len(y)+2.0)
        rows.append({
            "row":i,"signal_date":test.iloc[0]["signal_date"],
            "source":test.iloc[0]["source"],"destination":test.iloc[0]["destination"],
            "actual":int(test.iloc[0][target]),
            "p_model":float(pred["mean"][0]),
            "p_baseline":pbase,
        })
    out=pd.DataFrame(rows)
    return out,metrics(out["actual"],out["p_model"]),metrics(out["actual"],out["p_baseline"])


def full_fit(df,spec,target):
    X,y,_,features=prepare_xy(df,df.iloc[:1].copy(),spec,target)
    fit=fit_bayes_logit(X,y,spec["coef_prior_sd"],draws=POSTERIOR_DRAWS,seed=SEED+777)
    smp=fit["samples"]
    coef_rows=[]
    names=["INTERCEPT",*features]
    for j,name in enumerate(names):
        vals=smp[:,j]
        if name=="INTERCEPT":
            coef_rows.append({
                "feature":name,
                "coef_median":float(np.median(vals)),
                "coef_lo90":float(np.quantile(vals,0.05)),
                "coef_hi90":float(np.quantile(vals,0.95)),
                "odds_ratio_median":None,
                "odds_ratio_lo90":None,
                "odds_ratio_hi90":None,
            })
        else:
            ors=np.exp(vals)
            coef_rows.append({
                "feature":name,
                "coef_median":float(np.median(vals)),
                "coef_lo90":float(np.quantile(vals,0.05)),
                "coef_hi90":float(np.quantile(vals,0.95)),
                "odds_ratio_median":float(np.median(ors)),
                "odds_ratio_lo90":float(np.quantile(ors,0.05)),
                "odds_ratio_hi90":float(np.quantile(ors,0.95)),
            })
    return fit,pd.DataFrame(coef_rows)


def count_curve(df,target):
    spec=MODEL_SPECS["COUNT_PRIMARY"]
    X,y,_,_=prepare_xy(df,df.iloc[:1].copy(),spec,target)
    fit=fit_bayes_logit(X,y,spec["coef_prior_sd"],draws=POSTERIOR_DRAWS,seed=SEED+999)
    test=pd.DataFrame({"warning_domain_count_centered":[s-2 for s in range(5)]})
    Xnew=np.column_stack([np.ones(5),test.to_numpy(float)])
    pred=posterior_predict(fit,Xnew)

    rows=[]
    for s in range(5):
        g=df[df["warning_domain_count"]==s]
        empirical=None if len(g)==0 else float(g[target].mean())
        # Beta(1,1) posterior empirical benchmark by score bucket.
        a=1+(0 if len(g)==0 else int(g[target].sum()))
        b=1+(0 if len(g)==0 else int(len(g)-g[target].sum()))
        rng=np.random.default_rng(SEED+2000+s)
        beta=rng.beta(a,b,size=100000)
        rows.append({
            "warning_domain_count":s,
            "trades":int(len(g)),
            "events":int(g[target].sum()) if len(g) else 0,
            "empirical_rate":empirical,
            "beta_binomial_mean":float(a/(a+b)),
            "beta_binomial_lo90":float(np.quantile(beta,0.05)),
            "beta_binomial_hi90":float(np.quantile(beta,0.95)),
            "model_p_mean":float(pred["mean"][s]),
            "model_p_median":float(pred["median"][s]),
            "model_p_lo90":float(pred["lo90"][s]),
            "model_p_hi90":float(pred["hi90"][s]),
        })
    return pd.DataFrame(rows)


def calibration_table(pred):
    d=pred.copy()
    # fixed probability bands; no data-driven threshold optimization.
    bins=[-1e-9,0.15,0.30,0.45,1.000001]
    labels=["<15%","15-30%","30-45%",">=45%"]
    d["band"]=pd.cut(d["p_mean"],bins=bins,labels=labels,include_lowest=True,right=False)
    rows=[]
    for band in labels:
        g=d[d["band"]==band]
        if len(g)==0: continue
        rows.append({
            "band":band,"n":int(len(g)),"events":int(g["actual"].sum()),
            "mean_predicted":float(g["p_mean"].mean()),
            "observed_rate":float(g["actual"].mean()),
        })
    return pd.DataFrame(rows)


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    _,df,score_summary,baseline_return=build_dataset()
    df.to_csv(OUT/"model_dataset.csv",index=False)

    all_results=[]
    coef_frames=[]
    loo_frames=[]
    preq_frames=[]

    for target in ("y_loss","y_material_loss"):
        baseline_loo=loo_beta_baseline(df,target)
        for model_name,spec in MODEL_SPECS.items():
            loo,loo_m=loo_predictions(df,spec,target,seed_offset=0 if target=="y_loss" else 5000)
            loo["model"]=model_name; loo["target"]=target
            loo_frames.append(loo)

            preq,preq_m,preq_base=prequential(df,spec,target)
            preq["model"]=model_name; preq["target"]=target
            preq_frames.append(preq)

            fit,coef=full_fit(df,spec,target)
            coef["model"]=model_name; coef["target"]=target
            coef_frames.append(coef)

            all_results.append({
                "target":target,"model":model_name,"status":spec["status"],
                "loo_brier":loo_m["brier"],"loo_logloss":loo_m["logloss"],"loo_auc":loo_m["auc"],
                "loo_baseline_brier":baseline_loo["brier"],
                "loo_baseline_logloss":baseline_loo["logloss"],
                "prequential_n":preq_m["n"],
                "prequential_events":preq_m["events"],
                "prequential_brier":preq_m["brier"],
                "prequential_logloss":preq_m["logloss"],
                "prequential_auc":preq_m["auc"],
                "prequential_baseline_brier":preq_base["brier"],
                "prequential_baseline_logloss":preq_base["logloss"],
                "fit_success":fit["success"],
            })

    results=pd.DataFrame(all_results)
    results.to_csv(OUT/"model_validation_metrics.csv",index=False)
    coefs=pd.concat(coef_frames,ignore_index=True)
    coefs.to_csv(OUT/"posterior_coefficients.csv",index=False)
    loos=pd.concat(loo_frames,ignore_index=True)
    loos.to_csv(OUT/"loo_predictions.csv",index=False)
    preqs=pd.concat(preq_frames,ignore_index=True)
    preqs.to_csv(OUT/"prequential_predictions.csv",index=False)

    curves=[]
    for target in ("y_loss","y_material_loss"):
        c=count_curve(df,target); c["target"]=target; curves.append(c)
    curves=pd.concat(curves,ignore_index=True)
    curves.to_csv(OUT/"count_probability_curve.csv",index=False)

    cal=[]
    for target in ("y_loss","y_material_loss"):
        p=loos[(loos.model=="COUNT_PRIMARY")&(loos.target==target)]
        ct=calibration_table(p)
        ct["target"]=target
        cal.append(ct)
    calibration=pd.concat(cal,ignore_index=True)
    calibration.to_csv(OUT/"primary_loo_calibration.csv",index=False)

    # Historical risk cards from LOO only; these are what the model would say without its own label.
    cards=loos[(loos.model=="COUNT_PRIMARY")&(loos.target=="y_loss")].copy()
    cards=cards.merge(
        df[["signal_date","source","destination","warning_domain_count","net_roundtrip_return"]],
        on=["signal_date","source","destination"],how="left"
    )
    cards=cards.sort_values("p_mean",ascending=False)
    cards.to_csv(OUT/"historical_loo_risk_cards.csv",index=False)

    summary={
        "experiment":"RR_PROBABILISTIC_RISK_MODEL_V1",
        "workflow_mode":"STRESS_TEST_ONLY",
        "test_level":"GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
        "production_changes":"NONE",
        "baseline_mature_return":baseline_return,
        "n_trades":int(len(df)),
        "loss_events":int(df["y_loss"].sum()),
        "material_loss_events":int(df["y_material_loss"].sum()),
        "primary_model":"COUNT_PRIMARY",
        "primary_model_locked_before_results":True,
        "prior":{
            "intercept_mean":INTERCEPT_PRIOR_MEAN,
            "intercept_sd":INTERCEPT_PRIOR_SD,
            "count_coef_sd":MODEL_SPECS["COUNT_PRIMARY"]["coef_prior_sd"],
        },
        "validation":json.loads(results.to_json(orient="records")),
        "count_curve":json.loads(curves.to_json(orient="records")),
        "fundamental_layer":"NOT_AVAILABLE_POINT_IN_TIME",
        "interpretation_guardrail":"POST_SELECTION_FEATURE_DEFINITIONS; LOO_IS_NOT_TRUE_OOS",
    }
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n",encoding="utf-8")

    # Report
    lines=[
        "# RR PROBABILISTIC RISK MODEL V1","",
        "Mode: STRESS_TEST_ONLY","",
        f"Dataset: {len(df)} unique completed trades; {int(df['y_loss'].sum())} losses; {int(df['y_material_loss'].sum())} material losses <= -10%.",
        f"Strategy control: {100*baseline_return:+.1f}% MATURE median.","",
        "Primary model was locked before results: COUNT_PRIMARY (warning-domain count only).",
        "DOMAINS and HYBRID are diagnostic/exploratory only.","",
        "## Validation","",
        "|Target|Model|Status|LOO Brier|LOO base|LOO AUC|Prequential n|Preq Brier|Preq base|Preq AUC|",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _,r in results.iterrows():
        auc="n/a" if pd.isna(r["loo_auc"]) else f"{r['loo_auc']:.3f}"
        pauc="n/a" if pd.isna(r["prequential_auc"]) else f"{r['prequential_auc']:.3f}"
        lines.append(
            f"|{r['target']}|{r['model']}|{r['status']}|{r['loo_brier']:.3f}|{r['loo_baseline_brier']:.3f}|{auc}|"
            f"{int(r['prequential_n'])}|{r['prequential_brier']:.3f}|{r['prequential_baseline_brier']:.3f}|{pauc}|"
        )

    lines += ["","## Primary COUNT probability curve","",
              "|Target|Warnings|Trades|Events|Empirical|Bayes bucket mean (90%)|Logit model mean (90%)|",
              "|---|---:|---:|---:|---:|---|---|"]
    for _,r in curves.iterrows():
        emp="n/a" if pd.isna(r["empirical_rate"]) else f"{100*r['empirical_rate']:.1f}%"
        lines.append(
            f"|{r['target']}|{int(r['warning_domain_count'])}|{int(r['trades'])}|{int(r['events'])}|{emp}|"
            f"{100*r['beta_binomial_mean']:.1f}% ({100*r['beta_binomial_lo90']:.1f}-{100*r['beta_binomial_hi90']:.1f}%)|"
            f"{100*r['model_p_mean']:.1f}% ({100*r['model_p_lo90']:.1f}-{100*r['model_p_hi90']:.1f}%)|"
        )

    lines += ["","## Primary LOO calibration","",
              "|Target|Predicted band|N|Events|Mean predicted|Observed|",
              "|---|---|---:|---:|---:|---:|"]
    for _,r in calibration.iterrows():
        lines.append(
            f"|{r['target']}|{r['band']}|{int(r['n'])}|{int(r['events'])}|"
            f"{100*r['mean_predicted']:.1f}%|{100*r['observed_rate']:.1f}%|"
        )

    lines += [
        "",
        "## Guardrails",
        "",
        "- 30 trades / 7 losses is a very small dataset.",
        "- Warning-domain definitions were discovered on known history; LOO does not erase that post-selection bias.",
        "- Bayesian shrinkage reduces overconfidence but cannot create missing information.",
        "- No probability threshold, veto, or position-sizing rule is production-approved.",
        "- Fundamental point-in-time data is not yet available and is excluded.",
        "",
        "TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST",
    ]
    (OUT/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("\n".join(lines))


if __name__=="__main__":
    main()
