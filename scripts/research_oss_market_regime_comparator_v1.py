from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

from scripts.research_defensive_low_vol_untouched import ASSETS
from scripts.research_global_macro_risk_regime_v1 import (
    ANALYSIS_END,
    CRYPTO_DOWNLOAD_START,
    build_crypto_breadth,
    build_crypto_stress_episodes,
    known_2026_episode_check,
)
from scripts.research_relative_rotation_graph_intelligence import download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]
MARKET_DOWNLOAD_START = pd.Timestamp("2023-04-01")
SOURCE_NAME = "zhuy9/market-rotation"
SOURCE_LICENSE = "MIT"

SECTORS = ("XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC")
DEFENSIVE = ("XLV", "XLP", "XLU")
CYCLICAL = ("XLK", "XLY", "XLI", "XLF", "XLE", "XLB")
MARKET_TICKERS = (*SECTORS, "SPY", "QQQ", "IWM", "RSP", "IEF", "TLT", "HYG", "LQD", "GLD", "^VIX")
EVENT_OFFSETS = (-30, -14, -7, 0, 7, 14, 30)


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return _json_safe(value.item())
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def classify_external_regime(row: dict) -> tuple[str, str, list[str]]:
    spy = row.get("SPY_return_5d")
    pos = int(row.get("sector_positive_count_5d", 0))
    neg = int(row.get("sector_negative_count_5d", 0))
    disp = row.get("sector_dispersion_5d")

    confirmations: list[str] = []
    if _gt(row.get("gld_vs_spy_5d"), 0.0):
        confirmations.append("GLD_OUTPERFORMED_SPY")
    if _gt(row.get("ief_vs_spy_5d"), 0.0) or _gt(row.get("tlt_vs_spy_5d"), 0.0):
        confirmations.append("TREASURY_OUTPERFORMED_SPY")
    if _lt(row.get("hyg_vs_lqd_5d"), 0.0):
        confirmations.append("HYG_UNDERPERFORMED_LQD")
    if _gt(row.get("vix_return_5d"), 0.0):
        confirmations.append("VIX_ROSE")

    if _lt(spy, -0.01) and pos <= 4 and len(confirmations) >= 2:
        return "BROAD_RISK_OFF", ("high" if len(confirmations) >= 3 else "medium"), confirmations

    defensive = (
        _gte(row.get("defensive_spread_5d"), 0.01)
        and int(row.get("defensive_outperform_count", 0)) >= 2
        and (_lt(row.get("qqq_vs_spy_5d"), 0.0) or _lt(row.get("iwm_vs_spy_5d"), 0.0))
    )
    if defensive:
        conf = "high" if int(row.get("defensive_outperform_count", 0)) >= 3 else "medium"
        return "DEFENSIVE_ROTATION", conf, ["DEFENSIVE_SECTORS_LED"]

    risk_on = (
        _gt(spy, 0.01)
        and pos >= 7
        and _gte(row.get("rsp_vs_spy_5d"), 0.0)
        and _gte(row.get("hyg_vs_lqd_5d"), 0.0)
    )
    if risk_on:
        return "BROAD_RISK_ON", ("high" if pos >= 9 else "medium"), ["BROAD_PARTICIPATION", "CREDIT_STRENGTH"]

    internal = (
        spy is not None
        and pd.notna(spy)
        and abs(float(spy)) <= 0.02
        and pos >= 3
        and neg >= 3
        and _gte(disp, 0.02)
    )
    if internal:
        return "INTERNAL_ROTATION", ("high" if _gte(disp, 0.04) else "medium"), ["ELEVATED_SECTOR_DISPERSION"]

    return "MIXED", "low", []


def _gt(value, threshold: float) -> bool:
    return value is not None and pd.notna(value) and float(value) > threshold


def _lt(value, threshold: float) -> bool:
    return value is not None and pd.notna(value) and float(value) < threshold


def _gte(value, threshold: float) -> bool:
    return value is not None and pd.notna(value) and float(value) >= threshold


def market_availability_date(session_date: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(session_date).normalize() + pd.Timedelta(days=1)


def _normalize_yfinance_close(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        raise RuntimeError("yfinance returned no market data")
    if isinstance(raw.columns, pd.MultiIndex):
        level0 = set(map(str, raw.columns.get_level_values(0)))
        level1 = set(map(str, raw.columns.get_level_values(1)))
        if "Close" in level0:
            close = raw["Close"].copy()
        elif "Close" in level1:
            close = raw.xs("Close", axis=1, level=1).copy()
        else:
            raise RuntimeError("yfinance payload has no Close field")
    else:
        if "Close" not in raw.columns:
            raise RuntimeError("yfinance payload has no Close column")
        close = raw[["Close"]].copy()
        close.columns = ["SPY"]

    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    close = close.sort_index()
    close.columns = [str(c) for c in close.columns]
    return close


def download_market_panel(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    raw = yf.download(
        tickers=list(MARKET_TICKERS),
        start=start.date().isoformat(),
        end=(end + pd.Timedelta(days=1)).date().isoformat(),
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=True,
        group_by="column",
    )
    close = _normalize_yfinance_close(raw)
    missing = [ticker for ticker in MARKET_TICKERS if ticker not in close.columns]
    if missing:
        raise RuntimeError(f"market data missing required tickers: {missing}")
    return close[list(MARKET_TICKERS)]


def build_market_regimes(close: pd.DataFrame) -> pd.DataFrame:
    ret5 = close.pct_change(5, fill_method=None)
    ratio_rsp_spy = (close["RSP"] / close["SPY"]).pct_change(5, fill_method=None)
    ratio_hyg_lqd = (close["HYG"] / close["LQD"]).pct_change(5, fill_method=None)

    rows: list[dict] = []
    for date in close.index:
        r = ret5.loc[date]
        sector_values = pd.to_numeric(r[list(SECTORS)], errors="coerce").dropna()
        spy = r.get("SPY")
        defensive_values = pd.to_numeric(r[list(DEFENSIVE)], errors="coerce").dropna()
        cyclical_values = pd.to_numeric(r[list(CYCLICAL)], errors="coerce").dropna()

        row = {
            "market_session_date": pd.Timestamp(date),
            "available_date": market_availability_date(pd.Timestamp(date)),
            "SPY_return_5d": spy,
            "sector_positive_count_5d": int((sector_values > 0).sum()),
            "sector_negative_count_5d": int((sector_values < 0).sum()),
            "sector_known_count_5d": int(len(sector_values)),
            "sector_dispersion_5d": float(sector_values.std(ddof=1)) if len(sector_values) >= 2 else np.nan,
            "rsp_vs_spy_5d": ratio_rsp_spy.loc[date],
            "hyg_vs_lqd_5d": ratio_hyg_lqd.loc[date],
            "defensive_spread_5d": (
                float(defensive_values.mean() - cyclical_values.mean())
                if len(defensive_values) and len(cyclical_values)
                else np.nan
            ),
            "defensive_outperform_count": (
                int((defensive_values > float(spy)).sum())
                if pd.notna(spy)
                else 0
            ),
            "qqq_vs_spy_5d": (r.get("QQQ") - spy) if pd.notna(r.get("QQQ")) and pd.notna(spy) else np.nan,
            "iwm_vs_spy_5d": (r.get("IWM") - spy) if pd.notna(r.get("IWM")) and pd.notna(spy) else np.nan,
            "gld_vs_spy_5d": (r.get("GLD") - spy) if pd.notna(r.get("GLD")) and pd.notna(spy) else np.nan,
            "ief_vs_spy_5d": (r.get("IEF") - spy) if pd.notna(r.get("IEF")) and pd.notna(spy) else np.nan,
            "tlt_vs_spy_5d": (r.get("TLT") - spy) if pd.notna(r.get("TLT")) and pd.notna(spy) else np.nan,
            "vix_return_5d": r.get("^VIX"),
        }
        regime, confidence, reasons = classify_external_regime(row)
        row["market_regime"] = regime
        row["market_regime_confidence"] = confidence
        row["market_regime_reasons"] = "|".join(reasons)
        rows.append(row)
    return pd.DataFrame(rows)


def align_market_to_crypto(crypto: pd.DataFrame, market: pd.DataFrame) -> pd.DataFrame:
    left = crypto.sort_values("date").copy()
    right = market.sort_values("available_date").copy()
    out = pd.merge_asof(
        left,
        right,
        left_on="date",
        right_on="available_date",
        direction="backward",
    )
    return out


def add_crypto_mode(daily: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    out["crypto_mode"] = "NORMAL"
    for _, ep in episodes.iterrows():
        entry = pd.Timestamp(ep["entry_execution_date"])
        exit_value = ep.get("exit_execution_date")
        if pd.isna(exit_value):
            mask = out["date"] >= entry
        else:
            exit_date = pd.Timestamp(exit_value)
            mask = (out["date"] >= entry) & (out["date"] < exit_date)
        out.loc[mask, "crypto_mode"] = "DEFENSIVE"
    return out


def build_event_study(daily: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    by_date = daily.set_index("date")
    rows: list[dict] = []
    for _, ep in episodes.iterrows():
        for event_type, field in (("ENTRY_SIGNAL", "entry_signal_date"), ("EXIT_SIGNAL", "exit_signal_date")):
            signal = ep.get(field)
            if pd.isna(signal):
                continue
            signal = pd.Timestamp(signal)
            for offset in EVENT_OFFSETS:
                target = signal + pd.Timedelta(days=offset)
                if target not in by_date.index:
                    continue
                x = by_date.loc[target]
                if isinstance(x, pd.DataFrame):
                    x = x.iloc[-1]
                rows.append({
                    "event_type": event_type,
                    "signal_date": signal,
                    "offset_days": offset,
                    "date": target,
                    "breadth": int(x["breadth"]),
                    "crypto_mode": x["crypto_mode"],
                    "market_regime": x.get("market_regime"),
                    "market_session_date": x.get("market_session_date"),
                })
    return pd.DataFrame(rows)


def nearest_preceding_regime(
    daily: pd.DataFrame,
    episodes: pd.DataFrame,
    event_field: str,
    target_regime: str,
    lookback_days: int = 30,
) -> pd.DataFrame:
    rows: list[dict] = []
    for _, ep in episodes.iterrows():
        signal = ep.get(event_field)
        if pd.isna(signal):
            continue
        signal = pd.Timestamp(signal)
        window = daily[
            (daily["date"] <= signal)
            & (daily["date"] >= signal - pd.Timedelta(days=lookback_days))
            & (daily["market_regime"] == target_regime)
        ]
        if window.empty:
            rows.append({"signal_date": signal, "target_regime": target_regime, "found": False, "regime_date": pd.NaT, "lead_days": np.nan})
        else:
            row = window.iloc[-1]
            regime_date = pd.Timestamp(row["date"])
            rows.append({
                "signal_date": signal,
                "target_regime": target_regime,
                "found": True,
                "regime_date": regime_date,
                "lead_days": int((signal - regime_date).days),
            })
    return pd.DataFrame(rows)


def regime_distribution(daily: pd.DataFrame) -> pd.DataFrame:
    x = daily.dropna(subset=["market_regime"]).copy()
    counts = x.groupby(["crypto_mode", "market_regime"], as_index=False).size()
    totals = counts.groupby("crypto_mode")["size"].transform("sum")
    counts["share"] = counts["size"] / totals
    return counts.sort_values(["crypto_mode", "share"], ascending=[True, False])


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Research-only OSS market regime comparator against frozen crypto stress.")
    p.add_argument("--output-root", type=Path, default=REPO_ROOT / "research_artifacts" / "oss_market_regime_comparator_v1")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    panel, crypto_meta = download_panel(CRYPTO_DOWNLOAD_START, ANALYSIS_END)
    crypto = build_crypto_breadth(panel)
    episodes = build_crypto_stress_episodes(crypto)
    known_check = known_2026_episode_check(episodes)
    if not known_check["pass"]:
        raise RuntimeError("Frozen crypto stress integration check failed.")

    close = download_market_panel(MARKET_DOWNLOAD_START, ANALYSIS_END.tz_localize(None))
    market = build_market_regimes(close)
    daily = align_market_to_crypto(crypto, market)
    daily = add_crypto_mode(daily, episodes)

    event_study = build_event_study(daily, episodes)
    entry_leads = nearest_preceding_regime(daily, episodes, "entry_signal_date", "BROAD_RISK_OFF")
    exit_leads = nearest_preceding_regime(daily, episodes, "exit_signal_date", "BROAD_RISK_ON")
    distribution = regime_distribution(daily)

    run_id = f"OSS_MARKET_REGIME_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    market.to_csv(run_dir / "market_regime_sessions.csv", index=False)
    daily.to_csv(run_dir / "crypto_market_regime_daily.csv", index=False)
    episodes.to_csv(run_dir / "crypto_stress_episodes.csv", index=False)
    event_study.to_csv(run_dir / "event_study.csv", index=False)
    entry_leads.to_csv(run_dir / "entry_risk_off_leads.csv", index=False)
    exit_leads.to_csv(run_dir / "exit_risk_on_leads.csv", index=False)
    distribution.to_csv(run_dir / "regime_distribution_by_crypto_mode.csv", index=False)

    latest = daily.dropna(subset=["market_regime"]).iloc[-1]
    stress_rows = daily[daily["crypto_mode"] == "DEFENSIVE"]
    normal_rows = daily[daily["crypto_mode"] == "NORMAL"]
    summary = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "RESEARCH_ONLY_OSS_MARKET_REGIME_COMPARATOR_EXECUTED",
        "analysis_end": ANALYSIS_END.date().isoformat(),
        "external_methodology": {
            "source": SOURCE_NAME,
            "license": SOURCE_LICENSE,
            "threshold_tuning": False,
            "same_date_us_close_lookahead_allowed": False,
            "market_availability_lag_calendar_days": 1,
        },
        "frozen_crypto_rule_changed": False,
        "known_2026_episode_check": known_check,
        "crypto_dataset": crypto_meta,
        "episode_count": int(len(episodes)),
        "market_session_count": int(len(market)),
        "aligned_crypto_days_with_market_regime": int(daily["market_regime"].notna().sum()),
        "conditional_regime_rates": {
            "broad_risk_off_given_crypto_defensive": float((stress_rows["market_regime"] == "BROAD_RISK_OFF").mean()) if len(stress_rows) else None,
            "broad_risk_off_given_crypto_normal": float((normal_rows["market_regime"] == "BROAD_RISK_OFF").mean()) if len(normal_rows) else None,
            "broad_risk_on_given_crypto_defensive": float((stress_rows["market_regime"] == "BROAD_RISK_ON").mean()) if len(stress_rows) else None,
            "broad_risk_on_given_crypto_normal": float((normal_rows["market_regime"] == "BROAD_RISK_ON").mean()) if len(normal_rows) else None,
        },
        "entry_risk_off_leads": entry_leads.to_dict(orient="records"),
        "exit_risk_on_leads": exit_leads.to_dict(orient="records"),
        "latest_fixed_end_state": {
            "date": pd.Timestamp(latest["date"]).date().isoformat(),
            "crypto_breadth": int(latest["breadth"]),
            "crypto_mode": latest["crypto_mode"],
            "market_regime": latest["market_regime"],
            "market_regime_confidence": latest["market_regime_confidence"],
            "market_session_date": pd.Timestamp(latest["market_session_date"]).date().isoformat(),
        },
        "limitations": [
            "External regime is a U.S. cross-asset market regime, not a world-economy causal model.",
            "Yahoo adjusted history can be revised; point-in-time vendor vintages are not modeled.",
            "The +1 calendar-day market availability lag is conservative for crypto 00:00 UTC decisions.",
            "No external threshold was tuned on CryptoSignals evidence.",
            "Association is not a trading rule and does not authorize swaps.",
        ],
    }
    _write_json(run_dir / "summary.json", summary)
    print(json.dumps(_json_safe(summary), indent=2, sort_keys=True, allow_nan=False))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
