from __future__ import annotations

import argparse
import io
import json
import math
import os
import subprocess
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd

from scripts.research_global_macro_risk_regime_v1 import (
    ANALYSIS_END,
    CRYPTO_DOWNLOAD_START,
    build_crypto_breadth,
    build_crypto_stress_episodes,
)
from scripts.research_official_liquidity_data_feasibility_v1 import (
    H41_URL,
    RRP_URL,
    _local_name,
    _request_bytes,
)
from scripts.research_oss_market_regime_comparator_v1 import add_crypto_mode
from scripts.research_relative_rotation_graph_intelligence import download_panel

REPO_ROOT = Path(__file__).resolve().parents[1]
TOTAL_ASSETS_SERIES = "RESPPA_N.WW"
TGA_SERIES = "RESPPLLDT_N.WW"
RRP_START = "2023-05-05"
CHANGE_WEEKS = (4, 13, 26)
EXPECTED_EPISODES = 8


def _source_commit() -> str:
    value = os.environ.get("SOURCE_COMMIT_SHA")
    if value:
        return value
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()


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
    path.write_text(
        json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def extract_h41_series(payload: bytes, series_name: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        data_names = [n for n in zf.namelist() if n.lower().endswith("h41_data.xml")]
        if not data_names:
            raise RuntimeError("H41_data.xml not found")
        root = ET.parse(zf.open(data_names[0])).getroot()
        matches = []
        for elem in root.iter():
            if _local_name(elem.tag) != "Series":
                continue
            if elem.attrib.get("SERIES_NAME") == series_name:
                matches.append(elem)
        if len(matches) != 1:
            raise RuntimeError(f"{series_name}: expected one H41 Series, found {len(matches)}")
        elem = matches[0]
        attrs = dict(elem.attrib)
        if attrs.get("CURRENCY") != "USD" or attrs.get("UNIT_MULT") != "1000000":
            raise RuntimeError(f"{series_name}: unexpected units {attrs}")
        rows = []
        for obs in elem:
            if _local_name(obs.tag) != "Obs":
                continue
            date = obs.attrib.get("TIME_PERIOD")
            value = obs.attrib.get("OBS_VALUE")
            if not date or value in (None, ""):
                continue
            try:
                numeric = float(value)
            except ValueError:
                continue
            rows.append({"observation_date": pd.Timestamp(date), "value_usd_mn": numeric})
        out = pd.DataFrame(rows).sort_values("observation_date").reset_index(drop=True)
        if out.empty:
            raise RuntimeError(f"{series_name}: no numeric observations")
        out["series_name"] = series_name
        return out[["observation_date", "series_name", "value_usd_mn"]]


def fetch_h41_components() -> tuple[pd.DataFrame, dict]:
    payload = _request_bytes(H41_URL, timeout=180)
    assets = extract_h41_series(payload, TOTAL_ASSETS_SERIES).rename(
        columns={"value_usd_mn": "fed_total_assets_usd_mn"}
    ).drop(columns=["series_name"])
    tga = extract_h41_series(payload, TGA_SERIES).rename(
        columns={"value_usd_mn": "tga_usd_mn"}
    ).drop(columns=["series_name"])
    merged = assets.merge(tga, on="observation_date", how="inner", validate="one_to_one")
    meta = {
        "bytes": len(payload),
        "assets_rows": int(len(assets)),
        "tga_rows": int(len(tga)),
        "common_rows": int(len(merged)),
        "first_common_date": merged["observation_date"].min().date().isoformat(),
        "last_common_date": merged["observation_date"].max().date().isoformat(),
        "assets_series": TOTAL_ASSETS_SERIES,
        "tga_series": TGA_SERIES,
    }
    return merged, meta


def fetch_rrp_daily() -> tuple[pd.DataFrame, dict]:
    payload = _request_bytes(
        RRP_URL,
        params={"startDate": RRP_START, "pageSize": "5000"},
        timeout=90,
    )
    data = json.loads(payload.decode("utf-8"))
    operations = (data.get("repo") or {}).get("operations") or []
    rows = []
    for row in operations:
        if "reverse" not in str(row.get("operationType", "")).lower():
            continue
        if not row.get("operationDate"):
            continue
        amount = pd.to_numeric(row.get("totalAmtAccepted"), errors="coerce")
        if pd.isna(amount):
            continue
        rows.append(
            {
                "observation_date": pd.Timestamp(row["operationDate"]),
                "rrp_usd_mn": float(amount) / 1_000_000.0,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("NY Fed returned no usable reverse-repo observations")
    out = (
        out.groupby("observation_date", as_index=False)["rrp_usd_mn"]
        .sum()
        .sort_values("observation_date")
        .reset_index(drop=True)
    )
    meta = {
        "bytes": len(payload),
        "rows": int(len(out)),
        "first_date": out["observation_date"].min().date().isoformat(),
        "last_date": out["observation_date"].max().date().isoformat(),
        "source_amount_units": "USD",
        "normalized_units": "USD_millions",
    }
    return out, meta


def build_net_liquidity(
    h41: pd.DataFrame,
    rrp: pd.DataFrame,
) -> pd.DataFrame:
    x = h41.merge(rrp, on="observation_date", how="inner", validate="one_to_one")
    x = x.sort_values("observation_date").reset_index(drop=True)
    x["net_liquidity_usd_mn"] = (
        x["fed_total_assets_usd_mn"] - x["tga_usd_mn"] - x["rrp_usd_mn"]
    )
    # H.4.1 Wednesday values are conservatively exposed to crypto only on Friday.
    x["availability_date"] = x["observation_date"] + pd.Timedelta(days=2)
    for weeks in CHANGE_WEEKS:
        x[f"net_liquidity_change_{weeks}w_usd_mn"] = (
            x["net_liquidity_usd_mn"] - x["net_liquidity_usd_mn"].shift(weeks)
        )
    return x


def align_liquidity_to_crypto(
    crypto_daily: pd.DataFrame,
    liquidity: pd.DataFrame,
) -> pd.DataFrame:
    left = crypto_daily.sort_values("date").copy()
    right = liquidity.sort_values("availability_date").copy()
    out = pd.merge_asof(
        left,
        right,
        left_on="date",
        right_on="availability_date",
        direction="backward",
    )
    out["liquidity_age_days"] = (
        out["date"] - out["availability_date"]
    ).dt.days
    return out


def event_snapshots(
    daily: pd.DataFrame,
    episodes: pd.DataFrame,
) -> pd.DataFrame:
    by_date = daily.set_index("date")
    rows: list[dict] = []
    for episode_id, (_, ep) in enumerate(episodes.iterrows(), start=1):
        for event_type, field in (
            ("ENTRY_SIGNAL", "entry_signal_date"),
            ("EXIT_SIGNAL", "exit_signal_date"),
        ):
            value = ep.get(field)
            if pd.isna(value):
                continue
            date = pd.Timestamp(value)
            if date not in by_date.index:
                continue
            row = by_date.loc[date]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            record = {
                "episode_id": episode_id,
                "event_type": event_type,
                "signal_date": date,
                "breadth": int(row["breadth"]),
                "crypto_mode": row["crypto_mode"],
                "liquidity_observation_date": row.get("observation_date"),
                "liquidity_availability_date": row.get("availability_date"),
                "liquidity_age_days": row.get("liquidity_age_days"),
                "net_liquidity_usd_mn": row.get("net_liquidity_usd_mn"),
            }
            for weeks in CHANGE_WEEKS:
                col = f"net_liquidity_change_{weeks}w_usd_mn"
                change = row.get(col)
                record[col] = change
                record[f"change_{weeks}w_negative"] = bool(change < 0) if pd.notna(change) else None
                record[f"change_{weeks}w_positive"] = bool(change > 0) if pd.notna(change) else None
            rows.append(record)
    return pd.DataFrame(rows)


def sign_stats(
    daily: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> dict:
    result: dict = {"entry": {}, "exit": {}}
    normal = daily[daily["crypto_mode"] == "NORMAL"]
    defensive = daily[daily["crypto_mode"] == "DEFENSIVE"]
    entries = snapshots[snapshots["event_type"] == "ENTRY_SIGNAL"]
    exits = snapshots[snapshots["event_type"] == "EXIT_SIGNAL"]

    for weeks in CHANGE_WEEKS:
        col = f"net_liquidity_change_{weeks}w_usd_mn"

        normal_valid = normal[col].dropna()
        entry_valid = entries[col].dropna()
        normal_rate = float((normal_valid < 0).mean()) if len(normal_valid) else None
        entry_rate = float((entry_valid < 0).mean()) if len(entry_valid) else None
        entry_lift = (
            float(entry_rate / normal_rate)
            if entry_rate is not None and normal_rate not in (None, 0.0)
            else None
        )
        result["entry"][f"{weeks}w"] = {
            "event_sign": "negative",
            "signals_with_data": int(len(entry_valid)),
            "negative_count": int((entry_valid < 0).sum()),
            "event_rate": entry_rate,
            "normal_days_with_data": int(len(normal_valid)),
            "normal_baseline_rate": normal_rate,
            "descriptive_lift": entry_lift,
        }

        def_valid = defensive[col].dropna()
        exit_valid = exits[col].dropna()
        def_rate = float((def_valid > 0).mean()) if len(def_valid) else None
        exit_rate = float((exit_valid > 0).mean()) if len(exit_valid) else None
        exit_lift = (
            float(exit_rate / def_rate)
            if exit_rate is not None and def_rate not in (None, 0.0)
            else None
        )
        result["exit"][f"{weeks}w"] = {
            "event_sign": "positive",
            "signals_with_data": int(len(exit_valid)),
            "positive_count": int((exit_valid > 0).sum()),
            "event_rate": exit_rate,
            "defensive_days_with_data": int(len(def_valid)),
            "defensive_baseline_rate": def_rate,
            "descriptive_lift": exit_lift,
        }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only official Fed net-liquidity event study versus frozen crypto stress."
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT
        / "research_artifacts"
        / "official_fed_net_liquidity_vs_crypto_stress_v1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    h41, h41_meta = fetch_h41_components()
    rrp, rrp_meta = fetch_rrp_daily()
    liquidity = build_net_liquidity(h41, rrp)
    if liquidity.empty:
        raise RuntimeError("No same-date H41/RRP liquidity observations")

    panel, crypto_meta = download_panel(CRYPTO_DOWNLOAD_START, ANALYSIS_END)
    crypto = build_crypto_breadth(panel)
    episodes = build_crypto_stress_episodes(crypto)
    if len(episodes) != EXPECTED_EPISODES:
        raise RuntimeError(
            f"Frozen full-history episode gate failed: expected {EXPECTED_EPISODES}, got {len(episodes)}"
        )
    if int(episodes["exit_execution_date"].notna().sum()) != EXPECTED_EPISODES:
        raise RuntimeError("Expected all full-history episodes to be closed")

    daily = add_crypto_mode(crypto, episodes)
    daily = align_liquidity_to_crypto(daily, liquidity)
    snapshots = event_snapshots(daily, episodes)
    stats = sign_stats(daily, snapshots)

    run_id = f"OFFICIAL_FED_NET_LIQUIDITY_V1_{_source_commit()[:12]}"
    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    h41.to_csv(run_dir / "h41_official_components.csv", index=False)
    rrp.to_csv(run_dir / "nyfed_rrp_daily.csv", index=False)
    liquidity.to_csv(run_dir / "official_weekly_net_liquidity.csv", index=False)
    episodes.to_csv(run_dir / "crypto_stress_episodes.csv", index=False)
    daily.to_csv(run_dir / "crypto_daily_with_asof_liquidity.csv", index=False)
    snapshots.to_csv(run_dir / "event_liquidity_snapshots.csv", index=False)

    latest = daily.dropna(subset=["net_liquidity_usd_mn"]).iloc[-1]
    summary = {
        "run_id": run_id,
        "source_commit_sha": _source_commit(),
        "status": "RESEARCH_ONLY_OFFICIAL_FED_NET_LIQUIDITY_EVENT_STUDY_EXECUTED",
        "analysis_end": ANALYSIS_END.date().isoformat(),
        "production_change": False,
        "trading_actions": False,
        "frozen_crypto_rule_changed": False,
        "liquidity_design": {
            "identity": "RESPPA_N.WW - RESPPLLDT_N.WW - NYFED_ON_RRP",
            "units": "USD_millions",
            "same_observation_date_required": True,
            "availability_lag_calendar_days": 2,
            "change_horizons_weeks": list(CHANGE_WEEKS),
            "threshold_tuning": False,
        },
        "h41_source": h41_meta,
        "nyfed_rrp_source": rrp_meta,
        "liquidity_rows": int(len(liquidity)),
        "first_liquidity_observation": liquidity["observation_date"].min().date().isoformat(),
        "last_liquidity_observation": liquidity["observation_date"].max().date().isoformat(),
        "first_liquidity_available": liquidity["availability_date"].min().date().isoformat(),
        "last_liquidity_available": liquidity["availability_date"].max().date().isoformat(),
        "crypto_dataset": crypto_meta,
        "episode_count": int(len(episodes)),
        "event_snapshot_count": int(len(snapshots)),
        "sign_statistics": stats,
        "latest_fixed_end_state": {
            "crypto_date": pd.Timestamp(latest["date"]).date().isoformat(),
            "crypto_breadth": int(latest["breadth"]),
            "crypto_mode": latest["crypto_mode"],
            "liquidity_observation_date": pd.Timestamp(latest["observation_date"]).date().isoformat(),
            "liquidity_availability_date": pd.Timestamp(latest["availability_date"]).date().isoformat(),
            "net_liquidity_usd_mn": float(latest["net_liquidity_usd_mn"]),
            **{
                f"change_{weeks}w_usd_mn": (
                    float(latest[f"net_liquidity_change_{weeks}w_usd_mn"])
                    if pd.notna(latest[f"net_liquidity_change_{weeks}w_usd_mn"])
                    else None
                )
                for weeks in CHANGE_WEEKS
            },
        },
        "limitations": [
            "This is descriptive retrospective research, not a trading rule.",
            "H.4.1 current package is not an ALFRED vintage archive; historical revisions are not modeled.",
            "Wednesday H.4.1 observations are conservatively delayed to Friday availability.",
            "Only same-date Wednesday NY Fed ON RRP is combined with the H.4.1 components.",
            "Daily baseline rows repeat the latest weekly liquidity state and are serially correlated.",
            "Eight crypto episodes are not IID; descriptive lift is not statistical significance.",
        ],
    }
    _write_json(run_dir / "summary.json", summary)

    print(json.dumps(_json_safe(summary), indent=2, sort_keys=True, allow_nan=False))
    print(f"output={run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
