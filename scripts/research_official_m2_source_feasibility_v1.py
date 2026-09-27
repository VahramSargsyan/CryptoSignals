from __future__ import annotations

import argparse
import io
import json
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd

from scripts.research_official_liquidity_data_feasibility_v1 import (
    _local_name,
    _request_bytes,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
H6_URL = "https://www.federalreserve.gov/datadownload/Output.aspx?rel=H6&filetype=zip"
TARGET_SERIES = "M2.M"


def inspect_h6_m2(payload: bytes) -> tuple[pd.DataFrame, dict]:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        matches = []
        discovered = []
        for name in xml_names:
            try:
                root = ET.parse(zf.open(name)).getroot()
            except ET.ParseError:
                continue
            for elem in root.iter():
                if _local_name(elem.tag) != "Series":
                    continue
                series_name = elem.attrib.get("SERIES_NAME", "")
                if "M2" in series_name:
                    discovered.append({"file": name, **dict(elem.attrib)})
                if series_name == TARGET_SERIES:
                    matches.append((name, elem))

        if len(matches) != 1:
            raise RuntimeError(
                f"{TARGET_SERIES}: expected exactly one Series, found {len(matches)}; "
                f"M2-like={discovered[:30]}"
            )

        source_file, elem = matches[0]
        attrs = dict(elem.attrib)
        rows = []
        for obs in elem:
            if _local_name(obs.tag) != "Obs":
                continue
            period = obs.attrib.get("TIME_PERIOD")
            value = obs.attrib.get("OBS_VALUE")
            if not period or value in (None, ""):
                continue
            try:
                numeric = float(value)
            except ValueError:
                continue
            rows.append({"period": period, "m2_value": numeric})

        frame = pd.DataFrame(rows)
        if frame.empty:
            raise RuntimeError("M2.M resolved but has no numeric observations")
        frame["period_date"] = pd.to_datetime(frame["period"], errors="coerce")
        frame = frame.dropna(subset=["period_date"]).sort_values("period_date").reset_index(drop=True)

        meta = {
            "source_file": source_file,
            "series_name": TARGET_SERIES,
            "series_attributes": attrs,
            "rows": int(len(frame)),
            "first_period": frame["period"].iloc[0],
            "last_period": frame["period"].iloc[-1],
            "first_value": float(frame["m2_value"].iloc[0]),
            "last_value": float(frame["m2_value"].iloc[-1]),
            "zip_bytes": len(payload),
            "xml_files": xml_names,
        }
        return frame, meta


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Official H6 M2 source feasibility.")
    p.add_argument(
        "--output-root",
        type=Path,
        default=REPO_ROOT / "research_artifacts" / "official_m2_source_feasibility_v1",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = _request_bytes(H6_URL, timeout=180)
    frame, meta = inspect_h6_m2(payload)

    status = "PASS_OFFICIAL_M2_SOURCE_RESOLVED"
    report = {
        "status": status,
        "source": "Federal Reserve Board H.6 DDP",
        "url": H6_URL,
        "target_series": TARGET_SERIES,
        "metadata": meta,
        "checks": {
            "unique_series": True,
            "monthly_history_before_2022": bool(frame["period_date"].min() <= pd.Timestamp("2021-01-01")),
            "history_through_2026": bool(frame["period_date"].max() >= pd.Timestamp("2026-08-01")),
            "enough_12m_warmup_for_crypto_2023": bool(frame["period_date"].min() <= pd.Timestamp("2022-01-01")),
        },
    }

    out = args.output_root
    out.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out / "official_h6_m2_monthly.csv", index=False)
    (out / "summary.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
