from __future__ import annotations

import io
import json
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

H41_URL = "https://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&filetype=zip"
RRP_URL = "https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json"
RRP_START = "2023-05-05"
TOTAL_ASSETS_SERIES = "RESPPA_N.WW"
TGA_CODE = "DEPUSTG"

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "research_artifacts" / "official_liquidity_data_feasibility_v1"


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _request_bytes(url: str, *, params: dict[str, str] | None = None, timeout: int = 180) -> bytes:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "CryptoSignals research feasibility"})
    last = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                if getattr(response, "status", 200) != 200:
                    raise RuntimeError(f"HTTP {response.status} for {url}")
                return response.read()
        except Exception as exc:
            last = exc
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("unreachable") from last


def _series_rows(root: ET.Element) -> list[dict]:
    out: list[dict] = []
    for elem in root.iter():
        if _local_name(elem.tag) != "Series":
            continue
        attrs = {str(k): str(v) for k, v in elem.attrib.items()}
        obs = [x for x in elem if _local_name(x.tag) == "Obs"]
        dates = [x.attrib.get("TIME_PERIOD") for x in obs if x.attrib.get("TIME_PERIOD")]
        values = [x.attrib.get("OBS_VALUE") for x in obs if x.attrib.get("OBS_VALUE") not in (None, "")]
        out.append({
            "attrs": attrs,
            "observation_count": len(obs),
            "value_count": len(values),
            "first_date": min(dates) if dates else None,
            "last_date": max(dates) if dates else None,
        })
    return out


def _contains_token(attrs: dict[str, str], token: str) -> bool:
    token = token.upper()
    return any(token in str(v).upper() for v in attrs.values())


def inspect_h41(payload: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        names = sorted(zf.namelist())
        data_names = [n for n in names if n.lower().endswith("h41_data.xml")]
        if not data_names:
            data_names = [n for n in names if n.lower().endswith("_data.xml")]
        if not data_names:
            raise RuntimeError(f"No H41 data XML found in archive: {names[:30]}")
        data_name = data_names[0]
        root = ET.parse(zf.open(data_name)).getroot()
        series = _series_rows(root)

        attribute_keys = sorted({k for row in series for k in row["attrs"]})
        total_matches = [row for row in series if row["attrs"].get("SERIES_NAME") == TOTAL_ASSETS_SERIES]
        if not total_matches:
            total_matches = [row for row in series if _contains_token(row["attrs"], TOTAL_ASSETS_SERIES)]

        tga_attr_matches = [row for row in series if _contains_token(row["attrs"], TGA_CODE)]

        structure_hits: list[dict] = []
        for name in names:
            if not name.lower().endswith((".xml", ".txt")) or name == data_name:
                continue
            try:
                raw = zf.read(name)
                text = raw.decode("utf-8", errors="ignore")
            except Exception:
                continue
            low = text.lower()
            if "treasury" in low and "general account" in low:
                pos = low.find("general account")
                structure_hits.append({
                    "file": name,
                    "excerpt": text[max(0, pos - 350): pos + 500],
                })

        # If the data Series do not directly expose DEPUSTG, retain a compact inventory
        # of series whose attributes include likely Treasury/deposit concepts.
        tga_fallback_candidates = [
            row for row in series
            if any(
                token in " ".join(row["attrs"].values()).upper()
                for token in ("DEPUST", "TREAS", "DEPOSIT")
            )
        ]

        unique_tga_series = sorted({
            row["attrs"].get("SERIES_NAME")
            for row in tga_attr_matches
            if row["attrs"].get("SERIES_NAME")
        })

        return {
            "zip_names": names,
            "data_xml": data_name,
            "series_count": len(series),
            "series_attribute_keys": attribute_keys,
            "total_assets_matches": total_matches,
            "tga_code": TGA_CODE,
            "tga_attribute_matches": tga_attr_matches,
            "tga_fallback_candidates": tga_fallback_candidates[:100],
            "structure_tga_hits": structure_hits[:20],
            "unique_tga_series_names_from_code_match": unique_tga_series,
        }


def inspect_rrp(payload: bytes) -> dict:
    data = json.loads(payload.decode("utf-8"))
    repo = data.get("repo") or {}
    operations = repo.get("operations") or []
    reverse = [
        row for row in operations
        if "reverse" in str(row.get("operationType", "")).lower()
        or not row.get("operationType")
    ]
    dates = sorted({
        str(row.get("operationDate"))
        for row in reverse
        if row.get("operationDate")
    })
    sample = reverse[-3:] if len(reverse) >= 3 else reverse
    return {
        "top_level_keys": sorted(data.keys()),
        "repo_keys": sorted(repo.keys()) if isinstance(repo, dict) else [],
        "operation_count": len(operations),
        "reverse_candidate_count": len(reverse),
        "first_operation_date": dates[0] if dates else None,
        "last_operation_date": dates[-1] if dates else None,
        "operation_keys": sorted({k for row in reverse[:50] for k in row.keys()}),
        "latest_sample": sample,
    }


def main() -> int:
    DEFAULT_OUTPUT.mkdir(parents=True, exist_ok=True)
    summary: dict = {
        "status": "STARTED",
        "trading_actions": False,
        "production_change": False,
        "sources": {},
    }

    try:
        h41_payload = _request_bytes(H41_URL, timeout=180)
        h41 = inspect_h41(h41_payload)
        summary["sources"]["h41"] = {
            "reachable": True,
            "bytes": len(h41_payload),
            **h41,
        }
    except Exception as exc:
        summary["sources"]["h41"] = {"reachable": False, "error": f"{type(exc).__name__}: {exc}"}

    try:
        rrp_payload = _request_bytes(
            RRP_URL,
            params={"startDate": RRP_START, "pageSize": "5000"},
            timeout=90,
        )
        rrp = inspect_rrp(rrp_payload)
        summary["sources"]["nyfed_rrp"] = {
            "reachable": True,
            "bytes": len(rrp_payload),
            **rrp,
        }
    except Exception as exc:
        summary["sources"]["nyfed_rrp"] = {"reachable": False, "error": f"{type(exc).__name__}: {exc}"}

    h41 = summary["sources"].get("h41", {})
    rrp = summary["sources"].get("nyfed_rrp", {})
    total_ok = bool(h41.get("total_assets_matches"))
    tga_names = h41.get("unique_tga_series_names_from_code_match") or []
    rrp_ok = bool(rrp.get("reachable")) and int(rrp.get("reverse_candidate_count") or 0) > 0

    if not h41.get("reachable"):
        status = "BLOCKED_H41_UNREACHABLE"
    elif not total_ok:
        status = "BLOCKED_TOTAL_ASSETS_IDENTITY_UNRESOLVED"
    elif len(tga_names) != 1:
        status = "BLOCKED_TGA_IDENTITY_UNRESOLVED"
    elif not rrp_ok:
        status = "BLOCKED_NYFED_RRP_UNUSABLE"
    else:
        status = "PASS_OFFICIAL_LIQUIDITY_SOURCES_RESOLVED"

    summary["status"] = status
    summary["gate"] = {
        "h41_reachable": bool(h41.get("reachable")),
        "total_assets_resolved": total_ok,
        "tga_unique_series_names": tga_names,
        "rrp_usable": rrp_ok,
    }

    out = DEFAULT_OUTPUT / "summary.json"
    out.write_text(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False))
    print(f"output={out}")
    # Feasibility blocker is evidence, not a workflow failure. Only transport/parser crashes
    # outside the captured source blocks should fail the workflow.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
