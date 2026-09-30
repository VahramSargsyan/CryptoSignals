from __future__ import annotations

# v2 intentionally reuses the already runtime-tested v1 engine.
# Only the preregistered candidate/data pool and promotion gate are expanded.

import json
from pathlib import Path

import pandas as pd

import scripts.research_atom_out_replacement_stress_v1 as core

EXPANDED_CANDIDATES = (
    "DOGE", "LTC", "BCH", "DOT", "NEAR", "UNI",
    "ETC", "XLM", "SHIB", "OP", "ARB", "SUI",
)

core.POOL = tuple(dict.fromkeys(core.POOL + EXPANDED_CANDIDATES))
core.CANDIDATES = EXPANDED_CANDIDATES
core.OUT = core.ROOT / "research_artifacts" / "atom_out_expanded_replacement_stress_v2"


def strict_gate(result):
    n = result["neighbor_dependency"]
    return all([
        result["one_year"]["median_return"] > BASE["one_year"]["median_return"],
        result["two_year"]["median_return"] > BASE["two_year"]["median_return"],
        result["rolling_12"]["return_improvement_rate"] > 0.50,
        result["rolling_24"]["return_improvement_rate"] > 0.50,
        result["endpoint_sensitivity"]["return_improvement_rate"] >= 0.50,
        result["one_year"]["median_max_dd"] >= BASE["one_year"]["median_max_dd"] - 0.03,
        result["bull_neutral_long"]["median_return"] > BASE["bull_neutral_long"]["median_return"],
        min(n["positive_return_rate_1y"], n["positive_return_rate_2y"]) >= 0.50,
    ])


if __name__ == "__main__":
    # Run the frozen v1 engine first. Its artifact remains the full evidence record.
    rc = core.main()
    if rc != 0:
        raise SystemExit(rc)

    # Locate the just-created artifact and add the v2 strict-gate decision.
    run_dirs = sorted(p for p in core.OUT.iterdir() if p.is_dir())
    if not run_dirs:
        raise RuntimeError("v2 artifact directory missing")
    run_dir = run_dirs[-1]
    result_path = run_dir / "results.json"
    data = json.loads(result_path.read_text(encoding="utf-8"))

    global BASE
    BASE = data["base_u9"]

    eligible = [
        candidate for candidate in data["candidates"]
        if candidate["candidate"] in EXPANDED_CANDIDATES
    ]
    strict_pass = [candidate for candidate in eligible if strict_gate(candidate)]

    # Sort passers by temporal robustness first, then by two-year return.
    strict_pass = sorted(
        strict_pass,
        key=lambda r: (
            r["rolling_12"]["return_improvement_rate"],
            r["rolling_24"]["return_improvement_rate"],
            r["endpoint_sensitivity"]["return_improvement_rate"],
            min(
                r["neighbor_dependency"]["positive_return_rate_1y"],
                r["neighbor_dependency"]["positive_return_rate_2y"],
            ),
            r["two_year"]["median_return"],
        ),
        reverse=True,
    )

    data["v2_strict_gate"] = {
        "rule": "all 8 preregistered temporal+safety checks must pass",
        "passing_candidates": [r["candidate"] for r in strict_pass],
        "proposed_slot": strict_pass[0]["candidate"] if strict_pass else "NOTHING",
    }
    result_path.write_text(
        json.dumps(data, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )

    report = (run_dir / "report.md").read_text(encoding="utf-8")
    report += "\n## V2 strict temporal gate\n\n"
    report += "Passing candidates: " + (
        ", ".join(r["candidate"] for r in strict_pass)
        if strict_pass else "NONE"
    ) + "\n\n"
    report += "**V2 proposed slot: %s**\n" % (
        strict_pass[0]["candidate"] if strict_pass else "NOTHING"
    )
    (run_dir / "report.md").write_text(report, encoding="utf-8")

    print(
        "v2_strict_passers=" +
        (",".join(r["candidate"] for r in strict_pass) if strict_pass else "NONE")
    )
    print(
        "v2_proposed_slot=" +
        (strict_pass[0]["candidate"] if strict_pass else "NOTHING")
    )
