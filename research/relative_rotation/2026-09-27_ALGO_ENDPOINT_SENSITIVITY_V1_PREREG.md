# ALGO Endpoint Sensitivity v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Status: POST-AUDIT ROBUSTNESS CHECK

## Trigger

ALGO Deep Audit v1 found:
- ALGO occupancy in latest-year top-10 U8 routes is only ~3.6%;
- 60% of those U8 routes end in ALGO;
- U10 topology is more balanced (entries=exits, only 10% ending in ALGO).

This creates a specific end-of-sample concern for the latest-year U8 ranking.

## Fixed test

Re-run the full unconstrained 6,435-U8 exhaustive ranking for five trailing 365-day windows ending:

- 2026-05-31
- 2026-06-30
- 2026-07-31
- 2026-08-31
- 2026-09-26

For each window:
- evaluate every U8 from all 8 starting assets;
- rank by median terminal return;
- report ALGO frequency in top 10 / 50 / 100;
- report ALGO-containing universe median rank distribution;
- recompute ALGO matched-marginal pairwise win rate and median delta versus alternative 8th tokens;
- report top-10 common core;
- report ALGO HODL return over the same 365-day window.

## Interpretation

If ALGO appears strong only in the final 2026-09-26 window and collapses under earlier endpoints, treat it as end-of-sample contamination.

If ALGO remains enriched in top sets and has positive matched-marginal value across shifted endpoints, the end-of-sample explanation is weakened.

No U10 or live change is authorized.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
