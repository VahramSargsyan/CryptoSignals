# OFFICIAL LIQUIDITY DATA FEASIBILITY V1 — Evidence

Date: 2026-09-27
Branch: `research/global-macro-risk-regime-v1`
Mode: DIAGNOSTIC_ONLY + ECOSYSTEM_PLANNING
Status: PASS / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Successful run

- GitHub Actions run: `36299451187`
- Source commit: `2232a846c65cfe23ec49a5f8667b4e56bd3b084f`
- Artifact: `official-liquidity-data-feasibility-v1`
- Artifact ID: `10925305298`
- Gate: `PASS_OFFICIAL_LIQUIDITY_SOURCES_RESOLVED`

## Federal Reserve H.4.1

Official package:
`https://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&filetype=zip`

Observed package:
- size: 9,061,418 bytes
- files: `H41_data.xml`, `H41_struct.xml`, schema files
- data series: 1,332
- history through: 2026-09-23

### Total assets

Resolved directly from official metadata:
- component: `TA`
- distribution: `TOT`
- series type: `L`
- series: `RESPPA_N.WW`
- unit multiplier: 1,000,000 USD
- observations: 1,241
- first: 2002-12-18
- last: 2026-09-23

This is the H.4.1 total-assets series used as the WALCL-equivalent.

### Treasury General Account

Official H.4.1 structure defines:
- component `DEPUSTG`: Deposits with FR Banks, other than reserve balances: U.S. Treasury, General Account.

The package exposes district rows and aggregate transforms. Selecting the official raw aggregate dimensions:
- `COMPONENT=DEPUSTG`
- `DISTRIBUTION=TOT`
- `SERIESTYPE=L`
- `CURRENCY=USD`
- `UNIT=Currency`
- `UNIT_MULT=1000000`

resolves exactly one series:
- `RESPPLLDT_N.WW`
- observations: 1,241
- first: 2002-12-18
- last: 2026-09-23

Its history boundary is consistent with the total-assets series.

## NY Fed ON RRP

Official endpoint:
`https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json`

Probe from 2023-05-05:
- endpoint reachable: YES
- operations returned: 854
- reverse-repo candidates: 854
- first operation date: 2023-05-05
- last operation date: 2026-09-25
- `totalAmtAccepted` is present for each operation and can be converted from USD to H.4.1 USD-million units.

## Result

The original FRED CSV timeout is no longer a blocker for the Fed-liquidity layer.

A keyless official-source path is available:
1. Fed Board H.4.1 DDP -> total assets + TGA
2. NY Fed Markets API -> ON RRP

No third-party runtime source is required.

## Causality boundary for next pass

H.4.1 rows are Wednesday observations but are released later. The next pass will conservatively make a Wednesday H.4.1 composite available to crypto only on Friday (observation date +2 calendar days), so the weekly value cannot leak into a crypto close before publication.

ON RRP may be known earlier, but the composite waits for the slowest required component (H.4.1).

## Runtime impact

Production behavior changed: NONE
Paper-live behavior changed: NONE
Migration required: NO

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + LIVE_OFFICIAL_FED_H41 + LIVE_OFFICIAL_NYFED_RRP
