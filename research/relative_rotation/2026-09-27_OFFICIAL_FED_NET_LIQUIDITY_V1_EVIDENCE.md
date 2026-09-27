# OFFICIAL FED NET LIQUIDITY VS CRYPTO STRESS V1 — Evidence

Date: 2026-09-27
Branch: `research/fed-net-liquidity-recovery-v1-retry`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36304333113`
- Source commit: `cb2e60f38ea165b52a92003c3bc10388f48a6862`
- Artifact: `official-fed-net-liquidity-vs-crypto-stress-v1`
- Artifact ID: `10927055746`
- Workflow conclusion: SUCCESS

The prior failed run was caused only by a missing `yfinance` workflow dependency.
Methodology and liquidity parameters were not changed for the successful retry.

## Frozen design

Official net-liquidity identity:
`RESPPA_N.WW - RESPPLLDT_N.WW - NYFED_ON_RRP`

- H.4.1 observation date aligned to same-date NY Fed ON RRP;
- Wednesday H.4.1 observation conservatively available Friday (+2 calendar days);
- change horizons fixed before execution:
  - 4 weeks
  - 13 weeks
  - 26 weeks
- no threshold tuning;
- frozen crypto breadth episode definition unchanged.

Eight crypto stress entries and eight exits had liquidity data.

## Entry result

Question:
Was official net-liquidity change negative around crypto stress entry?

4-week:
- negative: 5/8 = 62.5%
- generic NORMAL-day negative baseline: 54.17%
- descriptive lift: ~1.15x

13-week:
- negative: 7/8 = 87.5%
- NORMAL baseline: 56.04%
- descriptive lift: ~1.56x

26-week:
- negative: 7/8 = 87.5%
- NORMAL baseline: 51.25%
- descriptive lift: ~1.71x

Interpretation:
The medium-horizon 13w/26w official liquidity deterioration is concentrated around crypto stress entries and supports the broader observation that systemic context is more informative for stress onset than for recovery timing.

## Exit result

Question:
Was official net-liquidity change positive around crypto stress exit?

4-week:
- positive: 3/8 = 37.5%
- generic DEFENSIVE-day positive baseline: 57.83%
- descriptive lift: ~0.65x

13-week:
- positive: 0/8 = 0%
- DEFENSIVE baseline: 49.11%

26-week:
- positive: 2/8 = 25%
- DEFENSIVE baseline: 51.25%
- descriptive lift: ~0.49x

Interpretation:
Official Fed net liquidity does NOT provide a useful standalone recovery/exit confirmation under this untuned design.
The signal is especially weak at 13w, where none of the eight crypto recovery exits occurred with positive liquidity change.

## Latest fixed-end state

Through 2026-09-26:
- crypto breadth: 8/8
- crypto mode: NORMAL
- latest official liquidity observation: 2026-09-23
- conservative availability date: 2026-09-25
- net liquidity: 5,799,926 USD mn
- 4w change: +29,151 USD mn
- 13w change: -29,340 USD mn
- 26w change: -19,046 USD mn

## Verdict

`OFFICIAL_FED_NET_LIQUIDITY_V1 = ENTRY_CONTEXT_PROMISING / EXIT_RECOVERY_WEAK / DO_NOT_USE_AS_STANDALONE_CASH_EXIT`

Supported:
- medium-horizon liquidity deterioration is enriched around crypto stress entry;
- official-source liquidity is useful as independent systemic-stress context.

Not supported:
- positive official net-liquidity change as a direct cash re-entry signal;
- promotion to trading rule;
- post-hoc threshold tuning.

## Limitations

- descriptive retrospective research;
- H.4.1 current package is not an ALFRED vintage archive;
- historical revisions are not modeled;
- weekly liquidity values create serially correlated daily as-of rows;
- eight episodes are not IID;
- descriptive lift is not statistical significance.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + OFFICIAL_H41 + NYFED_ON_RRP + CAUSAL_AVAILABILITY_ALIGNMENT
