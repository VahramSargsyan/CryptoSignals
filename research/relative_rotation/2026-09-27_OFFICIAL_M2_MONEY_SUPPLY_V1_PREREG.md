# OFFICIAL M2 MONEY SUPPLY VS CRYPTO STRESS V1 — Preregistration

Date: 2026-09-27
Branch: `research/m2-money-supply-context-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / UNTUNED / NO PRODUCTION CHANGE

## Question

Does causally available U.S. M2 money-supply growth provide independent context for:
1. crypto stress onset; and/or
2. crypto recovery / possible cash re-entry?

This pass is descriptive. It does NOT create a trading rule.

## Frozen crypto side

No change to:
- ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- own SMA200;
- breadth = count above own SMA200;
- crisis entry after 3 consecutive closes breadth <=3;
- recovery after 3 consecutive closes breadth >=5;
- next-open semantics;
- frozen LOW_VOL candidate;
- relative router or transaction cost.

Use the same fully eligible 8-asset history:
- first all-SMA200-ready date: 2023-11-20;
- one state reset at that boundary;
- eight closed crypto-stress episodes expected through 2026-09-26.

## Official M2 source

Use Federal Reserve Board H.6 Money Stock Measures.

Preferred series:
- H6/H6_M2/M2.M
- M2, seasonally adjusted
- monthly
- USD billions.

Do not use the mislabeled repository BTC file or any third-party money-supply proxy.

Do not use FRED graph CSV endpoint in this pass.

Source feasibility must pass first:
- official H.6 DDP data reachable;
- M2 monthly series resolves uniquely;
- units/frequency verified;
- history is long enough for 12-month change before the crypto evaluation period.

## Causal availability

H.6 is released monthly, generally on the fourth Tuesday at 1:00 p.m. Eastern Time.

For this V1:
- map each monthly observation to its actual official H.6 release date using the frozen 2022-2026 Federal Reserve release-date schedule;
- because the release occurs after the crypto 00:00 UTC daily close, expose a new M2 observation to the crypto engine only from the NEXT UTC calendar day;
- no backward fill before first available observation.

If release-date mapping fails for any monthly value needed in the 2023-2026 crypto period, abort rather than approximate silently.

## Frozen transformations

No parameter search.

For causally available seasonally adjusted M2 compute percentage change over:

- 3 months
- 6 months
- 12 months (YoY)

Formula:
`M2_CHANGE_N = M2_t / M2_(t-N) - 1`

All three horizons must be reported together.

No best horizon may be selected post hoc and called validated.

## Entry hypothesis

At frozen crypto stress-entry signal dates, M2 growth may be weaker than on generic NORMAL crypto days.

For each 3m / 6m / 12m horizon report:
- count and fraction of stress entries with M2 change < 0;
- NORMAL-day baseline fraction with M2 change < 0;
- descriptive lift.

Also report median M2 change at entries vs NORMAL days.

## Exit / recovery hypothesis

At frozen crypto recovery signal dates, M2 growth may be positive more often than on generic DEFENSIVE crypto days.

For each 3m / 6m / 12m horizon report:
- count and fraction of exits with M2 change > 0;
- DEFENSIVE-day baseline fraction with M2 change > 0;
- descriptive lift.

Also report median M2 change at exits vs DEFENSIVE days.

## Cross-check with official Fed net liquidity

This M2 study is independent from the already-executed net-liquidity study.

Do NOT combine signals in this V1.

Final evidence may place results side-by-side:
- official Fed net liquidity: assets - TGA - ON RRP;
- M2 money stock growth.

No composite score is allowed in this pass.

## Revision limitation

H.6 seasonally adjusted history can be revised.

This V1 uses the current official H.6 historical series with causal release-date alignment, not a full vintage archive.

Therefore:
- time-of-publication availability is modeled;
- historical revision bias is NOT eliminated;
- results are exploratory and cannot support production promotion.

## Required outputs

- source feasibility report;
- raw official M2 monthly series;
- monthly M2 with release/availability dates;
- daily causal M2 alignment to crypto dates;
- frozen crypto stress episodes;
- per-episode entry/exit M2 snapshots;
- aggregate 3m/6m/12m sign statistics;
- state-conditioned baselines;
- latest fixed-end M2 state through 2026-09-26.

## Interpretation discipline

Possible descriptive verdicts:
- ENTRY_CONTEXT_PROMISING
- RECOVERY_CONTEXT_PROMISING
- BOTH_CONTEXT_PROMISING
- WEAK
- MIXED

No trading rule.
No SMA / M2 horizon tuning.
No production change.
Any combined BTC + M2 + breadth re-entry rule requires a NEW preregistration after this evidence is known.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
