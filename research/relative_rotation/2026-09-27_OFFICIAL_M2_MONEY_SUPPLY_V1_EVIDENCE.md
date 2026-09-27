# OFFICIAL M2 MONEY SUPPLY VS CRYPTO STRESS V1 — Evidence

Date: 2026-09-27
Branch: `research/m2-money-supply-context-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / UNTUNED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- Source feasibility run: `36306932451`
- Source feasibility artifact: `10928005872`
- Event-study run: `36307341969`
- Event-study source commit: `bdcac47feba3e13e878c5678c35be406ac784e66`
- Event-study artifact: `10928016535`
- Workflow conclusion: SUCCESS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + OFFICIAL_H6_M2 + ACTUAL_RELEASE_DATE_ALIGNMENT + REAL_BINANCE_1D + 8_FROZEN_CRYPTO_STRESS_EPISODES

## Official source

Federal Reserve Board H.6:
- series: `M2.M`
- M2, seasonally adjusted
- monthly
- USD billions
- source probe resolved one unique series
- official source history: 1959-01 through 2026-08

To isolate the event study from transient H.6 DDP endpoint failures, the successful source-probe snapshot needed by the 2023-2026 study was frozen into the research branch:

`research/reference_data/official_h6_m2_monthly_2022_2026.csv`

SHA-256:
`0a14375baf3c5c38285565727375701ccda3e7b0a6dc531aabec311f261713e2`

This is transport isolation only; it does not change M2 values or methodology.

## Causality

Actual Federal Reserve H.6 release dates for 2022-2026 were frozen before execution.

A monthly M2 value becomes usable by the crypto research engine on the next UTC calendar day after the official H.6 release date.

No backward fill before availability.

Transformations were preregistered:
- 3-month percentage change
- 6-month percentage change
- 12-month / YoY percentage change

No horizon was tuned or selected after seeing results.

## Frozen crypto sample

- same 8 assets
- same own-SMA200 breadth
- same 3-close stress confirmation at breadth<=3
- same 3-close recovery confirmation at breadth>=5
- 8 closed stress episodes

## Stress-entry result

Question:
Is M2 contraction enriched at crypto stress entry?

### 3-month M2 change

At the 8 stress entries:
- negative: **0/8 = 0%**
- median change: **+0.82%**

Generic NORMAL crypto days:
- negative: **13.54%**
- median change: **+0.97%**

Descriptive lift for negative M2 at entry:
- **0.00x**

### 6-month M2 change

At stress entries:
- negative: **0/8 = 0%**
- median change: **+1.35%**

NORMAL days:
- negative: **20.83%**
- median change: **+1.48%**

Descriptive lift:
- **0.00x**

### 12-month M2 change

At stress entries:
- negative: **1/8 = 12.5%**
- median change: **+1.72%**

NORMAL days:
- negative: **31.67%**
- median change: **+2.52%**

Descriptive lift:
- **0.39x**

Interpretation:
Crypto stress entries in this 2023-2026 sample did NOT require M2 contraction.
Most occurred while M2 was already expanding.

Therefore:
`M2 contraction != necessary crisis-entry condition`.

## Recovery / exit result

Question:
Is positive M2 growth enriched specifically at frozen crypto recovery exits?

### 3-month M2

At exits:
- positive: **8/8 = 100%**
- median change: **+0.85%**

But generic DEFENSIVE crypto days:
- positive: **100%**
- median change: **+0.95%**

Descriptive lift:
- **1.00x**

### 6-month M2

At exits:
- positive: **8/8 = 100%**
- median change: **+1.35%**

Generic DEFENSIVE days:
- positive: **100%**
- median change: **+2.03%**

Descriptive lift:
- **1.00x**

### 12-month / YoY M2

At exits:
- positive: **8/8 = 100%**
- median change: **+1.97%**

Generic DEFENSIVE days:
- positive: **99.29%**
- median change: **+3.89%**

Descriptive lift:
- **~1.007x**

Interpretation:
Positive M2 growth was present at every recovery exit, but it was also present during almost the entire defensive state.

Therefore:
`M2 growth is broad liquidity regime context, not a standalone recovery timer`.

The sign of M2 growth does not discriminate recovery dates from ordinary crisis days in this sample.

## Latest fixed-end state — 2026-09-26

Latest causally available M2:
- observation month: 2026-08
- release date: 2026-09-22
- crypto availability: 2026-09-23
- M2: **23,342.8 USD bn**
- 3m change: **+1.43%**
- 6m change: **+3.29%**
- YoY change: **+5.66%**

Thus the current fixed-end context is monetary expansion on all three preregistered horizons.

## Comparison with official Fed net liquidity

Prior untuned official net-liquidity study:
- medium-horizon deterioration was enriched around crypto stress entry;
- positive net-liquidity change was weak at crypto recovery exits.

M2 V1:
- M2 contraction is not enriched at stress entry;
- positive M2 growth is nearly ubiquitous during defensive periods, so it does not identify the recovery date.

The two indicators therefore carry different information:
- `Fed assets - TGA - RRP` appears more useful for systemic stress context;
- broad M2 growth appears too slow/persistent to serve as a standalone timing switch.

## Verdict

`OFFICIAL_M2_MONEY_SUPPLY_V1 = LIQUIDITY_REGIME_CONTEXT_ONLY / NO_STANDALONE_ENTRY_OR_EXIT_TIMING_EDGE / DO_NOT_PROMOTE`

Supported:
- M2 should be retained as a macro regime feature;
- the popular intuition that expanding money supply is supportive to risk assets is compatible with the observed broad 2024-2026 expansion regime;
- M2 provides information distinct from Fed net liquidity.

Not supported:
- `M2 > 0 growth => buy crypto now`;
- negative M2 as a required stress-entry condition;
- positive M2 growth as a standalone cash-exit trigger;
- production use.

## Important limitation

The current official seasonally adjusted H.6 history can be revised.
This V1 models historical publication timing but does not reconstruct a full vintage/ALFRED history.

The event count is eight and observations are serially correlated.

No statistical-significance claim is made.

## Next research boundary

Do not tune 2m/4m/9m or other M2 horizons on this same sample and call the result validation.

A future separately preregistered recovery model may use M2 only as one slow regime layer together with independent faster evidence such as:
- BTC recovery;
- broad crypto breadth improvement;
- cross-asset risk recovery.

That combined rule is a new experiment, not part of M2 V1.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.
