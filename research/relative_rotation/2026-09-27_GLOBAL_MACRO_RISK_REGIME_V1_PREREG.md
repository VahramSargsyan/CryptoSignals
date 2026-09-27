# GLOBAL_MACRO_RISK_REGIME_V1 — Preregistered Diagnostic

Date: 2026-09-27  
Branch: `research/global-macro-risk-regime-v1`  
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING  
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Question

Can a simple causal macro-risk composite help explain or lead the start and end of crypto stress episodes defined by the already-frozen 8-asset breadth rule?

This pass is diagnostic only. It does not create a trading rule, does not authorize swaps, and does not change any frozen crypto parameter.

## Frozen crypto stress definition

Imported from the existing V1 implementation:

- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- own SMA200 per asset;
- stress trigger: 3 consecutive closes with breadth <= 3;
- recovery trigger: 3 consecutive closes with breadth >= 5;
- next-open execution semantics;
- no change to SMA200, 3/5 thresholds, confirmation length, VOL30, router logic, or costs.

The macro experiment only observes the stress/recovery dates.

## Macro inputs

No optimization is allowed before the first result.

1. `VIXCLS` — VIX level; higher = more risk.
2. `DTWEXBGS` — broad USD 20-observation return; stronger USD = more risk.
3. `DFII10` — 10Y real yield 20-observation change; higher real yields = more risk.
4. `BAA10Y` — Baa minus 10Y Treasury spread 20-observation change; widening spread = more risk.
5. `NASDAQCOM` — Nasdaq 20-observation return, sign inverted; falling equities = more risk.
6. `NFCI` — Chicago Fed National Financial Conditions Index level; higher/tighter = more risk.

NFCI is shifted by +5 calendar days from its week-ending observation label as a conservative publication-availability approximation.

## Composite construction

Each directional feature is transformed into a causal rolling z-score.

- daily series: 252-observation window, minimum 126;
- weekly NFCI: 52-observation window, minimum 26;
- composite: equal-weight mean;
- at least 4 of 6 components must be available;
- no optimized weights;
- no optimized threshold;
- no parameter search.

## Outputs

The first run must produce:

- crypto stress episodes;
- derived daily macro risk score aligned causally to crypto dates;
- event study at -30, -14, -7, 0, +7, +14, +30 days around stress-entry and recovery signals;
- lead/lag correlations with macro leading crypto stress intensity by 0, 7, 14, 21, 30 days;
- a reproduction check that the known 2026 V1 episode executes into defense on 2026-04-01 and out on 2026-08-23;
- current fixed-end state through 2026-09-26.

## Interpretation discipline

After the first run:

- do not turn the best-looking lag into a trading rule automatically;
- do not tune macro transforms or weights on the same evidence and call it validation;
- treat any apparent edge as hypothesis generation;
- if the pattern looks useful, the next step must be a separately preregistered rule and validation boundary;
- if the pattern is weak/inconsistent, preserve the negative result.

## Runtime impact

Production behavior changed: `NONE`  
Paper-live trading behavior changed: `NONE`  
Migration required: `NO`
