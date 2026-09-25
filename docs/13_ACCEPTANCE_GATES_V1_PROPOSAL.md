# Strategy Acceptance Gates v1 — PROPOSAL BEFORE 2026 OOS

Status: PROPOSAL — NOT YET AUTHORIZED  
WORKFLOW_MODE: ECOSYSTEM_PLANNING  
RISK_CLASS: L3 — this policy controls research promotion decisions  
MIGRATION_REQUIRED: NO

## 1. Purpose

This document exists to prevent moving the goalposts after the protected 2026 out-of-sample period is opened.

The numeric gates below are proposed **after pre-OOS 2021–2025 research and before any 2026 strategy evaluation**.

No 2026 candle may be used to modify these gates.

Once approved, this exact document/commit becomes the acceptance-policy source for the first 2026 OOS run.

## 2. Protected candidates

The first OOS run evaluates the already frozen strategy identities only:

- `VAHRAM_ORIGINAL_V1` / 1.0.0
- `VAHRAM_TRUE_STOCHRSI_V2` / 2.0.0

Their formulas, thresholds, scoring logic and default execution interpretation must not change before the first OOS result.

The trading engine remains:

- `LONG_ONLY_SIGNAL_FLIP_V1`
- signal calculated at candle T close;
- execution at T+1 open;
- fee = 10 bps;
- slippage = 5 bps.

Changing any strategy formula or the trading policy creates a new candidate and invalidates the old blind-test claim for that changed candidate.

## 3. First OOS window

The first OOS evaluation is **2026 YTD only**, from:

`2026-01-01T00:00:00Z`

through the latest fully closed daily UTC candle available at execution time.

It is not described as a complete 2026-year validation.

If sample-size gates are not met, the correct status is `NEEDS_MORE_DATA`; the system must not lower the thresholds merely to force a verdict.

## 4. Mandatory run-integrity gate

An OOS result is eligible for any research status only if all are true:

1. source commit, strategy/version, dataset IDs, engine, fees, slippage and period are recorded;
2. all evaluated datasets have zero critical candle-quality issues under the current canonical validator;
3. no 2026 data was used to tune the tested strategy/version or this policy;
4. signal timestamp remains candle close and execution remains next-candle open;
5. the same accepted source is used for all compared candidates;
6. no runtime error silently removes a requested symbol;
7. complete evidence artifacts and a permanent compact manifest are produced.

Failure here means **INVALID EVIDENCE / RERUN REQUIRED**, not REJECTED.

## 5. Minimum evidence gate

### Global strategy decision

Before a global ACCEPTED or REJECTED decision is allowed, require:

- at least **20 closed OOS trades** across the tracked universe;
- trades in at least **5 distinct symbols**;
- at least **30 OOS BUY event-study observations**;
- at least **30 OOS SELL event-study observations**.

If these are not met:

`NEEDS_MORE_DATA`

No economic threshold is relaxed.

### Regime-specific candidate decision

A future predeclared regime-gated strategy requires at least:

- **15 closed OOS trades**;
- trades in at least **4 distinct symbols**;
- at least **40 OOS event-study observations** for the direction being claimed.

If not met:

`NEEDS_MORE_DATA`

## 6. Global ACCEPTED gate

A candidate is globally `ACCEPTED` only when **all** of the following OOS gates pass.

### Trading economics

- pooled profit factor **>= 1.10**;
- pooled expectancy **> 0**;
- pooled average net trade return **> 0**;
- pooled median net trade return **> 0**.

### Cross-symbol robustness

Across the current 13-symbol universe:

- positive strategy total return on at least **7 / 13 symbols**;
- strategy beats its same-symbol cost-adjusted buy-and-hold benchmark on at least **5 / 13 symbols**;
- median symbol max drawdown **<= 50%**.

These thresholds intentionally require breadth without demanding that a timing strategy beat buy-and-hold on every strongly trending asset.

### Signal-direction sanity

For each direction that the strategy publishes operationally:

- at least **3 of the 4** OOS event horizons (+1/+3/+7/+14) must have average directional return **> 0**;
- the +7 or +14 horizon must be among the positive horizons.

If a strategy publishes both BUY and SELL, both directions must satisfy this gate for a global ACCEPTED status.

A strategy cannot hide a structurally weak direction inside a profitable aggregate.

## 7. REJECTED gate

With the minimum evidence gate satisfied, the candidate becomes `REJECTED` if either condition is true:

### Fatal economics

Any **two or more** of these fail:

- profit factor >= 1.00;
- expectancy > 0;
- average net trade return > 0;
- positive total return on at least 6 / 13 symbols.

OR

### Directional failure

A published BUY or SELL direction has:

- average directional return <= 0 at **all four** +1/+3/+7/+14 horizons,
- with the minimum event sample satisfied.

Rejected strategies and all evidence remain stored. They are never deleted.

## 8. Mixed OOS result

If minimum evidence exists but the candidate passes neither the ACCEPTED gate nor the REJECTED gate:

`OUT_OF_SAMPLE_TESTED`

It remains unpromoted.

This prevents forcing every noisy result into a binary decision.

## 9. OVERFIT_RISK

Use `OVERFIT_RISK` when OOS evidence materially reverses the pre-OOS hypothesis, especially when:

- pre-OOS economics were positive but OOS profit factor is < 1.00 and expectancy <= 0;
- a pre-OOS regime effect loses its directional sign in OOS;
- performance depends on a narrow parameter/configuration neighborhood that fails nearby sensitivity checks.

OVERFIT_RISK may coexist conceptually with rejection evidence, but the Strategy Registry should store one current status plus notes/evidence links.

## 10. REGIME_SPECIFIC gate

The H8 post-hoc findings **cannot themselves promote V1 or V2 to REGIME_SPECIFIC**.

A REGIME_SPECIFIC decision requires a separately versioned, predeclared strategy candidate with:

- exact regime gate frozen before its OOS run;
- exact entry/exit/position rules;
- exact strategy ID/version;
- the same no-lookahead regime calculation;
- its own OOS evidence.

For such a candidate, after the minimum regime-specific sample is met, require all:

- profit factor **>= 1.20**;
- expectancy **> 0**;
- average and median net trade return **> 0**;
- median symbol max drawdown **<= 40%** among symbols where it traded;
- positive total return in at least **60% of symbols where it traded**;
- claimed signal direction average directional return **> 0 at 3/4 horizons**, including +7 or +14.

Outside its declared regime, the runtime must emit no trade signal for that regime-specific candidate.

## 11. H8 hypotheses allowed into OOS

The existing H8 evidence may be carried into OOS only as **predeclared hypotheses**, not accepted strategies.

Primary hypothesis:

`TRUE_STOCHRSI_V2 BUY signals are stronger under VOL_HIGH than globally.`

Secondary hypothesis:

`BUY behaves as a mean-reversion signal during BROAD_BEAR, but may be unstable by year.`

Warning hypothesis:

`SELL may be directionally weak under the current interpretation.`

These hypotheses may be confirmed or contradicted using the untouched 2026 OOS data.

They must not be rewritten after the OOS result is seen.

## 12. What 2026 OOS may and may not decide

The first 2026 YTD run may produce:

- `OUT_OF_SAMPLE_TESTED`;
- `NEEDS_MORE_DATA`;
- `ACCEPTED`;
- `REJECTED`;
- `OVERFIT_RISK`.

It may **not** produce `REGIME_SPECIFIC` for the existing V1/V2 merely by selecting the best H8 bucket after seeing 2026.

A new regime-gated strategy must be frozen first and validated separately.

## 13. Promotion after ACCEPTED

Even ACCEPTED does not mean live trading.

Required next state:

`PAPER_LIVE`

Only after paper-live observation may a candidate become:

`LIVE_SIGNALS`

Telegram remains downstream from this gate.

## 14. Freeze rule

After approval, changing any numeric threshold or decision rule in this document requires:

1. a new acceptance-policy version;
2. explicit reason;
3. a new truly unseen validation period for any candidate affected by the change.

The already opened 2026 data may never become blind again.
