# OSS MARKET REGIME FULL-HISTORY V1 — Evidence

Date: 2026-09-27  
Branch: `research/global-macro-risk-regime-v1`  
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING  
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36298766880`
- Source commit: `1c6b7a2b93177a9f3e237dbcadcd2536cdf4df7d`
- Artifact: `oss-market-regime-full-history-v1`
- Artifact ID: `10925022402`
- Run ID: `OSS_MARKET_REGIME_FULL_HISTORY_V1_1c6b7a2b9317`
- Workflow conclusion: SUCCESS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + REAL_BINANCE_1D + REAL_YAHOO_ADJUSTED_DAILY + FULL_HISTORY_RETROSPECTIVE_REPLICATION

## Boundary

This is retrospective replication, not untouched validation.

- Binance panel: 2023-05-05 through 2026-09-26
- first fully SMA200-ready crypto breadth date: 2023-11-20
- state reset: first fully eligible date
- frozen crypto thresholds changed: NO
- external OSS thresholds changed: NO

## Episode inventory

Eight closed crypto-stress episodes were identified:

| ID | Defensive execution entry | Exit | Defensive days | RISK_OFF lead to entry signal | RISK_ON lead to exit signal |
|---|---|---|---:|---:|---:|
| 1 | 2024-04-20 | 2024-05-20 | 30 | 0d | not found <=30d |
| 2 | 2024-06-14 | 2024-07-17 | 33 | 13d | 0d |
| 3 | 2024-08-07 | 2024-08-26 | 19 | 0d | not found <=30d |
| 4 | 2024-08-30 | 2024-09-29 | 30 | 20d | 8d |
| 5 | 2024-10-03 | 2024-10-14 | 11 | 22d | 23d |
| 6 | 2024-11-05 | 2024-11-09 | 4 | 0d | 1d |
| 7 | 2025-02-27 | 2025-07-18 | 141 | 0d | 10d |
| 8 | 2025-11-02 | 2026-08-23 | 294 | 15d | 8d |

The two long episodes reinforce the previously observed over-defense problem. In uninterrupted full-history state, the final episode begins in November 2025 rather than at the untouched-window reset in March 2026.

## Entry relationship

For 8 crypto entry signals, nearest preceding `BROAD_RISK_OFF`:

- same day: 4/8 = 50.0%
- within 7d: 4/8 = 50.0%
- within 14d: 5/8 = 62.5%
- within 30d: 8/8 = 100.0%
- median lead when found: 6.5 days

### State-conditioned baseline

Across all 480 crypto NORMAL days:

- same-day BROAD_RISK_OFF: 12.29%
- BROAD_RISK_OFF within previous 7d: 32.29%
- within 14d: 45.42%
- within 30d: 66.04%

Observed entry enrichment versus generic NORMAL days:

- same day: 50.0% / 12.29% = ~4.07x
- 7d: 50.0% / 32.29% = ~1.55x
- 14d: 62.5% / 45.42% = ~1.38x
- 30d: 100% / 66.04% = ~1.51x

The strongest descriptive concentration is therefore same-day BROAD_RISK_OFF, not the broad 30-day window.

## Exit relationship

For 8 crypto exit signals, nearest preceding `BROAD_RISK_ON`:

- same day: 1/8 = 12.5%
- within 7d: 2/8 = 25.0%
- within 14d: 5/8 = 62.5%
- within 30d: 6/8 = 75.0%
- median lead when found: 8 days

### State-conditioned baseline

Across all 562 crypto DEFENSIVE days:

- same-day BROAD_RISK_ON: 3.74%
- BROAD_RISK_ON within previous 7d: 16.19%
- within 14d: 25.98%
- within 30d: 46.09%

Observed exit enrichment versus generic DEFENSIVE days:

- same day: 12.5% / 3.74% = ~3.35x, but only one event
- 7d: 25.0% / 16.19% = ~1.54x
- 14d: 62.5% / 25.98% = ~2.41x
- 30d: 75.0% / 46.09% = ~1.63x

The most interesting recovery hypothesis is therefore not same-day RISK_ON. It is the appearance of BROAD_RISK_ON roughly 1–2 weeks before crypto breadth confirms recovery.

## Raw regime labels are not sufficient

Across the whole eligible history:

During crypto DEFENSIVE:
- MIXED: 53.91%
- BROAD_RISK_OFF: 15.84%
- INTERNAL_ROTATION: 14.41%
- DEFENSIVE_ROTATION: 12.10%
- BROAD_RISK_ON: 3.74%

During crypto NORMAL:
- MIXED: 66.67%
- BROAD_RISK_OFF: 12.29%
- DEFENSIVE_ROTATION: 10.63%
- INTERNAL_ROTATION: 7.08%
- BROAD_RISK_ON: 3.33%

Therefore the daily regime label itself has weak separation. Event timing around transitions is more informative than holding a permanent RISK_ON/RISK_OFF classification.

## Statistical caution

A simple without-replacement tail calculation against the state-conditioned daily base rates makes two contrasts look notable:
- same-day BROAD_RISK_OFF at entry: 4/8 versus 12.29% NORMAL-day base rate;
- BROAD_RISK_ON within 14d before exit: 5/8 versus 25.98% DEFENSIVE-day base rate.

These are NOT treated as formal p-values because daily regime observations are serially correlated and the eight episodes are not independent IID draws.

No statistical-significance claim is made.

## Verdict

`OSS_MARKET_REGIME_FULL_HISTORY_V1_VERDICT = PROMISING_TRANSITION_CONTEXT / RAW_REGIME_NOT_A_TRADING_GATE`

What is supported:
- cross-asset BROAD_RISK_OFF is concentrated around crypto stress entries;
- BROAD_RISK_ON is enriched in the 1–2 weeks preceding crypto breadth recovery;
- the recovery relationship is consistent with the over-defense hypothesis and deserves a distinct test.

What is not supported:
- replacing crypto breadth with the external regime;
- exiting defense on the first BROAD_RISK_ON day;
- using a 30-day lookback as a rule merely because all eight entries had some RISK_OFF event in that broad window;
- any automatic trading or swap.

## Next research boundary

The next independent layer should be liquidity/funding rather than retuning this regime:
- Fed net liquidity / funding conditions;
- use a separately preregistered, causal data contract;
- prefer licensed OSS methodology;
- preserve external source dates;
- no action hook.

Candidate OSS references already identified:
- `gloom-sh/gloomberb` — MIT; same-date net liquidity `WALCL - WDTGAL - RRPONTSYD`
- `boom90lb/prism` — MIT; causal telemetry blocks for curve, liquidity, real yield/breakeven and volatility

## Runtime impact

Production behavior changed: NONE  
Paper-live trading behavior changed: NONE  
Migration required: NO
