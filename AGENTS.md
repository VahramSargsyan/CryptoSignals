# Instructions for AI maintainers

GOVERNANCE_VERSION: **VAHRAM_APP_GOVERNANCE v1.0.0**  
CANONICAL_SOURCE: `VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.0.0.md`  
LAST_SYNC: **2026-09-25**

Read the universal governance when accessible, then apply this repository's local rules. If the private canonical source is unavailable, the local P0 rules below remain fail-safe authority.

Read `PROJECT_GOVERNANCE.md` before implementation work.

For strategy-platform work also read:

1. `docs/00_STRATEGY_LAB_BLUEPRINT.md`
2. `docs/01_STRATEGY_LAB_ROADMAP.md`
3. `docs/02_STRATEGY_REGISTRY_STANDARD.md`
4. `docs/03_OSS_REUSE_SHORTLIST.md`
5. `docs/04_DATA_BACKTEST_STANDARD.md`

Rules:

- select workflow mode first;
- preserve legacy behavior;
- do not modify code during AUDIT_ONLY / STRESS_TEST_ONLY / DIAGNOSTIC_ONLY;
- no full rewrite without explicit FULL_REBUILD_ALLOWED;
- use REUSE_FIRST but verify licenses before copying code;
- preserve `VAHRAM_ORIGINAL_V1`;
- use one shared backtest semantics for fair comparison;
- do not claim a strategy is accepted without reproducible evidence;
- schema/ID/relation changes require MIGRATION_PLAN + rollback;
- do not commit secrets;
- report exact TEST_LEVEL and residual risks.

## LINK grid MA research memory

For any new moving-average, exit, or runner proposal involving `VAHRAM_LINK_LEVEL_GRID_V1`, read `docs/evidence/2026-09-26_LINK_GRID_MA_EXIT_RESEARCH_DECISION_LOG_V1.md` before proposing or rerunning a hypothesis.

Do not repeat a recorded MA experiment unless there is future unseen evidence, a materially different mechanism, or a documented bug/semantic correction. A future proposal must explicitly state how it differs from the recorded direct-MA-exit, BASE/WIDE-regime, and MA-runner experiments.


## LINK grid volatility / regime research memory

For any new volatility-based or multi-indicator exit-regime proposal involving `VAHRAM_LINK_LEVEL_GRID_V1`, also read `docs/evidence/2026-09-26_LINK_GRID_VOLATILITY_REGIME_RESEARCH_V1.md` before proposing or rerunning the idea.

Already tested families include ATR/BBW/realized-volatility selectors, +3/+12 vs +6/+18 vs +9/+24 regime logic, MA used in the same context, MA+volatility combinations, ADX combinations, and high-volatility brake rules. A future proposal must state what is materially new.


## LINK grid external crowd / market-context research memory

For new proposals involving funding, Fear & Greed, market breadth, stablecoin liquidity, open interest, or liquidations for `VAHRAM_LINK_LEVEL_GRID_V1`, read `docs/evidence/2026-09-26_LINK_GRID_EXTERNAL_CROWD_CONTEXT_RESEARCH_V1.md` first.

Funding, Fear & Greed, and a breadth/stablecoin context have already been backtested as +3/+12 / +6/+18 / +9/+24 exit selectors. Open interest and liquidations were **not** given performance results because equal trustworthy history was not materialized in that pass. Do not silently substitute proxies or repeat thresholds without a materially new hypothesis/data boundary.


## LINK grid capital / H-L range research memory

For new proposals involving capital-depth allocation, Micro/Mid capital split, or changing the H/L lookback for `VAHRAM_LINK_LEVEL_GRID_V1`, read `docs/evidence/2026-09-26_LINK_GRID_CAPITAL_RANGE_LOOKBACK_RESEARCH_V1.md` first.

Already tested: p=0..3 depth powers, p=0.25/0.5/0.75 compromises, Micro/Mid splits, H/L windows from 0.5y through 3y on the full 3-year evaluation, refined 2.25–2.9y windows, plus diagnostic 4y/5y shorter-window comparisons. Do not promote a full-period maximum without checking recent-window drawdown and stability.


## LINK grid known-strategy benchmark memory

For future comparisons of `VAHRAM_LINK_LEVEL_GRID_V1` against common external strategies, read `docs/evidence/2026-09-26_LINK_GRID_KNOWN_STRATEGY_BENCHMARK_V1.md` first.

Already benchmarked on the corrected five-asset development sample: Buy & Hold, monthly DCA, SMA200 trend, SMA50/200 trend, 12-month momentum, Donchian 20/10, Bollinger 20,2 mean reversion, and RSI14 30/70 mean reversion. Do not claim clean superiority because WIDE was previously researched on the same sample.


## LINK grid unseen top-10 XRP/TRX validation memory

For claims about cross-asset generalization of `VAHRAM_LINK_LEVEL_GRID_V1`, read `docs/evidence/2026-09-26_GRID_UNSEEN_TOP10_XRP_TRX_VALIDATION_V1.md`.

XRP and TRX were tested without parameter tuning. WIDE remained profitable and beat BASE on both, but Buy & Hold beat WIDE on both. Treat this as cross-asset sanity evidence, not proof of universal superiority or clean future OOS.


## Volatile-asset stress-test memory

Before asserting that `VAHRAM_LINK_LEVEL_GRID_V1` simply improves with higher volatility, read `docs/evidence/2026-09-26_GRID_VOLATILE_ASSET_STRESS_TEST_V1.md`.

TWT, DOGE, AVAX and SHIB were tested on a common 2024-05-10 through 2026-03-28 window with canonical 1095-day H/L mechanics. The result refines the hypothesis: raw volatility alone is insufficient; repeated recovery / mean-reverting path structure appears more important. PEPE remains canonically untested because the pinned snapshot in that pass had only 1059 daily rows.


## Grid universe quality / suitability research memory

Before proposing new large-cap Grid candidates, read `docs/evidence/2026-09-26_GRID_UNIVERSE_QUALITY_SUITABILITY_SCREEN_V1.md`.

A 15-asset old/large-cap research universe was screened with a separate QUALITY/SURVIVAL gate and canonical Grid test. Primary research candidates: LINK, SOL, ETH, ADA, XLM. Secondary/caveated: HBAR, UNI, DOGE, AVAX, LTC. BTC, BNB, XRP, TRX, BCH remain important survival/benchmark assets but were not first-choice WIDE candidates on the common window. Do not confuse this survivor-conditioned screen with live eligibility.
