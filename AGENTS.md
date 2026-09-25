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
