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