# CryptoSignals Project Governance

GOVERNANCE_VERSION: **VAHRAM_APP_GOVERNANCE v1.0.0**  
CANONICAL_SOURCE: `VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.0.0.md`  
LAST_SYNC: **2026-09-25**

**Universal authority override:** v1.0.0 is the current cross-project baseline. Any v0.4.9 language below is retained only as historical/local provenance. Local rules may be stricter but must not silently weaken v1.0.0.

Local profile: **INVESTMENT_STRATEGY_LAB**  
Status: ACTIVE

This repository inherits the proven maintenance principles developed for VBOS, adapted to a strategy-research project.

## 1. Workflow mode is mandatory

Every task selects exactly one mode before changes:

- AUDIT_ONLY
- STRESS_TEST_ONLY
- DIAGNOSTIC_ONLY
- PATCH_FIX
- BUILD_NEW_APP
- ECOSYSTEM_PLANNING
- PRODUCTION_OBSERVATION
- FULL_REBUILD_ALLOWED

FULL_REBUILD_ALLOWED requires explicit Vahram approval.

## 2. P0 safety rules

- Understand the existing system before modifying it.
- Do not rewrite working legacy behavior from scratch without explicit permission.
- Audit/stress-test/diagnostic requests do not authorize fixes.
- PATCH_FIX changes only the declared problem unless scope is explicitly expanded.
- Schema, column, ID or relation changes require a MIGRATION_PLAN and rollback path.
- Never claim runtime verification from static checks.
- Preserve canonical data and existing reproducible evidence.
- Secrets, API tokens, broker credentials and Telegram tokens must never be committed.
- Unknown or ambiguous OSS license means no direct code copying.
- Production/live strategy behavior must never be changed silently.

## 3. Documentation-first gate

Before a major strategy family, shared engine, architecture change, cross-module integration, live runtime or data-contract change, define:

1. business/research objective;
2. canonical source of truth;
3. affected modules/files;
4. data/schema impact;
5. external dependencies;
6. acceptance evidence;
7. rollback/fallback;
8. promotion path from research to paper/live.

A small isolated bug fix does not require a large blueprint.

## 4. REUSE_FIRST -> VERIFY_BEFORE_ADOPT

Before building a generic component from scratch, search for mature OSS.

Every material adoption must verify:

1. repository and exact commit/tag;
2. license/provenance;
3. architecture fit;
4. security/direct/transitive dependencies;
5. minimum justified adoption scope;
6. local adapter/boundary;
7. project-owned tests;
8. notices/residual risks.

Default policy:

- MIT / BSD / Apache: may be ADAPT candidates after review.
- GPL / AGPL / custom / source-available: REFERENCE_ONLY unless compatibility is explicitly approved.
- No license: REFERENCE_ONLY; do not copy code.

## 5. Investment research evidence rules

A strategy is a hypothesis until it has reproducible evidence.

Every meaningful strategy result must record:

- strategy ID + version;
- source commit SHA;
- dataset/source;
- symbols/timeframe;
- period;
- exact parameters;
- fees;
- slippage assumption;
- entry/exit convention;
- trade count;
- metrics;
- known data gaps;
- validation type;
- market-regime breakdown when available.

Do not optimize and validate on the same final period.

Preserve rejected strategies and their evidence. Do not delete failed experiments merely because they performed poorly.

## 6. Original-strategy protection

The current historical algorithm is preserved as `VAHRAM_ORIGINAL_V1`.

Do not silently change its formulas. A material behavior change creates a new strategy/version.

## 7. Shared-engine rule

Strategies must communicate through stable contracts and published outputs, not hidden direct dependencies.

Preferred pattern:

```text
market data -> features -> strategy -> standardized output -> evidence store
                                              |
                                              v
                                      other/meta strategies
```

A strategy may consume another strategy's published result, but should not depend on its private internal implementation.

## 8. Google Sheets / Apps Script rules

When Google Sheets becomes an operational backend:

- Sheet is a database/backend, not a manual-edit workflow.
- Canonical tables, indexes/cache and derived analytics are separate layers.
- Stable IDs and ID-only relations are preferred.
- KNOWN ID -> NEVER unbounded FULL SCAN.
- SHEETS I/O -> BATCH + BOUNDED.
- SAVE -> canonical critical work only.
- DERIVED analytics -> outside the hot mutation path.
- DIAGNOSTICS -> from day one.
- Idempotency and minimal lock scope are required for mutations.
- Large history should not be forced into Sheets when a reproducible external/raw store is more appropriate.

No direct structural edits in a production Google Sheet. Production schema changes require repository-owned migration/update logic, backup, rehearsal, acceptance and postflight verification.

## 9. Test honesty

Report exact evidence level, for example:

- STATIC_ONLY
- LOCAL_TESTED
- BACKTEST_EXECUTED
- OUT_OF_SAMPLE_TESTED
- WALK_FORWARD_TESTED
- GOOGLE_APPS_SCRIPT_TESTED
- PAPER_LIVE_OBSERVED
- LIVE_RUNTIME_VERIFIED

Never collapse static review into a generic PASS.

## 10. Research -> production separation

Research may remain flexible. Production is gated.

```text
IDEA
 -> EXPERIMENTAL
 -> BACKTESTED
 -> OUT_OF_SAMPLE_TESTED
 -> WALK_FORWARD_TESTED
 -> ACCEPTED / REJECTED / REGIME_SPECIFIC
 -> PAPER_LIVE
 -> LIVE_SIGNALS
```

A future Lab/Live repository split is allowed when operational maturity justifies it.

## 11. Final task report

For APP/research-platform changes report:

- workflow mode;
- what changed;
- files to copy/deploy, if any;
- exact test level;
- migration requirement;
- what to verify after installation/promotion;
- residual risks.