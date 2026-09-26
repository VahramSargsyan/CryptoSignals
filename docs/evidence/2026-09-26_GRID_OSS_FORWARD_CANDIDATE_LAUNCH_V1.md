# Grid OSS Forward Candidate Launch v1 — 2026-09-26

Date: 2026-09-26  
Mode: **BUILD_NEW_APP**  
Execution: **PAPER ONLY**  
Canonical strategy mutation: **NO**  
Strategy promotion: **NO**

## Frozen profile

`MID_OSS_ATR50_TRAIL7`

Rules:

- MID-only;
- normalized capital: 2000 per symbol;
- linear-depth p=1;
- 1095-candle causal H/L;
- ATR14;
- H/L refresh gate: ATR shift >50%;
- refresh cooldown: 60 daily candles;
- price escape may refresh the range after cooldown;
- MID recovery: +18 sublevels;
- current MID percentage targets;
- target touch arms the exit;
- actual SELL occurs only on a later candle after 7% retracement from the observed post-target peak;
- fee: 10 bps;
- slippage: 5 bps;
- no real orders.

## Why one 7% profile

Historical research found a 6–8% plateau. Forward observation freezes one midpoint candidate at 7% to avoid running three near-duplicate profiles and selecting the best after seeing forward data.

## Paper comparison

The new profile runs beside, not instead of:

- CONTROL_BASE
- CANDIDATE_WIDE
- MICRO_ONLY_WIDE
- MID_ONLY_WIDE

Paper start remains:

`2026-09-26T00:00:00Z`

Universe remains:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

## Evidence behavior

Candidate events include:

- BUY
- ARM_EXIT
- SELL

`ARM_EXIT` is diagnostic evidence. Notification policy counts only BUY/SELL and does not notify merely because a trailing exit became armed.

Every candidate event receives a deterministic `RUN-OSS-...` identifier tied to dataset/source/config/start.

## Verification

PR: #47

Repository CI after candidate integration:

- **119 tests passed**

Candidate engine includes a structural-equivalence test against the original OSS research harness on a shared deterministic dataset:

- total return: exact match within 1e-12
- max drawdown: exact match within 1e-12
- closed trade count: exact match

Runtime smoke:

- GitHub Actions run: `36237171449`
- smoke paper start: `2026-09-20T00:00:00Z`
- completed smoke candles: 6
- profiles: `MID_ONLY_WIDE`, `MID_OSS_ATR50_TRAIL7`
- runtime status: `PAPER_LIVE_OBSERVATION`
- artifact: `10904825361`
- candidate event ledger: present
- deterministic `RUN-OSS-...` IDs: present
- real orders: false

During the six-day smoke window both profiles had the same marked equity because no candidate-specific trailing exit or ATR-gated range divergence had yet occurred. This is expected and is not treated as performance evidence.

## Migration

See:

`docs/migrations/2026-09-26_GRID_OSS_FORWARD_CANDIDATE_MIGRATION_PLAN.md`

No database/schema/user-data migration exists. The change is an additive paper-live module relationship.

## Promotion boundary

No parameter retuning is allowed inside this observation line.

The candidate remains:

**FROZEN FOR FORWARD PAPER / NOT PROMOTED**

Future evidence should compare the frozen candidate against `MID_ONLY_WIDE` on genuinely new closed candles.

## Test level

`TEST_LEVEL: CI_REGRESSION + STRUCTURAL_EQUIVALENCE + GITHUB_ACTIONS_RUNTIME_SMOKE`

No result authorizes live-money execution.
