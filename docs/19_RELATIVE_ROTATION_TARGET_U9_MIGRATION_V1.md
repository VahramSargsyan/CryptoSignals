# Relative Rotation Target U9 Migration Plan v1

Date: 2026-09-28
Mode: PATCH_FIX

## Source state

Current main monitor universe before this patch:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Target state

Bull-neutralized target universe:

TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP

## Membership delta

KEEP:
- TWT
- PEPE
- BNB
- TRX
- AAVE
- FIL

SUNSET / EXIT-ONLY:
- ATOM
- SOL
- LINK
- HBAR

ADD:
- AVAX
- ALGO
- XRP

## Migration behavior

This is not an immediate rebalance.

The monitor observes the union of TARGET + SUNSET assets.

Routing rules:

1. If the real held asset is a SUNSET asset, keep it until a valid confirmed outbound signal to a TARGET asset appears.
2. Only TARGET destinations are actionable.
3. After leaving a SUNSET asset, do not recommend re-entry into ATOM/SOL/LINK/HBAR.
4. If the real held asset is already in TARGET, all future actionable destinations remain inside TARGET.
5. Actual execution stays manual.
6. After every real swap, update config/relative_rotation_paper_live_v1.json held_asset to the asset actually held.

## Evidence used

Bull-neutralized exhaustive search:
- 27,824 universes U6..U15
- conservative target: TWT/PEPE/BNB/TRX/AAVE/AVAX/FIL/ALGO/XRP
- bull-neutralized mature median: about +1323%
- current U10 under same neutralization: about +779%
- latest 2Y: about +1180%
- latest 1Y: about +156%
- rolling 24m positive rate: 100%

Migration stress test:
- 27 monthly starts from 2024-01 through 2026-03
- transition beats source-U10 median in 85.2% of start dates
- all four sunset assets achieved 100% exit completion
- median exit timing:
  - ATOM: 19 days
  - SOL: 2 days
  - LINK: 3 days
  - HBAR: 3 days

## Important residual risk

The target remains a historical research selection, not untouched forward validation.

Trailing one-year endpoint sensitivity is regime-dependent:
- the target does not beat current U10 at every nearby 2026 endpoint.

Therefore the migration is signal-driven and gradual rather than an immediate full rebalance.

## Rollback

Rollback is operationally simple because execution is manual:
- do not execute the suggested migration swap;
- keep current held_asset unchanged;
- revert target/sunset config and routing guard in Git if the monitoring logic proves defective.

No exchange API keys or automatic order placement are introduced.

## Forward-hypothesis activation gate

The current TARGET U9 is transitional and is expected to become TARGET U10 after a tenth candidate is selected.

Research hypotheses that depend on portfolio-level U10 reference behavior must not start forward validation during this changing-membership phase.

Canonical registry:

`research/relative_rotation/FORWARD_HYPOTHESIS_REGISTRY_V1.md`

Activation config:

`config/relative_rotation_forward_hypotheses_v1.json`

Current status:

`BLOCKED_UNIVERSE_NOT_FROZEN`

After the tenth candidate is selected:

1. freeze and version the final TARGET U10 membership;
2. record a forward-validation start date;
3. activate eligible hypothesis tracking without backfilling old candles as forward evidence;
4. allow Telegram research reminders only for causal future matches;
5. keep research reminders separate from accepted strategy signals.
