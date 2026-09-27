# ATOM Sunset Migration v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/atom-sunset-migration-v1
GitHub Actions run: 36342261693
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Scope

Current U10 under test:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

This audit changes only ATOM behavior.

No live universe or paper monitor was changed.

## Variants

BASE_U10:
normal entries into and exits from ATOM.

NO_ATOM_U9:
TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

ATOM_EXIT_ONLY:
ATOM may be held initially, but all incoming transitions to ATOM are forbidden.
Outgoing ATOM transitions remain active.
Once capital leaves ATOM, it cannot return.

ATOM_4X25_SIGNAL_MIGRATION:
four 25% ATOM tranches.
Each tranche exits on the next distinct valid outbound ATOM signal.
After exit, that tranche follows the no-ATOM U9 router.

Frozen strategy:
- Binance Spot 1D
- 180d rolling median
- 15% ARM
- 3% reversal confirmation
- strongest max-dislocation router
- next-open execution
- 0.1% transition cost

## Fixed-window comparison

Cross-universe medians use the nine common non-ATOM starters.

| Variant | Mature ~35m | Latest 2Y | Latest 1Y |
|---|---:|---:|---:|
| BASE_U10 | +2356.75% | +723.31% | +89.82% |
| NO_ATOM_U9 | +5639.09% | +1718.15% | +18.55% |
| ATOM_EXIT_ONLY | +5639.09% | +1718.15% | +18.55% |

For non-ATOM starting assets, NO_ATOM_U9 and ATOM_EXIT_ONLY are identical by construction.

Interpretation:
removing ATOM materially improved the long historical horizon, but the normal U10 was stronger over the exact latest one-year regime for routes that did not start in ATOM.

This argues against treating ATOM removal as universally superior in every regime.

## ATOM-start comparison

### Mature

BASE_U10 from ATOM:
+2142.74%

ATOM_EXIT_ONLY from ATOM:
+5640.05%

Median max drawdown:
- BASE_U10: -71.04%
- EXIT_ONLY: -59.67%

### Latest 2Y

BASE_U10:
+709.46%

EXIT_ONLY:
+784.83%

### Latest 1Y

BASE_U10:
+116.90%

EXIT_ONLY:
+171.67%

Latest-1Y max drawdown:
- BASE_U10: -53.73%
- EXIT_ONLY: -59.67%

Thus for capital already held in ATOM, the historical EXIT_ONLY rule improved terminal return in all three fixed windows, although latest-year drawdown was somewhat worse.

## Historical staged-migration study

Migration start dates:
monthly from 2024-01-01 through 2026-03-01.

Total starts:
27

Evaluation end:
2026-09-26

### Completion

Full four-tranche migration completed:
27 / 27 = 100%

Median timing:
- 25% migrated: 19 days
- 50%: 45 days
- 75%: 61 days
- 100%: 71 days

Observed days to full migration:
- minimum: 9 days
- median: 71 days
- mean: 85 days
- maximum: 247 days

Therefore a signal-driven 4x25 migration was usually a 1–3 month process but could take much longer in sparse-signal regimes.

## Migration performance

Across the 27 monthly migration starts, all strategies are evaluated to the common endpoint 2026-09-26.

Median terminal returns:

- normal BASE_U10 from ATOM: +347.88%
- full-capital EXIT_ONLY: +420.07%
- staged 4x25 migration: +404.12%

Win rates versus normal BASE_U10:

- EXIT_ONLY beat BASE_U10: 27 / 27 = 100%
- 4x25 staged beat BASE_U10: 25 / 27 = 92.59%

4x25 staged beat full EXIT_ONLY:
13 / 27 = 48.15%

Thus historical evidence favors forbidding ATOM re-entry.
The exact choice between one-signal full exit and staged exit is less decisive:
- full EXIT_ONLY had better median performance;
- staged exit reduced concentration in one execution/signal date;
- staged migration occasionally lagged because part of the portfolio remained in ATOM longer.

## Two staged-migration exceptions

4x25 staged migration underperformed normal BASE_U10 in only two of the 27 start dates:

- migration start 2025-10-01:
  BASE_U10 +115.59%
  staged +74.00%
  EXIT_ONLY +170.03%

- migration start 2025-12-01:
  BASE_U10 +99.22%
  staged +90.62%
  EXIT_ONLY +149.53%

These cases reinforce that delaying later tranches can be costly when the first valid exit route is already favorable.

## Exit destinations

Across 108 tranche exits (27 starts × 4 tranches), first destinations were:

- BNB: 28
- PEPE: 27
- TWT: 17
- FIL: 17
- TRX: 7
- HBAR: 5
- SOL: 4
- AAVE: 3

No fixed replacement asset was required.

The migration therefore works as a graph exit, not as “sell ATOM and buy one chosen coin”.

This is preferable methodologically because the destination is determined by the same relative-rotation signal rather than hindsight.

## Operational architecture

The evidence supports treating ATOM as a SUNSET / LEGACY asset:

1. DISABLE_INCOMING_ATOM
   No future graph transition may enter ATOM.

2. KEEP_OUTGOING_ATOM
   Existing ATOM can still generate a valid exit signal.

3. NO_REENTRY_AFTER_EXIT
   Once a tranche leaves ATOM, ATOM is no longer eligible for that tranche.

4. OPTIONAL_STAGED_MIGRATION
   Existing ATOM can be split into several tranches if the user prioritizes execution/timing diversification over the historically higher median result of full EXIT_ONLY.

5. COSMOS CONSOLIDATION
   Other Cosmos-ecosystem assets may be operationally converted into ATOM first if that is the chosen practical route.
   In that case ATOM should be treated as a temporary consolidation/transit asset, not a strategic destination.

This audit does not evaluate staking unbonding, liquid-staking conversion spreads, taxes, or token-specific Cosmos exit routes.

## Important limitation

The results are historical and partly overlap the research periods used to discover the weakness of mandatory ATOM.

The very large long-horizon NO_ATOM result should not be interpreted as a future return expectation.

The most robust conclusion is narrower:

Preventing re-entry into ATOM did not damage the historical ATOM-start migration tests and improved terminal return in all 27 tested monthly starts.

## Research status

ATOM:
SUNSET_CANDIDATE / EXIT_ONLY_CANDIDATE

Not:
FORCED_IMMEDIATE_SELL

Not:
FUTURE_DESTINATION

Live/paper-live:
UNCHANGED
