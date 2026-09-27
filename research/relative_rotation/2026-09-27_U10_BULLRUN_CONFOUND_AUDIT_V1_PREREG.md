# U10 Bull-Run Confound Audit v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Question

Is the apparent strength of U10 = canonical U8 + FIL + HBAR mainly explained by one or more constituent tokens having an exceptional historical bull run from a depressed base?

If so, token membership is allowed to change or a token may be dropped in later research. No live membership changes are made in this audit.

## Fixed candidate pool

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

User-held mandatory seed assets for alternative-universe diagnostics:
ATOM, TWT, PEPE

Canonical U8:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

U10:
canonical U8 + FIL + HBAR

## Price-explosion definitions fixed before results

All metrics use Binance Spot daily closes from the common dataset.

PRIMARY EXPLOSIVE flag:
A token is flagged if either:
- maximum low-to-high drawup within any 90-day rolling window is >= +200% (price >= 3x the prior-window low), OR
- maximum low-to-high drawup within any 180-day rolling window is >= +400% (price >= 5x the prior-window low).

Sensitivity bands:
- STRICT: 90d drawup >= +100% OR 180d drawup >= +200%.
- LENIENT: 90d drawup >= +300% OR 180d drawup >= +600%.

These thresholds classify price paths only. They do not use strategy outcomes.

## Tests

1. Compute for each of the 15 assets:
   - maximum 30d close return;
   - maximum 90d close return;
   - maximum 180d close return;
   - maximum 90d low-to-high drawup;
   - maximum 180d low-to-high drawup;
   - total common-history return;
   - PRIMARY / STRICT / LENIENT flags.

2. Report which U10 tokens are PRIMARY explosive.

3. Node ablation over the mature window:
   - remove each non-mandatory U10 token one at a time;
   - compare return, ATOM-start, drawdown and route changes.
   This measures whether an explosive token is structurally indispensable.

4. Bull-period contribution diagnostic:
   For each U10 token, identify its single strongest 90d drawup interval.
   Re-run U10 on the mature window while neutralizing positive mark-to-market returns of that token only during that strongest interval, without changing the signal/router path.
   This is a P&L attribution stress test, not a tradable counterfactual.

5. Multi-token bull neutralization:
   Neutralize positive holding returns during each PRIMARY-flagged U10 token's strongest 90d interval simultaneously.

6. Alternative U8 distribution:
   Among all 792 U8s containing ATOM/TWT/PEPE, label each by number of PRIMARY-explosive members.
   Compare last-year and two-year outcome distributions by explosive-member count.

7. Bull-clean candidate construction:
   Keep ATOM/TWT/PEPE as mandatory user-held assets even if one is flagged.
   Exclude PRIMARY-explosive optional assets.
   Enumerate all possible U8s from the remaining optional pool.
   If fewer than five optional assets remain, report that rather than weakening the rule.
   Compare the best structural/diversity candidates only as diagnostic; do not select by future return.

## Guardrails

- No token is removed from live U8/U10 based on this run.
- If PEPE/ATOM/TWT is explosive, it remains in mandatory-seed diagnostics because that is a user constraint, but the confound is reported explicitly.
- No thresholds are changed after viewing results.
- Historical bull-run classification is not a forecast that the token cannot rally again.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
