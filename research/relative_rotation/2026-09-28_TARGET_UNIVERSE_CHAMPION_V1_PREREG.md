# Target Universe Champion v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY

## Goal

Find the strongest whole-universe composition from the fixed 15-token research pool without:
- mandatory ATOM/TWT/PEPE constraints,
- forcing a specific universe size,
- performing one-token replacement around the current U10.

Then compare the selected target candidate against the current U10 and derive a gradual membership migration map.

No live strategy change is authorized by this research run.

## Candidate pool

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

## Search space

Universe sizes U6 through U15 inclusive.

Total combinations:
27,824.

U5 and smaller are excluded deliberately. The current task is to find a serious diversified rotation graph, not a minimal concentrated portfolio.

## Current production-transition reference

CURRENT_U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

This is only the migration reference. It receives no ranking bonus.

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No parameter tuning.

## Stage 1 — exhaustive mature ranking

Every U6..U15 universe is evaluated from every member asset as a starting asset.

Mature window:
2023-10-31 -> 2026-09-26

Primary raw ranking:
median terminal return across all member starts.

Report:
- RAW CHAMPION
- top 20 overall
- best universe at each size U6..U15
- distribution by universe size
- current U10 rank against the entire search space

## Stage 2 — robustness shortlist

Freeze the shortlist before robustness outcomes are inspected:

- top 100 universes by mature median overall;
- plus best 5 universes from each size U6..U15;
- plus CURRENT_U10;
- deduplicate.

For every shortlisted universe evaluate:

Fixed windows:
- latest 2Y: 2024-09-27 -> 2026-09-26
- latest 1Y: 2025-09-27 -> 2026-09-26

Rolling windows:
- 12m monthly starts from mature start
- 24m monthly starts from mature start

Metrics:
- mature median return
- latest 2Y median
- latest 1Y median
- worst rolling 12m median
- positive rolling-12m rate
- worst rolling 24m median
- positive rolling-24m rate
- median max drawdown
- worst member-start return
- transitions/conflicts

## TARGET CHAMPION selection rule

Do NOT simply pick the largest backtest number.

Among the frozen shortlist:

1. Require:
   - latest 1Y median > 0
   - latest 2Y median > 0
   - rolling 12m positive-window rate >= 60%
   - rolling 24m positive-window rate >= 80%

2. Convert these five higher-is-better metrics to percentile ranks within the shortlist:
   - mature median return
   - latest 2Y median
   - latest 1Y median
   - worst rolling 12m median
   - worst rolling 24m median

3. ROBUST_PROFIT_SCORE = mean of those five percentile ranks.

4. TARGET CHAMPION = highest ROBUST_PROFIT_SCORE among eligible candidates.

Tie-break:
- higher mature median;
- then smaller universe size;
- then lexicographic key.

No universe-size bonus or penalty is otherwise applied.

## Stage 3 — bull-run attribution

For RAW CHAMPION and TARGET CHAMPION:

For each member asset:
- identify its strongest 90d low-to-high drawup within the strategy-common history;
- PRIMARY explosive if 90d >= +200% or 180d >= +400%.

Then, one token at a time:
- keep the strategy route unchanged;
- neutralize positive strategy-equity days only while the route is holding that token during its strongest 90d drawup interval.

Also neutralize all PRIMARY-explosive member intervals simultaneously.

Report:
- raw mature median;
- per-token neutralized mature median;
- all-PRIMARY neutralized mature median;
- dependence ratio = neutralized/raw terminal wealth ratio.

This is attribution only, not a tradable counterfactual.

## Stage 4 — endpoint sensitivity

For TARGET CHAMPION and CURRENT_U10, evaluate trailing 365d windows ending:
- 2026-05-31
- 2026-06-30
- 2026-07-31
- 2026-08-31
- 2026-09-26

Report terminal median return and drawdown.

TARGET is not considered stable if it beats CURRENT_U10 only at the final endpoint.

## Stage 5 — migration map

Compare TARGET CHAMPION with CURRENT_U10:

- KEEP = intersection
- EXIT = CURRENT_U10 - TARGET
- ADD = TARGET - CURRENT_U10

No arbitrary one-for-one mapping is forced.

Migration priority is descriptive:
1. assets in EXIT are sunset candidates;
2. assets in ADD are admission candidates;
3. actual capital movement should follow valid relative-rotation signals rather than a forced immediate swap.

Also report whether each EXIT asset is actually used by TARGET (it cannot be by definition) and whether its removal from CURRENT_U10 changes historical route behavior.

## Guardrails

- This is historical optimization and therefore not untouched future validation.
- RAW CHAMPION is explicitly hindsight-only.
- TARGET CHAMPION is still research-only even though it uses robustness filters.
- Do not merge PR #54 or alter live/paper-live membership from this run alone.
- No automatic trading.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
