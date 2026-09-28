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


## Grid vs published OSS-strategy benchmark memory

Before proposing another comparison against public crypto strategies, read `docs/evidence/2026-09-26_GRID_VS_OSS_STRATEGIES_BENCHMARK_V1.md`.

Already benchmarked on the canonical five-asset D1 development sample with normalized fees/slippage: Gekko Fibonacci 8/21/55, Zenbot MACD default, Zenbot SRSI_MACD default, and Zenbot Bollinger default. The strongest external candidate was Gekko Fibonacci, nearly matching Grid BASE in aggregate and beating WIDE on ETH/BNB/BTC. Treat Zenbot D1 results as portability tests because their defaults were designed for intraday periods.


## Grid bar-normalized timeframe research memory

Before proposing that the canonical Grid should use 1095 candles on any timeframe, read `docs/evidence/2026-09-26_GRID_BAR_NORMALIZED_TIMEFRAME_RESEARCH_V1.md`.

Already tested preliminarily: H1 on BTC/ETH/BNB; BTC 15m/5m/1m mechanics/cost smoke tests; same-calendar D1 versus 4H on BTC/ETH; recent 4H bear diagnostics on ETH/SOL/LINK. The Grid mechanics survive below D1, but 1095 bars is **not** proven scale-invariant. D1 beat 4H on the same calendar period, and 1m WIDE +6 spacing fell below the rough round-trip cost assumption. Future work should use one pinned 1m source aggregated deterministically across all timeframes.


### Expanded timeframe evidence — 2026-09-26

The bar-normalized timeframe log now includes stronger coverage: full five-asset 4H, full five-asset 15m, and longer BTC 5m/1m boundary probes. The strongest conclusion remains that 1095 bars are **not** scale-invariant. D1 materially beat 4H on the same calendar period; 5m is cost-sensitive; 1m WIDE Micro spacing falls below modeled round-trip friction. Read the evidence log before proposing lower-timeframe tuning.


### Dual-timeframe research memory

The timeframe evidence now also covers independent D1/H4 capital books (75/25, 50/50, 25/75) and D1 entries with entry-time-frozen H4-derived +6/+18 exit spacing. Neither improved the canonical D1 return profile; H4 exit assistance mainly reduced drawdown at the cost of higher turnover and lower return. Read the timeframe evidence before proposing the same variants.


### Micro / Mid redundancy research memory

Before proposing simplification of the two capital pools, read `docs/evidence/2026-09-26_GRID_MICRO_MID_REDUNDANCY_STRESS_TEST_V1.md`.

Current evidence: Micro and Mid are independent engines with no nonlinear capital synergy. The canonical 20/80→80/20 scan shows a return/drawdown tradeoff rather than universal dominance; 50/50 is not a special optimum. Supplementary annual tests show the stronger layer can change by regime. Do not remove a layer without an exact canonical 100/0 vs 0/100 endpoint test or an explicitly different shared-wallet hypothesis.


### Expanded Micro / Mid universe evidence

The Micro/Mid redundancy log now includes a 15-asset common-window test. Across all 15, Micro-only had higher aggregate return (+170.91% vs +147.17%) but Mid used far fewer exits. On the Tier A quality/Grid subset (LINK/SOL/ETH/ADA/XLM), Micro and Mid were effectively tied (+244.93% vs +245.61%), Mid had slightly lower median DD, and Mid used 68.6% fewer exits. Read the evidence before proposing layer deletion.


## Relative Rotation U9/U10 universe research memory — 2026-09-28

Before any new ATOM replacement, U9/U10 universe-selection, HBAR-addition, or "find the tenth token" experiment for the relative-rotation strategy, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_U9_U10_UNIVERSE_DECISION_LOG_V1.md`

and the underlying canonical evidence:

`research/relative_rotation/2026-09-28_ROBUST_EXHAUSTIVE_U9_U10_ATOM_REPLACEMENT_V1_EVIDENCE.md`

Frozen research candidate:

`RR_TARGET_U10_CANDIDATE_HBAR_V1 = CURRENT_TARGET_U9 + HBAR`

Do not repeat the completed 15-token exhaustive U9/U10 search merely to rediscover the same result. A rerun requires materially new unseen data, a documented engine/semantic correction, materially changed cost assumptions, a materially changed token pool/mechanism, or an explicitly separate confirmatory/forward-validation protocol. Any future run must state what is materially new.

The candidate is not production-approved. Current production/paper-live U9 remains unchanged until a separate promotion gate is satisfied.


## Relative Rotation multibook diversification research memory — 2026-09-28

Before proposing or rerunning a three-book diversification architecture for `RR_TARGET_U10_CANDIDATE_HBAR_V1`, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_MULTIBOOK_DIVERSIFICATION_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_DIVERSIFICATION_V1_EVIDENCE.md`

Already tested:
- V2_FREE: three independent books with collisions allowed;
- V3_COLLISION_GUARD: three independent books forced to end in three distinct assets.

Frozen findings:
- V2 is rejected as a diversification mechanism because independent paths rapidly converge into the same asset;
- V3 is rejected in its current strict form because concentration falls but historical return collapses and long-window drawdown does not consistently improve.

Do not rerun the same V2/V3 experiment without a materially new reason. `MAX_2_OF_3_BOOKS_PER_ASSET` has since been tested separately as `V4_MAX2_SHADOW_FALLBACK`; read `docs/evidence/2026-09-28_RELATIVE_ROTATION_MULTIBOOK_MAX2_SHADOW_DECISION_LOG_V1.md` before proposing or rerunning it.


## Relative Rotation shadow/defensive multibook memory — 2026-09-28

Before concluding that three-book diversification failed, or before proposing another TRX/USDT substitute, shadow-routing, low-vol parking, or actual-vs-shadow state experiment for `RR_TARGET_U10_CANDIDATE_HBAR_V1`, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_MULTIBOOK_SHADOW_DEFENSIVE_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_SHADOW_DEFENSIVE_FALLBACK_V1_EVIDENCE.md`

Important distinction:
- the earlier `V3_COLLISION_GUARD` rejection applies only to the strict-stay implementation;
- `V3_SHADOW_DEFENSIVE_FALLBACK` is a materially different dual-state architecture and is classified `VIABLE_RESEARCH_ARCHITECTURE / NOT_PRODUCTION_APPROVED`.

Frozen shadow result:
- `shadow_core_asset` continues ordinary U10 routing;
- `actual_asset` may park in the lowest-volatility feasible U10 token;
- TRX was not hardcoded but represented about 49.5% of MATURE parking book-days;
- MATURE median return +673.9%, median max DD -45.3%, median daily largest-asset share 43.6%, actual collision days 0%.

Do not rerun this exact shadow/defensive experiment on the same history unless there is materially new data, a semantic correction, changed costs/universe/portfolio rules, or a separately preregistered confirmation/forward protocol.

`MAX_2_OF_3_BOOKS_PER_ASSET` has now been tested separately as `V4_MAX2_SHADOW_FALLBACK`; read the MAX2 decision log below before proposing or rerunning it.


## Relative Rotation MAX2 multibook memory — 2026-09-28

Before proposing or rerunning a two-books-per-asset architecture for `RR_TARGET_U10_CANDIDATE_HBAR_V1`, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_MULTIBOOK_MAX2_SHADOW_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_MULTIBOOK_MAX2_SHADOW_FALLBACK_V1_EVIDENCE.md`

Frozen result for `V4_MAX2_SHADOW_FALLBACK`:
- MATURE median return +2223.8%;
- MATURE median max DD -57.1%;
- median daily largest-asset share 80.5%;
- median peak concentration 95.1%;
- full 3-in-1 physical convergence 0%;
- 2+1 structure 98.7% of MATURE days;
- TRX represented about 96.6% of aggregate MATURE defensive parking book-days.

Interpretation: MAX2 is a viable research compromise that preserves much more upside than MAX1 shadow while blocking literal 3-in-1 occupancy, but it does not create strong value diversification. Do not rerun unchanged on the same data.


## Relative Rotation U10 regime robustness memory — 2026-09-28

Before describing `RR_TARGET_U10_CANDIDATE_HBAR_V1` as all-weather / all-season, or before rerunning bull-vs-bear attribution, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_U10_REGIME_ROBUSTNESS_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_REGIME_ROBUSTNESS_V1_EVIDENCE.md`

Frozen classification:

`BULL_DEPENDENT`

Key MATURE conditioned evidence:
- BTC_BULL: +1257.2% median conditioned return;
- BTC_BEAR: -15.4%, positive starts 0%;
- BROAD_BULL: +30366.8%;
- BROAD_BEAR: -88.3%, positive starts 0%;
- BROAD_SIDEWAYS: -8.7%.

Important nuance:
- the strategy historically lost less than BTC / broad U10 on BTC_BEAR days, but still lost in absolute terms;
- prior bull-neutral evidence only showed the strategy was not dependent on a narrow set of extreme bull days; it did not prove bear-market profitability.

Do not rerun unchanged on the same history. The next materially distinct research question is a preregistered defensive-overlay test, potentially using the existing research candidate `DEFENSIVE_LOW_VOL_CRYPTO_SMA200_BREADTH_3_5_CONFIRM3_VOL30`. That candidate remains not production-approved.


## Relative Rotation U10 pure BTC SMA200 regime memory — 2026-09-28

Before claiming the frozen U10 is all-weather or before rerunning a pure BTC SMA200 split, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_U10_PURE_BTC_SMA200_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_PURE_BTC_SMA200_REGIME_V1_EVIDENCE.md`

Frozen result:
- BTC above SMA200: U10 +4454.9% conditioned MATURE return; 100% positive starts;
- BTC below SMA200: U10 -28.3%; 0% positive starts;
- BTC itself on below-SMA200 days: -55.9%.

This independently confirms the H8 50/200 classification that the base U10 is bull-dependent in absolute-return terms. Below SMA200 the strategy shows relative resilience, not positive bear-market profitability.

Do not rerun unchanged on the same history.


## Relative Rotation U10 TradingView TOTAL + SMA memory — 2026-09-28

Before discussing broad market-cap regime transitions for the frozen U10, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_U10_TRADINGVIEW_TOTAL_SMA_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_MONTHLY_TRADINGVIEW_TOTAL_SMA_V1_EVIDENCE.md`

Canonical market-cap source for this research family:

`TradingView CRYPTOCAP:TOTAL`

with exact daily SMA50 / SMA100 / SMA200 / SMA300.

Do NOT use run `36435470008`; it is `INVALID_SOURCE_SEMANTICS` because CoinMarketCap historical-page `globalMetrics.marketCap` was current-site metadata rather than historical market cap.

Corrected runtime:
- run `36436522995`
- artifact `10975563877`
- SHA256 `b8d5ebfd7397bccd9024eb1e37aef243904dd429ff680b70e41bb8c45b5e69e0`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

Frozen current state at 2026-09-26:

`RECOVERY / BULL_BUILDING, NOT FULL_BULL_CONFIRMED`

Values:
- TOTAL ~$2.87T
- SMA50 ~$2.55T
- SMA100 ~$2.35T
- SMA200 ~$2.39T
- SMA300 ~$2.51T

Key sequence:
- 2026-08-17: confirmed exit FULL_BEAR
- 2026-08-19: TOTAL above SMA200
- 2026-08-21: TOTAL above SMA300
- 2026-08 and 2026-09 month-end: BULL_BUILDING

Do not promote this descriptive regime framework into production without a separate preregistered defensive-overlay/timing test.


### Repository cache for CRYPTOCAP:TOTAL

For research requiring TradingView `CRYPTOCAP:TOTAL` daily history on or before
2026-09-26, use the repository cache first:

- `data/market/cryptocap_total_d1.csv`
- `data/market/cryptocap_total_d1.meta.json`
- `data/market/README.md`

Frozen cache identity:
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- dataset SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`
- source run: `36436522995`
- source artifact: `10975563877`

Do NOT fetch TradingView again merely to reproduce analysis fully covered by this
cache. External refresh is justified only for dates after the cached end date,
a documented source/semantic correction, or failed cache validation.

The monthly TOTAL research script is cache-first and was validated without
`tvdatafeed` installed in run `36439261228`.


## Relative Rotation U10 TOTAL SMA25/50/100 crossover memory — 2026-09-28

Before proposing SMA25/50 or SMA25/100 market-cap crosses as U10 trading signals, read:

`docs/evidence/2026-09-28_RELATIVE_ROTATION_U10_TOTAL_SMA25_50_100_CROSS_DECISION_LOG_V1.md`

and:

`research/relative_rotation/2026-09-28_RR_U10_TOTAL_SMA25_50_100_CROSS_V1_EVIDENCE.md`

Frozen interpretation:
- `SMA25_CROSS_ABOVE_50` is an early attention trigger, not a supported standalone buy rule;
- the strongest tested bullish confirmation was `SMA25 > SMA50`, followed by `SMA50 > SMA100`;
- median confirmation lag for that sequence was ~29 days;
- 60d U10 median after confirmation was +22.7% with 100% positive outcomes among only 4 full-horizon events;
- sample size is small and not production-approved;
- bearish crosses are risk-attention events, not automatic U10 exits.

Current 2026 sequence:
- 2026-07-22 SMA25 > SMA50
- 2026-08-23 SMA25 > SMA100
- 2026-08-27 SMA50 > SMA100 / BULL_BUILDING

Do not rerun unchanged on the same cached history. Use the persistent repository TOTAL cache.
