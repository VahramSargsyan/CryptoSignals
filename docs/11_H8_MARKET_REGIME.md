# H8 — Crypto Market Regime Layer

WORKFLOW_MODE: BUILD_NEW_APP  
RISK_CLASS: L3 — shared research classification and evidence semantics  
MIGRATION_REQUIRED: NO

## Definition of Ready

CAPABILITY_ID: CRYPTO_STRATEGY_LAB_H8  
USER_RESULT: pre-OOS strategy evidence can answer not only whether a signal worked, but under which crypto market conditions it worked.  
CANONICAL_SOURCE_OF_TRUTH: the same canonical Binance daily datasets used by strategy comparison.  
SCOPE: BTC trend, broad crypto trend, market breadth, BTC realized volatility, cross-symbol volume participation, regime-tagged event/trade evidence, by-symbol/by-year/pooled summaries.  
OUT_OF_SCOPE: macro/Dalio inputs, order-book liquidity, 2026 OOS reveal, optimization, Google Sheets, Telegram/live trading.  
DO_NOT_TOUCH: strategy formulas, LONG_ONLY_SIGNAL_FLIP_V1 semantics, legacy scripts, protected 2026 period.  
DEPENDENCIES: existing H1-H7 candle/data/comparison pipeline; pandas only.  
SHARED_HOTSPOTS: canonical candle timestamps, strategy signal timestamps, evidence summaries.  
SCHEMA / ID / RELATION IMPACT: no persisted production schema.  
MIGRATION_PLAN: not required.  
ROLLBACK: revert H8 PR; H0-H7 remain intact.  
PROMOTION_PATH: unit/regression tests -> PR -> main -> pre-OOS regime research workflow -> artifact inspection -> permanent small evidence summary.  
FRESHNESS_FINGERPRINT: main SHA + regime request JSON + dataset IDs + regime config.  
REVALIDATE_IF: regime formula/config changes, universe changes materially, timeframe changes, canonical candle semantics change, or H7 execution semantics change.  
UNRESOLVED_DECISIONS: NONE for H8 implementation. Acceptance gates remain a separate freeze before 2026 OOS.

## No-lookahead rule

Every regime label for candle T uses only information available at or before candle T close.

A signal calculated on T close may therefore be tagged with the T regime and still execute no earlier than T+1 open.

Future candles never participate in T regime classification.

## Regime dimensions

### BTC trend

Trailing moving averages:

- fast: 50 daily candles;
- slow: 200 daily candles.

Labels:

- BTC_BULL: BTC close > slow SMA and fast SMA > slow SMA;
- BTC_BEAR: BTC close < slow SMA and fast SMA < slow SMA;
- BTC_TRANSITION: enough history exists but the two conditions disagree;
- UNKNOWN: not enough trailing history.

### Market breadth

For every eligible tracked symbol:

    close > trailing 50D SMA

market_breadth is the fraction of eligible symbols satisfying that condition.

A symbol is eligible only after it has enough actual post-listing history. Pre-listing time is never treated as missing history.

At least 5 eligible symbols are required.

Labels:

- BREADTH_STRONG: breadth >= 0.60;
- BREADTH_WEAK: breadth <= 0.40;
- BREADTH_MIXED: otherwise;
- UNKNOWN: insufficient eligible assets/history.

### Broad crypto trend

Derived transparently from breadth:

- BREADTH_STRONG -> BROAD_BULL;
- BREADTH_WEAK -> BROAD_BEAR;
- BREADTH_MIXED -> BROAD_SIDEWAYS;
- insufficient history -> UNKNOWN.

This deliberately avoids inventing a proprietary market-cap-weighted index in H8.

### BTC volatility

BTC daily close returns are converted to trailing 20D realized annualized volatility.

The comparison baseline is the trailing 252-observation median of that 20D volatility.

Labels:

- VOL_HIGH: current realized volatility >= 1.25 × trailing baseline;
- VOL_LOW: current realized volatility <= 0.80 × trailing baseline;
- VOL_NORMAL: between those boundaries;
- UNKNOWN: insufficient history or non-positive baseline.

The baseline is trailing only, so classification does not use future volatility.

### Volume participation

Raw token volumes cannot be added across assets because units differ.

Instead H8 asks, for each eligible asset:

    current volume > that asset's own trailing 20D volume mean

volume_breadth is the fraction satisfying that condition.

Labels:

- VOLUME_HIGH: volume breadth >= 0.60;
- VOLUME_LOW: volume breadth <= 0.40;
- VOLUME_NORMAL: otherwise;
- UNKNOWN: insufficient eligible assets/history.

This is a market participation measure, not a claim to measure true order-book liquidity. Spread/depth liquidity can be a later data-source extension.

## Composite regime

regime_id is the explicit tuple:

    BTC_TREND | BROAD_CRYPTO_TREND | VOLATILITY_REGIME | VOLUME_REGIME

The underlying dimensions remain stored separately; the composite is not allowed to hide them.

## Evidence attachment

Event-study rows are tagged by the regime at signal candle close.

Trading rows are tagged by the regime at the entry signal candle close, before next-open execution.

H8 outputs:

- annotated event observations;
- annotated trades;
- event regime summary by symbol;
- pooled event regime summary;
- pooled event regime summary by calendar year;
- trade regime summary by symbol;
- pooled trade regime summary;
- pooled trade regime summary by calendar year.

Pooled cross-symbol results are descriptive. They are not a multi-asset portfolio backtest.

## Protected OOS

The first H8 request remains bounded to:

2021-01-01 <= candles < 2026-01-01

The 2026 held-out period remains unopened.

After H8 pre-OOS evidence is captured, acceptance gates must be frozen before any 2026 strategy evaluation is allowed.
