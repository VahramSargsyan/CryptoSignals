# WEIGHTED_MULTI_SIGNAL_V2 — Short Research R1

WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L3 — new shared short execution semantics**  
STRATEGY_FORMULA_CHANGED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**

## Question

The corrected 2026 V2 event study showed stronger bearish SELL information at +7/+14 candles than bullish BUY information. The existing long-only engine could use SELL only as an exit.

R1 asks a narrower question:

> What happens if the exact same frozen V2 signals are executed with a separate 1x collateralized short engine?

No strategy indicator or threshold was changed.

## Engine

`SHORT_ONLY_SIGNAL_FLIP_V1`

- SELL -> open 1x short next open;
- BUY -> cover next open;
- 10 bps fee;
- 5 bps slippage;
- intraday-high zero-equity liquidation;
- after zero equity, no further trades execute for that symbol/run.

## Development 2021–2025

Warm-up: 2020.  
Evaluation: 2021–2025.

- 220 trades
- win rate 45.45%
- average trade -0.09%
- median trade -1.08%
- PF 0.989
- positive total return 6/13 symbols
- median symbol max drawdown 68.35%
- median symbol total return -43.95%
- zero-equity liquidations: 2

Liquidated:

- ETHUSDT, 2021
- OSMOUSDT, 2023

Year PF:

- 2021: 1.076
- 2022: 1.285
- 2023: 0.363
- 2024: 0.703
- 2025: 1.819

The short side is therefore not a stable all-history solution.

## Nonblind 2026 YTD

Warm-up: 2025.  
Evaluation: 2026-01-01 through 2026-09-24.

- 48 trades
- win rate 47.92%
- average trade +1.76%
- median trade -1.44%
- PF 1.280
- positive total return 8/13 symbols
- median symbol max drawdown 28.04%
- median symbol total return +4.83%
- zero-equity liquidations: 0

For comparison, corrected long V2 over the same 2026 evaluation period:

- PF 1.033
- average trade +0.27%
- median trade -3.81%
- positive total return 4/13 symbols
- median symbol max drawdown 38.90%

The 2026 short execution is descriptively stronger than long execution, but this period is nonblind.

## MA context

Entry strength is context only:

- 80 = core confirmation without fully aligned MA50/100/200;
- 100 = core confirmation with aligned MA context.

Development:

- strength 80: PF 0.865
- strength 100: PF 1.351

2026:

- strength 80: PF 0.846
- strength 100: PF 1.728

This repeated separation is a useful hypothesis. It is still post-hoc because the short engine and this comparison were built after examining bearish directional evidence.

A future **MA-aligned short-only profile** must be frozen as a separate execution hypothesis before future unseen data is used.

## Important benchmark caveat

In short mode, `benchmark_return` is a continuous 1x short benchmark with the same zero-equity liquidation rule.

Many long-term rising crypto assets drive that benchmark to -100%. Therefore “beat short benchmark” is not a strong quality gate by itself.

## Reserved unseen boundary

For any short hypothesis derived here:

`2026-09-26T00:00:00Z` onward is future unseen evidence.

Do not tune the frozen short profile from that period.

## Conclusion

The research supports three factual statements:

1. V2 short execution is not robust across 2021–2025.
2. It is materially stronger in the already-known 2026 period than V2 long execution.
3. Fully bearish-aligned MA context is repeatedly associated with better short trades in both development and 2026.

It does **not** establish an accepted short strategy or authorize live/paper-live shorting.
