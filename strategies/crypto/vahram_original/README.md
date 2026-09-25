# VAHRAM_ORIGINAL_V1 — frozen baseline

This module freezes the historical CryptoSignals behavior that existed at legacy commit:

\`e363ffd8a4d29492bb6e685181770c39ca3047d7\`

Protected behavior:

- Bollinger Bands: 20-period mean, pandas rolling sample standard deviation, ±2σ;
- the historical function called \`StochRSI\`: Close normalized inside rolling 14-Close min/max, then 3-period smoothing, ×100;
- volume moving average: 20 periods;
- BUY / SELL thresholds from the legacy analyzers;
- strength-version candle body filter \`< 0.2 => ignored\`;
- legacy Strength 0–100 scoring and action/change thresholds.

The historical \`StochRSI\` name is intentionally preserved as behavior, not corrected. Canonical Stochastic RSI must be implemented under a new strategy/version (\`VAHRAM_TRUE_STOCHRSI_V2\`).

The old \`scripts/*\` files remain untouched in this phase. This module is a research baseline and regression oracle for future refactors.
