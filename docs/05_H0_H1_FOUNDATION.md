# H0/H1 Strategy Lab Foundation

Workflow mode: \`BUILD_NEW_APP\`

## Scope

This phase intentionally does only two things:

1. freeze the existing historical strategy behavior as \`VAHRAM_ORIGINAL_V1\`;
2. add the first shared historical-data/backtest correctness foundation.

Legacy \`scripts/*\` are not modified.

## H0 — frozen original

Frozen legacy source commit:

\`e363ffd8a4d29492bb6e685181770c39ca3047d7\`

Regression fixture source:

\`data/SAGAUSDT.csv\`, historical rows 2025-03-03 through 2025-05-02.

Expected strength-version signals in that fixture:

- 2025-04-02 BUY — 67.2;
- 2025-04-25 SELL — 74.9;
- 2025-04-26 SELL — 87.3;
- 2025-04-28 SELL — 66.7.

This fixture protects the non-canonical historical \`StochRSI\` behavior rather than correcting it.

## H1 foundation — historical data

Added shared primitives for:

- legacy-to-canonical OHLCV column normalization;
- UTC timestamp normalization;
- deterministic dataset IDs;
- duplicate detection;
- out-of-order detection;
- gap detection within the available listing history;
- OHLC invariant checks;
- negative-volume detection;
- closed-candle filtering;
- canonical timestamp sorting.

No missing candle is auto-filled. No suspicious data is silently repaired.

## Backtest foundation

Default execution policy is explicit and conservative:

    signal calculated on candle T close
    -> entry on candle T+1 open

Fees and slippage are explicit configuration fields in the run manifest.

The foundation deliberately does not yet implement portfolio/trade exits, event-study metrics, optimization, regimes, Sheets, Telegram, or live trading.

## Freshness findings before implementation

- PR #1 is merged into \`main\`.
- The old documentation branch remains, but \`main\` is the implementation base.
- \`tokens.csv\` currently lists 13 symbols.
- repository \`data/\` currently contains only 4 symbol CSV files: BTCUSDT, ATOMUSDT, TWTUSDT, SAGAUSDT.
- \`requirements.txt\` is currently empty even though legacy scripts import pandas and python-binance.

These are tracked as current-state facts, not silently fixed in H0/H1.
