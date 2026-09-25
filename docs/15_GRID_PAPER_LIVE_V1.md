# Grid Paper Live v1 — design

Date: 2026-09-25  
Workflow mode: **PRODUCTION_OBSERVATION**  
Execution: **PAPER ONLY — NO BROKER / NO REAL ORDERS**

## Objective

Run the level-grid strategy forward on newly closed Binance daily candles without using future data.

The first observation horizon is:

- 7 completed daily candles;
- then 30 completed daily candles.

The system must be useful even with zero real trading capital.

## Frozen paper start

```text
paper_start_utc = 2026-09-26T00:00:00Z
```

This avoids retroactively pretending that the strategy was active for the partially completed 2026-09-25 UTC candle.

The first complete paper-trading candle is therefore 2026-09-26 UTC and becomes observable only after it closes.

## Universe

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Each symbol receives independent normalized capital:

```text
Micro = 1000
Mid   = 1000
Total = 2000
```

No capital moves between symbols.

## Profiles

Two profiles run in parallel so the new research candidate has a live control.

### CONTROL_BASE

- linear-depth allocation, p=1
- Micro exit: +1 sublevel
- Mid deep recovery: +10 sublevels
- Mid target scale: 1.0
- positive-profit reinvestment: 100%
- permanent token runner: 0%

### CANDIDATE_WIDE

- linear-depth allocation, p=1
- Micro exit: +6 sublevels
- Mid deep recovery: +18 sublevels
- Mid target scale: 1.0
- positive-profit reinvestment: 100%
- permanent token runner: 0%

All other engine assumptions remain equal.

## H/L and data rule

At paper start, each symbol requires the preceding 1095 daily candles before the first trade.

Range logic remains the current research assumption:

```text
lookback = 1095 daily candles
refresh = every 30 daily candles
current candle is excluded from H/L calculation
```

The 30-candle refresh rule is not owner-frozen and is a residual research risk.

## Runtime model

The GitHub job is stateless by design.

Every daily run:

1. downloads only history available up to the latest closed daily candle;
2. starts with the same fixed normalized paper capital at the fixed paper start;
3. deterministically replays only candles that have actually closed since launch;
4. reconstructs current paper positions/equity;
5. emits a daily report and event ledger.

This avoids hidden mutable state and makes every paper snapshot reproducible from market data + source SHA + config.

## Schedule

GitHub Actions schedule:

```text
00:20 UTC every day
≈ 04:20 Armenia time
```

GitHub scheduling can be delayed by runner queues; the strategy always uses closed candles only, so a delay does not introduce look-ahead.

Manual dispatch is also available.

## Reports

Every run stores a 90-day artifact containing:

- report.md
- report.json
- current summary by symbol/profile
- event ledger
- closed-trade ledger

The report is also written to the GitHub Actions job summary.

Milestones:

- WEEK_1 after 7 completed paper candles;
- MONTH_1 after 30 completed paper candles.

## Notification channels

Optional notification transport:

### Telegram

Repository secrets:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

### Email / SMTP

Repository secrets:

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `REPORT_EMAIL_FROM`
- `REPORT_EMAIL_TO`

No secret is committed to the repository.

The runtime must continue successfully when notification secrets are absent.

## Notification policy

To avoid daily spam, notification is sent when:

- the latest closed candle generated at least one BUY or SELL event; or
- exactly 7 completed paper candles are available; or
- exactly 30 completed paper candles are available.

Artifacts and GitHub job summaries are still produced every day.

## Evidence level

Before the first live candle:

```text
LOCAL/CI TESTED only
```

After scheduled observation begins:

```text
PAPER_LIVE_OBSERVED
```

No paper result authorizes real-money execution.

## Rollback / stop

Disable the scheduled workflow or remove its cron trigger.

There is no broker state, exchange key, order, or real balance to unwind.
