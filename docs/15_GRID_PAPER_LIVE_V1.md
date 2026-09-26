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

Five profiles run in parallel. The original four profiles remain intact, and one additive OSS-derived MID candidate is observed as a separate shadow profile.

### CONTROL_BASE

- both Micro and Mid active
- linear-depth allocation, p=1
- Micro exit: +1 sublevel
- Mid deep recovery: +10 sublevels
- Mid target scale: 1.0
- positive-profit reinvestment: 100%
- permanent token runner: 0%

### CANDIDATE_WIDE

- both Micro and Mid active
- linear-depth allocation, p=1
- Micro exit: +6 sublevels
- Mid deep recovery: +18 sublevels
- Mid target scale: 1.0
- positive-profit reinvestment: 100%
- permanent token runner: 0%

### MICRO_ONLY_WIDE

- Micro layer only
- WIDE Micro exit: +6 sublevels
- p=1
- positive-profit reinvestment: 100%
- permanent token runner: 0%
- normalized to the same total starting capital of 2000 per symbol

### MID_ONLY_WIDE

- Mid layer only
- WIDE Mid deep recovery: +18 sublevels
- existing Mid percentage targets unchanged
- p=1
- positive-profit reinvestment: 100%
- permanent token runner: 0%
- normalized to the same total starting capital of 2000 per symbol

The strategy engine still computes the existing independent Micro and Mid pools. For the two single-layer profiles, paper-live projects only the selected independent layer and scales that layer from 1000 to the common 2000-unit normalized comparison capital. This does not create a new trading mechanism and does not move cash between layers.

### MID_OSS_ATR50_TRAIL7

- separate research candidate engine; canonical `strategy.py` is unchanged
- MID-only
- normalized starting capital: 2000 per symbol
- linear-depth allocation p=1
- WIDE Mid recovery: +18 sublevels
- existing Mid percentage targets unchanged
- 1095-candle causal H/L
- ATR14-gated H/L refresh
- refresh eligibility begins after a 60-candle cooldown
- refresh when ATR has shifted by more than 50% from the prior refresh anchor, or price escapes the active range
- once the normal MID target is reached, the exit is armed rather than filled immediately
- actual SELL occurs on a later candle after a 7% retracement from the observed post-target peak
- fee 10 bps; slippage 5 bps
- no real orders

The 7% value is frozen for forward observation because the historical research showed a 6–8% plateau. The forward system deliberately runs one candidate, not three near-duplicate tuned profiles.

All other engine assumptions remain equal where applicable.

## H/L and data rule

At paper start, each symbol requires the preceding 1095 daily candles before the first trade.

Canonical profile range logic remains:

```text
lookback = 1095 daily candles
refresh = every 30 daily candles
current candle is excluded from H/L calculation
```

The new `MID_OSS_ATR50_TRAIL7` profile keeps the same causal 1095-candle lookback but uses its frozen ATR14 / 50% / 60-candle refresh gate instead of the 30-candle refresh cadence.

Both refresh policies remain research assumptions.

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

- the latest closed candle generated at least one BUY or SELL event in any observed profile; or
- exactly 7 completed paper candles are available; or
- exactly 30 completed paper candles are available.

For event-driven notifications, Telegram/email text identifies the profile and symbol that generated the BUY/SELL event. Profiles without an event on that candle are omitted from the compact event message.

Artifacts and GitHub job summaries are still produced every day.

## Forward Micro / Mid decision boundary

Historical evidence and forward evidence are kept separate.

The single-layer profiles and the new OSS candidate do not retroactively change historical results. They all start from the already frozen paper boundary:

```text
2026-09-26T00:00:00Z
```

The purpose is now twofold:

1. observe whether the historical Micro-vs-Mid relationship persists on genuinely new closed daily candles;
2. compare the frozen `MID_OSS_ATR50_TRAIL7` candidate against `MID_ONLY_WIDE` without further parameter tuning.

No single day, week, or alert is treated as sufficient promotion evidence.

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
