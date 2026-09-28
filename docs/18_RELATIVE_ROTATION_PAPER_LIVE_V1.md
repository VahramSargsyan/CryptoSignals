# Relative Rotation Paper Live v1

Date: 2026-09-27  
Workflow mode: PATCH_FIX  
Status: PAPER_LIVE_MONITOR / MANUAL_EXECUTION_ONLY

## Purpose

Operate the target-U9 transition monitor in forward observation without changing its trading logic and without enabling automatic exchange execution.

Transition monitor union:

- TARGET: TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP
- SUNSET / EXIT-ONLY: ATOM, SOL, LINK, HBAR

The monitor downloads all 13 assets so a real legacy holding can still produce an outbound signal.
Actionable destinations are filtered to TARGET only.

Target graph: 9 nodes / 36 undirected target pairs.
Monitor union: 13 nodes / 78 undirected observed pairs.
The signal engine remains 180d median / 15% ARM / 3% reversal / strongest max-dislocation router.

## Frozen relative-rotation parameters

The monitor preserves the research baseline:

- timeframe: Binance Spot 1D closed candles
- pair ratio: right asset close / left asset close
- rolling median: 180 days
- ARM threshold: 15%
- post-ARM extreme tracking: yes
- reversal confirmation: 3%
- conflict router: strongest confirmed max dislocation
- order execution: none
- real rotations: manual approval only

State path:

`NO_SIGNAL -> ARMED -> EXTREME_TRACKING -> CONFIRMED`

`ARMED` is only a warning. It is not a rotation instruction. A historical-model rotation exists only after the 3% reversal confirmation.

## Current held asset and persistent sunset watches

The current configured held asset is stored in:

`config/relative_rotation_paper_live_v1.json`

Initial value for this patch:

`ATOM`

After Vahram manually executes a confirmed rotation, update this file to the asset actually held. The workflow never changes it automatically because the workflow cannot know whether a manual swap was really executed.

The same config also contains:

`"watch_assets": ["ATOM", "SOL", "LINK", "HBAR"]`

All sunset assets remain independently monitored for outbound TARGET-bound ARMED/CONFIRMED events.

Migration guard:
- a sunset asset may remain held until a valid outbound signal appears;
- every actionable destination must belong to TARGET;
- once capital leaves a sunset asset, the monitor does not recommend re-entry into any sunset asset;
- execution remains manual only.

## Telegram notification policy

Telegram is sent only when at least one of these events happens on the latest closed daily candle:

1. a new TARGET-bound `ARMED` event exists from the configured held asset;
2. a new TARGET-bound `CONFIRMED` event exists from the configured held asset;
3. a new TARGET-bound outbound `ARMED` or `CONFIRMED` event exists from a persistent sunset watch asset;
4. a manual verification run uses `--force-notify`.

The defensive overlay is disabled during the membership migration so it cannot conflict with the target/sunset routing rules.

No repeated daily warning is sent merely because a pair remains armed. The monitor recomputes the complete state from historical closed candles on every run, so it does not need hidden mutable workflow state for deduplication.

## Defensive research overlay

The monitor reports the documented candidate:

`DEFENSIVE_LOW_VOL_CRYPTO_SMA200_BREADTH_3_5_CONFIRM3_VOL30`

Rules:

- defensive breadth is not active during migration; when re-enabled it must be evaluated on TARGET assets only;
- enter defensive mode after 3 consecutive closes with breadth <= 3;
- choose the token with the lowest 30-day realized close-to-close volatility at entry;
- keep that defensive token fixed until exit;
- exit after 3 consecutive closes with breadth >= 5;
- no USDT is required by this overlay.

Important: this remains `PROMISING_RESEARCH_CANDIDATE / NOT_PRODUCTION_APPROVED`. The existing 3/5 breadth thresholds were not retuned for target-U9 transition in this patch, so the overlay must not be interpreted as production-approved target-U9 transition defensive logic. The alert is evidence, not an automatic instruction.

## Daily workflow

Workflow:

`.github/workflows/relative-rotation-paper-live-v1.yml`

Scheduled time after merge to the default branch:

`00:20 UTC` daily, shortly after the Binance daily candle closes.

For Armenia this is normally around `04:20` local time.

Each run:

1. downloads closed Binance Spot 1D candles for all 13 monitor-union assets;
2. builds a common 13-asset observation panel;
3. recomputes all 78 observed pair state machines;
4. filters held-asset events so only TARGET destinations are actionable;
5. independently filters sunset-watch events so only TARGET destinations are actionable;
6. blocks sunset re-entry at the recommendation layer;
7. writes JSON/Markdown/CSV evidence;
8. sends Telegram only if notification policy allows it;
9. uploads the evidence as a GitHub Actions artifact.

## Output evidence

Each run writes under:

`paper_artifacts/relative_rotation_paper_live_v1/<RUN_ID>/`

Files:

- `report.json`
- `report.md`
- `notification.txt`
- `pair_events.csv`
- `pair_states.csv`
- `defensive_diagnostics.csv`

## Manual real-rotation procedure

When Telegram reports `ROTATION CONFIRMED`:

1. do not assume an order was executed;
2. Vahram decides manually whether to act;
3. if acted, record the real swap in `research/relative_rotation/REAL_ROTATION_LOG.md`;
4. update `held_asset` in `config/relative_rotation_paper_live_v1.json` to the token actually held;
5. preserve exact exchange execution evidence when available: timestamp, quantities, fees, slippage, order/trade ID;
6. compare the real execution with the historical-model signal later.

## Safety boundary

- no exchange API key;
- no private trading key;
- no automatic order placement;
- no wallet transaction signing;
- no automatic mutation of the held-asset configuration;
- Telegram is advisory/paper-live only.

## Test level

Local static/unit verification for this patch covers:

- HIGH dislocation -> ARMED -> CONFIRMED direction;
- LOW dislocation direction;
- strongest-confirmed router conflict selection;
- 3-close defensive entry/exit;
- defensive asset frozen until exit.

GitHub Actions additionally runs the monitor against live Binance public market data.

`TEST_LEVEL: UNIT_REGRESSION + GITHUB_ACTIONS_LIVE_PUBLIC_DATA` only after the workflow run succeeds.


## 2026-09-29 — Frozen U10 forward observation

Universe version:

`RR_TARGET_U10_FROZEN_V1`

Forward start:

`2026-09-29T00:00:00Z`

TARGET:
- TWT
- PEPE
- BNB
- TRX
- AAVE
- AVAX
- FIL
- ALGO
- XRP
- HBAR

SUNSET / EXIT-ONLY / WATCH:
- ATOM
- SOL
- LINK

The configured real held asset remains ATOM, therefore migration mode stays
enabled until Vahram manually executes a confirmed outbound rotation from ATOM
into TARGET and updates the config.

HBAR is no longer a sunset/watch asset; it is part of the frozen TARGET U10.

Forward evidence rules:
- no pre-2026-09-29 candle counts as forward evidence;
- no historical backfill;
- TARGET membership remains frozen during this forward version;
- manual execution only;
- defensive overlay remains disabled.

### Telegram clarification

A sunset watch alert is independent of the configured held asset.

Example:
- held asset = ATOM
- watch alert = LINK -> FIL

This does not instruct ATOM -> FIL and does not mean LINK is currently held.

Watch ARMED messages must explicitly say:
- PREWATCH;
- not a swap signal;
- current held asset is unchanged;
- 3% reversal confirmation is still required.

Telegram sender must obey `report.should_notify`; no-signal scheduled runs are
not sent unless force-notify is explicitly requested.

The meaningful D1 live cadence remains once after the Binance daily close.
Repeated intraday runs would evaluate the same closed candle and are not treated
as new forward evidence.
