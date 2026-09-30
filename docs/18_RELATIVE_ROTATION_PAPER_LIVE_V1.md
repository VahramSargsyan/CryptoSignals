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

Telegram policy has two scheduled layers:

1. the morning `00:20 UTC` run always sends one useful status message;
2. if a new TARGET-bound `ARMED` or `CONFIRMED` event exists, the existing
   event alert keeps priority;
3. if there is no new event, the morning message shows the strongest current
   outbound Relative Rotation candidates for each live book, their percentage
   dislocation from the 180-day pair median, the remaining distance to the
   15% ARM threshold, both assets' USDT close prices from the same closed D1
   candle, and the implied direct conversion rate `1 FROM = X TO`;
4. persistent sunset-watch `ARMED`/`CONFIRMED` events keep their existing
   alert behavior;
5. a manual verification run may use `--force-notify`;
6. the dedicated 22:30 Yerevan evening schedule may repeat only a same-candle
   live-book `CONFIRMED` event as an execution reminder.

The evening reminder is deliberately separate from the morning event identity:
the morning alert remains preserved in dedupe state and one evening repeat is
allowed for the same `CONFIRMED`. It never repeats `ARMED / PREWATCH`, never
creates a new signal, and never changes the frozen D1 signal candle. If the real
trade has already been executed, the reminder must not be treated as a second
trade; update the real position/log as part of the manual execution procedure.

The defensive overlay is disabled during the membership migration so it cannot conflict with the target/sunset routing rules.

The morning quiet-market snapshot is informational only. A line such as
`ALGO -> FIL: 8.40% отклонение; до ARM 15%: 6.60 п.п.` means the current pair
ratio is 8.40% away from its 180-day median in the model's prospective outbound
direction. It is not an `ARMED` or `CONFIRMED` signal.

Price context is taken from the exact same closed Binance D1 candle used by the
signal engine. Example:

`Цена закрытия: LINK $15; ALGO $0.15; 1 LINK = 100 ALGO.`

This is a signal-candle reference snapshot, not a live execution quote. It is
also shown beside current ARMED/CONFIRMED Telegram events. The 22:30 reminder
repeats the same signal-candle price reference; it does not claim to show the
22:30 market price.

No repeated event warning is created merely because a pair remains armed. The
morning status may still display the current state, while event deduplication
continues to use stable event IDs.

## Route conflict warning rule — 2026-09-30

Status:

`ACCEPTED_RUNTIME_GUARD / ROUTER_UNCHANGED / ONE_CLICK_EXECUTION_BLOCKED_ON_CONFLICT`

Purpose:

Prevent a valid `CONFIRMED` route from being presented in isolation when the
same market state already shows that another outbound candidate may be the more
natural final destination.

This rule changes **signal presentation and manual-decision context only**. It
does not change the frozen Relative Rotation router, does not auto-replace the
primary `CONFIRMED` route, and does not execute a trade.

### Detection

For every live book with a primary confirmed route:

`SOURCE -> A = CONFIRMED`

the signal layer must also inspect all current outbound candidate states from
the same `SOURCE`.

A route conflict exists when:

1. another allowed TARGET destination `B` is currently `ARMED` or
   `CONFIRMED` from the same `SOURCE`;
2. `SOURCE -> B` has a larger `max_dislocation` than the primary
   `SOURCE -> A` confirmed route; and
3. the direct destination pair `A <-> B` is itself currently oriented
   `A -> B` as `ARMED` or `CONFIRMED`.

Interpretation:

`SOURCE -> A -> B`

may be an avoidable intermediate path while a direct:

`SOURCE -> B`

candidate is already developing or confirmed.

The destination-pair relationship must come from the actual Relative Rotation
pair state machine for `A/B`. It must never be inferred by mathematical
transitivity alone.

### Mandatory Telegram content

When a route conflict exists, Telegram must **not** present only the primary
`SOURCE -> A` confirmation.

The same signal must include a clearly separated warning block containing:

- the primary `SOURCE -> A` route and its status;
- the stronger competing `SOURCE -> B` candidate;
- each route's `max_dislocation`;
- each route's current reversal-from-extreme progress and whether 3% is reached;
- the direct `A -> B` destination-pair state;
- `A -> B` `max_dislocation` and reversal progress when available;
- explicit text that `A` may be an intermediate destination;
- explicit text that the warning does **not** automatically override the frozen
  router and manual execution remains a human decision.

Required warning concept:

```text
⚠️ ROUTE CONFLICT

Primary confirmed route:
SOURCE -> A

Stronger competing candidate:
SOURCE -> B

Destination relation:
A -> B = ARMED / CONFIRMED

Possible intermediate path:
SOURCE -> A -> B

Direct alternative under observation:
SOURCE -> B

This warning does not automatically replace the confirmed route.
Manual execution remains required.
```

### Severity

- `A -> B = ARMED` -> `ROUTE_CONFLICT_WARNING`
- `A -> B = CONFIRMED` -> `ROUTE_CONFLICT_HIGH`

A `CONFIRMED` destination-pair conflict is stronger evidence that the primary
destination may be temporary, but it still does not silently change the
accepted router.

### Historical stress-test boundary

The 2026-09-30 historical stress test found that using destination dominance as
an automatic hard-router override was not robust across rolling windows.

Therefore the accepted production rule is:

`NETWORK STATE MUST BE SHOWN -> HUMAN DECIDES`

not:

`NETWORK STATE AUTOMATICALLY OVERRIDES PRIMARY_CONFIRMED`.

The alerting rule may be reconsidered for routing only after separately frozen
confirmatory/forward evidence.

### Runtime implementation — 2026-09-30 patch

Runtime enforcement is implemented in the paper-live monitor and Telegram
sender.

For each live-book primary CONFIRMED route the monitor now:
- evaluates stronger same-source ARMED/CONFIRMED TARGET candidates;
- evaluates the real direct pair state between primary and competing
  destinations;
- writes route-conflict evidence into report.json and Telegram text;
- keeps the frozen strongest-CONFIRMED router unchanged;
- blocks Telegram one-click execution when a route conflict exists;
- requires a separate manual review before a conflicting route can be recorded
  through Telegram control.

The guard is intentionally fail-safe at the execution-control layer: a
conflicting candidate is not silently auto-routed to the alternative token.

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

Scheduled times after merge to the default branch:

- `00:20 UTC` daily, shortly after the Binance daily candle closes
  (normally about `04:20` in Armenia);
- `18:30 UTC` daily (normally `22:30` in Armenia) for the execution fallback
  reminder.

The 22:30 run recomputes the same already-closed D1 state. It sends only a
same-candle live-book `CONFIRMED` reminder and does not create additional
forward evidence. GitHub Actions cron can start later than the nominal minute
when the hosted runner queue is busy.

Each run:

1. downloads closed Binance Spot 1D candles for all 13 monitor-union assets;
2. builds a common 13-asset observation panel;
3. recomputes all 78 observed pair state machines;
4. filters held-asset events so only TARGET destinations are actionable;
5. independently filters sunset-watch events so only TARGET destinations are actionable;
6. blocks sunset re-entry at the recommendation layer;
7. stores the latest closed-candle USDT price snapshot in `report.json`;
8. writes JSON/Markdown/CSV evidence;
9. sends Telegram only if notification policy allows it;
10. uploads the evidence as a GitHub Actions artifact.

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

The morning scheduled run is allowed to send the informational rotation
snapshot even when `report.should_notify=false`. Manual/no-schedule runs still
obey `report.should_notify` unless `--force-notify` is explicitly requested.
The 22:30 schedule remains CONFIRMED-only and does not send a quiet-market
snapshot.

The meaningful D1 live cadence remains once after the Binance daily close.
Repeated intraday runs evaluate the same closed candle and are not treated as
new forward evidence. The scheduled 22:30 Yerevan run is the explicit exception
to ordinary notification dedupe: it may repeat the morning same-candle
`CONFIRMED` once as an execution reminder, with a separate reminder event ID.


## 2026-09-29 — Two real live books

Canonical live state is now multi-book:

- BOOK_1: ATOM
- BOOK_2: 100 LINK at forward tracking start

The top-level `held_asset=ATOM` remains only as a compatibility alias for
BOOK_1.

Each book is evaluated independently by the same frozen U10 signal engine.
A held asset is not also emitted as an independent sunset watch during the same
run.

Therefore LINK signals are now BOOK_2 signals, not watch-only messages.

Position quantities do not change automatically. After Vahram manually executes
a confirmed rotation, update only the affected book with:
- new held asset;
- actual received quantity;
- execution timestamp/details in the real rotation log.

The monitor never infers that a Telegram signal was executed.


## 2026-09-30 — Route Conflict Guard runtime patch

Strategy runtime identifier:

`RELATIVE_ROTATION_TARGET_U10_FORWARD_V1_1_ROUTE_CONFLICT_GUARD`

Impact:
- signal-context calculation: changed;
- Telegram signal presentation: changed;
- Telegram one-click execution eligibility: changed;
- 180d / 15% ARM / 3% reversal mechanics: unchanged;
- strongest-CONFIRMED baseline router: unchanged;
- TARGET U10 membership: unchanged;
- persistent position schema: unchanged.

MIGRATION_REQUIRED: NO.

Risk class: L2 — business/safety feature.

Impact map:
- DIRECTLY_AFFECTED: pair-network route-conflict analysis, report payload,
  Telegram signal/reminder text, Telegram execution-control eligibility;
- TRANSITIVE_DEPENDENCIES: manual execution recorder consumes the report
  candidate and refuses conflicting one-click execution;
- UNAFFECTED: market-data ingestion, 180d median, ARM/reversal state machine,
  U10 membership, position quantities, exchange execution (none);
- SCALE_CRITICAL_TRIGGER: NO.

Rollback:
revert this patch to restore the previous presentation/control behavior. No
position data migration is required because the guard does not mutate
position state by itself.
