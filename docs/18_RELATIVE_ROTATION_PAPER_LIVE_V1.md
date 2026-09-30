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
6. execution timing policy is fixed in Yerevan time:
   - primary manual execution slot: `04:20`;
   - if that slot is missed, do not chase the signal during daytime;
   - fallback manual execution window: `23:00–24:00`;
   - fallback reminders are scheduled inside that window and only for an
     unresolved live-book `CONFIRMED` route.

The fallback-window reminder is deliberately separate from the morning event
identity. The morning alert remains preserved in dedupe state and one fallback
repeat is allowed for the same unresolved `CONFIRMED`. It never repeats
`ARMED / PREWATCH`, never creates a new signal, and never changes the frozen D1
signal candle. If the real trade has already been executed, the reminder must
not be treated as a second trade; update the real position/log as part of the
manual execution procedure.

Execution timing is a manual-discipline rule, not a new market signal. A missed
04:20 slot does not create permission to chase the move at arbitrary daytime
prices. The next strategy window is 23:00–24:00 Yerevan, provided the route is
still unresolved and the current report does not block it with
`DESTINATION DOMINANCE` routing.

The defensive overlay is disabled during the membership migration so it cannot conflict with the target/sunset routing rules.

The morning quiet-market snapshot is informational only. A line such as
`ALGO -> FIL: 8.40% отклонение; до ARM 15%: 6.60 п.п.` means the current pair
ratio is 8.40% away from its 180-day median in the model's prospective outbound
direction. It is not an `ARMED` or `CONFIRMED` signal.

Price context is taken from the exact same closed Binance D1 candle used by the
signal engine. Example:

`Цена закрытия: LINK $15; ALGO $0.15; 1 LINK = 100 ALGO.`

This is a signal-candle reference snapshot, not a live execution quote. It is
also shown beside current ARMED/CONFIRMED Telegram events. Fallback-window
reminders repeat the same signal-candle price reference; they do not claim to
show a live 23:00–24:00 market price.

No repeated event warning is created merely because a pair remains armed. The
morning status may still display the current state, while event deduplication
continues to use stable event IDs.

## Destination Dominance automatic route rule — 2026-09-30

Status:

`ACTIVE_PRODUCTION_ROUTER / FORWARD_WATCH_REQUIRED`

Runtime rule:

`DESTINATION_DOMINANCE_IMMEDIATE_STRONGER_V1`

### Detection and automatic selection

For a live book, let the baseline strongest-CONFIRMED router produce:

`SOURCE -> A = CONFIRMED`

The strategy then inspects the network before presenting the actionable route.

Automatic override occurs when:

1. another allowed TARGET destination `B` is currently `ARMED` or
   `CONFIRMED` from the same `SOURCE`;
2. `SOURCE -> B` has larger `max_dislocation` than the baseline
   `SOURCE -> A`;
3. the actual direct destination pair is currently oriented
   `A -> B` as `ARMED` or `CONFIRMED`.

When all three conditions are true, the effective strategy route becomes:

`SOURCE -> B`

instead of:

`SOURCE -> A`.

If more than one destination qualifies, choose the qualifying `B` with the
largest `SOURCE -> B max_dislocation`.

The actual `A/B` pair state is mandatory. The strategy must never infer this
relationship from mathematical transitivity alone.

### Evidence preservation

The effective event remains actionable as `CONFIRMED`, but its evidence must
preserve:

- the original baseline `SOURCE -> A CONFIRMED` trigger;
- the original state of `SOURCE -> B` at override time
  (`ARMED` or `CONFIRMED`);
- the direct `A -> B` pair state;
- the selected override rule;
- the effective `SOURCE -> B` route.

This prevents a future maintainer from incorrectly concluding that the
`SOURCE -> B` pair necessarily had its own 3% reversal confirmation.

### Telegram / execution behavior

When the rule fires:

- Telegram must clearly mark `DESTINATION DOMINANCE / AUTO ROUTE`;
- the displayed actionable route is the effective `SOURCE -> B`;
- the original `SOURCE -> A` remains visible as the baseline trigger;
- manual execution remains required;
- Telegram one-click confirmation, when enabled, refers to the effective
  `SOURCE -> B` route;
- no exchange order is placed automatically.

### Why this rule was promoted

Historical stress testing showed improved primary aggregate results:

| Window | Baseline | DD auto-route |
|---|---:|---:|
| 1Y median | +155.75% | +170.78% |
| 2Y median | +1850.89% | +1945.56% |
| Mature / approx 3Y median | +3167.27% | +3359.32% |

The rule also reduced median transition counts in the tested paths.

Real use then exposed an additional reason to care about route length:
unnecessary intermediate conversions can impose material spread / routing /
slippage / liquidity costs that were not captured by the historical 0.1%
transition-cost assumption.

### ⚠️ Mandatory attention: rolling instability

This rule is **not** treated as universally proven superior.

Historical rolling diagnostics were mixed:

- rolling 12m: 9 better / 5 equal / 9 worse;
- rolling 12m worst delta: about `-30.48 pp`;
- rolling 24m: 3 better / 4 equal / 4 worse;
- rolling 24m worst delta: about `-81.53 pp`;
- LINK-start 1Y diagnostic worsened from about `+229.58%` baseline to
  `+175.27%` with the auto-route rule.

Therefore:

`FORWARD_WATCH_REQUIRED = TRUE`

Every real automatic override should later be compared with the skipped
baseline destination where practical.

This production promotion was an explicit user decision on 2026-09-30 despite
the known rolling-window instability. The reason must not be forgotten or
silently rewritten as if the rule had been uniformly dominant historically.

Full preserved evidence:

`research/relative_rotation/2026-09-30_DESTINATION_DOMINANCE_AUTO_ROUTE_V1_EVIDENCE.md`

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
- `19:00 UTC` daily (normally `23:00` in Armenia) for the fallback execution
  window;
- `19:30 UTC` daily (normally `23:30` in Armenia) as a retry opportunity;
- `19:50 UTC` daily (normally `23:50` in Armenia) as the final nominal retry
  inside the fallback window.

The 23:00/23:30/23:50 runs recompute the same already-closed D1 state. Dedupe
normally allows only the first successful fallback reminder for a given closed
candle and unresolved route. These runs do not create additional forward
evidence. GitHub Actions cron can start later than the nominal minute when the
hosted runner queue is busy, so the scheduler cannot guarantee exact wall-clock
delivery.

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

## Real execution-cost lesson — 2026-09-30

Status:

`ACCEPTED_OPERATIONAL_RISK_RULE / DOCUMENTED_FROM_REAL_USE / NOT_YET_BACKTEST_RECALIBRATED`

### Why this exists

The historical Relative Rotation research used a modeled transition cost of
`0.1%` per rotation.

The first real BOOK_2 rotation exposed that this assumption can be materially
too optimistic for actual manual execution.

Vahram's real-use observation:

- the practical learning cost of the episode was approximately `USD 150`;
- some direct conversion quotes appeared roughly `3%` worse than the
  reference value;
- routes involving ALGO / BNB showed quoted deterioration of roughly `7%`
  in the observed cases.

These percentages are **not** documented as a universal exchange fee and must
not be remembered that way. They are user-observed effective execution-cost
signals. The observed difference may combine:

- explicit fee;
- bid/ask spread;
- route selection;
- liquidity;
- price impact;
- slippage;
- aggregator / wallet conversion path;
- execution timing.

The exact decomposition was not independently measured.

### Permanent operational conclusion

`DIRECT SWAP != AUTOMATICALLY CHEAPEST ROUTE`

A direct asset-to-asset conversion must not be assumed to cost the historical
`0.1%` model assumption.

Before every real manual rotation:

1. inspect the executable quote for the intended direct route;
2. compare it with at least one practical alternative route when available;
3. compare the received quantity, not only the displayed percentage fee;
4. record the effective all-in loss versus the market/reference value;
5. do not execute a materially expensive route blindly only because the
   strategy signal is `CONFIRMED`;
6. keep `ROUTE_CONFLICT` analysis separate from execution-cost analysis —
   one protects against a bad destination sequence, the other against an
   expensive conversion path.

This does **not** mean that direct swaps are permanently forbidden.

It means:

`NO BLIND DIRECT SWAP -> VERIFY REAL ALL-IN EXECUTION COST FIRST`

### Why route length now matters more

With negligible transaction cost, a path such as:

`LINK -> ALGO -> TRX`

can look almost equivalent to:

`LINK -> TRX`.

With real execution losses in the several-percent range, an unnecessary
intermediate hop can become economically important because every hop can incur
a new spread / price-impact / fee loss.

Therefore avoiding an unnecessary intermediate destination may create value
from two separate sources:

1. better Relative Rotation destination selection;
2. one fewer real conversion cost.

### Backtest interpretation warning

Historical performance numbers produced with `0.1%` transition cost must be
read as model results, not as a verified estimate of real executable returns.

Before treating the historical return figures as execution-realistic, research
must separately stress-test materially higher all-in transition-cost scenarios,
including at minimum:

- `1%`;
- `3%`;
- `5%`;
- `7%`.

This future stress test must also report the cost of avoidable intermediate
rotations.

Until that work is completed:

`HISTORICAL_RETURN != VERIFIED_REAL_NET_RETURN`

### Memory / future-maintainer note

This section exists specifically so that years later the project does not lose
the reason execution routing became a first-class concern.

The rule was learned from real manual use, not from backtest optimization.

## Manual real-rotation procedure

When Telegram reports `ROTATION CONFIRMED`:

1. do not assume an order was executed;
2. Vahram decides manually whether to act;
3. if acted, record the real swap in `research/relative_rotation/REAL_ROTATION_LOG.md`;
4. update `held_asset` in `config/relative_rotation_paper_live_v1.json` to the token actually held;
5. preserve exact exchange execution evidence when available: timestamp, quantities, fees, slippage, order/trade ID;
6. before execution, preserve the quoted receive amount for the intended route and, when practical, at least one alternative route;
7. record the effective all-in execution loss versus the contemporaneous reference value when it can be estimated;
8. compare the real execution with the historical-model signal later.

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
The 23:00–24:00 fallback schedules remain CONFIRMED-only and do not send a
quiet-market snapshot.

The meaningful D1 live cadence remains once after the Binance daily close.
Repeated intraday runs evaluate the same closed candle and are not treated as
new forward evidence. The scheduled 23:00–24:00 Yerevan fallback runs are the
explicit exception to ordinary notification dedupe: the first successful one
may repeat the unresolved morning `CONFIRMED` once as an execution reminder,
with a separate reminder event ID.


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


## 2026-09-30 — Yerevan execution-window policy

Strategy runtime identifier:

`RELATIVE_ROTATION_TARGET_U10_FORWARD_V1_2_EXECUTION_WINDOW`

Accepted manual execution timing:

```text
PRIMARY: 04:20 Yerevan
IF MISSED: NO MIDDAY CHASE
FALLBACK: 23:00–24:00 Yerevan
```

Fallback eligibility:
- the BOOK still holds the source asset;
- the route remains the latest unresolved `CONFIRMED` candidate;
- any qualifying destination-dominance conflict has already been resolved into the effective route;
- execution remains manual only.

Workflow nominal fallback reminders:
- 23:00 Yerevan;
- 23:30 Yerevan;
- 23:50 Yerevan.

Dedupe prevents three successful reminders from becoming three execution
commands. The extra schedules are retry opportunities because GitHub-hosted cron
may start late.

Impact:
- signal mathematics: unchanged;
- TARGET U10: unchanged;
- 180d / 15% ARM / 3% reversal: unchanged;
- route-conflict guard: unchanged;
- execution timing discipline and Telegram reminder schedule: changed;
- automatic exchange execution: still none.

MIGRATION_REQUIRED: NO.


## 2026-09-30 — Destination Dominance auto-route promotion

Strategy runtime identifier:

`RELATIVE_ROTATION_TARGET_U10_FORWARD_V1_3_DESTINATION_DOMINANCE_AUTO_ROUTE`

Production change:

```text
BEFORE:
SOURCE -> A CONFIRMED
stronger SOURCE -> B + A -> B active
=> warning / manual review

NOW:
SOURCE -> A CONFIRMED
stronger SOURCE -> B + A -> B active
=> automatic effective route SOURCE -> B
```

Selection:
- strongest qualifying B by max_dislocation;
- B may itself be ARMED or CONFIRMED;
- actual A -> B relationship is mandatory;
- manual exchange execution only.

Known risk:
- aggregate historical windows improved;
- rolling-window evidence was mixed and sometimes materially worse;
- rule is post-selected from historical data;
- forward monitoring is mandatory.

Impact:
- route selection: changed;
- Telegram effective route: changed;
- Telegram execution confirmation target: changed;
- 180d median: unchanged;
- 15% ARM: unchanged;
- 3% reversal state machine: unchanged;
- TARGET U10: unchanged;
- execution timing 04:20 / 23:00–24:00 Yerevan: unchanged;
- exchange automation: still none.

MIGRATION_REQUIRED: NO.

Rollback:
revert the V1_3 routing patch to restore warning-only destination dominance
behavior. Persistent book schema is unchanged.
