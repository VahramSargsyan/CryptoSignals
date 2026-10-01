# Relative Rotation — Real Manual Rotation Log

Status: ACTIVE_MANUAL_EXECUTION_LOG  
Strategy: RELATIVE_ROTATION_TARGET_U10_FORWARD_V1  
Universe: RR_TARGET_U10_FROZEN_V1  
Execution authority: VAHRAM_MANUAL_ONLY

## Forward live books

Forward tracking start:

`2026-09-29T00:00:00Z`

### BOOK_1 — primary branch

Current held asset:

`ATOM`

Starting-state provenance:

- real TWT -> ATOM rotation occurred on 2026-09-26;
- detailed quantities and screenshot-derived evidence remain in
  `research/atom_twt_rotation/REAL_ROTATION_LOG.md`;
- BOOK_1 quantity is not invented here because it was not supplied in the
  2026-09-29 multi-book state update.

### BOOK_2 — secondary rotation branch

Current held asset:

`TRX`

Current tracked quantity:

`3950.7453 TRX`

Starting tracked quantity:

`100 LINK`

Provenance:

- the user explicitly confirmed that 100 LINK is available as the second real
  branch at the start of the frozen-U10 forward test;
- this is a starting position snapshot, not an exchange fill reconstructed by
  the repository;
- no LINK -> destination trade is recorded until the user actually executes it.

### BOOK_3 — PEPE starting branch (RECORDED / ACTIVE ROUTING PENDING)

Current held asset:

`PEPE`

Current tracked quantity:

`69341307.9537981079 PEPE`

Starting tracked quantity:

`69341307.9537981079 PEPE`

Starting-state provenance:

- user-provided Binance balance screenshot captured at approximately `22:00` local time (Asia/Yerevan) on `2026-10-01`;
- screenshot shows `69,341,307.9537981079 PEPE`;
- screenshot displayed value: approximately `308.57 USDT`;
- Vahram separately reported historical purchase cost of approximately `500 USDT`; this is context for later PnL comparison only and is not an RR routing rule;
- this is a starting position snapshot, not a reconstructed exchange fill or strategy rotation;
- BOOK_3 forward tracking baseline starts from this PEPE quantity; pre-tracking history must not be counted as BOOK_3 forward performance evidence;
- active paper-live routing is intentionally not enabled yet because the current bounded notification replay would surface a pre-registration PEPE -> TRX CONFIRMED event from 2026-09-28 as unresolved; that retrospective event is not valid BOOK_3 forward evidence;
- activation requires a safe no-pre-registration-replay path for BOOK_3, without changing BOOK_1 / BOOK_2 legacy replay behavior.

## Recording rule

For every future real rotation record the book independently:

- book ID;
- rotation ID;
- signal closed-candle date;
- signal state (`CONFIRMED` required for canonical strategy evidence);
- from asset;
- to asset;
- historical-model pair and dislocation;
- manual execution timestamp;
- quantity sent;
- quantity received;
- fee and slippage if known;
- order/trade ID if available;
- that book's configured held asset after execution;
- that book's configured quantity after execution;
- notes about any divergence from the paper-live signal.

A signal is not a trade. The log changes position state only after Vahram
confirms a real manual execution.

## Execution-cost lesson learned from first real rotation

Recorded: `2026-09-30`

Context:

The first live use of BOOK_2 revealed an execution issue that was not captured
by the historical backtest assumptions.

User-reported practical observation:

- Vahram estimates that discovering this issue cost approximately `USD 150`;
- some direct conversion quotes appeared to imply roughly `3%` all-in
  deterioration versus the reference value;
- observed ALGO / BNB-related conversion quotes reached roughly `7%`
  deterioration in the cases inspected.

Important evidence boundary:

- these values are not proven fixed exchange fees;
- the exact split between fee, spread, liquidity, price impact, slippage and
  routing was not independently measured;
- therefore the canonical term is `observed effective execution cost`, not
  `exchange fee`.

Permanent lesson:

```text
BACKTEST COST ASSUMPTION: 0.1%
REAL EXECUTION: MUST BE QUOTED AND VERIFIED
DIRECT SWAP: NOT ASSUMED CHEAPEST
INTERMEDIATE HOPS: MAY MULTIPLY REAL COST
```

For every future rotation, preserve when practical:

- direct-route quoted receive amount;
- alternative-route quoted receive amount;
- chosen route;
- reference price / reference cross-rate;
- actual sent quantity;
- actual received quantity;
- explicit fee if shown;
- estimated effective all-in execution loss.

This lesson is operational evidence from real use and is intentionally retained
even if later fee structures or liquidity conditions change.

## Entries

### ROT-BOOK2-20260929-001 — LINK → ALGO

Book:

`BOOK_2`

Signal evidence:

- signal closed candle: `2026-09-28T00:00:00+00:00`;
- signal state: `CONFIRMED`;
- model pair: `ALGO/LINK`;
- route: `LINK -> ALGO`;
- maximum dislocation before confirmation: `34.00710076496041%`;
- deviation from the 180d median at confirmation: `27.008480381822797%`;
- reversal from post-ARM extreme: `5.0642875558253975%`;
- strategy threshold: 15% ARM / 3% reversal confirmation.

Manual execution:

- user confirmed the swap was completed on `2026-09-29`;
- user-provided exchange completion screenshot shows `-100 LINK`;
- the same screenshot shows `+10723.76037691 ALGO`;
- screenshot status bar shows approximately `08:01` local time (Asia/Yerevan);
- exact exchange execution timestamp is not independently visible in the screenshot;
- effective aggregate cross ratio from the confirmed quantities:
  `1 LINK = 107.2376037691 ALGO`;
- fee: unknown from the provided screenshot;
- slippage: unknown from the provided screenshot;
- exchange order/trade ID: not visible.

Position after execution:

- configured held asset: `ALGO`;
- configured quantity: `10723.76037691 ALGO`;
- BOOK_2 remains manual-execution-only;
- this execution matches the latest canonical BOOK_2 confirmed route and is not recorded as a strategy divergence.


### ROT-BOOK2-20260930-002 — ALGO → TRX

Book:

`BOOK_2`

Execution evidence:

- Vahram explicitly confirmed that the tracked BOOK_2 position was rotated from ALGO into TRX on `2026-09-30`;
- the pre-rotation canonical BOOK_2 position was `10723.76037691 ALGO`;
- the provided Binance TRX balance screenshot shows total balance `3950.7453 TRX`;
- screenshot clock: approximately `23:35` local time (Asia/Yerevan);
- the exact exchange fill timestamp is not visible in the screenshot;
- screenshot average cost: approximately `$0.3378 / TRX`;
- screenshot displayed market value: approximately `$1334.95`;
- screenshot unrealized PnL is approximately flat immediately after the rotation;
- fee / slippage / exact execution route / trade ID are not visible in the supplied screenshot.

Signal evidence boundary:

- this update confirms the real execution and current held asset;
- the Relative Rotation signal state and exact report event for this execution were not re-verified from a GitHub Actions artifact in this update;
- therefore this entry does not invent a CONFIRMED event payload that is not present in the supplied execution evidence.

Implied conversion from tracked quantities:

- `10723.76037691 ALGO -> 3950.7453 TRX`;
- aggregate implied ratio: `1 ALGO = 0.3684104419664763 TRX`;
- inverse ratio: `1 TRX = 2.7143638889882373 ALGO`.

Position after execution:

- configured held asset: `TRX`;
- configured quantity: `3950.7453 TRX`;
- future Relative Rotation search for BOOK_2 starts from `TRX`;
- BOOK_2 remains manual-execution-only.

### ROT-BOOK2-20260930-002 — ALGO → TRX

Book:

`BOOK_2`

Execution evidence:

- user explicitly confirmed the real manual rotation into TRX on `2026-09-30`;
- source position before execution was the canonical tracked BOOK_2 position:
  `10723.76037691 ALGO`;
- user-provided Binance TRX balance screenshot shows:
  - total balance: `3950.7453 TRX`;
  - displayed average cost: `$0.3378`;
  - displayed value: approximately `$1334.95`;
  - screenshot clock: approximately `23:35` local time (Asia/Yerevan);
- exact exchange fill timestamp, explicit fee, slippage and trade/order ID are not visible in the supplied screenshot;
- signal state was not independently re-verified as part of this recording update, so this entry records the real execution and position state without inventing missing signal evidence.

Position after execution:

- configured held asset: `TRX`;
- configured quantity: `3950.7453 TRX`;
- future Relative Rotation search for BOOK_2 starts from `TRX`;
- BOOK_2 remains manual-execution-only;
- BOOK_1 remains unchanged at `ATOM`.

