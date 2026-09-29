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

`ALGO`

Current tracked quantity:

`10723.76037691 ALGO`

Starting tracked quantity:

`100 LINK`

Provenance:

- the user explicitly confirmed that 100 LINK is available as the second real
  branch at the start of the frozen-U10 forward test;
- this is a starting position snapshot, not an exchange fill reconstructed by
  the repository;
- no LINK -> destination trade is recorded until the user actually executes it.

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
