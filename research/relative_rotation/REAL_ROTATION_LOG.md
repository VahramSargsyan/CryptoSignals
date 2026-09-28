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

### BOOK_2 — secondary LINK branch

Current held asset:

`LINK`

User-confirmed starting tracked quantity:

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

No post-freeze BOOK_1 or BOOK_2 manual rotation has been logged yet.
