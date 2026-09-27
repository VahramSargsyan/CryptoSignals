# CRYPTO MASTER LEDGER

STATUS: DRAFT_CANONICAL_ACCOUNTING_SOURCE
PROJECT: VahramSargsyan/CryptoSignals
WORKFLOW_MODE: ECOSYSTEM_PLANNING
CREATED: 2026-09-26
PURPOSE: one place for crypto capital, accounts, holdings, real transactions, strategies, signals, policies, reconciliation and source evidence.

> This file is the current consolidated source of truth for research/accounting.
> It does NOT yet replace exchange statements or the legacy Google Sheets.
> Future Google Sheets / DB implementation must be created only after the accounting blueprint is confirmed.

---

## 1. Accounting principles

1. External money added to crypto is recorded only once as CAPITAL_IN.
2. Internal transfers between Binance / Bitget / Keplr / Trust Wallet are NOT new capital.
3. Token-to-token swaps are transactions, not deposits/withdrawals.
4. Current holdings are stored by account/wallet.
5. Strategy ownership is separate from physical custody location.
6. Historical/reconstructed values must be marked as VERIFIED / PARTIAL / ESTIMATED.
7. All currencies keep original amount and base-USDT equivalent when known.
8. Current snapshots never overwrite transaction history.
9. A transaction with missing fee/order-id remains valid but marked PARTIAL.
10. Manual approval remains required for real trading until a separate auto-execution decision is explicitly approved.

### Evidence priority

1. Exchange/wallet transaction/order statement.
2. Exchange/wallet balance screenshot at a known time.
3. Google Sheets historical transaction log.
4. Reconstructed calculation.
5. Model estimate.

---

## 2. External capital contributed

SOURCE: Google Sheet `Calculator` → `Мои средства`.

VERIFICATION_STATUS: SOURCE_DATA_VERIFIED

Recorded external contributions:
- Total AMD contributed: **2,788,000 AMD**
- Historical USDT equivalent recorded in the sheet: **7,122.694287524165 USDT**
- Number of non-zero contribution rows: **25**

Rule:
- This number is the current baseline for `CRYPTO_NET_CONTRIBUTED`.
- Internal transfers and token rotations must never increase this value.
- If later evidence shows an unrecorded real deposit/withdrawal, add a new capital-flow record instead of rewriting history.

---

## 3. Current custody snapshot — 2026-09-26

### Binance
Observed portfolio total: **~2,054.88 USDT**

Known visible holdings:
- LINK: **~122.8036653387 LINK**, displayed value ~1,751.06 USDT
- PEPE: **~69,341,093.325230094 PEPE**, displayed value ~303.71 USDT
- ATOM: **64.91320282 ATOM** last observed at 16:49 after the real TWT→ATOM rotation; not fully visible in the later cropped overview screenshot

Custody role proposal:
- EXECUTION_ACCOUNT / active trading

### Keplr
Observed wallet total: **~819 USDT**
Observed:
- staked portion: **~659 USDT**
- spendable portion: **~160 USDT**
- ATOM visible: **~81.65235 ATOM**
- other Cosmos assets exist but full composition is not yet reconciled

Custody role proposal:
- COSMOS_SELF_CUSTODY / STAKING

### Trust Wallet
Observed wallet total: **~211.44 USDT**

Visible holdings:
- TWT: **205 TWT**, displayed value ~122.06 USDT
- ETH: **0.02849074 ETH**, displayed value ~76.59 USDT
- BNB: **0.01650626 BNB**, displayed value ~12.77 USDT
- LTP: **0.23435138 LTP**, negligible displayed value

Custody role proposal:
- HOT_SELF_CUSTODY / pre-execution holding

### Bitget
Observed wallet total: **~106.26 USDT**

Visible holdings:
- BGB: **29.95333997 BGB**, displayed value ~60.94 USDT
- ATOM: **24.48941863 ATOM**, displayed value ~45.29 USDT

Custody role proposal:
- LEGACY / CONSOLIDATION_CANDIDATE

### Current observed total
Approximate total across the four locations:
- **~3,191.58 USDT**

Against recorded external contributions:
- contributed: 7,122.69 USDT
- current observed: ~3,191.58 USDT
- approximate difference: **-3,931.11 USDT**
- approximate capital retention: **44.8%**
- approximate drawdown vs contributed capital: **-55.2%**

Important:
This comparison is a capital-accounting snapshot, not a tax PnL statement.
It does not yet include a complete reconciliation of withdrawals, fees, staking rewards, airdrops or unrecorded legacy assets.

---

## 4. Strategy registry

### STRAT-ATOM-TWT-RELATIVE-ROTATION-V1
STATUS: ACTIVE_RESEARCH / PAPER_LIVE_CANDIDATE

Pair:
- ATOM / TWT

Frozen candidate logic:
- lookback: **180 days**
- reference: rolling median of TWT/ATOM
- ARM threshold: **15% relative dislocation**
- reaching 15% does NOT cause immediate rotation
- after ARM, track the pair-ratio extreme
- reversal confirmation: **3% from the post-ARM extreme**
- signal on completed observation; conservative historical execution model used next daily open
- ATOM held on the ATOM side may continue staking
- live intraday execution requires separate 1h/4h validation

Historical status:
- strong on ATOM/TWT
- not universal across arbitrary token pairs
- pair eligibility must be validated before applying this strategy to another pair

### STRAT-LINK-GRID
STATUS: EXISTING / SEPARATE_STRATEGY

Notes:
- LINK grid history exists in Google Sheet `trader's diary`.
- Do not merge LINK grid logic with Relative Rotation unless separately tested.

### STRAT-PEPE-LEGACY-GRID
STATUS: LEGACY_POSITION / ROTATION_SOURCE_CANDIDATE

Historical source:
- Google Sheet `trader's diary` → `PEPE` and `Журнал сделок`

Journal-derived, unreconciled totals:
- recorded PEPE buys: ~107,090,351.1 PEPE
- recorded PEPE sells: ~39,802,824 PEPE
- journal net: ~67,287,527.1 PEPE
- recorded buy USDT: ~799.68
- recorded sell USDT: ~294.96
- reconstructed net cash spent: **~504.72 USDT**

Current Binance PEPE:
- ~69.341M PEPE

Reconciliation status:
- PARTIAL
- current amount differs from journal net by ~2.05M PEPE
- journal contains duplicate-looking / undated legacy rows
- do not use 504.72 USDT as final tax/cost basis until reconciled

---

## 5. Real transaction ledger

### ROT-20260926-001 — TWT → ATOM
STATUS: PARTIAL_EVIDENCE / REAL_EXECUTION

Date:
- 2026-09-26

Execution:
- two separate **direct TWT → ATOM swap orders**
- no intermediate TWT → USDT → ATOM conversion

Aggregate before:
- 198.3612438519 TWT
- displayed value before: ~119.08 USDT

Aggregate after:
- 64.91320282 ATOM
- displayed value after: ~118.40 USDT

Derived aggregate cross-rate:
- 1 ATOM ≈ 3.055791969 TWT
- 1 TWT ≈ 0.327247408 ATOM

Unknown:
- exact split between the two orders
- exact execution timestamp of each order
- exact fee for each order
- exact order IDs
- exact slippage

Important:
The ~0.68 USDT difference between screenshots is NOT recorded as fee because the screenshots were taken at different times.

Detailed evidence:
- `research/atom_twt_rotation/REAL_ROTATION_LOG.md`

---

## 6. Asset policy registry

### PEPE — EMERGENCY / OPPORTUNITY ROTATION POLICY

POLICY_STATUS: FROZEN_DRAFT_BY_USER_INTENT

Default:
- Do **not** exit PEPE to cash/USDT solely because PEPE is down.
- PEPE may remain held while no superior validated rotation target exists.

Potential rotation:
PEPE may be considered for conversion into another crypto asset only when ALL are true:

1. The target is in the approved/liquid research universe.
2. The target has fallen materially more than PEPE on a relative basis or is materially discounted versus PEPE.
3. The pair `PEPE/TARGET` passes the Relative Rotation pair-eligibility tests.
4. A pair-specific signal is present; do not assume ATOM/TWT parameters automatically transfer.
5. Real execution remains manually approved.

Cash / USDT:
- CASH_EXIT_DISABLED_BY_DEFAULT for price weakness alone.
- A cash exit can be considered only under a separately defined market-wide risk-off rule or an operational/security emergency (exchange/network/custody risk).
- No global market-risk rule is implemented yet.

Current PEPE role:
- `LEGACY_GRID_POSITION + POTENTIAL_ROTATION_SOURCE`

---

## 7. Proposed future unified accounting model — DRAFT, NOT IMPLEMENTED

Goal:
One canonical place where ChatGPT can append verified events and where Vahram can see the full crypto picture without manually joining four wallets and old Sheets.

Conceptual entities:

### CAPITAL_FLOWS
Only real money entering/leaving the crypto ecosystem.
Examples:
- AMD → crypto capital in
- fiat withdrawal out
Never:
- Binance → Keplr
- TWT → ATOM

### ACCOUNTS
Custody locations:
- Binance
- Keplr
- Trust Wallet
- Bitget
- future hardware wallet

### ASSETS
Token identity and network metadata.

### TRANSACTIONS
- BUY
- SELL
- SWAP
- TRANSFER
- STAKE
- UNSTAKE
- REWARD
- FEE

### HOLDING_SNAPSHOTS
Observed holdings by account and timestamp.
Used for reconciliation, not as transaction history.

### STRATEGIES
- LINK_GRID
- ATOM_TWT_RELATIVE_ROTATION
- PEPE_LEGACY_GRID
- future eligible relative-rotation pairs

### SIGNALS
- PREWATCH
- NEAR_ARM
- ARMED
- EXTREME_UPDATE
- REVERSAL_CONFIRMING
- ROTATION_CONFIRMED
- NO_SIGNAL

### ASSET_POLICIES
Per-asset rules such as PEPE emergency rotation policy.

### RECONCILIATION
Compare expected holdings from transactions with observed exchange/wallet balances.

### SOURCES
Pointers to screenshots, Google Sheets, exchange statements and GitHub evidence.

---

## 8. Draft migration plan

MIGRATION_STATUS: NOT_STARTED

Phase 1 — preserve legacy evidence:
- keep `Calculator`
- keep `trader's diary`
- keep screenshots/evidence
- do not rewrite historical source files

Phase 2 — import only verified facts:
- external capital flows
- current accounts
- current holdings
- real TWT→ATOM rotation
- PEPE/LINK legacy trade history with quality flags

Phase 3 — reconcile:
- current wallet balances vs imported transaction history
- identify duplicates, missing transfers and unrecorded rewards

Phase 4 — choose canonical runtime:
- likely Google Sheets initially, because it is editable from ChatGPT and easy to inspect from phone
- later DB migration must preserve stable IDs and accounting history

No schema has been created yet. This is a discovery/blueprint draft, not a final database.

---

## 9. Open reconciliation items

1. Complete Keplr asset list and staking positions.
2. Confirm whether Binance ATOM 64.91320282 remains there or has moved.
3. Split the two 2026-09-26 TWT→ATOM orders when order history is available.
4. Reconcile PEPE current ~69.341M vs journal net ~67.288M.
5. Reconcile LINK current holding against historical grid transactions.
6. Determine whether any fiat withdrawals occurred that must reduce net contributed capital.
7. Classify BGB, ETH, BNB, LTP and other Cosmos assets: strategic / legacy / cleanup.
8. Confirm future canonical accounting home before building the final Google Sheet schema.

---

## 10. Next research task

PEPE relative-rotation search:
1. identify historically eligible PEPE pairs;
2. reject pairs with one-way/trending relative ratios;
3. test 15%+3% only as a reference, not as assumed production parameters;
4. select robust candidate pairs;
5. only then calculate current live signal using a consistent daily data source through 2026-09-26;
6. no real conversion without manual approval.


---

## 11. PEPE pair scan — preliminary historical screen

RUN_DATE: 2026-09-26
STATUS: PRELIMINARY_CANDIDATE_SCREEN
TEST_LEVEL: HISTORICAL_ROLLING_1Y_WINDOWS
DATA_COVERAGE: PEPE common history begins 2023-05-05 and current dataset ends 2026-03-28

Reference engine:
- 180d rolling median
- ARM 15%
- reversal confirmation 3%
- next-open execution
- 0.1% swap cost
- tested from both starting assets
- 1-year rolling windows shifted by ~60 days

Important limitation:
PEPE has a much shorter history than ATOM/TWT. The windows overlap and are not independent. These results are candidate-screening evidence only, not production approval.

### Strongest historical candidates from the tested liquid universe

| Pair | Median excess vs 50/50 | Windows beating 50/50 | Median excess vs best HODL | Median switches/year |
|---|---:|---:|---:|---:|
| PEPE/BNB | +81.2% | 83.3% | +34.7% | 5 |
| PEPE/SOL | +77.0% | 77.8% | +63.3% | 4 |
| PEPE/TRX | +76.8% | 72.2% | +37.6% | 6 |
| PEPE/AAVE | +61.4% | 66.7% | +34.1% | 4 |
| PEPE/LINK | +55.9% | 66.7% | +23.4% | 2.5 |

Secondary / weaker:
- PEPE/TWT
- PEPE/AVAX
- PEPE/FIL
- PEPE/ATOM
- PEPE/ETH

Rejected in this first screen due weak/negative stability:
- PEPE/ALGO
- PEPE/ADA
- PEPE/XRP
- PEPE/HBAR

### Current research shortlist
CANDIDATE_ONLY:
1. PEPE/BNB
2. PEPE/SOL
3. PEPE/TRX
4. PEPE/AAVE
5. PEPE/LINK

No pair is yet marked ELIGIBLE_FOR_REAL_ROTATION.

### Live signal status
CURRENT_LIVE_SIGNAL: NOT_YET_VERIFIED

Reason:
- the consistent GitHub daily dataset ends 2026-03-28;
- an exact 180d median + post-ARM extreme + 3% retrace signal for 2026-09-26 requires a consistent daily series through today;
- partial public web history is not sufficient to reconstruct the full signal without mixing incompatible data sources.

Next signal step:
- source Apr–Sep 2026 daily prices for PEPE and the shortlist from one consistent provider;
- recompute pair-specific 180d median;
- identify whether PEPE is currently expensive/cheap relative to each target;
- determine ARMED / EXTREME_TRACKING / REVERSAL_CONFIRMING / ROTATION_CONFIRMED;
- notify only as research signal; manual execution remains required.


---

## 12. PEPE live-direction pre-screen — 2026-09-26

STATUS: PREWATCH_RESEARCH_ONLY
NOT_A_CONFIRMED_ROTATION_SIGNAL

Purpose:
Use the last common verified historical endpoint (2026-03-28) and recent public 2026-09-25 closes to identify which shortlist targets moved in the direction that could justify a full 180d signal reconstruction.

Reference PEPE:
- 2026-03-28 close: 0.00000335
- recent 2026-09-25 close used for screen: ~0.000004421
- PEPE change: ~+31.97%

Target relative changes from 2026-03-28 to recent 2026-09-25:
- PEPE/TRX ratio: **+23.20%** → TRX became materially cheaper relative to PEPE over this coarse six-month comparison.
- PEPE/BNB ratio: **+4.68%** → BNB became only slightly cheaper relative to PEPE.
- PEPE/SOL ratio: **-6.12%** → PEPE became cheaper relative to SOL.
- PEPE/AAVE ratio: **-11.56%** → PEPE became cheaper relative to AAVE.
- PEPE/LINK ratio: **-13.98%** → PEPE became cheaper relative to LINK.

Interpretation:
- **PEPE/TRX is the first live-signal reconstruction priority.**
- This does NOT prove the 15% ARM condition because the production rule is measured against the rolling 180d median, not against the 2026-03-28 endpoint.
- BNB remains a strong historical pair but is not close enough on this coarse directional screen to infer an ARM condition.
- SOL/AAVE/LINK currently moved in the opposite relative direction for a PEPE→target rotation under this coarse screen.

Required before any real PEPE rotation:
1. reconstruct a consistent daily Apr–Sep 2026 PEPE/TRX series;
2. calculate the exact 180d rolling median;
3. find first >=15% PEPE-relative-rich ARM event, if any;
4. track post-ARM extreme;
5. verify >=3% reversal from that extreme;
6. only then mark ROTATION_CONFIRMED.


---

## 13. Multi-asset relative-rotation graph — first full stress test

RUN_DATE: 2026-09-26  
STATUS: PROMISING_RESEARCH / NOT_PRODUCTION_APPROVED  
WORKFLOW_MODE: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING

Universe:
- ATOM
- TWT
- PEPE
- BNB
- SOL
- TRX
- AAVE
- LINK

Graph size:
- 8 assets
- 28 unique pair relationships

Reference engine:
- 1D closed candles
- 180d rolling median
- ARM 15%
- reversal 3%
- next-open execution
- 0.1% swap cost

Primary OOS window:
- 2025-03-29 -> 2026-03-28

Baseline TOP-1 router result:
- median return across starting assets: **+41.6%**
- equal-weight 8-asset benchmark: **-29.5%**
- best single HODL in period: TRX **+37.0%**
- max drawdown: **~62.2%**
- median rotations: ~8

Observed example route from TWT:
- TWT -> PEPE -> TRX -> LINK -> TWT -> ATOM -> PEPE -> ATOM -> TWT

Important findings:
1. A dynamically generated token cycle appeared without predefining the route.
2. Router quality is critical. Strongest-extreme / confirmation / causal-percentile rules produced the same +41.6% median OOS in this sample; weak/naive conflict rules degraded sharply.
3. Equal TOP-2 split remained profitable but weaker: **+31.7%** median OOS.
4. Equal TOP-3 split fell to **+6.0%**.
5. Simple split diversification did not improve drawdown.
6. Strict permanent train-only pair filtering failed OOS, suggesting that a weak standalone edge can still be useful as a regime-specific transition.
7. Parameter sensitivity remains material; do not optimize to the best visible row.

Same-window GRID comparison (2025-03-29 -> 2026-03-28), reproduced from documented corrected grid mechanics:
- Rotation TOP-1 median: **+41.6%**, max DD ~62.2%
- LINK grid: **+20.9%**, max DD ~31.6%
- ETH grid: **+12.5%**, max DD ~26.8%
- SOL grid: **-4.1%**, max DD ~29.0%
- BNB grid: **-4.3%**, max DD ~14.0%
- BTC grid: **-3.0%**, max DD ~8.6%

Interpretation:
- On this exact one-year window, rotation produced the stronger return result than each reproduced grid case.
- Grid remained materially better on drawdown.
- This is not proof that rotation universally dominates grid.
- A future hybrid should test: RELATIVE ROUTER chooses the asset; GRID/HOLD determines how capital works inside that asset; RISK_OFF remains a separate layer.

Detailed evidence:
- `research/relative_rotation/2026-09-26_MULTI_ASSET_ROTATION_GRAPH_STRESS_TEST.md`

Next research:
1. weighted TOP-1/TOP-2 conflict splits: 80/20, 70/30, 60/40, 50/50;
2. walk-forward router validation;
3. conflict-by-conflict attribution;
4. CORE HODL + ROTATION;
5. staking-aware ATOM treatment;
6. separate RISK_OFF layer;
7. GRID + ROTATION hybrid;
8. extend live-consistent data through 2026-09-26 before current-signal use.

Manual execution remains required for all real trades.


### Relative-rotation walk-forward + RISK_OFF update — 2026-09-26

Follow-up evidence after the first 8-asset / 28-pair graph test:

Sequential non-overlapping 180-day TOP-1 windows, unchanged router:
- +374.2% — strongly positive
- -29.2% — strongly negative
- +138.3% — strongly positive
- +20.1% — moderately positive
- -44.9% — strongly negative

Summary:
- 2 strongly positive windows
- 1 moderately positive window
- 2 strongly negative windows

Main failure mode:
- network converges into one asset and then relative logic can remain silent while that asset falls sharply in absolute terms;
- 2024 failure: long ATOM stale hold, roughly 150 days after final transition, ~44% post-transition decline;
- late-2025/early-2026 failure: final TWT stale hold, 52 days, ~39% post-transition decline.

First independent absolute-risk smoke test:
- relative router continues to choose a shadow crypto target;
- actual capital moves to USDT while that target is below its own causal SMA;
- routing continues in the background while capital is in USDT;
- USDT yield modeled at 0%;
- 0.1% transition cost applied.

Same 2025-03-29 -> 2026-03-28 OOS year:
- no risk gate: +41.6% median, ~62.2% max DD
- SMA100 gate: +63.5% median, ~27.0% max DD
- SMA200 gate: +37.4% median, ~15.2% max DD
- SMA300 gate: +42.7% median, ~15.2% max DD

Sequential-window implication:
- simple risk gates largely removed the two large negative stale-hold regimes;
- SMA200 was the most balanced of the three tested candidates across the five sequential 180-day windows, but this is NOT a frozen rule;
- high USDT occupancy is a material trade-off introduced only by the experimental RISK_OFF gate and requires further validation;
- the baseline relative-rotation strategy itself remains continuously invested in crypto tokens between rotations and does not hold USDT as a default state;
- do not optimize to the best visible SMA result after the fact.

Current conceptual stack:
1. RELATIVE ROTATION GRAPH = WHERE inside crypto;
2. ABSOLUTE RISK GATE = CRYPTO vs USDT;
3. optional GRID/HOLD engine = HOW capital works while resident in the selected asset.

Detailed evidence remains in:
- `research/relative_rotation/2026-09-26_MULTI_ASSET_ROTATION_GRAPH_STRESS_TEST.md`

Status: RESEARCH_ONLY / NOT_PRODUCTION_APPROVED / MANUAL_EXECUTION_REQUIRED


### BTC defensive fallback experiment — 2026-09-26

User hypothesis:
- keep the baseline relative-rotation concept continuously invested in crypto;
- when an absolute RISK_OFF gate would otherwise move to USDT, use BTC as the fallback token instead.

Scope:
- BTC is not yet a ninth full graph node;
- main graph remains 8 assets / 28 relative pairs;
- BTC is fallback only while the shadow target fails its own causal SMA gate.

Same 2025-03-29 -> 2026-03-28 OOS year:
- baseline no gate: +41.6% median, ~62.2% max DD
- SMA100 -> BTC fallback: +32.1% median, ~48.2% max DD
- SMA200 -> BTC fallback: +10.4% median, ~53.7% max DD
- SMA300 -> BTC fallback: +15.0% median, ~53.7% max DD

Sequential 180-day median returns:
- baseline: +374.2%, -29.2%, +138.3%, +20.1%, -44.9%
- SMA100 -> BTC: +286.9%, +6.9%, +120.2%, +48.0%, -32.9%
- SMA200 -> BTC: +360.4%, +7.6%, +74.7%, +46.7%, -40.0%
- SMA300 -> BTC: +56.4%, +21.5%, +75.4%, +52.2%, -40.0%

Interpretation:
- BTC can act as a defensive crypto fallback in some regimes;
- it is not equivalent to cash and does not eliminate broad crypto drawdowns;
- the late-2025/early-2026 regime remained materially negative because BTC itself fell;
- if BTC becomes a full ninth graph node later, the complete graph expands from 28 to 36 unique pairs.

Status: RESEARCH_ONLY / DEFENSIVE_CRYPTO_CANDIDATE / MANUAL_EXECUTION_REQUIRED


### BTC full-node test — 2026-09-26

BTC was promoted experimentally from fallback-only status to a full ninth relative-rotation node.

Graph:
- 9 assets
- 36 unique pairs
- same 180d / 15% ARM / 3% reversal / next-open / 0.1% cost engine
- no BTC-specific tuning

Direct comparison versus the 8-node baseline:
- 2023-10-31 -> 2024-04-27: 8-node +374.2% vs 9-node +352.6%
- 2024-04-28 -> 2024-10-24: -29.2% vs -32.4%
- 2024-10-25 -> 2025-04-22: +138.3% vs +139.5%
- 2025-04-23 -> 2025-10-19: +20.1% vs +8.3%
- 2025-10-20 -> 2026-03-28: -44.9% vs -47.8%

Highlighted 1Y OOS window 2025-03-29 -> 2026-03-28:
- 8-node: +41.6% median
- 9-node + BTC: -24.0% median
- positive starts: 8/8 vs 1/9

Failure mechanism:
- BTC introduced PEPE -> BTC -> ATOM transitions that displaced the productive 8-node PEPE -> TRX -> LINK -> TWT route.

Current decision:
- BTC_FULL_GRAPH_NODE = REJECTED_FOR_NOW under the common universal pair parameters
- keep canonical graph baseline at 8 assets / 28 pairs
- BTC may still be researched separately as DEFENSIVE_CRYPTO fallback or market-regime reference
- BTC is not classified as useless; only the full-node role failed this first stress test

Detailed evidence:
- `research/relative_rotation/2026-09-26_MULTI_ASSET_ROTATION_GRAPH_STRESS_TEST.md`


### New-node probation candidate — 2026-09-27

Follow-up to BTC full-node failure:

Edge attribution showed that the damaging topology was mainly:
- PEPE -> BTC -> ATOM

Single-edge ablation on 2025-03-29 -> 2026-03-28:
- full unrestricted 9-node graph: ~-24.3% median
- remove only PEPE/BTC: +41.6% median
- remove only ATOM/BTC: +51.4% median

Important causal check using only pre-OOS data through 2025-03-28:
- ATOM/BTC standalone excess vs 50/50: ~-72.7 pp -> could have been rejected before OOS
- PEPE/BTC: ~+12.9 pp -> could not be rejected by a simple one-period positive screen

One-period edge admission was not robust enough.

Research candidate: NEW_NODE_PROBATION
- a new edge remains observable but cannot affect routing until it has positive standalone excess vs its 50/50 pair benchmark in two consecutive completed 180-day periods;
- this is topology protection, not RISK_OFF;
- do not permanently delete weak edges solely from one bad regime.

Causal walk-forward results where enough prior windows existed:
- 2024-10-25 -> 2025-04-22: baseline +138.3%, full-9 +135.8%, probation +138.3%
- 2025-04-23 -> 2025-10-19: baseline +20.1%, full-9 +6.6%, probation +20.1%
- 2025-10-20 -> 2026-03-28: baseline -44.9%, full-9 -44.9%, probation -44.9%

Interpretation:
- probation prevented BTC from degrading the established graph in all three available validation folds;
- it did not solve broad-market stale-hold losses;
- validation sample is still small, so this is not production-approved.

Current lifecycle concept for future nodes/edges:
DISCOVERED -> OBSERVE_ONLY -> PROBATION -> ELIGIBLE -> ACTIVE
with possible demotion ACTIVE -> WATCH -> PROBATION.

Canonical active graph remains 8 assets / 28 pairs.

Detailed evidence:
- `research/relative_rotation/2026-09-26_MULTI_ASSET_ROTATION_GRAPH_STRESS_TEST.md`


### Internal defensive crypto candidate — 2026-09-27

Goal:
- protect the existing 8-node / 28-pair rotation graph without assuming a move to USDT;
- remain continuously invested in approved crypto tokens.

Failed concepts first:
- daily strongest-token overlay: excessive churn, weak robustness;
- stale-hold timer only: reduced churn but did not reliably solve the late-2025/early-2026 failure;
- broad-market stress + momentum leader: selected recent winners such as BNB/TWT and missed TRX's defensive behavior.

Key regime fact:
2025-10-20 -> 2026-03-28 HODL returns inside the 8-token universe:
- TRX ~-1.5%
- BNB ~-44.0%
- ATOM ~-48.5%
- PEPE ~-53.1%
- LINK ~-53.9%
- SOL ~-56.1%
- AAVE ~-57.1%
- TWT ~-67.2%

TRX was already the lowest-volatility token at the first broad-market stress triggers, while momentum still favored BNB/TWT.

Research candidate: DEFENSIVE_LOW_VOL_CRYPTO
Reference configuration:
- market breadth = number of 8 tokens above own SMA200;
- enter defensive mode after 3 consecutive closes with breadth <= 3;
- defensive token = lowest realized 30-day daily-close volatility among the 8;
- remain 100% in that token; no USDT;
- relative router continues in background;
- exit after 3 consecutive closes with breadth >= 5;
- then return to the current shadow target;
- 0.1% actual transition cost.

Highlighted 2025-03-29 -> 2026-03-28 OOS year:
- base 8-node rotation: +41.6% median, ~-62.2% DD
- low-vol defensive crypto: **+49.4% median, ~-47.1% DD**
- all 8 starts remained positive
- median defensive transitions: ~3

Sequential 180-day median returns:
- base: +374.2%, -29.2%, +138.3%, +20.1%, -44.9%
- low-vol defensive: +423.9%, +13.2%, +106.3%, +39.7%, -0.8%

120-day robustness:
- base: +405.9%, -32.1%, -30.4%, +132.7%, +9.5%, +54.1%, -30.8%
- low-vol defensive: +405.9%, -10.9%, -13.6%, +95.7%, +6.0%, +35.7%, -11.1%

Parameter-neighborhood check:
- enter breadth 3-4 / exit 4-6 with 3-day confirmation produced broadly similar protection;
- stricter enter <=2 or slower 5-day confirmation degraded results;
- do not pick the historically best visible row after the fact.

Status:
- PROMISING_RESEARCH_CANDIDATE
- NOT_PRODUCTION_APPROVED
- designed after observing failure regimes, so hindsight bias remains a major residual risk
- real execution remains manual only

Detailed evidence:
- `research/relative_rotation/2026-09-26_MULTI_ASSET_ROTATION_GRAPH_STRESS_TEST.md`


---

## 14. Cross-asset macro transition context — 2026-09-27

STATUS: PROMISING_TRANSITION_CONTEXT / NOT_A_TRADING_GATE  
MODE: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING

Independent OSS comparator:
- methodology source: `zhuy9/market-rotation` (MIT);
- U.S. cross-asset regimes: BROAD_RISK_OFF / DEFENSIVE_ROTATION / BROAD_RISK_ON / INTERNAL_ROTATION / MIXED;
- external thresholds were not tuned on CryptoSignals evidence;
- U.S. session D is exposed to crypto only on D+1 calendar day to avoid same-date U.S.-close look-ahead.

Full-history replication:
- eligible crypto history begins: 2023-11-20;
- eight closed crypto-stress episodes;
- frozen crypto thresholds changed: NO;
- external OSS thresholds changed: NO.

Entry relationship:
- same-day BROAD_RISK_OFF before crypto entry signal: **4/8 = 50.0%**;
- within 7d: **4/8 = 50.0%**;
- within 14d: **5/8 = 62.5%**;
- within 30d: **8/8 = 100.0%**;
- median lead when found: **6.5 days**.

State-conditioned NORMAL-day baselines:
- same-day BROAD_RISK_OFF: 12.29%;
- within 7d: 32.29%;
- within 14d: 45.42%;
- within 30d: 66.04%.

Strongest descriptive entry enrichment:
- same-day BROAD_RISK_OFF: ~4.07x versus generic NORMAL days.

Exit relationship:
- same-day BROAD_RISK_ON before crypto exit signal: **1/8 = 12.5%**;
- within 7d: **2/8 = 25.0%**;
- within 14d: **5/8 = 62.5%**;
- within 30d: **6/8 = 75.0%**;
- median lead when found: **8 days**.

Interpretation:
- cross-asset stress is concentrated around crypto stress entries;
- raw daily external regime labels are not sufficient to replace crypto breadth;
- BROAD_RISK_ON looks more interesting as transition context roughly 1–2 weeks before crypto recovery than as a same-day exit rule;
- no statistical-significance claim is made because the eight episodes are not independent IID events and regime observations are serially correlated.

Canonical evidence:
- `research/relative_rotation/2026-09-27_OSS_MARKET_REGIME_COMPARATOR_V1_EVIDENCE.md`
- `research/relative_rotation/2026-09-27_OSS_MARKET_REGIME_FULL_HISTORY_EVIDENCE.md`

Current verdict:

`PROMISING_TRANSITION_CONTEXT / RAW_REGIME_NOT_A_TRADING_GATE`

---

## 15. Cash-defense destination hypothesis — 2026-09-27

STATUS: EXECUTED / LOW_VOL_DOMINANT_UNDER_FROZEN_TIMING / NOT_PRODUCTION_APPROVED  
CANDIDATE: `CASH_DEFENSE_DESTINATION_ABLATION_V1`

Question:
If crypto stress is already confirmed by the frozen breadth state machine, is it better to leave the crypto asset class entirely and hold a cash/stable-value proxy instead of rotating into the lowest-volatility crypto token?

Why this question is now justified:

1. **External cross-asset context supports stress-entry timing.**
   - all 8 historical crypto stress entries had a BROAD_RISK_OFF observation within the previous 30 days;
   - 4/8 had BROAD_RISK_OFF on the same day;
   - same-day entry enrichment was ~4.07x versus generic NORMAL days.

2. **Internal low-vol crypto defense reduces risk but remains exposed to crypto.**
   Untouched 2026 validation:
   - base rotation: +103.36% median return / -36.68% median max DD;
   - low-vol defense: +32.92% / -18.34%;
   - defense occupied 144/182 days (~79.1%);
   - verdict: real risk control, but excessive upside sacrifice.

3. **A separate earlier USDT smoke test showed that leaving crypto can cut drawdown much more aggressively.**
   Same 2025-03-29 -> 2026-03-28 OOS year under a DIFFERENT absolute-SMA trigger:
   - no risk gate: +41.6% median / ~-62.2% max DD;
   - SMA100 -> USDT: +63.5% / ~-27.0%;
   - SMA200 -> USDT: +37.4% / ~-15.2%;
   - SMA300 -> USDT: +42.7% / ~-15.2%.

Important boundary:
The USDT smoke test does **not** prove that the new cross-asset macro signal should trigger cash. It used a different entry gate. It only establishes that a true crypto-vs-cash layer can materially change drawdown behavior and therefore deserves an apples-to-apples test.

### Required next test: destination ablation first

Hold entry/exit timing fixed to the already-frozen crypto defensive state machine:
- enter after 3 consecutive closes with breadth <=3;
- exit after 3 consecutive closes with breadth >=5;
- same next-open semantics;
- same 0.1% transition cost;
- relative router continues in shadow.

Compare only destination:
- Variant A: frozen lowest-VOL30 crypto token;
- Variant B: cash/stable-value proxy at 0% modeled yield.

This isolates the question:
`LOW_VOL_CRYPTO vs CASH`
without changing the trigger.

Required comparison:
- median return;
- max drawdown;
- worst start;
- positive starts;
- defensive occupancy;
- transition count;
- opportunity cost during recovery;
- sequential 180d and 120d robustness.

### Only after destination ablation

A second, separately preregistered candidate may test whether external macro confirmation improves cash-entry timing:
- crypto breadth remains the primary stress state;
- external BROAD_RISK_OFF is confirmation/context, not a replacement;
- no post-hoc choice of 0/7/14/30-day window;
- do not use the broad 30-day result as an automatic entry rule merely because it covered 8/8 episodes.

### Exit remains the critical unresolved problem

Cash protects against broad crypto downside more completely than a low-vol crypto token, but it also earns little/no modeled upside while defense remains active.

Therefore the existing over-defense problem becomes **more important**, not less, if capital moves to cash.

The ongoing official Fed net-liquidity research is relevant specifically to this recovery/exit problem, but no liquidity result is yet promoted into a trading rule.

Runtime impact:
- production changed: NONE;
- paper-live changed: NONE;
- real swaps authorized: NO;
- manual confirmation remains required.

TEST_LEVEL: DOCUMENTATION_SYNC / EXISTING_EXECUTED_EVIDENCE_ONLY


### Cash-defense destination ablation — executed result

Run:
- GitHub Actions: `36300363884`
- source commit: `25f0b97e7ffce27daede34d7166427e674bc1056`
- artifact ID: `10924184874`
- workflow: SUCCESS
- reproduction gate: PASS

Only destination changed. Timing stayed frozen.

Reproduction period 2025-03-29 -> 2026-03-28:
- LOW_VOL: +49.05% median return / -47.13% median max DD;
- CASH: +5.93% / -43.20%;
- cash improved DD by only ~3.93pp while losing ~43.12pp of median return.

Previously opened 2026-03-29 -> 2026-09-26:
- LOW_VOL: +32.92% / -18.34%;
- CASH: +20.99% / -18.34%;
- cash produced no median DD improvement and reduced return by ~11.93pp;
- positive starts: LOW_VOL 8/8, CASH 7/8.

Full eligible history 2023-11-20 -> 2026-09-26:
- baseline: +646.96% / -71.23%;
- LOW_VOL: +887.02% / -47.13%;
- CASH: +258.06% / -67.56%.

Full-history cash versus LOW_VOL:
- median return difference: ~-628.97pp;
- median max-DD difference: ~-20.43pp, meaning CASH drawdown was materially worse.

Eight defensive episodes:
- CASH beat LOW_VOL on defensive-block return: 2/8;
- LOW_VOL beat CASH: 6/8;
- frozen low-vol token was TRX in all eight episodes.

Robustness:
- 5 complete non-overlapping 180d windows;
- 8 complete non-overlapping 120d windows;
- total 13 windows.

Return:
- CASH better: 3/13;
- LOW_VOL better: 9/13;
- equal: 1/13;
- in 180d windows CASH beat LOW_VOL return in 0/5.

Drawdown:
- CASH better: 6/13;
- LOW_VOL better: 2/13;
- equal: 5/13.

Interpretation:
- zero crypto exposure does not automatically mean a lower portfolio max drawdown;
- under the frozen slow breadth 3/5 timing, cash often locks in a prior loss and stays flat while TRX compounds during defense;
- that lost defensive-period appreciation can keep portfolio equity further below its old peak, producing a worse max drawdown than LOW_VOL;
- cash remains potentially useful only under a different, separately preregistered timing regime.

Verdict:

`CASH_DEFENSE_DESTINATION_ABLATION_V1_VERDICT = LOW_VOL_DOMINANT_UNDER_FROZEN_TIMING`

Do not replace frozen LOW_VOL with CASH under the existing breadth entry/exit timing.

Canonical evidence:
- `research/relative_rotation/2026-09-27_CASH_DEFENSE_DESTINATION_ABLATION_V1_EVIDENCE.md`

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + 180D_120D_ROBUSTNESS


---

## 16. Fixed cash crisis quarantine research — 2026-09-27

### V1 — immediate re-arm after fixed cash exit

STATUS: EXECUTED / REJECTED_FOR_PROMOTION

Preregistered durations:
- 14d / 21d / 30d / 45d / 60d

V1 semantics:
- frozen breadth<=3 x3 entry;
- fixed cash duration;
- return to current shadow target;
- crisis detector immediately begins counting a fresh low streak again.

Result:
- materially duration-sensitive;
- persistent crises caused repeated cash ping-pong;
- full-history median cash entries ranged from 11 to 33 depending on duration;
- no duration consistently beat frozen LOW_VOL on both return and drawdown;
- no 180d robustness window showed a duration consistently dominating LOW_VOL on both dimensions.

Verdict:
`CASH_CRISIS_QUARANTINE_V1 = DURATION_SENSITIVE / RETRIGGER_CHURN / DO_NOT_PROMOTE`

Evidence:
- `research/relative_rotation/2026-09-27_CASH_CRISIS_QUARANTINE_V1_EVIDENCE.md`

### V2 — one cash quarantine per crisis episode

STATUS: EXECUTED / PROMISING_ARCHITECTURE / NOT_PRODUCTION_APPROVED

Semantic correction:
- after fixed cash exit, relative rotations resume immediately;
- crisis detector becomes POST_CASH_DISARMED;
- another cash exit is forbidden until the existing frozen recovery condition breadth>=5 x3 is observed;
- that recovery condition only re-arms future crisis detection and does not keep capital in cash;
- after re-arm, a future cash entry requires a completely fresh breadth<=3 x3 sequence.

Run:
- GitHub Actions: `36301213660`
- source commit: `72c71ee641f258f7d7c97735eec9d043683dd046`
- artifact ID: `10926076102`
- workflow: SUCCESS
- reproduction gate: PASS

Reproduction 2025-03-29 -> 2026-03-28:
- frozen LOW_VOL: +49.05% / -47.13%
- 14d: +54.46% / -59.30%
- 21d: +66.22% / -53.81%
- 30d: +63.83% / -53.30%
- 45d: +4.29% / -53.30%
- 60d: +23.48% / -53.30%

Opened 2026-03-29 -> 2026-09-26:
- baseline: +103.36% / -36.68%
- frozen LOW_VOL: +32.92% / -18.34%
- 14d: +97.37% / -36.68%
- 21d: +91.34% / -36.68%
- 30d: +86.76% / -36.68%
- 45d: +59.41% / -34.77%
- 60d: +77.25% / -26.54%

Full eligible history 2023-11-20 -> 2026-09-26:
- baseline: +646.96% / -71.23%
- frozen LOW_VOL: +887.02% / -47.13%
- frozen-timing CASH: +258.06% / -67.56%
- 14d: +567.39% / -69.53%
- 21d: +1,142.45% / -65.42%
- 30d: +1,684.58% / -65.04%
- 45d: +965.68% / -65.04%
- 60d: +470.70% / -68.31%

Important:
- 30d is the strongest full-history return row, but it is NOT selected or validated;
- all five durations were preregistered together;
- the ranking is sample-sensitive;
- all durations remain materially worse than LOW_VOL on full-history drawdown.

Robustness, 13 non-overlapping windows:
- LOW_VOL return wins by V2 duration:
  - 14d beats LOW_VOL in 4/13;
  - 21d in 7/13;
  - 30d in 8/13;
  - 45d in 6/13;
  - 60d in 4/13.
- LOW_VOL drawdown is harder to beat:
  - every duration beats LOW_VOL drawdown in only 2/13 windows.
- both return and drawdown improved together:
  - 14d 2/13;
  - 21d 2/13;
  - 30d 2/13;
  - 45d 2/13;
  - 60d 1/13.

Interpretation:
- one-quarantine-per-crisis removes the V1 re-trigger churn;
- fixed cash quarantine can recover much of the active-router upside that frozen LOW_VOL sacrifices;
- fixed time alone is not a robust risk-control exit;
- after quarantine expiry, capital can spend months back inside crypto while the same crisis remains unresolved;
- the next research problem is re-entry timing, not cash-entry timing.

Verdict:

`CASH_CRISIS_QUARANTINE_REARM_V2 = PROMISING_RETURN_RECOVERY / RISK_CONTROL_WEAK / DURATION_SENSITIVE / DO_NOT_PROMOTE`

Canonical evidence:
- `research/relative_rotation/2026-09-27_CASH_CRISIS_QUARANTINE_REARM_V2_EVIDENCE.md`

Next boundary:
- do not pick 30d from this sample;
- future work should preserve one cash reaction per crisis and test a separately preregistered recovery/re-entry condition;
- macro/Fed-liquidity context may be tested for re-entry only in a new experiment.

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + 180D_120D_ROBUSTNESS


---

## 17. One TRX defense per crisis V4 — 2026-09-27

STATUS: EXECUTED / DO_NOT_PROMOTE  
MODE: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING

Hypothesis:
- frozen crisis entry breadth<=3 x3;
- fixed defensive asset TRX;
- shadow router continues;
- first NEW shadow-router transition after entry exits TRX directly into the new target;
- no TRX -> old shadow -> new target intermediate trade;
- after exit, ordinary router resumes;
- another TRX defense is forbidden until breadth>=5 x3 re-arms the detector;
- next crisis requires a fresh breadth<=3 x3.

Run:
- GitHub Actions: `36302318501`
- source commit: `a3445d70d981d5d7694e5bcc8e5b1bb326788c02`
- artifact ID: `10925409207`
- workflow: SUCCESS
- reproduction gate: PASS

Old V3 reference:
- 2025-03-29 -> 2026-03-28:
  - return -2.78%
  - DD -56.57%
  - repeated defensive churn
  - rejected.

V4 reproduction 2025-03-29 -> 2026-03-28:
- BASELINE: +43.82% / -61.57%
- frozen LOW_VOL: +49.05% / -47.13%
- V4: +5.08% / -57.05%
- worst start -5.56%
- positive starts 4/8
- median actual transitions 8
- median TRX entries 2
- median router-reactivation exits 2
- defensive exposure ~15.21%
- POST_TRX_DISARMED exposure ~54.66%.

Opened 2026-03-29 -> 2026-09-26:
- prior BASELINE: +103.36% / -36.68%
- prior frozen LOW_VOL: +32.92% / -18.34%
- V4: +105.02% / -36.68%
- positive starts 8/8
- TRX exposure ~0.55%
- POST_TRX_DISARMED exposure ~78.57%.

Interpretation:
- V4 recovered nearly all baseline upside in 2026 because the first router signal arrived almost immediately after TRX entry;
- therefore it also lost almost all LOW_VOL drawdown protection.

Full eligible history 2023-11-20 -> 2026-09-26:
- BASELINE: +646.96% / -71.23%
- frozen LOW_VOL: +887.02% / -47.13%
- V4: +1,209.45% / -67.85%
- median TRX entries: 4
- defensive exposure ~18.52%
- POST_TRX_DISARMED exposure ~42.23%.

The strong full-history return is path-dependent:
- e.g. one TRX block ran 2024-06-14 -> 2024-11-26 before a new router transition;
- several breadth-defined stress/recovery episodes were effectively merged.

Robustness:
- 180d: V4 beats LOW_VOL return 2/5, drawdown 1/5, both 1/5.
- 120d: V4 beats LOW_VOL return 2/8, drawdown 0/8, both 0/8.

Conclusion:

`ONE_TRX_DEFENSE_PER_CRISIS_V4_VERDICT = CHURN_FIXED / FULL_HISTORY_RETURN_HIGH / ROBUSTNESS_FAIL / LOW_VOL_STILL_BETTER_FOR_RISK_CONTROL / DO_NOT_PROMOTE`

Main engineering finding:
- direct TRX -> new shadow target execution is correct;
- forbidding repeated TRX defense inside the same crisis fixes old V3 ping-pong;
- but first router activity is still not reliable evidence that broad market stress is safe enough to abandon defense.

Canonical evidence:
- `research/relative_rotation/2026-09-27_ONE_TRX_DEFENSE_PER_CRISIS_V4_EVIDENCE.md`

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + 180D_120D_ROBUSTNESS
