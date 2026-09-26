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
- +374.2%
- -29.2%
- +138.3%
- +20.1%
- -44.9%

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
- high USDT occupancy is a material trade-off and requires further validation;
- do not optimize to the best visible SMA result after the fact.

Current conceptual stack:
1. RELATIVE ROTATION GRAPH = WHERE inside crypto;
2. ABSOLUTE RISK GATE = CRYPTO vs USDT;
3. optional GRID/HOLD engine = HOW capital works while resident in the selected asset.

Detailed evidence remains in:
- `research/relative_rotation/2026-09-26_MULTI_ASSET_ROTATION_GRAPH_STRESS_TEST.md`

Status: RESEARCH_ONLY / NOT_PRODUCTION_APPROVED / MANUAL_EXECUTION_REQUIRED
