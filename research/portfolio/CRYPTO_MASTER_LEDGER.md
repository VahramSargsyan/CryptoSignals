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
