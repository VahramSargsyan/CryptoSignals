# VAHRAM_LINK_LEVEL_GRID_V1 — registry record

strategy_id: VAHRAM_LINK_LEVEL_GRID_V1  
strategy_name: VAHRAM_LINK_LEVEL_GRID  
strategy_version: 1.0.0-experimental  
status: BACKTESTED  
created_at: 2026-09-25  
source_commit_sha: f33dde450eb33edb9ebc1b56b76445fa7245c020  
supersedes_strategy_id: none

## Decision

The executable research implementation has completed a real historical LINKUSDT backtest and is therefore recorded as **BACKTESTED**.

It is **not** recorded as **ACCEPTED**, **PAPER_LIVE**, or **LIVE_SIGNALS**.

Reason: the repository acceptance standard requires predefined acceptance gates and stronger validation before a strategy can be promoted to ACCEPTED. Several product rules are also still explicitly non-canonical.

## Strategy definition

- market: Binance Spot
- initial symbol: LINKUSDT
- timeframe: 1D
- direction: long-only accumulation / staged exit
- grid: 16 main levels x 4 sublevels = 64 sublevels
- capital architecture:
  - independent Micro pool
  - independent Mid pool
  - reserved per-slot budgets
- Micro research behavior:
  - entries on sublevels
  - exit one sublevel above entry
- Mid research behavior:
  - entry at main-level A boundary
  - upper levels exit at current percentage target
  - deeper levels exit at first of percentage target or 10-sublevel recovery
- range:
  - trailing H/L built causally from prior daily candles only
  - up to approximately 3 years of history

## Current non-canonical research assumptions

- range refresh cadence: 30 daily candles
- minimum history before first trading range: 1095 prior daily candles
- default capital allocation: linear_depth_reserved
- Mid entry location: A boundary
- open lots retain entry-time targets after range refresh

These assumptions are configurable/research-only and must not be confused with owner-frozen rules.

## Backtest evidence

backtest_run_id: RUN-ec8cfd26fda605c20008  
dataset_id: BINANCE:LINKUSDT:1D:07c433b5760a267a  
period: 2023-09-25 through 2026-09-24  
candles: 1096  
timeframe: 1D  
fees: 10 bps  
slippage: 5 bps  
allocation_preset: linear_depth_reserved  
closed_trade_count: 1495  
total_return: +140.4020%  
micro_total_return: +140.4247%  
mid_total_return: +140.3793%  
benchmark_buy_hold_return: +89.6299%  
max_drawdown: 41.1522%  
open_micro_lots_end: 47  
open_mid_lots_end: 13  
test_level: LIVE DATA BACKTEST + FULL REPOSITORY REGRESSION SUITE  
tests: 60 passed

GitHub Actions run: 36132903127  
Artifact ID: 10862951975  
Evidence document: docs/evidence/2026-09-25_LINK_LEVEL_GRID_FIRST_RUN.md

## Promotion requirements

Before status can move from BACKTESTED to ACCEPTED:

- freeze the exact H/L refresh rule;
- freeze canonical Micro and Mid allocation curves;
- freeze canonical Mid entry placement;
- confirm percentage-target derivation;
- run sensitivity tests around unresolved assumptions;
- perform held-out/out-of-sample validation;
- apply the repository acceptance gates defined before reviewing that validation.

## Notes

The first historical run is evidence that the implementation works and can be tested reproducibly. It is not evidence that the current research assumptions are the final intended strategy or that future returns are expected.


## Cross-asset evidence

### SOLUSDT — 2026-09-25

Validation type: CROSS_ASSET_RESEARCH / NOT FORMAL OOS  
Run ID: `RUN-fc372692d9a8fa379eb2`  
Dataset: `BINANCE:SOLUSDT:1D:2e54d39d19fd7152`  
Candles: 1096  
Strategy total return: +41.9064%  
Micro return: +40.2921%  
Mid return: +43.5208%  
Buy-and-hold return: +504.5455%  
Max drawdown: 44.8148%  
Closed trades: 1155  
GitHub Actions run: 36133871517  
Artifact ID: 10862838818  
Evidence: `docs/evidence/2026-09-25_SOL_CROSS_ASSET_GRID_VALIDATION.md`

Interpretation: the current research implementation remained profitable on SOL without symbol-specific tuning, but materially underperformed passive SOL holding over this specific three-year period. Strategy status remains `BACKTESTED`.


### ETHUSDT — 2026-09-25

Validation type: CROSS_ASSET_RESEARCH / NOT FORMAL OOS  
Run ID: `RUN-d2e7ae9c84b3c219ee36`  
Dataset: `BINANCE:ETHUSDT:1D:a4ee296f034719d5`  
Candles: 1096  
Strategy total return: +63.9134%  
Micro return: +56.5784%  
Mid return: +71.2484%  
Buy-and-hold return: +70.0533%  
Max drawdown: 32.3323%  
Closed trades: 1433  
GitHub Actions run: 36134629875  
Artifact ID: 10862039461  
Evidence: `docs/evidence/2026-09-25_ETH_CROSS_ASSET_GRID_VALIDATION.md`

Interpretation: the current research implementation remained profitable on ETH without symbol-specific tuning and finished close to buy-and-hold over the same three-year period. The Mid layer individually exceeded buy-and-hold on its normalized pool. Strategy status remains `BACKTESTED`.


## Backtest methodology correction — 2026-09-25

Owner clarification established that a 3-year trading evaluation requires a full 3-year prehistory before the first trade.

Canonical research window structure from this point forward:

```text
6 years total data
3 years prehistory only
3 years trading/evaluation
```

The earlier LINK, SOL and ETH 3-year-only runs are retained as runtime smoke tests but are superseded as performance evidence.

Corrected LINK run:

- run ID: `RUN-93c77fa26c49e02ceb21`
- dataset: `BINANCE:LINKUSDT:1D:1262fdbb884059ba`
- dataset candles: 2191
- prehistory candles: 1095
- trading candles: 1096
- strategy return: +482.2161%
- Micro: +471.1002%
- Mid: +493.3320%
- buy-and-hold: +89.6299%
- max drawdown: 35.9760%
- closed trades: 625
- GitHub Actions run: 36135794525
- evidence: `docs/evidence/2026-09-25_GRID_PREHISTORY_METHOD_CORRECTION.md`

Status remains `BACKTESTED`. The result is not promoted to `ACCEPTED` because range-refresh cadence and several portfolio rules remain research assumptions.


## Corrected cross-asset evidence — ETH and SOL

Methodology: 6 years total data, first 3 years prehistory only, final 3 years trading/evaluation.

### ETHUSDT corrected

- run: `RUN-47390258b7de2f2509c5`
- dataset: `BINANCE:ETHUSDT:1D:62c8d7d5b63d300a`
- strategy return: +119.8053%
- Micro: +121.4294%
- Mid: +118.1812%
- buy-and-hold: +70.0533%
- max drawdown: 38.9967%
- closed trades: 1081
- GitHub Actions run: 36136235756
- artifact: 10865300723

### SOLUSDT corrected

- run: `RUN-648214d6dbb89478db0b`
- dataset: `BINANCE:SOLUSDT:1D:60fe3a76bb1d67b3`
- strategy return: +736.0574%
- Micro: +696.1725%
- Mid: +775.9423%
- buy-and-hold: +504.5455%
- max drawdown: 51.5712%
- closed trades: 1041
- GitHub Actions run: 36136246985
- artifact: 10865075929

Evidence: `docs/evidence/2026-09-25_CORRECTED_ETH_SOL_CROSS_ASSET_VALIDATION.md`

These corrected runs supersede the earlier three-year-only ETH/SOL performance figures. Strategy status remains `BACKTESTED`.


## Corrected cross-asset evidence — BNB and BTC

Methodology: 6 years total data, first 3 years prehistory only, final 3 years trading/evaluation.

### BNBUSDT corrected

- run: `RUN-764773cb6222272429ba`
- dataset: `BINANCE:BNBUSDT:1D:a07a7f7566e5243b`
- strategy return: +104.1704%
- Micro: +93.3718%
- Mid: +114.9690%
- buy-and-hold: +272.8709%
- max drawdown: 26.3147%
- closed trades: 997
- GitHub Actions run: 36140374482
- artifact: 10866761636

### BTCUSDT corrected

- run: `RUN-2ab3460b6a87f04de04f`
- dataset: `BINANCE:BTCUSDT:1D:52681302302c81b5`
- strategy return: +69.6046%
- Micro: +60.8840%
- Mid: +78.3252%
- buy-and-hold: +221.5825%
- max drawdown: 16.2595%
- closed trades: 926
- GitHub Actions run: 36140388792
- artifact: 10866426854

Evidence: `docs/evidence/2026-09-25_CORRECTED_BNB_BTC_CROSS_ASSET_VALIDATION.md`

Interpretation: profitable on both assets under the current research implementation, but below buy-and-hold over this specific period. Strategy status remains `BACKTESTED`.


## Optimizer research — 2026-09-25

A first cross-asset parameter search was run on LINK, ETH, SOL, BNB and BTC using the corrected 6-year / 3-year-prehistory methodology.

The 3-year trading period was split into an earlier parameter-selection segment and a final 365-day temporal holdout.

Key findings:

- equal capital by depth maximized the historical selection segment but failed the temporal holdout and increased drawdown;
- the existing linear-depth allocation was less profitable in-sample but materially more stable in the holdout;
- a more bottom-heavy allocation further reduced historical return while improving holdout behavior and drawdown;
- wider exit distances materially improved full-period returns across all five assets, but the temporal-holdout improvement was small;
- no optimizer candidate is accepted as canonical.

Promising exit research candidate:

```text
Micro recovery = 6 sublevels
Mid recovery   = 18 sublevels
Mid target     = 1.00 x current target percentages
capital powers = 1 / 1
```

It improved full-period returns on all five tested assets, but did not materially improve the clean temporal holdout versus the current baseline.

Evidence: `docs/evidence/2026-09-25_LEVEL_GRID_OPTIMIZER_V1.md`

Status remains `BACKTESTED`. Optimizer outputs are hypotheses, not strategy promotions.


## Reinvestment / runner research — 2026-09-25

The existing backtest was confirmed to already reinvest 100% of positive realized profit into the same grid slot.

A 24-candidate exploratory scan across LINK, ETH, SOL, BNB and BTC tested:

- profit reinvestment: 0%, 50%, 100%;
- permanent token runner: 0%, 10%, 25%, 50%;
- current BASE exits and the wider 6/18 research exits.

Key findings:

- 100% reinvestment increased historical return on all five assets, with materially higher drawdown;
- permanent runners improved BTC and BNB in this historical period;
- permanent runners reduced returns sharply on LINK, ETH and SOL;
- the best shared five-asset configuration remained full reinvestment with no permanent runner;
- no asset-specific runner setting is promoted because the same history was used to discover it.

Evidence: `docs/evidence/2026-09-25_REINVEST_RUNNER_EXPLORATION_V1.md`

Status remains `BACKTESTED`.


## Paper-live launch protocol — 2026-09-25

Forward paper observation is configured to start from the first full UTC candle beginning:

```text
2026-09-26T00:00:00Z
```

Universe:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Two profiles are observed in parallel:

- `CONTROL_BASE`: Micro +1 / Mid +10;
- `CANDIDATE_WIDE`: Micro +6 / Mid +18.

Shared settings:

- linear-depth allocation p=1;
- 100% positive-profit reinvestment;
- no permanent runner;
- 10 bps fees;
- 5 bps slippage;
- 1095-candle H/L lookback;
- 30-candle H/L refresh remains a research assumption.

Runtime verification before launch:

- GitHub Actions run: `36150717357`;
- 68 repository tests: passed;
- live Binance data download: passed;
- paper engine status: `PAPER_LIVE_WAITING`;
- latest closed candle in verification run: `2026-09-24`;
- artifact: `10871851428`.

The strategy registry status remains `BACKTESTED` until at least one post-launch daily candle is actually observed. The configured workflow itself does not authorize live-money execution.

Protocol: `docs/15_GRID_PAPER_LIVE_V1.md`.


## Moving-average exit research decision — 2026-09-26

Status: **TESTED / NOT PROMOTED / DUPLICATE-WORK GUARD ACTIVE**

Explored research families:

- direct MA exits replacing fixed recovery targets;
- MA regime selection between BASE (+1/+10) and WIDE (+6/+18);
- MA-managed runners after fixed recovery.

Summary:

- direct MA exits underperformed WIDE in the recorded five-asset exploratory comparison and increased trade churn;
- MA50/100 was the strongest simple round-number regime selector, but remained below constant WIDE in aggregate;
- post-hoc MA40/90 slightly exceeded WIDE in full-period aggregate return but lost on 3/5 assets and only 6/15 asset-year windows, so it is not promoted;
- MA-managed runners reduced drawdown modestly but reduced shared historical return; the closest 10%-of-profit runner still remained below WIDE in aggregate.

No canonical strategy, paper-live profile, or status is changed by this research.

Decision log: `docs/evidence/2026-09-26_LINK_GRID_MA_EXIT_RESEARCH_DECISION_LOG_V1.md`.
