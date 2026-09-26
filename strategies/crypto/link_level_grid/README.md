# VAHRAM_LINK_LEVEL_GRID_V1

Status: **BACKTESTED / RESEARCH IMPLEMENTATION / NOT ACCEPTED FOR LIVE USE**  
Timeframe: **1D only**  
Initial symbol: **LINKUSDT**

The strategy now has executable code in:

- `strategy.py` — grid geometry, capital reservation, Micro/Mid execution and portfolio-aware backtest;
- `scripts/run_link_level_grid_backtest.py` — end-to-end Binance download + backtest runner;
- `.github/workflows/link-level-grid-backtest.yml` — GitHub Actions runner.

## Confirmed owner rules implemented

- 16 main levels;
- 4 sublevels per main level;
- 64 total sublevels;
- dynamic broad H/L range from up to roughly 3 years of daily candles;
- Micro and Mid are independent capital pools;
- deeper Mid entries can exit at the first of:
  - the current percentage target;
  - a 10-sublevel recovery;
- example `64 -> 54`;
- lower-level capital is protected through reserved per-slot budgets.

## Research assumptions that are NOT owner-frozen yet

To make the strategy executable without pretending unresolved details are known, the code labels these assumptions explicitly:

1. trailing H/L range refresh every **30 daily candles** by default;
2. first trading candle requires **a full 3-year prehistory (1095 daily candles)**;
3. default clean capital curve is `linear_depth_reserved`:
   weights 1..16, normalized to 100%, so deeper levels receive progressively more reserved capital;
4. Mid entries occur at each main level's lower **A** boundary;
5. Micro exit is one sublevel above entry, matching historical workbook behavior;
6. open lots retain the target calculated from the grid that existed when they were opened;
7. same-candle buy and sell of the same slot is blocked.

These are research parameters, not hidden claims about the final strategy.

## Capital presets

### `linear_depth_reserved` — default research baseline

Each layer owns its own 100% capital pool.

Main-level weights:

```text
1, 2, 3, ..., 16
```

normalized to 100%.

Micro divides each main-level reserve equally among its four sublevels.

### `equal_reserved`

Every main level receives the same capital share.

Useful as a neutral comparison.

### `historical_observed`

Reproduces the historical workbook allocation shape as closely as the current evidence allows.

This preset is retained for comparison only because the owner explicitly identified the original capital distribution as a mistake.

## Required prehistory before trading

A three-year strategy evaluation requires three years of prior daily history before the first simulated trade.

Default structure:

```text
6 years downloaded
first 3 years = prehistory only
final 3 years = trading/evaluation
```

At the first trading candle, H/L is built from the preceding three years. No orders are allowed during the prehistory segment.

This requirement supersedes the earlier 90-candle warmup research implementation.

## Look-ahead protection

The rolling range on candle T is built only from candles before T.

The current candle's high/low cannot change the range used to trade that same candle.

This is essential for a meaningful historical test.

## Run locally

```bash
python -m scripts.run_link_level_grid_backtest \
  --symbol LINKUSDT \
  --history-years 6 \
  --trade-years 3 \
  --allocation-preset linear_depth_reserved
```

Outputs:

```text
research_artifacts/link_level_grid/<RUN_ID>/
  canonical_candles.csv
  trades.csv
  equity_curve.csv
  range_history.csv
  allocation_table.csv
  summary.json
  download_metadata.json
```

## Important interpretation boundary

The strategy registry status is **BACKTESTED** because a historical run completed successfully. This does not mean **ACCEPTED** under the repository acceptance gates. A successful run proves that the strategy mechanics can be simulated deterministically.

It does **not** prove the strategy is profitable, accepted, or that the remaining assumptions match the owner's final intended rules.

Those assumptions should be frozen after reviewing the first LINK evidence.


## Research optimizer

Parameter-search tooling is available in:

- `scripts/optimize_link_level_grid.py`;
- `.github/workflows/link-level-grid-optimizer.yml`;
- `.github/workflows/link-level-grid-exit-refine.yml`.

The optimizer can vary capital-depth exponents and exit distances while preserving the default strategy behavior when optimizer parameters are unset.

Selection uses an earlier training segment and reports a later temporal holdout separately. Optimizer winners are research hypotheses only and do not automatically replace canonical strategy rules.

Evidence: `docs/evidence/2026-09-25_LEVEL_GRID_OPTIMIZER_V1.md`.


## Reinvestment and token runners

Research controls now make two previously implicit choices explicit:

- `profit_reinvest_fraction` — fraction of positive realized profit compounded back into the same slot;
- `runner_fraction` — fraction of an exited position left unsold as long-term token inventory.

Default behavior is unchanged:

```text
profit_reinvest_fraction = 1.0
runner_fraction = 0.0
```

So the historical strategy results already included full per-slot profit compounding.

Evidence: `docs/evidence/2026-09-25_REINVEST_RUNNER_EXPLORATION_V1.md`.


## Paper-live observation

A scheduled forward-observation workflow is defined in:

- `.github/workflows/grid-paper-live-v1.yml`;
- `scripts/run_grid_paper_live.py`;
- `scripts/send_grid_paper_report.py`.

Paper start:

```text
2026-09-26T00:00:00Z
```

Profiles run in parallel:

- `CONTROL_BASE`: Micro +1 / Mid +10;
- `CANDIDATE_WIDE`: Micro +6 / Mid +18.

Both use linear-depth capital, 100% positive-profit reinvestment and no permanent runner.

The job runs daily after the UTC daily candle closes and produces a reproducible report from newly available closed candles only. It does not send broker orders and does not require exchange trading credentials.

Optional Telegram/SMTP reporting is controlled only through GitHub repository secrets.

Protocol: `docs/15_GRID_PAPER_LIVE_V1.md`.


## Moving-average exit research decision memory — 2026-09-26

MA-based exit research has already tested three families without changing grid entries:

- MA directly replacing fixed recovery exits;
- MA selecting BASE versus WIDE exits;
- MA-managed runners after WIDE recovery.

Current decision: **TESTED / NOT PROMOTED**. Constant WIDE (+6/+18) remains the stronger shared historical reference after the explored MA variants; MA40/90 produced a slightly higher aggregate development result in one post-hoc regime-selector scan but did not show broad enough asset/year stability to promote.

Future chats should not repeat MA25/50, MA50/100, MA100/200, MA40/90, direct-MA exits, or MA-runner variants without materially new evidence or a new mechanism.

Decision log: `docs/evidence/2026-09-26_LINK_GRID_MA_EXIT_RESEARCH_DECISION_LOG_V1.md`.


## Volatility / multi-indicator exit research — 2026-09-26

A second exit-regime research pass tested ATR%, Bollinger bandwidth, realized volatility, MA pairs, and simple MA/volatility/ADX combinations for choosing among +3/+12, +6/+18, and +9/+24 exits.

Current decision: **TESTED / NOT PROMOTED**. Constant WIDE (+6/+18) remains the strongest shared historical return reference. The closest volatility-only candidate was inverse ATR logic (+251.54% geometric vs +253.03% WIDE), while a BBW high-volatility brake reduced median drawdown (36.82% vs 38.67%) at a modest return cost (+249.84%). Multi-indicator combinations did not improve on WIDE.

Decision log: `docs/evidence/2026-09-26_LINK_GRID_VOLATILITY_REGIME_RESEARCH_V1.md`.


## External crowd / market-context research — 2026-09-26

A first non-price-only context pass tested funding, Fear & Greed, and broad-market/stablecoin context as selectors among +3/+12, +6/+18, and +9/+24 exits while leaving grid entries untouched.

On the common 2023-09-25 through 2026-08-15 horizon, constant WIDE returned +168.87% geometric across five assets. Funding reached +170.62% with lower median drawdown (37.31% vs 38.67%) but did not survive the independent-window robustness check (7/15 wins; negative mean/median return difference). Fear & Greed and the simple 2-of-3 consensus did not improve the control. Broad-market context showed 9/15 window wins but lower full-period compound.

Open interest and liquidation effects remain untested; no proxy result is reported as real OI/liquidation evidence.

Decision: **INTERESTING EXTERNAL INFORMATION / NO PROMOTION**.

Evidence: `docs/evidence/2026-09-26_LINK_GRID_EXTERNAL_CROWD_CONTEXT_RESEARCH_V1.md`.


## Capital distribution / H-L lookback research — 2026-09-26

A structural research pass varied grid-depth capital allocation and the trailing H/L window while keeping WIDE exits (+6/+18) fixed.

Key findings:

- equal capital by level (p=0) raised full-period five-asset geometric return to +281.59% versus +253.03% for p=1, but median drawdown increased to 46.31% versus 38.67% and the final annual reset window turned slightly negative;
- p=1 with H/L around 2.25–2.9 years was consistently competitive with the 3-year baseline;
- p=1 / 2.75y produced the best full-period p=1 result at +265.94% geometric return with 38.58% median DD, but did not show a year-by-year robustness edge;
- 2.25–2.4y showed better annual-window stability;
- 4y/5y H/L require longer raw history for a fair 3-year comparison and are not promoted from the shorter diagnostic windows.

Decision: **TESTED / NOT PROMOTED**. Paper-live remains p=1 with 1095-candle H/L.

Evidence: `docs/evidence/2026-09-26_LINK_GRID_CAPITAL_RANGE_LOOKBACK_RESEARCH_V1.md`.


## Known-strategy benchmark — 2026-09-26

A standing benchmark now compares the grid with common external approaches on the same corrected five-asset daily datasets and the same fee/slippage assumptions.

On the development sample, WIDE (+6/+18) produced +253.03% five-asset geometric return with 38.67% median max drawdown, versus +196.94% / 67.55% for Buy & Hold and lower aggregate returns for SMA200 trend, 12-month momentum, SMA50/200, Donchian 20/10, monthly DCA, RSI mean reversion, and Bollinger mean reversion.

Important: this is **development-sample benchmarking**, not clean OOS proof; WIDE and other grid parameters were already studied on these same assets/history.

Evidence: `docs/evidence/2026-09-26_LINK_GRID_KNOWN_STRATEGY_BENCHMARK_V1.md`.


## Unseen top-10 cross-asset check — XRP / TRX — 2026-09-26

Two additional large-cap assets, XRP and TRX, were tested with frozen WIDE parameters and no symbol-specific tuning. On the common 2023-09-25 through 2026-01-05 window:

- XRP WIDE: +196.29%, DD 32.45%; BASE +171.45%; Buy & Hold +366.52%;
- TRX WIDE: +43.82%, DD 8.79%; BASE +35.99%; Buy & Hold +248.69%.

WIDE beat BASE on both new assets, reducing concern that the +6/+18 improvement exists only in the original five-asset set. However, Buy & Hold remained much stronger on both, so this is **generalization evidence for the grid mechanism**, not universal return superiority.

Evidence: `docs/evidence/2026-09-26_GRID_UNSEEN_TOP10_XRP_TRX_VALIDATION_V1.md`.


## Volatile-asset stress test — 2026-09-26

A common-window stress test on TWT, DOGE, AVAX and SHIB found that raw realized volatility alone does not guarantee a strong grid result. DOGE was profitable under the grid while Buy & Hold was negative, but TWT, AVAX and SHIB remained negative despite high volatility. BASE also outperformed WIDE on all four assets in that specific window.

Current interpretation: the grid appears to prefer **volatility with repeated recovery / mean reversion**, not volatility dominated by persistent directional decline.

PEPE was not forced into a shortened-lookback test; the pinned snapshot had only 1059 daily bars, below the canonical 1095-day H/L warmup.

Evidence: `docs/evidence/2026-09-26_GRID_VOLATILE_ASSET_STRESS_TEST_V1.md`.


## Quality + Grid universe screen — 2026-09-26

A 15-asset old/large-cap universe was tested on a common 2023-09-25 through 2026-03-28 window with frozen WIDE mechanics.

Aggregate:

- WIDE geometric return: +160.98%
- BASE: +159.01%
- Buy & Hold: +72.67%
- WIDE positive: 15/15
- WIDE beat Buy & Hold: 10/15
- WIDE median DD: 43.18% versus Buy & Hold 70.26%

Primary deep-research candidates are LINK, SOL, ETH, ADA and XLM. HBAR, UNI, DOGE, AVAX and LTC remain valuable but have specific quality/risk or parameter-fit caveats. BTC, BNB, XRP, TRX and BCH remain controls / possible Hold-trend assets rather than being forced into Grid.

Important: the universe intentionally contains surviving old large-cap assets, so survivorship bias is material.

Evidence: `docs/evidence/2026-09-26_GRID_UNIVERSE_QUALITY_SUITABILITY_SCREEN_V1.md`.


## Published OSS strategy benchmark — 2026-09-26

MIT-licensed external strategy logic was benchmarked against the Grid on the same corrected five-asset D1 datasets and normalized execution semantics.

Strongest external result:

- Gekko Fibonacci 8/21/55: +225.57% geometric, 39.93% median DD;
- Grid BASE: +226.41%, 35.98%;
- Grid WIDE: +253.03%, 38.67%;
- Buy & Hold: +196.94%, 67.55%.

Fibonacci beat WIDE on ETH, BNB and BTC; Grid dominated on LINK and SOL. Zenbot MACD/SRSI_MACD/Bollinger defaults were also tested, but their original defaults are intraday-oriented and the D1 results are portability evidence only.

Evidence: `docs/evidence/2026-09-26_GRID_VS_OSS_STRATEGIES_BENCHMARK_V1.md`.


## Bar-normalized timeframe research — 2026-09-26

The hypothesis “use the last 1095 bars on any timeframe” was stress-tested without changing canonical Grid behavior.

Key findings:

- H1 mechanics remained viable in a preliminary BTC/ETH/BNB sample and WIDE beat BASE on all three;
- BTC 15m and 5m smoke tests remained positive, but transaction-cost headroom compressed sharply;
- on BTC 1m, average WIDE +6 spacing was only ~0.165% versus a rough ~0.30% round-trip fee/slippage assumption;
- on the same 2024-07-01 through 2026-03-31 calendar period, BTC D1 returned +4.08% versus BTC 4H -11.72%, and ETH D1 +21.56% versus ETH 4H +6.20%.

Decision: Grid mechanics are not D1-only, but **1095 candles is not yet a universal scale-free invariant**. Calendar memory appears to matter.

Evidence: `docs/evidence/2026-09-26_GRID_BAR_NORMALIZED_TIMEFRAME_RESEARCH_V1.md`.


### Expanded bar-normalized timeframe pass — 2026-09-26

The timeframe evidence now includes:

- 4H: all five canonical assets, 3285 post-warmup bars each;
- 15m: all five canonical assets, 1785 post-warmup bars each;
- 5m: longer BTC boundary probe;
- 1m: longer BTC boundary probe.

On the same 2024-09 to 2026-03 calendar period, D1 WIDE produced +19.32% five-asset geometric return with 28.25% median DD, versus +5.93% / 40.57% for BAR-NORMALIZED 4H. At 1m, average +6 Micro distance was ~0.246%, below the ~0.30% modeled round-trip friction.

Decision remains: **do not change canonical D1 or paper-live**.

Evidence: `docs/evidence/2026-09-26_GRID_BAR_NORMALIZED_TIMEFRAME_RESEARCH_V1.md`.


### D1/H4 capital and exit-assist test — 2026-09-26

Fixed D1/H4 capital splits did not beat pure D1 on the common 2024-09-20 through 2026-03-21 period. D1 entries with H4-derived exit spacing reduced drawdown on all five assets but lowered aggregate geometric return from +19.67% to +16.13% and increased closed exits from 932 to 2227.

Evidence: `docs/evidence/2026-09-26_GRID_BAR_NORMALIZED_TIMEFRAME_RESEARCH_V1.md`.


## Micro / Mid redundancy test — 2026-09-26

The two capital layers were reviewed specifically for architectural redundancy.

Key finding:

- Micro and Mid do not interact financially, so the combined result is a weighted blend rather than a new source of edge.
- Existing canonical WIDE p=1 split scan: 20/80 = +257.23% / 39.90% DD, 50/50 = +253.03% / 38.67%, 80/20 = +248.80% / 36.73%.
- Mid-heavy increased return modestly; Micro-heavy lowered drawdown.
- Supplementary annual stress checks show relative leadership can switch by regime.

No simplification promoted yet.

Evidence: `docs/evidence/2026-09-26_GRID_MICRO_MID_REDUNDANCY_STRESS_TEST_V1.md`.


### Expanded Micro / Mid redundancy — 2026-09-26

15-asset common-window test:

- Micro-only: +170.91% geometric / 41.39% median DD / 2,303 exits
- Mid-only: +147.17% / 42.77% / 721 exits
- Current 50/50: +160.98% / 43.18% / 3,024 exits

Tier A (LINK/SOL/ETH/ADA/XLM):

- Micro: +244.93%, 884 exits
- Mid: +245.61%, 278 exits
- 50/50: +247.36%, 1,162 exits

Mid-only is therefore a serious simplification candidate for a quality-filtered universe, but not a universal replacement across all tested tokens.

Evidence: `docs/evidence/2026-09-26_GRID_MICRO_MID_REDUNDANCY_STRESS_TEST_V1.md`.
