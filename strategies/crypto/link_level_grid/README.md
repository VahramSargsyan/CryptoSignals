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
