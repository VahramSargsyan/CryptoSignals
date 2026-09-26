# Grid OSS Mechanism Harvest v1 — 2026-09-26

Date: 2026-09-26  
Workflow mode: **BUILD_NEW_APP / RESEARCH ONLY**  
Canonical strategy mutation: **NO**  
Paper-live mutation: **NO**  
Live-money authorization: **NO**

## Objective

Test whether mature open-source grid implementations contain mechanisms worth adapting into the current `MID_ONLY_WIDE` research line, while keeping the canonical strategy untouched.

## External sources reviewed

- `enarjord/passivbot` @ `507dedc68ffee07da047a8b74bea61355625037e` — trailing/retracement entry/close, exposure governance, unstuck, forager, optimizer.
- `jordantete/grid_trading_bot` @ `72cfdb44b79174c08cb2dc994f7edbfeb7582c9b` — 8-level arithmetic grid, 50% initial allocation, ATR14 spacing, ATR multiplier 0.5, 30% regrid threshold, 60-bar cooldown.
- `hummingbot/hummingbot` @ `9af100d6822da7d2d0291a906c730ef172284ee2` — grid executor state machine, max open orders, activation bounds, triple-barrier risk controls.
- `Drakkar-Software/OctoBot` @ `85ae511a56dc3c782fcc528ba3a96d29eb6e5e03` — grid/staggered order lifecycle and automatic grid health restoration.
- `btschwertfeger/infinity-grid` @ `752f33d8c92a65fa3f3c4408af6523bc42a6b013` — moving/infinite grid, reinvestment, multiple grid modes.

No upstream source code is copied into the canonical strategy. The research harness reimplements only documented mechanisms for controlled comparison.

## Frozen comparison contract

- SAME_DATASET: **yes**
- SAME_DATASET_VERSION: **yes**
- SAME_SCENARIOS: **yes** for MID candidates; external port uses the same datasets/period/costs but a different strategy engine by design.
- SAME_ENVIRONMENT: **yes** within the local research runner.
- Assets: BTC, ETH, BNB, SOL, LINK.
- Dataset: five frozen six-year canonical artifacts already used by the current benchmark.
- Prehistory: 2020-09-25 through 2023-09-24.
- Evaluation: 2023-09-25 through 2026-09-24.
- Timeframe: 1D.
- Fee: 10 bps.
- Slippage: 5 bps.
- Baseline: WIDE Mid only, p=1, recovery +18 sublevels, target scale 1.0, 100% reinvestment, no runner.

## Baseline reproduction control

The standalone harness reproduces the previously verified frozen MID-only endpoint:

- geometric return: **+260.01%**
- median max drawdown: **40.60%**
- closed exits: **266**

This control is required before interpreting any OSS candidate result.

## Headline results

| Candidate | Geometric return | Median DD | Closed trades | Interpretation |
|---|---:|---:|---:|---|
| MID_BASELINE | +260.01% | 40.60% | 266 | frozen control |
| MID_EXIT_RETRACE_0.07 | +264.93% | 40.83% | 253 | Passivbot-inspired delayed/trailing close |
| MID_ATR_REGRID_GATE_0.50 | +268.23% | 38.18% | 267 | ATR-gated H/L refresh |
| MID_OSS_COMBO_ATR50_EXIT_0.07 | +278.03% | 38.45% | 256 | combined research candidate |
| MID_EXPOSURE_CAP_0.80 | +198.85% | 33.40% | 257 | exposure-governor stress case |
| JORDAN_DYNAMIC_GRID_PORT | +45.26% | 39.98% | 2028 | external dynamic-grid reference port |

## Candidate 1 — trailing-confirmed entry

Mechanism: when a MID boundary is touched, do not buy immediately; arm the slot and wait for an upward retracement from the observed trough on a later daily candle. This is a daily-compatible adaptation of Passivbot trailing-entry semantics, not an exact Passivbot runtime.

| Retracement | Geometric return | Median DD | Trades |
|---:|---:|---:|---:|
| 0.5% | +202.79% | 40.45% | 252 |
| 1.0% | +202.73% | 40.48% | 252 |
| 2.0% | +201.99% | 40.69% | 252 |
| 3.0% | +203.81% | 40.03% | 253 |
| 5.0% | +196.55% | 38.94% | 245 |

Observation: every tested confirmation threshold materially reduced return versus MID baseline. It can reduce drawdown at the widest threshold, but the historical return cost is large. **Not a promotion candidate.**

## Candidate 2 — trailing-confirmed exit

Mechanism: once the existing MID target is reached, arm a trailing exit and close only after a later retracement from the post-target peak. Same-day target-touch/retracement is not assumed, avoiding unsupported OHLC path ordering.

| Retracement | Geometric return | Median DD | Trades |
|---:|---:|---:|---:|
| 4% | +253.75% | 40.66% | 263 |
| 5% | +256.15% | 40.78% | 261 |
| 6% | +263.88% | 40.98% | 256 |
| 7% | +264.93% | 40.83% | 253 |
| 8% | +264.79% | 40.65% | 249 |
| 9% | +261.85% | 40.70% | 248 |
| 10% | +248.65% | 40.66% | 247 |

Observation: the 6–9% region is above baseline return, with 6–8% forming a small plateau. Drawdown is roughly flat. This is **PROMISING / NOT PROMOTED** because the same history was used to discover the range.

## Candidate 3 — ATR-gated H/L refresh

Mechanism: preserve the current 1095-candle causal H/L geometry, but do not rebuild it every 30 days. Rebuild after a 60-bar cooldown only when ATR14 has moved by the configured threshold from the ATR at the last rebuild (or price escapes the active H/L). This adapts the dynamic-regrid trigger from OSS without replacing our global H/L model.

| ATR shift gate | Geometric return | Median DD | Trades |
|---:|---:|---:|---:|
| 15% | +263.89% | 39.12% | 266 |
| 20% | +257.54% | 40.18% | 264 |
| 30% | +258.45% | 40.56% | 263 |
| 40% | +260.56% | 40.51% | 262 |
| 50% | +268.23% | 38.18% | 267 |

Observation: the response is not monotonic; 50% was materially better in this sample, while the upstream-like 30% threshold was slightly below baseline return. Treat 50% as a hypothesis, not an optimized constant.

## Candidate 4 — exposure governor

Mechanism: before a new MID entry, block the order if deployed cost basis plus the new slot would exceed a fraction of current marked equity. This is the spot-safe analogue of wallet exposure governance.

| Cap | Geometric return | Median DD | Trades |
|---:|---:|---:|---:|
| 50% | +137.88% | 27.56% | 237 |
| 60% | +149.32% | 31.06% | 244 |
| 70% | +174.98% | 32.51% | 252 |
| 80% | +198.85% | 33.40% | 257 |
| 90% | +216.41% | 38.34% | 260 |

Observation: the governor does what a safety mechanism should do — materially reduces drawdown — but at a large historical return cost. Keep it as a future risk-control tool, not as a return enhancement.

## Combined candidate — ATR gate + trailing exit

ATR gate fixed at 50%; exit-retracement sensitivity:

| Exit retracement | Geometric return | Median DD | Trades |
|---:|---:|---:|---:|
| 5% | +269.85% | 38.44% | 263 |
| 6% | +277.65% | 38.56% | 258 |
| 7% | +278.03% | 38.45% | 256 |
| 8% | +274.04% | 38.31% | 253 |
| 9% | +271.21% | 38.34% | 251 |

The 6–8% neighborhood remains above the current MID baseline. The 7% point records **+278.03%** geometric return with **38.45%** median DD and **256** exits. This is the strongest result in this harvest, but it is still development-sample evidence only.

### Per-asset: baseline vs combined candidate

| Asset | MID baseline | Combo ATR50 + exit7 | Baseline DD | Combo DD |
|---|---:|---:|---:|---:|
| BTC | +87.53% | +98.14% | 18.11% | 19.21% |
| ETH | +136.73% | +159.44% | 40.60% | 38.45% |
| BNB | +131.25% | +169.77% | 27.09% | 20.28% |
| LINK | +538.78% | +501.43% | 41.85% | 43.75% |
| SOL | +822.17% | +825.64% | 52.73% | 51.69% |

## External strategy reference — jordantete dynamic grid port

The public repository default/example dynamic-grid settings were ported to the same daily frozen datasets: 8 arithmetic levels, 50% initial base allocation, ATR14 spacing, 0.5×ATR spacing multiplier, 30% ATR regrid threshold, 60-bar cooldown.

Important limitation: **this is not execution of the upstream package**. Its live order manager, exchange reconciliation, and intrabar sequencing are replaced by a deterministic daily-OHLC cell model so the comparison can use exactly the same frozen datasets. Therefore this result measures the strategy concept under our evidence contract, not the upstream bot product.

Aggregate result: **+45.26% geometric return / 39.98% median DD / 2,028 transitions**. It is far below MID baseline return and dramatically more active.

Per asset:

| Asset | Return | Max DD | Transitions |
|---|---:|---:|---:|
| BTC | +22.82% | 35.24% | 270 |
| ETH | -13.19% | 55.56% | 365 |
| BNB | +55.24% | 33.05% | 491 |
| LINK | +95.07% | 53.45% | 453 |
| SOL | +100.32% | 39.98% | 449 |

## Passivbot direct-strategy limitation

Passivbot v8 default `trailing_martingale` is not directly benchmarked as if it were apples-to-apples. Its maintained default profile uses 1-minute and 1-hour volatility/EMA inputs, futures-style wallet exposure, multi-position logic, and unstuck behavior. The frozen canonical artifacts here are 1D OHLCV. Running a simplified daily substitute and calling it “Passivbot performance” would be misleading.

What *is* tested here are isolated Passivbot mechanisms that can be represented honestly on daily bars: retracement entry/exit and exposure governance.

## Decision

- **Do not change canonical MID or paper-live yet.**
- Reject trailing-entry confirmation as a shared candidate on this evidence.
- Keep exposure governor as a risk-control research tool only.
- Keep ATR-gated H/L refresh as a research candidate; 50% needs clean validation.
- Keep trailing-exit 6–8% as a research candidate.
- Promote `ATR gate 50% + trailing exit 6–8%` only to **NEXT_VALIDATION_CANDIDATE**, not to strategy status.
- External dynamic-grid port does not justify replacing the current MID design.

## Next validation gate

Before any promotion, freeze one candidate *without further tuning* and test it on data/assets not used to select these parameters. Forward paper observation is preferred because the five canonical assets were used for discovery.

## APP test report

- RISK_BASED_TEST_CLASS: **L1 (research-only additive files)**
- IMPACT_MAP_PRESENT: **yes**
- DIRECTLY_AFFECTED: research harness + evidence only
- TRANSITIVE_DEPENDENCIES: none in production path
- UNAFFECTED: canonical `strategy.py`, paper-live workflow, Telegram, live execution
- SCALE_BASELINE_STATUS: **INHERITED / NOT AFFECTED**
- SCALE_CRITICAL_TRIGGER: **no**
- REFERENCE_RETEST_REQUIRED: **yes — frozen canonical benchmark used**
- EXTENDED_STRESS_RETEST_REQUIRED: **no**
- SANDBOX_RUNTIME_TESTED: **yes**
- PRODUCTION_OBSERVATION_REQUIRED: **yes before promotion, not for research merge**
- BACKUP_REQUIRED: **no**
- ROLLBACK_READY: **yes — additive research files only**
- DATA_COMPATIBILITY_CHECK: **not_applicable**

## Test level

`TEST_LEVEL: LOCAL_RUNTIME_FROZEN_CANONICAL_ARTIFACTS + DETERMINISM/CAUSALITY_UNIT_TESTS`

No result in this document authorizes live-money execution.
