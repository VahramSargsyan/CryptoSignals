# U8 Risk, Transition Ledger & Entry-Date Stress — Strategy Lab Evidence

Date: 2026-09-27  
Family: RELATIVE_ROTATION_GRAPH_8_V1 / U8  
Status: BACKTESTED / PAPER_LIVE_MONITOR  
Scope: historical risk interpretation and entry-date stress  
Live trading logic changed: NO

## Why this record exists

Earlier U8 reporting exposed a large historical max drawdown near `-71.23%`, but that number was easy to misread as a loss of 71.23% of the original capital.

The follow-up research separated three different things:

1. transition costs on every rotation;
2. peak-to-trough drawdown from accumulated equity;
3. loss versus a fresh investor's original capital at different entry dates.

These must remain separate in future U8 reporting.

## Frozen U8 mechanics

Universe:

- ATOM
- TWT
- PEPE
- BNB
- SOL
- TRX
- AAVE
- LINK

Parameters:

- Binance Spot 1D closed candles
- rolling median: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost: 0.1% per executed transition

## Study A — Transition Ledger

Technical evidence:

- [U8 Transition Ledger prereg](../../research/relative_rotation/2026-09-27_U8_TRANSITION_LEDGER_V1_PREREG.md)
- [U8 Transition Ledger evidence](../../research/relative_rotation/2026-09-27_U8_TRANSITION_LEDGER_V1_EVIDENCE.md)
- runner: `scripts/research_u8_transition_ledger_v1.py`
- workflow: `.github/workflows/u8-transition-ledger-v1.yml`
- successful live-data validation run: `36322314111`

Mature evaluation:

- 2023-10-31 -> 2026-09-26
- 1062 days / about 34.9 months
- initial normalized capital: 10,000 USDT
- executed transitions: 21
- final equity after modeled costs: 95,491.91 USDT
- return from first-open capital: +854.92%

### Transition cost conclusion

The modeled 0.1% cost is charged at **every executed rotation**, not once.

Across 21 transitions:

- zero-cost shadow terminal equity: 97,519.47 USDT
- cost-adjusted terminal equity: 95,491.91 USDT
- terminal fee drag: 2,027.56 USDT
- multiplicative drag: 2.0791%

Accounting identity:

`actual / zero_cost = (1 - 0.001)^21 = 0.979208675965`

## Study B — Drawdown interpretation

For the long ATOM-start continuation path:

- peak: 141,044.55 USDT on 2025-09-20
- trough: 40,576.26 USDT on 2026-06-06
- peak-to-trough drawdown: -71.23%

That `-71.23%` is **not** a loss of 71.23% of the original 10,000 USDT.

At the trough:

- equity: 40,576.26 USDT
- versus initial 10,000 USDT: +305.76%

The lowest equity versus original capital occurred at the beginning of the mature test:

- minimum: 9,570.48 USDT
- date: 2023-11-03
- loss versus initial capital: -4.30%
- daily closes below 10,000 USDT: 5

After recovering above the initial 10,000 USDT, that long-start path never again closed below the original capital through 2026-09-26.

## Study C — Fresh-entry stress around the major drawdown

Technical evidence:

- [U8 Drawdown Entry Stress prereg](../../research/relative_rotation/2026-09-27_U8_DRAWDOWN_ENTRY_STRESS_V1_PREREG.md)
- [U8 Drawdown Entry Stress evidence](../../research/relative_rotation/2026-09-27_U8_DRAWDOWN_ENTRY_STRESS_V1_EVIDENCE.md)
- runner: `scripts/research_u8_drawdown_entry_stress_v1.py`
- workflow: `.github/workflows/u8-drawdown-entry-stress-v1.yml`
- successful live-data validation run: `36324114083`

Each scenario starts a fresh 10,000 USDT ATOM position while retaining the historical U8 warm-up/state before the chosen capital start date.

| Fresh entry | Minimum equity | Worst loss vs initial | Final equity 2026-09-26 | Final return |
|---|---:|---:|---:|---:|
| 2024-09-20 — one year before the peak | 8,690.93 | -13.09% | 40,163.70 | +301.64% |
| 2025-01-01 — start of 2025 | 6,854.37 | -31.46% | 22,779.50 | +127.80% |
| 2025-09-20 — at historical peak date | 3,333.54 | -66.66% | 7,845.13 | -21.55% |
| 2026-01-01 — already inside decline | 4,775.17 | -52.25% | 11,237.86 | +12.38% |

### Main risk conclusion

The apparent protection of original capital in the long 2023 start is strongly path-dependent.

An early investor had time to accumulate a large profit cushion before the -71.23% drawdown.

A fresh investor starting near the 2025 peak had no such cushion and historically experienced a loss of up to 66.66% of original capital.

Therefore future U8 risk reporting must not rely on one long-history max-drawdown number alone.

## Required U8 risk fields going forward

Every serious U8 risk report should show separately:

1. **MAX DRAWDOWN FROM PRIOR PEAK**
2. **MINIMUM EQUITY VS INITIAL CAPITAL**
3. **DAYS BELOW INITIAL CAPITAL**
4. **ENTRY DATE / START-ASSET ASSUMPTION**
5. **RECOVERY DATE / TIME TO RECOVERY**
6. **TRANSITION COST ASSUMPTION**
7. **FINAL RETURN FROM THE SAME STARTING-CAPITAL BASE**

## Recommended next research

Run a rolling-entry stress matrix across the mature history:

- fresh 10,000 USDT start every month;
- same frozen U8 mechanics;
- record minimum equity versus initial capital;
- max peak-to-trough drawdown;
- days below initial;
- recovery time;
- terminal return.

This would turn four hand-picked entry dates into a broader entry-date distribution.

## Evidence quality

Transition Ledger:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST`

Drawdown Entry Stress:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST`

This Strategy Lab card is a documentation/index layer over those validated studies.

## Residual risks

- historical performance does not establish future performance;
- fresh-entry scenarios above all start in ATOM;
- the modeled 0.1% transition cost does not separately model additional spread/slippage;
- the current entry stress set samples four dates, not all possible entry dates.
