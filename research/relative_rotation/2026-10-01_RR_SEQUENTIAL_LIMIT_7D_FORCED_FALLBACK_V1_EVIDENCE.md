# RR SEQUENTIAL LIMIT 7D FORCED FALLBACK V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research question

What happens to the full path-dependent Relative Rotation strategy if the exact-3% sequential execution idea is given a fixed one-week timeout and then forced to complete at market?

Frozen execution policy:

1. RR confirms a route.
2. For a direct confirmed route, try to recapture the exact 3% reversal:
   - source SELL limit first;
   - destination BUY limit only after SELL fill.
3. Maximum pending time: 168h.
4. If source SELL never fills by 168h:
   - sell source at market proxy;
   - immediately buy destination at market proxy.
5. If source SELL fills but destination BUY does not:
   - force destination BUY at market proxy at 168h.
6. RR signals arriving while capital is still transferring are ignored.
7. DDG / non-applicable routes execute immediately at next open because a canonical exact-3% effective-destination target is not defined.
8. Fee model for execution policy:
   - 0.1% source SELL;
   - 0.1% destination BUY.

## Runtime identity

- Branch: `research-run/sequential-limit-recapture-v1`
- GitHub Actions run: `36853453789`
- Source commit: `d42dc1aec1342fa3fa37fbbc37190f614cd8cbaf`
- Artifact ID: `11156592789`
- Artifact SHA256: `7cf5a68aa6dd2049260e616048763b1cefc285fbe14d0a8361962f24cd690292`

TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Controls

Three full path-dependent policies were compared:

1. `CANONICAL_NEXT_OPEN_ONE_FEE`
   - current research convention;
   - next-open execution;
   - one 0.1% transition cost.

2. `NEXT_OPEN_TWO_FEE_CONTROL`
   - same RR route and next-open timing;
   - 0.1% sell + 0.1% buy.

3. `LIMIT_7D_FALLBACK_TWO_FEE`
   - exact-3% sequential limits;
   - force market completion at 168h;
   - signals while pending are ignored;
   - two fees.

This separates fee-model impact from execution-delay/path impact.

## Full strategy results

| Window | Policy | Median return | Worst start | Median DD | Worst DD |
|---|---|---:|---:|---:|---:|
| Validation 1Y | Canonical one-fee | +188.7% | +37.4% | -62.4% | -73.6% |
| Validation 1Y | Next-open two-fee | +185.8% | +36.1% | -62.5% | -73.7% |
| Validation 1Y | **7d limit + fallback** | **+42.2%** | **-16.4%** | **-71.6%** | **-76.9%** |
| Last 2Y | Canonical one-fee | +2102.3% | +1850.2% | -62.4% | -62.4% |
| Last 2Y | Next-open two-fee | +2061.9% | +1815.4% | -62.5% | -62.5% |
| Last 2Y | **7d limit + fallback** | **+1024.5%** | **+695.1%** | **-71.6%** | **-71.6%** |
| MATURE | Canonical one-fee | +3588.4% | +2792.4% | -62.4% | -62.4% |
| MATURE | Next-open two-fee | +3502.7% | +2729.4% | -62.5% | -62.5% |
| MATURE | **7d limit + fallback** | **+1538.5%** | **+1127.2%** | **-71.6%** | **-71.6%** |

## Capital impact

Relative to the more realistic same two-fee next-open control:

- Validation 1Y: **0.523x**
- Last 2Y: **0.560x**
- MATURE: **0.448x**

Relative to canonical one-fee:

- Validation 1Y: **0.518x**
- Last 2Y: **0.550x**
- MATURE: **0.437x**

Thus the fixed 7-day wait destroys roughly half of final capital versus immediate path execution.

## Fee effect is small

MATURE:

- canonical one-fee: +3588.4%
- next-open two-fee: +3502.7%

So changing from one fee to two fees reduces the final factor only modestly.

The large loss of performance comes from delayed execution and changed path, not fee accounting.

## MATURE execution behavior

Unique direct execution attempts:
- 29

Outcomes:
- full exact-3% limit recapture: **21 / 29 = 72.4%**
- source SELL timeout -> force both at market: **5**
- source SELL filled but destination BUY timed out -> force BUY at market: **3**

Median completion time across direct attempts:
- **39h**

Even though most direct attempts complete with the favorable limits, the remaining long pending periods are enough to alter the RR path materially.

## Skipped RR opportunity

Metric correction: the original variable named `skipped_signal_days` counts **daily bars spent with a transfer pending**, not the literal number of RR signals that appeared.

Median **pending days**:

- Validation 1Y: **30.0**
- Last 2Y: **52.5**
- MATURE: **61.0**

This naming correction does not change return, drawdown, execution, or path results. A later trailing study adds a separate `RR_OPPORTUNITIES_WHILE_PENDING` metric.

Median completed policy rotations:

- Validation 1Y: **9.5**
- Last 2Y: **18.0**
- MATURE: **23.5**

The key failure mechanism is therefore not:

`LIMIT TARGET BAD`

but:

`WAITING FOR TARGET CHANGES THE STATE MACHINE`

## Drawdown

The 7-day policy does not compensate its lower return with materially lower risk.

MATURE median DD:

- canonical: **-62.4%**
- two-fee next-open: **-62.5%**
- 7-day limit/fallback: **-71.6%**

Therefore the fixed wait is inferior on both return and drawdown in this historical test.

## Interpretation

The previous feasibility study showed:

- exact 3% recapture is economically attractive when filled;
- 7-day direct-path fill rate is roughly 71-72%.

This full-path test shows why that is not enough.

A 72% favorable fill rate does **not** translate into a better strategy because the opportunity cost of waiting is larger than the captured execution improvement.

The strategy edge remains strongly path-dependent.

Frozen interpretation:

`FIXED_7D_EXACT3_WAIT_THEN_MARKET = REJECTED`

The useful part of the original execution idea is not the fixed wait itself, but the possibility of capturing part of the 3% **without materially delaying the RR state transition**.

## Next distinct hypothesis

The user proposed a materially different architecture:

`PROGRESSIVELY_TIGHTENING_LIMIT / TRAILING EXECUTION`

Conceptually:
- start from a favorable limit;
- use recent candle extrema to move the order progressively toward current market;
- preserve SELL-first -> BUY-second order;
- ensure deterministic completion by a hard deadline;
- optimize the tradeoff between:
  - average execution improvement;
  - waiting time;
  - skipped RR signals;
  - full strategy return.

This is not tested in V1.

It should be preregistered separately, because tuning the tightening schedule on the same outcomes can easily overfit.

## Production boundary

No production/live/paper/Telegram/exchange order behavior changed.

## Residual risks

- Hourly wick-touch remains an optimistic full-fill proxy.
- Fallback market execution uses hourly open at the 168h deadline as a proxy.
- Slippage, queue priority, partial fills, spread, and exchange outages are not modeled.
- DDG exact-3% execution is not included; those routes remain next-open.
