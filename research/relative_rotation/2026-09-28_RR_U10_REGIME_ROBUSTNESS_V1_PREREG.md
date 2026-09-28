# RR U10 REGIME ROBUSTNESS V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

Does the frozen single-book relative-rotation U10 depend mainly on bull markets, or does it also produce positive historical returns during bear / sideways regimes?

This is a regime-robustness test of the strategy itself. It does not test MAX1/MAX2 multibook overlays.

## Frozen strategy

Universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Mechanics:
- Binance Spot D1
- rolling median lookback: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- confirmed at close T -> next available daily open
- strongest max-dislocation routing
- 0.1% transition cost
- no look-ahead
- evaluate all 10 possible starting assets

Primary analysis window:

`MATURE = 2023-10-31 -> 2026-09-26`

The 1Y/2Y windows remain available as context, but regime attribution is centered on MATURE.

## Regime definitions

Reuse the repository H8 definitions; do not optimize thresholds.

### BTC trend

Trailing daily SMA:
- fast = 50
- slow = 200

Labels:
- `BTC_BULL`: BTC close > SMA200 and SMA50 > SMA200
- `BTC_BEAR`: BTC close < SMA200 and SMA50 < SMA200
- `BTC_TRANSITION`: enough history exists but the bull/bear conditions disagree
- `UNKNOWN`: insufficient trailing history

BTC regime is calculated causally from data available at that close.

### Broad U10 trend

For each U10 asset:

`close > trailing 50D SMA`

Breadth = fraction of U10 assets above their 50D SMA.

Labels:
- `BROAD_BULL`: breadth >= 0.60
- `BROAD_BEAR`: breadth <= 0.40
- `BROAD_SIDEWAYS`: otherwise
- `UNKNOWN`: insufficient history

At least 5 eligible assets are required.

This is the same H8 breadth framing, applied to the frozen U10 rather than inventing a new market index.

## Regime attribution

For each of the 10 start-asset equity curves:

1. calculate daily strategy equity and daily return;
2. attach BTC trend and broad-U10 trend to each day;
3. for each regime:
   - count days;
   - compound only the strategy returns earned on days carrying that regime label;
   - average / median daily return;
   - positive-day rate;
   - regime-conditioned drawdown (other regime days treated as flat);
4. identify contiguous regime episodes and report:
   - episode count;
   - median episode return;
   - worst episode return;
   - best episode return;
   - positive-episode rate;
   - longest episode length;
5. summarize across all 10 possible start assets:
   - median;
   - worst start;
   - best start.

## Calendar robustness

Also report regime-conditioned strategy returns by calendar year for 2024, 2025 and 2026-to-cutoff where regime data exists.

This is diagnostic only; years are not used to select a preferred rule.

## Interpretation

Possible outcomes:

- `BULL_DEPENDENT`: bear-regime conditioned returns are consistently negative / fragile.
- `REGIME_ROBUST_HISTORICALLY`: both bull and bear regime evidence are positive across most/all starting states.
- `MIXED_REGIME`: bear performance depends materially on which regime definition, episode, or starting state is used.

These are descriptive research labels, not production promotion.

"All-season" here means regime-robust across bull/bear/sideways market conditions, not calendar month seasonality.

## Persistence / no-repeat

After a successful run, persist:
- exact preregistration;
- source SHA;
- run ID;
- artifact ID/hash;
- regime coverage;
- aggregate and episode results;
- decision/no-repeat log.

Do not rerun unchanged on the same historical data without materially new information.

## Production boundary

No paper/live, Telegram, exchange, universe, or portfolio-allocation changes.

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
