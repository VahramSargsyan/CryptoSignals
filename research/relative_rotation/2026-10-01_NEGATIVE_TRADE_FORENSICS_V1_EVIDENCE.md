# NEGATIVE TRADE FORENSICS V1 — EVIDENCE

Date: 2026-10-01  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Research question

Can the available Relative Rotation research layers — including previously rejected or non-production indicators — jointly distinguish losing U10+DDG trades from winning trades better than any individual indicator?

This study is a forensic / hypothesis-generation pass. It is not production approval.

## Runtime identity

### Forensic ledger run
- Branch: `research-run/negative-trade-forensics-v1`
- GitHub Actions run: `36813374856`
- Source commit: `d6ce932eac0a354d24568a9f6fc1f55882936094`
- Artifact ID: `11140378074`
- Artifact SHA256: `073addcc796a3e13215652c574189d93df9b0006ed23f3299e25cdb79508e5e6`

### Full-strategy composite-gate counterfactual
- GitHub Actions run: `36813630389`
- Source commit: `6dbc453c7c328e6bd69520cf42d709ddc0e1c583`
- Artifact ID: `11140568192`
- Artifact SHA256: `46801d6c983b23941894aaa662369ad36fa0e665420eb67454611d61b235784d`

TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen strategy

Current U10:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Core:
- rolling pair median: 180d
- ARM: 15%
- reversal: 3%
- DDG destination-dominance override: 1.50x
- signal close T -> next daily open
- 0.1% transition cost

MATURE:
- 2023-10-31 through 2026-09-26

Baseline validation:
- median MATURE return: **+3588.4%**

## Trade definition

A completed trade is the destination asset held from the transition execution open until the next transition execution open.

For forensic classification:

`net round-trip trade return = destination exit-open / entry-open * (1-cost)^2 - 1`

Identical realized episodes repeated after start-state convergence are deduplicated. The number of originating starts is retained separately.

## Coverage

- unique completed trade episodes: **30**
- positive episodes: **23**
- negative episodes: **7**
- baseline unique-trade loss rate: **23.3%**

## Every negative trade

| Signal | Route | Exit execution | Hold | Net trade | Signal strength | Primary reversal | BTC | Broad U10 | TOTAL | Warning domains |
|---|---|---|---:|---:|---:|---:|---|---|---|---:|
| 2026-01-05 | PEPE -> AVAX | 2026-02-13 | 38d | **-38.6%** | 30.5% | 4.0% | BTC_BEAR | BROAD_BULL | MIXED | 4 |
| 2024-07-06 | ALGO -> FIL | 2024-11-06 | 122d | **-14.9%** | 18.3% | 5.6% | BTC_TRANSITION | BROAD_BEAR | MIXED | 3 |
| 2024-01-14 | TRX -> XRP | 2024-06-20 | 157d | **-14.6%** | 16.8% | 3.0% | BTC_BULL | BROAD_BEAR | FULL_BULL | 3 |
| 2024-12-07 | TWT -> BNB | 2025-02-02 | 56d | **-13.1%** | 19.0% | 6.4% | BTC_BULL | BROAD_BULL | FULL_BULL | 3 |
| 2025-02-01 | BNB -> TWT | 2025-02-10 | 8d | **-10.4%** | 15.5% | 3.1% | BTC_BULL | BROAD_BEAR | MIXED | 4 |
| 2026-02-12 | AVAX -> TWT | 2026-05-19 | 95d | **-8.1%** | 20.0% | 7.9% | BTC_BEAR | BROAD_BEAR | BEAR_BUILDING | 4 |
| 2024-06-19 | XRP -> ALGO | 2024-07-07 | 17d | **-0.3%** | 30.3% | 3.1% | BTC_BULL | BROAD_BEAR | BEAR_BUILDING | 3 |

## Warning domains

Four independent research families were collapsed into broad domains.

### 1. RR confirmation caution
Examples:
- primary reversal below 5%;
- effective signal below 20/25%.

### 2. Network / destination caution
Examples:
- destination underperforms source over 30/60/90d;
- destination rank worse than source across horizons;
- bottom-half destination momentum;
- previously tested support/momentum HOLD features.

### 3. Market caution
Examples:
- BTC below SMA200;
- BTC bear state;
- broad U10 bear breadth;
- defensive 200d breadth condition.

### 4. TOTAL / ratio caution
Examples:
- TOTAL not bullish + ETH/BTC below SMA100;
- TOTAL bearish + persistent BTC/ETH above SMA200;
- persistent BTC/ETH long-term BTC-strength warning.

## Core forensic result

| Warning domains active | Trades | Losses | Loss rate | Median trade |
|---:|---:|---:|---:|---:|
| 0 | 1 | 0 | 0.0% | +16.6% |
| 1 | 2 | 0 | 0.0% | +38.5% |
| 2 | 8 | 0 | **0.0%** | +30.7% |
| 3 | 11 | 4 | 36.4% | +5.1% |
| 4 | 8 | 3 | 37.5% | +4.9% |

Historical observation:

**All 7 losing episodes occurred with 3 or 4 warning domains active. No losing episode occurred with only 0-2 warning domains.**

But this is not sufficient for a veto:
- 19 trades had 3-4 warning domains;
- only 7 were losses;
- 12 were still winners.

Therefore the combined score has high historical recall but insufficient precision for automatic rejection.

## Individual indicators

Strongest descriptive single warnings:

| Indicator | Support | Losses | Loss rate | Loss recall |
|---|---:|---:|---:|---:|
| defensive low breadth | 8 | 4 | 50.0% | 57.1% |
| BTC below SMA200 | 6 | 3 | 50.0% | 42.9% |
| BTC_BEAR | 4 | 2 | 50.0% | 28.6% |
| weak signal + destination worse 30/60 | 11 | 5 | 45.5% | 71.4% |
| BROAD_BEAR | 11 | 5 | 45.5% | 71.4% |
| TOTAL not bullish | 11 | 5 | 45.5% | 71.4% |
| high-attention TOTAL/ETHBTC pattern | 7 | 3 | 42.9% | 42.9% |
| signal <20% | 12 | 5 | 41.7% | 71.4% |

No single indicator reliably isolates bad trades.

### Important failed momentum result

`DEST_WORSE_SOURCE_30_60_90` and `DEST_RANK_WORSE_SOURCE_ALL` were true for all 7 losses.

However they were true in **27 of 30 total trades**.

This describes the contrarian nature of RR more than it identifies failure. It is not a useful standalone veto.

## Delayed confirmation tools

Local route-survival check within five days:

| Variant | Losing trades changed/blocked | Winning trades changed/blocked |
|---|---:|---:|
| reversal 4% | 0/7 | 1/23 |
| reversal 5% | 2/7 | 3/23 |
| 2 confirmation closes | 0/7 | 6/23 |
| 3 confirmation closes | 1/7 | 11/23 |

Thus:
- 4% does not catch any historical losing episode;
- 5% catches only 2/7 locally;
- 2-close persistence catches none;
- 3-close persistence destroys many more winning routes than losing routes.

This is consistent with prior system-wide follow-through research.

## Case notes

### 2026-01-05 PEPE -> AVAX: -38.6%

Strong signal, but almost every independent risk family disagreed:
- destination worse than source over 30/60/90;
- BTC_BEAR and BTC below SMA200;
- defensive low breadth active;
- TOTAL not bullish;
- ETH/BTC below SMA100;
- high-attention risk pattern;
- reversal only ~4.0%.

This is the cleanest multi-layer historical warning case.

### 2024-07-06 ALGO -> FIL: -14.9%

- weak 18.3% signal;
- FIL bottom-half / worse than ALGO over 30/60/90;
- BROAD_BEAR;
- BTC below SMA200 / transition regime;
- defensive low breadth;
- TOTAL not bullish.

This is a weak-RR + weak-network + weak-market case.

### 2024-01-14 TRX -> XRP: -14.6%

Known harmful fork:
- weak 16.8% signal;
- minimum 3.01% reversal;
- XRP worse than TRX over 30/60/90;
- bottom-half destination;
- BROAD_BEAR.

But:
- BTC was BULL;
- TOTAL was FULL_BULL;
- no TOTAL/ratio risk warning.

This demonstrates why macro-only filters cannot solve the XRP fork.

### 2024-12-07 TWT -> BNB: -13.1%

- sub-20% signal;
- BNB worse than TWT over 30/60/90;
- bottom-half destination;
- BTC and broad U10 were bullish;
- TOTAL was FULL_BULL;
- persistent BTC/ETH above SMA200 supplied the main independent rotation-risk warning.

This demonstrates why broad-market weakness is not required for a losing RR trade.

### 2025-02-01 BNB -> TWT: -10.4%

Dense warning cluster:
- 15.5% signal;
- ~3.1% reversal;
- destination worse than source across horizons;
- BROAD_BEAR;
- TOTAL not bullish;
- persistent BTC/ETH long warning;
- ETH/BTC below SMA100;
- high-attention risk pattern.

### 2026-02-12 AVAX -> TWT: -8.1%

The strongest macro-risk configuration:
- destination weak across 30/60/90;
- BTC_BEAR and below SMA200;
- BROAD_BEAR;
- 200d breadth defensive warning;
- TOTAL BEAR_BUILDING;
- BTC/ETH long warning;
- ETH/BTC below SMA100;
- combined risk-200 active.

The reversal itself was strong (~7.9%), showing that strong reversal confirmation cannot override a deeply adverse environment.

### 2024-06-19 XRP -> ALGO: -0.3%

Near-flat loss:
- strong 30.3% dislocation;
- weak ~3.1% reversal;
- destination worse over 30/60/90;
- BROAD_BEAR;
- defensive breadth warning;
- TOTAL BEAR_BUILDING.

This would be a questionable candidate for aggressive filtering because the realized loss was economically small.

## Exploratory combined patterns

The strongest static pair/triple observations reached 66-75% loss rates, including:
- signal <20% + TOTAL not bullish;
- signal <20% + BROAD_BEAR;
- signal <20% + destination weakness + TOTAL not bullish;
- low 200d breadth + TOTAL not bullish.

These are **post-selection historical discoveries**.

Validation support is very small, often one or two events. They are not production rules.

## Full-strategy counterfactual

The candidate warning rules were then used as actual HOLD gates, allowing them to change the future path.

| Policy | Discovery | Validation 1Y | 2Y | MATURE | MATURE DD |
|---|---:|---:|---:|---:|---:|
| BASE_DDG | +1159.3% | +188.7% | +2102.3% | **+3588.4%** | -62.4% |
| SCORE >=3 HOLD | +292.5% | **-12.1%** | +345.7% | +264.6% | **-78.6%** |
| SCORE >=4 HOLD | +612.3% | +161.9% | +2057.8% | +1792.6% | **-51.6%** |
| RR+NETWORK+MARKET HOLD | +852.2% | +168.6% | **+2369.0%** | +2795.0% | **-52.6%** |
| signal<20 + TOTAL not bull | +783.9% | **+198.0%** | +1549.5% | +2572.2% | -61.2% |
| signal<20 + BROAD_BEAR | +955.6% | **+198.0%** | +1466.1% | **+3091.5%** | -61.2% |
| low breadth + TOTAL not bull | +1051.2% | +44.8% | +1024.3% | +1590.9% | -76.9% |

## Main decision

### What works

The combination of independent indicators contains useful historical **risk information**.

Especially:
- losing trades cluster only when several independent warning families are active;
- risk layers can reduce drawdown in some full-strategy counterfactuals;
- some combined gates remain profitable in validation.

### What does not work

Using the composite score as a hard automatic veto does not preserve the original edge.

The harshest example:
- `SCORE >=3 HOLD` catches the historical loss cluster descriptively;
- but as a strategy rule it collapses MATURE return from +3588% to +265% and makes DD worse at -78.6%.

This is a direct demonstration of path dependence:
blocking a historically bad-looking trade also changes every later route.

### Best current interpretation

`MULTI_INDICATOR_WARNING_SCORE = USEFUL_FOR_RISK_ATTENTION, NOT_PROVEN_AS_AUTO_VETO`

The strongest next use is:
- manual/high-attention review;
- forward shadow logging;
- possibly a future position-sizing overlay rather than binary HOLD.

A sizing overlay requires a separate preregistered test.

## Fundamental layer

Point-in-time historical fundamentals were not available in the repository.

They were intentionally excluded rather than backfilled with future knowledge.

Status:

`FUNDAMENTAL_POINT_IN_TIME = NOT_AVAILABLE_YET`

The 7 losing episodes are now a ready-made benchmark set for a future fundamental layer.

## No-repeat rule

Do not rerun this exact forensic study on the same history merely to rediscover the same patterns.

A repeat requires:
1. new unseen trades;
2. corrected strategy semantics;
3. a newly available point-in-time fundamental dataset;
4. a preregistered warning-score sizing rule;
5. another explicitly distinct causal hypothesis.

## Production boundary

- no production routing changes;
- no universe change;
- no automatic veto added;
- no Telegram/exchange behavior changed;
- no sizing changes authorized.

