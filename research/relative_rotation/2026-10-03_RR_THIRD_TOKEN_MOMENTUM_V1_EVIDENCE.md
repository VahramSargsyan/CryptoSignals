# RR THIRD-TOKEN MOMENTUM V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-third-token-momentum-v1`
- Draft PR: #112
- GitHub Actions run: `37136796433`
- Job: `111242856661`
- Artifact ID: `11278203701`
- Artifact: `rr-third-token-momentum-v1-2`
- Artifact SHA256: `cef429ebbf0a16c2b139c1a2ee428eb1164b2e38e91dcea8e2a19107b6bdbd51`
- Fixed as-of: `2026-10-03T15:55:00Z`
- Latest common closed D1 candle: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`

## Research question

When canonical Relative Rotation produces a primary CONFIRMED route `SOURCE -> B`,
does a different TARGET token `C` that already has stronger short-horizon
momentum at the signal close systematically outperform B afterwards?

Primary causal alternative:
- strongest 14-day momentum TARGET token excluding SOURCE and B;
- candidate selected using only data known at signal close;
- comparison begins at the next daily open.

Robustness alternative:
- strongest 7-day momentum candidate.

## Dataset

Primary confirmed route-opportunities:
- **3426**

The sample is route-opportunity evidence, not 3426 independent portfolio trades.
Multiple sources/dates share market regimes and prices.

## Primary M14 result

Among events where the strongest third token already had higher 14-day momentum
than the baseline RR destination:

| Horizon | N with forward data | Candidate beats baseline | Median relative excess | Mean relative excess |
|---:|---:|---:|---:|---:|
| 3d | 3136 | 50.1% | +0.00% | +0.90% |
| 7d | 3122 | 49.7% | -0.12% | +1.53% |
| 14d | 3090 | 50.3% | +0.15% | +2.95% |
| 30d | 3037 | 49.2% | -0.37% | +4.76% |

The median result is effectively flat at every horizon.

The positive means with near-zero/negative medians indicate a right-skewed
distribution: a minority of large momentum continuations can create attractive
individual examples without producing a reliable typical edge.

## Stronger M14 threshold

Even when the third token's 14-day momentum exceeded the baseline destination
by at least **10 percentage points**:

| Horizon | N | Candidate beats baseline | Median relative excess |
|---:|---:|---:|---:|
| 3d | 2413 | 49.2% | -0.13% |
| 7d | 2403 | 48.1% | -0.41% |
| 14d | 2371 | 50.7% | +0.29% |
| 30d | 2330 | 48.9% | -0.52% |

A larger momentum gap did not create a stable continuation advantage.

## M7 robustness

The 7-day momentum alternative also failed to show a robust typical advantage.

For events where the strongest third token had higher M7 than baseline:

| Horizon | N | Candidate beats baseline | Median relative excess |
|---:|---:|---:|---:|
| 3d | 3011 | 48.2% | -0.33% |
| 7d | 3001 | 45.7% | -1.18% |
| 14d | 2970 | 51.3% | +0.38% |
| 30d | 2925 | 47.7% | -1.15% |

Classification:

`NAIVE_THIRD_TOKEN_MOMENTUM_OVERRIDE = NOT_SUPPORTED`

## LINK -> ALGO live-case reconstruction

Signal close:
`2026-09-28T00:00:00Z`

Canonical primary signal:
- route: `LINK -> ALGO`
- pair: `ALGO/LINK`
- max dislocation: **34.01%**
- reversal from extreme: **5.06%**
- required reversal: 3%

### What was knowable at the signal close

| Asset | M7 | M14 | M7 rank | M14 rank | LINK pair state |
|---|---:|---:|---:|---:|---|
| HBAR | +30.73% | +57.06% | 1 | 1 | ARMED_FORWARD |
| ALGO | +20.89% | +41.52% | 2 | 2 | CONFIRMED_FORWARD |
| AVAX | -5.48% | +40.37% | 9 | 3 | ARMED_FORWARD |
| PEPE | -11.74% | +21.68% | 10 | 4 | ARMED_FORWARD |
| AAVE | +2.45% | +16.55% | 4 | 5 | NONE |
| FIL | +7.86% | +13.65% | 3 | 6 | ARMED_FORWARD |
| TWT | -0.29% | +7.77% | 5 | 7 | ARMED_FORWARD |
| BNB | -4.43% | +6.04% | 8 | 8 | ARMED_FORWARD |
| XRP | -2.57% | +5.18% | 6 | 9 | ARMED_FORWARD |
| TRX | -2.64% | -0.74% | 7 | 10 | ARMED_FORWARD |

AAVE was **not** the strongest momentum alternative at the signal close:
- M14 rank: **5 / 10**
- M7 rank: **4 / 10**
- ALGO itself was rank **2 / 10** on both M7 and M14.

Thus the later AAVE strength was not an obvious causal momentum choice on
2026-09-28.

### Destination Dominance audit for AAVE

At the 2026-09-28 close:

`LINK -> AAVE`
- state: **NONE**
- directional deviation magnitude: **9.65%**
- below the 15% ARM threshold.

`ALGO -> AAVE`
- state: **NONE**
- directional deviation magnitude: **8.04%**

Accepted DDG 1.5x rule would have required LINK -> AAVE max dislocation of at
least:

**51.01%**

because baseline LINK -> ALGO max dislocation was 34.01%.

Actual LINK/AAVE relation was only about 9.65% and was not armed.

Therefore:

`AAVE_DDG_ELIGIBLE_AT_SIGNAL = FALSE`

The strategy did not ignore a valid AAVE override. AAVE did not satisfy the
accepted route topology at the time.

## What happened afterwards

From the next daily open (2026-09-29) to the latest available closed data in
this run:

- AAVE: **+20.14%**
- ALGO: **-6.47%**
- TRX: **-0.24%**
- HBAR: **-16.10%**

At the fixed +3-day open horizon:
- AAVE: **+14.69%**
- ALGO: **-9.85%**
- TRX: **-0.21%**

So AAVE was a genuine ex-post winner in this episode.

However, HBAR was the actual strongest M7/M14 momentum token at the signal
close and subsequently fell sharply. This is an important counterexample to
the intuitive rule "buy whichever token is strongest now."

## Interpretation

The live AAVE episode is real but does not generalize into a naive momentum
override rule.

The historical evidence says:

1. large momentum continuations do occur;
2. they create memorable missed-winner cases;
3. the typical strongest-third-token candidate is essentially a coin flip
   versus the RR destination;
4. even a +10 percentage-point momentum advantage does not fix this;
5. the accepted RR/DDG route correctly rejected AAVE on 2026-09-28 because
   neither required AAVE relation was armed.

The right-skewed mean suggests a narrower future question may still be useful:
identify the small subset of momentum continuations with very large upside
without replacing the RR core.

Any such test must use a new preregistered feature family rather than retuning
M7/M14 after seeing this result.

## Decision

- no production routing change;
- no momentum override;
- no universe change;
- no Telegram command change;
- no position sizing change.

Frozen classification:

`AAVE_2026_09_28 = REAL_EX_POST_MISSED_WINNER`

but

`NAIVE_THIRD_TOKEN_MOMENTUM_OVERRIDE = NOT_SUPPORTED`

## Artifact files

- `route_opportunities.csv`
- `aggregate_summary.csv`
- `link_algo_case_all_targets.csv`
- `link_algo_case_ddg.json`
- `summary.json`
- `report.md`

## No-repeat rule

Do not rerun M7/M14 strongest-third-token selection on the same history simply
to search for a better threshold.

A repeat requires:
- genuinely new forward data;
- a new preregistered structural feature;
- a materially different candidate definition;
- or a documented implementation bug.
