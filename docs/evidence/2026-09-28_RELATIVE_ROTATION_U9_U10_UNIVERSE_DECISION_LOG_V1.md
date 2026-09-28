# Relative Rotation U9/U10 Universe Decision Log V1

Date: 2026-09-28  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode at creation: PATCH_FIX (documentation/research-state only)  
Production strategy change: NONE

## Canonical current production target

The current paper/live migration target in `main` remains:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP`

This is the current U9 target and is **not** changed by this document.

## Frozen U10 candidate

The candidate to test for promotion from the current U9 to U10 is now frozen as:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Candidate ID:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Definition:

`CURRENT_TARGET_U9 + HBAR`

HBAR is the only token added in this candidate. No other token substitution is implied.

## Why this candidate is frozen

The exhaustive U9/U10 stress test on 2026-09-28 evaluated:

- all 5,005 possible U9 universes from the 15-token pool;
- all 3,003 possible U10 universes from the same pool;
- 8,008 universes total;
- 1Y, 2Y and MATURE windows;
- matched U10-minus-U9 marginal contribution;
- rolling 12m / 24m finalist diagnostics;
- endpoint sensitivity;
- broad-bull-neutralized finalist diagnostics.

Canonical evidence:

`research/relative_rotation/2026-09-28_ROBUST_EXHAUSTIVE_U9_U10_ATOM_REPLACEMENT_V1_EVIDENCE.md`

GitHub Actions run:

`36401115596`

Artifact ID:

`10960970166`

Source commit:

`09f9e353a47fc65b9e8e8af0b71149fd047d7bf9`

Artifact SHA256:

`fb3cdb86e74aebf7d26378dcd9c54f139e79d2f7f89b29393b1444e729818e9a`

TEST_LEVEL:

`GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Key frozen findings

Current U9 robust rank:

`27 / 5005`

Current U9 + HBAR robust U10 rank:

`3 / 3003`

Historical comparison:

| Metric | Current U9 | Current U9 + HBAR |
|---|---:|---:|
| Latest 1Y median return | +156.3% | +155.7% |
| Latest 2Y median return | +1180.5% | +1850.9% |
| MATURE median return | +1678.6% | +3167.3% |
| MATURE median max DD | -62.4% | -62.4% |
| Rolling 12m median return | +81.8% | +150.7% |
| Rolling 12m worst window | -11.5% | -11.4% |
| Rolling 12m positive-window rate | 91.3% | 95.7% |
| Rolling 24m median return | +268.9% | +563.0% |
| Rolling 24m worst window | +105.1% | +268.6% |
| Rolling 24m positive-window rate | 100% | 100% |
| Shifted 1Y endpoint median | +37.7% | +38.4% |
| Shifted 1Y endpoint worst | +14.5% | +16.3% |
| Shifted endpoint positive rate | 100% | 100% |
| Bull-neutral MATURE median | +8.3% | +32.2% |
| Bull-neutral worst start | -2.8% | +17.7% |
| Bull-neutral positive-start rate | 77.8% | 100% |

Matched HBAR contribution across 2,002 exact U10-vs-matched-U9 contexts:

- latest 1Y median delta: +0.1 pp;
- latest 2Y median delta: +64.4 pp;
- MATURE median delta: +89.6 pp;
- positive-context rate: 51.6% / 89.4% / 78.3% for 1Y / 2Y / MATURE.

## ATOM conclusion

ATOM is not a U10 candidate.

Across the same 2,002 matched contexts:

- 1Y median delta: -4.6 pp;
- 2Y median delta: -135.0 pp;
- MATURE median delta: -264.3 pp;
- MATURE positive-context rate: 11.3%.

The working research conclusion remains:

`ATOM OUT`

## No-repeat rule

Do **not** rerun the 2026-09-28 exhaustive 15-token U9/U10 experiment merely to rediscover the same result.

A repeat is justified only if at least one of these is true:

1. materially new unseen market data has accumulated;
2. a bug or semantic error in the engine is documented;
3. execution-cost/slippage assumptions are materially changed;
4. the token pool is materially changed;
5. the relative-rotation mechanism itself changes;
6. a deliberately separate confirmatory / forward-validation protocol is being run.

Any future run must reference this decision log and explain what is materially new.

## Promotion gate

`RR_TARGET_U10_CANDIDATE_HBAR_V1` is a **frozen candidate**, not an accepted production target.

Before production promotion, require a separate confirmatory or forward-validation stage defined without searching for another historical optimum.

Until that stage succeeds:

- current production target stays U9;
- HBAR remains the frozen tenth-token candidate;
- ATOM remains sunset / exit-only under the current migration policy;
- no automatic real-money execution is authorized.

## Residual risks

- historical/post-selection evidence is not unseen evidence;
- the common 15-token panel begins in 2023;
- modeled transition cost is fixed at 0.1%;
- time-varying liquidity, spread and slippage are not separately modeled;
- large compounded historical returns are path-sensitive.
