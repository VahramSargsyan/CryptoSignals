# ATOM OUT Replacement Research — Combined Evidence v1/v2

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Production changes: NONE
Branch: research/atom-out-replacement-v1

## Question

For the ATOM-free base network:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

should a tenth token replace ATOM, or should the strategy remain U9?

## Important repository-state correction

The earlier chat note saying PR #54 was still open was stale.

PR #54 was merged on 2026-09-27 at 19:09:06 UTC.
This research does not revert or modify that production/paper-live change.

## Frozen mechanics

Both screens used the existing relative-rotation research engine:

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation routing
- next-open execution
- 0.1% modeled transition cost
- same nine U9 start assets for fair candidate comparison

## V1 — original remaining pool

Candidates:

AVAX, ETH, ALGO, ADA, XRP

GitHub Actions run: 36345708669
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

Base U9:
- latest 1Y median return: +18.55%
- latest 1Y median max DD: -71.12%
- latest 2Y median return: +1718.15%
- latest 2Y median max DD: -59.67%

| Candidate | Formal checks | 1Y delta | 2Y delta | Rolling 12M win | Rolling 24M win | Shifted endpoint win | Bull-neutral delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| AVAX | 5/8 | +14.14% | +216.92% | 4.3% | 9.1% | 16.7% | +15.71 pp |
| ADA | 2/8 | -27.41% | -420.44% | 43.5% | 72.7% | 0.0% | -17.48 pp |
| ETH | 1/8 | -1.06% | -1024.01% | 17.4% | 0.0% | 0.0% | -76.14 pp |
| ALGO | 1/8 | +0.00% | +0.00% | 8.7% | 18.2% | 0.0% | +0.00 pp |
| XRP | 1/8 | -4.86% | -74.49% | 4.3% | 18.2% | 0.0% | -48.81 pp |

### AVAX detail

Latest 1Y:
- median return: +32.69%
- median max DD: -73.58%
- delta vs U9: +14.14 pp return, -2.45 pp drawdown

Latest 2Y:
- median return: +1935.07%
- median max DD: -62.43%
- delta vs U9: +216.92 pp return, -2.75 pp drawdown

Long-path use:
- occupancy: 4.72%
- entries/exits: 22 / 22
- completed-spell win rate: 59.09%
- median completed-spell return: +31.68%
- top transition neighbor: BNB
- top-neighbor share: 29.55%

Neighbor leave-one-out:
- positive incremental return in 8/9 1Y removals
- positive incremental return in 9/9 2Y removals
- most sensitive 1Y removal: PEPE
- most sensitive 2Y removal: FIL

However temporal robustness is weak:
- rolling 12M: only 1 positive window, 14 unchanged, 8 negative out of 23
- rolling 24M: only 1 positive, 2 unchanged, 8 negative out of 11
- shifted latest-1Y endpoint: positive only at the exact 2026-09-26 endpoint; negative at 30/60/90/120/180-day shifts

Therefore AVAX is a valid challenger but not a robust production replacement.

## V2 — expanded pool

Preregistered candidates:

DOGE, LTC, BCH, DOT, NEAR, UNI, ETC, XLM, SHIB, OP, ARB, SUI

GitHub Actions run: 36346057667
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

A stronger gate was preregistered before V2:
all 8 temporal + safety checks must pass.

Key results:

| Candidate | V1-style checks | 1Y delta | 2Y delta | Rolling 12M win | Rolling 24M win | Endpoint win |
|---|---:|---:|---:|---:|---:|---:|
| LTC | 5/8 | +25.67% | +34.44% | 17.4% | 36.4% | 50.0% |
| SUI | 3/8 | +46.12% | -558.94% | 43.5% | 63.6% | 16.7% |
| DOT | 2/8 | -36.82% | -564.71% | 26.1% | 45.5% | 50.0% |
| ARB | 2/8 | +52.30% | -735.80% | 8.7% | 0.0% | 16.7% |
| XLM | 1/8 | -11.13% | -170.76% | 17.4% | 36.4% | 16.7% |
| NEAR | 1/8 | -2.38% | -978.02% | 8.7% | 0.0% | 16.7% |
| UNI | 1/8 | +0.00% | -986.01% | 8.7% | 0.0% | 0.0% |
| ETC | 1/8 | +0.00% | -11.85% | 4.3% | 9.1% | 0.0% |
| SHIB | 1/8 | +0.00% | +0.00% | 4.3% | 9.1% | 0.0% |
| BCH | 1/8 | -59.63% | -914.59% | 4.3% | 9.1% | 0.0% |
| DOGE | 1/8 | +0.00% | -1061.82% | 0.0% | 0.0% | 0.0% |
| OP | 0/8 | -55.95% | -1387.18% | 21.7% | 0.0% | 0.0% |

Strict V2 passers: NONE.

V2 preregistered decision:

**NOTHING / U9**

### LTC detail

LTC was the strongest expanded challenger, but still failed the strict temporal gate.

- latest 1Y median return: +44.22%
- latest 1Y median max DD: -67.16%
- latest 2Y median return: +1752.59%
- latest 2Y median max DD: -59.67%
- rolling 12M improvement: 17.39%
- rolling 24M improvement: 36.36%
- shifted endpoint improvement: 50%
- long occupancy: 7.87%
- entries/exits: 18 / 18
- completed-spell win rate: 61.11%
- median completed-spell return: +2.27%
- top neighbor: PEPE
- top-neighbor share: 52.78%
- neighbor positive-rate: 100% 1Y / 77.78% 2Y

Despite good latest-window numbers, temporal consistency was insufficient.

## ATOM reference arm

ATOM was also run only as a reference in V1, not as an eligible replacement winner.

It improved the exact latest 1Y endpoint but failed the 2Y and rolling robustness:
- rolling 12M improvement rate: 39.13%
- rolling 24M improvement rate: 0%
- bull-neutralized long result worse than U9
- 2Y neighbor leave-one-out positive rate: 0%

This supports the original concern that keeping ATOM is not justified by one favorable recent endpoint.

## Research conclusion

Current evidence does not identify a robust tenth token.

Production/research decision from the candidate screen:

**ATOM OUT -> NOTHING IN**

Use the nine-token network as the new research baseline:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Reserve/challenger order for future forward observation:
1. AVAX — strongest original-pool challenger, but temporally fragile.
2. LTC — strongest expanded-pool challenger, but fails rolling consistency.
3. SUI — interesting rolling-24 behavior, but fails 2Y, endpoint and broader safety checks.

Do not promote AVAX/LTC/SUI from this backtest alone.

## Residual risks

- historical performance does not establish future performance;
- repeated candidate screening creates selection/data-snooping risk;
- the broad-bull neutralization definition is a research stress, not a literal tradable counterfactual;
- modeled cost is 0.1% and does not separately model changing spread/slippage;
- the current live/paper configuration was already changed by merged PR #54 and is not altered by this research branch;
- next production change, if requested, must be a separate PATCH_FIX with rollback and live/paper regression tests.

