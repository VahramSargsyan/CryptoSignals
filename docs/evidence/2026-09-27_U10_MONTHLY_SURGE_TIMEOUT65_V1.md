# U10 Monthly Surge Pullback + 65d Timeout v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-timeout65-v1
GitHub Actions run: 36345356154
Source commit: 4428f02dc28f9310232a385b4d77c67152079749
Artifact ID: 10940485506
Artifact digest: sha256:15a49a663f7ff7018ad7d702c40ae4642a3b47505517eddf97ca2ebfed0b28f8
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Complete the preregistered P1/P2 cash cycle with one explicit fixed fallback:

if natural -25% re-entry has not occurred, re-enter all cash at the first daily open on or after 65 calendar days from the actual cash-out execution.

No other P1/P2 parameter was changed.

No 45/55/75/90-day sweep was performed.

Older 2020-2022 data were deliberately not used.

## Canonical U10

The 65-day timeout was never needed.

| Variant | No-timeout final | 65d final | Delta | Max DD | Natural reentries | Timeout reentries | Cash days |
|---|---:|---:|---:|---:|---:|---:|---:|
| P1 | 270,653.52 | 270,653.52 | 0.00% | -66.36% | 3 | 0 | 66 -> 66 |
| P2 | 265,751.95 | 265,751.95 | 0.00% | -66.36% | 3 | 0 | 63 -> 63 |

Canonical P1 natural cash cycles were approximately:
- 44 days
- 6 days
- 16 days

Therefore the 65d guardrail has no effect on the canonical U10 history.

## 791 alternative U10s

### P1

Timeout was used in:

331 / 791 = 41.85% of alternative universes.

Compared with P1 without timeout:

- timeout improved terminal equity in 30 / 791 = 3.79%
- timeout worsened terminal equity in 301 / 791 = 38.05%
- timeout had no effect in 460 / 791 = 58.15%

The key split was exact:

- all 30 cases where timeout improved were cases where the natural -25% re-entry never occurred before the dataset ended;
- all 301 cases where timeout worsened had a natural -25% re-entry later than day 65.

Among the 30 rescued cases:
- median terminal improvement from adding timeout: +21.56%
- q25/q75: +17.16% / +27.08%
- range: +11.64% to +33.86%

Among the 301 harmed cases:
- median terminal damage from adding timeout: -7.93%
- q25/q75: -16.41% / -4.47%
- range: -16.41% to -4.47%

Despite usually being worse than no-timeout when it fired, P1+65d still finished above the original U10 baseline in:

791 / 791 = 100.00% of alternatives.

Max DD improved versus baseline in:

90.77%.

Both terminal equity and max DD improved versus baseline in:

90.77%.

This is because the no-timeout P1 advantage was already large enough that many premature timeout re-entries remained above baseline, while the 30 permanently stuck-cash cases were rescued.

### P2

Timeout was used in:

236 / 791 = 29.84%.

Compared with P2 without timeout:

- better: 30 / 791 = 3.79%
- worse: 206 / 791 = 26.04%
- unchanged: 555 / 791 = 70.16%

Again the split was exact:

- all 30 improved cases had no natural -25% re-entry before the dataset end;
- all 206 harmed cases eventually received a natural -25% re-entry after day 65.

Among rescued cases:
- median timeout improvement: +22.40%
- q25/q75: +14.43% / +24.53%
- range: +8.18% to +30.65%

Among harmed cases:
- median timeout damage: -8.11%
- q25/q75: -15.84% / -8.11%
- range: -15.84% to -6.89%

P2+65d finished above baseline in:

97.35% of alternatives.

Max DD improved in:

90.52%.

Both improved in:

87.86%.

## Where 65d was actually invoked

P1 timeout cycles:

- 2024-11: 196 cycles
- 2025-05: 35 cycles
- 2025-07: 120 cycles

P2 timeout cycles:

- 2024-11: 91 cycles
- 2025-05: 35 cycles
- 2025-07: 120 cycles

Natural -25% eventually occurred after day 65 in most of these.

For P1 eventual natural re-entry waits:
- minimum: 69 days
- median: 98 days
- q75: 131 days
- 90th percentile: 199 days
- maximum: 200 days

For P2:
- minimum: 97 days
- median: 123 days
- q75: 123 days
- 90th percentile: 167 days
- maximum: 192 days

Therefore day 65 was systematically earlier than the eventual natural re-entry in these regimes.

## The 30 genuinely stuck-cash cases

For both P1 and P2, exactly 30 alternative U10 universes had no natural -25% re-entry before the historical window ended.

All 30 came from the July 2025 surge regime.

Every one of these 30 universes contained:

- ATOM
- TWT
- PEPE
- ALGO
- AVAX
- FIL

plus four other assets from the frozen candidate pool.

This is the same broad July-2025 regime previously identified as the strongest counterexample to the assumption that +100% surges are followed by -25% pullbacks.

In these cases, forcing re-entry after 65 days was beneficial because otherwise 30% of capital remained parked while the market continued higher.

## Main finding

A fixed 65-day timeout is **not** a good general re-entry rule.

When the natural -25% pullback eventually arrived:
- waiting longer was always historically better than forcing re-entry at day 65.

When the natural -25% never arrived before the end of the data:
- the timeout was always historically better than remaining in cash.

Therefore the real problem is not choosing the right number of days.

The problem is detecting whether the original deep-pullback thesis has been invalidated.

This strongly supports moving from a fixed-time fallback to a **market-state invalidation fallback**.

## Next return-rule hypothesis

A conceptually clean non-time-based fallback is:

**LOCKED-PEAK RECLAIM**

After cash-out:
- continue to prefer natural re-entry at -25% from the locked peak;
- if instead frozen-U10 reference equity closes back at or above the locked peak before -25% occurs, treat the expected deep correction as invalidated;
- re-enter the parked cash at the next open.

This uses the actual market state rather than an arbitrary elapsed-day constant.

It directly addresses the July-2025 failure mode:
the market resumed upward behavior instead of delivering the expected deep pullback.

This rule must be separately preregistered and tested before older 2020-2022 history is opened.

## Anti-overfit conclusion

The 65d test was useful precisely because it mostly failed relative to the no-timeout overlay when it was invoked.

Do not optimize a better fixed duration from the existing 2023-2026 data.

Freeze the failure evidence and test one market-state fallback next.

Only after the return cycle is complete should the whole frozen rule be taken to older 2020-2022 data.

## Guardrail

No live/paper behavior changed.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
