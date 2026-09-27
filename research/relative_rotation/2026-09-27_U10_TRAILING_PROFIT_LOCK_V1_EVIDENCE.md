# U10 Trailing Profit Lock v1 — Evidence

Date: 2026-09-27
Mode: PATCH_FIX / research-only capital-management overlay
Validated branch: research/u10-trailing-profit-lock-v1
GitHub Actions run: 36336178272
Source commit: 2a8986bf9c300436c94a192cb930f37840d052cf
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST

## Question

After U10 first reaches 10x original capital, do not sell immediately.

Instead:
- activate a trailing monitor;
- track frozen-U10 running peak;
- after a decline of at least 2x original capital (20,000 USDT) from the running peak, sell part of the active position on the next open;
- independently test 20%, 30%, 40%, and 50% cash-out fractions;
- lock the peak;
- independently test re-entry at -5%, -10%, -15%, -20%, -25%, -30%, -35%, -40%, and -45% from the locked peak;
- re-enter the parked cash on the next open after a trigger close.

Total parameter combinations:

4 cash-out fractions × 9 re-entry depths = 36 scenarios.

No U10 signal logic was changed.

## Shared trigger path

Original capital:

10,000 USDT

10x activation threshold:

100,000 USDT

10x activation close:

2025-09-19

Running peak after activation:

127,668.80 USDT on 2025-09-20

Trailing giveback threshold:

20,000 USDT = 2x original capital

Cash-out trigger close:

102,424.14 USDT on 2025-09-22

Actual decline from locked peak at trigger:

25,244.66 USDT / -19.77%

Cash-out execution:

2025-09-23 while holding ATOM

The same activation, peak, cash-out trigger, and cash-out execution were verified for all 36 scenarios.

## Re-entry trigger behavior

Locked peak:

127,668.80 USDT

Observed trigger groups:

| Requested re-entry drawdown | Trigger close | Re-entry execution |
|---:|---|---|
| -5% | 2025-09-24 | 2025-09-25 |
| -10% | 2025-09-24 | 2025-09-25 |
| -15% | 2025-09-24 | 2025-09-25 |
| -20% | 2025-09-25 | 2025-09-26 |
| -25% | 2025-10-10 | 2025-10-11 |
| -30% | 2025-10-10 | 2025-10-11 |
| -35% | 2025-10-10 | 2025-10-11 |
| -40% | 2025-10-10 | 2025-10-11 |
| -45% | not reached | not reached |

Several requested thresholds share the same execution date because the model evaluates daily closes, so one close can cross multiple threshold levels at once.

## Baselines

Frozen U10 no-overlay final equity:

219,485.20 USDT

Frozen U10 no-overlay total return:

+2,094.85%

Prior immediate-20%-at-10x / cash-forever final equity:

196,457.56 USDT

## Full 36-scenario matrix

| Cash-out | Re-entry DD | Re-entry | Min equity | Max DD | Final equity | Delta vs baseline |
|---:|---:|---|---:|---:|---:|---:|
| 20% | 5% | 2025-09-25 | 70,804.01 | -53.73% | 219,481.84 | -3.36 / -0.00% |
| 20% | 10% | 2025-09-25 | 70,804.01 | -53.73% | 219,481.84 | -3.36 / -0.00% |
| 20% | 15% | 2025-09-25 | 70,804.01 | -53.73% | 219,481.84 | -3.36 / -0.00% |
| 20% | 20% | 2025-09-26 | 71,298.01 | -53.73% | 221,013.14 | +1,527.94 / +0.70% |
| 20% | 25% | 2025-10-11 | 76,632.59 | -53.73% | 237,549.56 | +18,064.37 / +8.23% |
| 20% | 30% | 2025-10-11 | 76,632.59 | -53.73% | 237,549.56 | +18,064.37 / +8.23% |
| 20% | 35% | 2025-10-11 | 76,632.59 | -53.73% | 237,549.56 | +18,064.37 / +8.23% |
| 20% | 40% | 2025-10-11 | 76,632.59 | -53.73% | 237,549.56 | +18,064.37 / +8.23% |
| 20% | 45% | not reached | 77,108.42 | -47.52% | 196,052.50 | -23,432.70 / -10.68% |
| 30% | 5% | 2025-09-25 | 70,803.47 | -53.73% | 219,480.16 | -5.04 / -0.00% |
| 30% | 10% | 2025-09-25 | 70,803.47 | -53.73% | 219,480.16 | -5.04 / -0.00% |
| 30% | 15% | 2025-09-25 | 70,803.47 | -53.73% | 219,480.16 | -5.04 / -0.00% |
| 30% | 20% | 2025-09-26 | 71,544.46 | -53.73% | 221,777.11 | +2,291.91 / +1.04% |
| 30% | 25% | 2025-10-11 | 79,546.34 | -53.73% | 246,581.75 | +27,096.55 / +12.35% |
| 30% | 30% | 2025-10-11 | 79,546.34 | -53.73% | 246,581.75 | +27,096.55 / +12.35% |
| 30% | 35% | 2025-10-11 | 79,546.34 | -53.73% | 246,581.75 | +27,096.55 / +12.35% |
| 30% | 40% | 2025-10-11 | 79,546.34 | -53.73% | 246,581.75 | +27,096.55 / +12.35% |
| 30% | 45% | not reached | 80,260.08 | -43.89% | 184,336.16 | -35,149.04 / -16.01% |
| 40% | 5% | 2025-09-25 | 70,802.93 | -53.73% | 219,478.48 | -6.72 / -0.00% |
| 40% | 10% | 2025-09-25 | 70,802.93 | -53.73% | 219,478.48 | -6.72 / -0.00% |
| 40% | 15% | 2025-09-25 | 70,802.93 | -53.73% | 219,478.48 | -6.72 / -0.00% |
| 40% | 20% | 2025-09-26 | 71,790.91 | -53.73% | 222,541.08 | +3,055.89 / +1.39% |
| 40% | 25% | 2025-10-11 | 82,460.09 | -53.73% | 255,613.93 | +36,128.73 / +16.46% |
| 40% | 30% | 2025-10-11 | 82,460.09 | -53.73% | 255,613.93 | +36,128.73 / +16.46% |
| 40% | 35% | 2025-10-11 | 82,460.09 | -53.73% | 255,613.93 | +36,128.73 / +16.46% |
| 40% | 40% | 2025-10-11 | 82,460.09 | -53.73% | 255,613.93 | +36,128.73 / +16.46% |
| 40% | 45% | not reached | 83,411.75 | -39.84% | 172,619.81 | -46,865.39 / -21.35% |
| 50% | 5% | 2025-09-25 | 70,802.39 | -53.73% | 219,476.80 | -8.40 / -0.00% |
| 50% | 10% | 2025-09-25 | 70,802.39 | -53.73% | 219,476.80 | -8.40 / -0.00% |
| 50% | 15% | 2025-09-25 | 70,802.39 | -53.73% | 219,476.80 | -8.40 / -0.00% |
| 50% | 20% | 2025-09-26 | 72,037.37 | -53.73% | 223,305.05 | +3,819.86 / +1.74% |
| 50% | 25% | 2025-10-11 | 85,373.84 | -53.73% | 264,646.11 | +45,160.91 / +20.58% |
| 50% | 30% | 2025-10-11 | 85,373.84 | -53.73% | 264,646.11 | +45,160.91 / +20.58% |
| 50% | 35% | 2025-10-11 | 85,373.84 | -53.73% | 264,646.11 | +45,160.91 / +20.58% |
| 50% | 40% | 2025-10-11 | 85,373.84 | -53.73% | 264,646.11 | +45,160.91 / +20.58% |
| 50% | 45% | not reached | 86,563.41 | -35.28% | 160,903.46 | -58,581.74 / -26.69% |

## Key observations

### 5%–15% re-entry

The market was already beyond these shallow thresholds immediately after cash-out.

All 5%, 10%, and 15% cases re-entered on 2025-09-25.

Their terminal results are almost identical to no overlay, with only the extra cash-out/re-entry transaction costs remaining.

### 20% re-entry

Re-entry occurred on 2025-09-26.

This produced small terminal improvements versus baseline:
- +0.70% for 20% cash-out;
- +1.04% for 30%;
- +1.39% for 40%;
- +1.74% for 50%.

### 25%–40% re-entry

All thresholds from 25% through 40% were crossed by the same daily close.

Trigger:

2025-10-10

Execution:

2025-10-11

These scenarios produced the highest terminal equity in this historical path.

Terminal uplift scaled with the protected fraction:
- 20% cash-out: +8.23% vs baseline;
- 30%: +12.35%;
- 40%: +16.46%;
- 50%: +20.58%.

Highest observed terminal equity:

264,646.11 USDT

Scenario:

50% cash-out + re-entry at any threshold from -25% through -40%.

### 45% re-entry

The -45% threshold was never reached.

Therefore no cash re-entered and these scenarios behaved as permanent cash protection.

They produced the highest capital floors and shallowest drawdowns, but the lowest terminal equity.

Highest post-cashout floor:

86,563.41 USDT

Scenario:

50% cash-out + -45% re-entry threshold not reached.

Shallowest post-cashout max drawdown:

-35.28%

Same scenario.

## Important nuance on max drawdown

For re-entered 5%–40% scenarios, the later portfolio again becomes fully exposed to frozen U10.

Therefore their later percentage max drawdown can still reach the same approximately -53.73% as baseline even when:
- the absolute capital floor is higher;
- terminal equity is higher.

The cash overlay changed the quantity accumulated on re-entry, not the later percentage path of the underlying U10 holding.

## Main finding

On this one historical path, the middle re-entry zone of -25% through -40% was materially different from both very shallow re-entry and no re-entry:

- shallow -5% to -15% effectively canceled the protection almost immediately;
- -20% gave only modest benefit;
- -25% through -40% waited long enough to buy back materially lower and increased terminal equity;
- -45% never re-entered and maximized protection at the cost of future upside.

This is a single-path observation, not proof that 25%–40% is generally optimal.

## Next robustness requirement

Before any capital-management rule is accepted, repeat this matrix across:
- rolling historical entry dates;
- multiple 10x-like activation levels;
- alternative trailing giveback amounts;
- multiple market cycles where sufficient data exist.

The current result is especially sensitive to daily-close threshold clustering, where several nominal thresholds map to one actual execution date.

## Residual risks

- one historical activation/peak path only;
- thresholds are evaluated on daily closes;
- multiple requested thresholds can collapse to the same real execution date;
- cash is modeled as non-yielding USDT;
- stablecoin/counterparty risk is not modeled;
- real spread/slippage may exceed 0.1%;
- historical performance does not establish future performance.
