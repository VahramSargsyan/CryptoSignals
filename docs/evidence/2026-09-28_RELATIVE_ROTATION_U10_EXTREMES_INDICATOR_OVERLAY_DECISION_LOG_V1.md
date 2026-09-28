# Relative Rotation U10 Extremes Indicator Overlay Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical evidence

`research/relative_rotation/2026-09-28_RR_U10_EXTREMES_INDICATOR_OVERLAY_V1_EVIDENCE.md`

Runtime:
- run: `36462593685`
- source SHA: `07cc0d9228bc71f96b5658439f1f5a846b6f27f4`
- artifact: `10987558717`
- artifact SHA256: `41031266646e8d321ae2a9fd21e784b93f6cc3bc28347d73d27e20998f731096`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen thresholds

- explosive U10 month: >= +40%
- severe U10 dip month: <= -15%

## Frozen conclusions

### Explosive months

7 historical months.

- TOTAL positive MoM: 6/7
- TOTAL bullish state: 5/7
- ETH/BTC positive MoM: 4/7
- ETH/BTC above SMA25: 5/7
- ETH/BTC above SMA50: 5/7
- ETH/BTC above SMA100: 4/7
- ETH/BTC above SMA200: 5/7

Upside is therefore not explained by one ETH/BTC rule.

### Severe dip months

6 historical months.

- TOTAL bullish state: 0/6
- ETH/BTC above SMA100: 0/6
- ETH/BTC above SMA50: 1/6
- TOTAL positive MoM: 1/6

Frozen risk pattern:

`TOTAL_NOT_BULL + ETHBTC_BELOW_SMA100 = HIGH_ATTENTION_RISK_PATTERN`

This is descriptive only, based on six events, and is not production-approved.

## Exit / return structure

Key deterioration:
- 2025-10-14: TOTAL loses fast bull structure while still above SMA200/SMA300.
- 2025-11-03: TOTAL loses SMA200; ETH/BTC below SMA25/50/100.
- 2026-02-19: FULL_BEAR, long confirmation arrives much later.

False recovery:
- 2026-05-02: TOTAL BULL_BUILDING but still below SMA200/300 and ETH/BTC below all tested SMAs.
- 2026-05-22: back to MIXED.

True recovery sequence:
- 2026-08-17: exit FULL_BEAR; ETH/BTC already above all tested SMAs.
- 2026-08-19: TOTAL regains SMA200.
- 2026-08-21: TOTAL regains SMA300.
- 2026-08-27: BULL_BUILDING.
- 2026-09-26: BULL_BUILDING remains active.

## Interpretation

The indicator stack appears more useful for **risk attention** than for forecasting explosive U10 months.

Do not infer:
- automatic cash exit;
- automatic re-entry;
- guaranteed altseason;
- guaranteed U10 drawdown.

A separate cash-withdrawal / allocation rule requires its own preregistered validation.

## No-repeat / production boundary

Do not rerun unchanged on the same historical window.

No live/paper, Telegram, exchange, universe, allocation, or execution behavior changed.
