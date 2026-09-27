# OSS MARKET REGIME COMPARATOR V1 — Full-History Replication Preregistration

Date: 2026-09-27  
Branch: `research/global-macro-risk-regime-v1`  
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING  
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Question

Does the independently defined `zhuy9/market-rotation` regime relationship observed around the untouched 2026 crypto stress episode repeat across all available frozen 8-asset crypto breadth episodes?

## Frozen inputs

No change to:
- crypto universe
- SMA200
- breadth <=3 entry threshold
- breadth >=5 recovery threshold
- 3-close confirmation
- next-open semantics
- VOL30 or low-vol defensive selection
- relative router
- external market regime rules or thresholds

## History boundary

Use the existing Binance 1D panel beginning 2023-05-05.

Crypto breadth becomes eligible only after every asset has a valid own SMA200. The full-history state machine starts from the first fully eligible crypto breadth date with an explicit state reset.

This is a retrospective descriptive replication, NOT untouched validation.

## External regime

Use the exact same OSS comparator rules and conservative U.S.-session D -> crypto date D+1 availability rule used in the untouched run.

No retuning and no new market indicators are allowed in this pass.

## Required outputs

For every crypto stress episode:
- entry signal and execution dates
- exit signal and execution dates
- nearest preceding BROAD_RISK_OFF within 30 calendar days of entry signal
- nearest preceding BROAD_RISK_ON within 30 calendar days of exit signal
- external regime at entry signal
- external regime at exit signal
- regime distribution during each defensive episode

Aggregate:
- number of episodes
- fraction of entries with BROAD_RISK_OFF within 0/7/14/30 days
- fraction of exits with BROAD_RISK_ON within 0/7/14/30 days
- median lead days when found
- external regime distribution in crypto NORMAL vs DEFENSIVE
- per-year breakdown where sample size allows

## Interpretation discipline

- Do not choose a lead window after seeing results and call it a rule.
- Report all preregistered 0/7/14/30-day windows.
- Do not optimize external thresholds.
- Do not convert results into automatic swaps.
- A repeated association is hypothesis generation only.
- Preserve negative/mixed evidence.

## Runtime impact

Production behavior changed: NONE  
Paper-live trading behavior changed: NONE  
Migration required: NO
