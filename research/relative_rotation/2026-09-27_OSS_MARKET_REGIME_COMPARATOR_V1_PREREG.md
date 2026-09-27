# OSS MARKET REGIME COMPARATOR V1 — Preregistered Diagnostic

Date: 2026-09-27
Branch: `research/global-macro-risk-regime-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Goal

Test whether an independently designed, open-source cross-asset market regime can explain, precede, or confirm the frozen 8-asset crypto stress episodes.

This is intentionally separate from the FRED composite experiment. It exists to avoid inventing or tuning a macro rule after seeing the crypto results.

## External methodology source

Reference project: `zhuy9/market-rotation`
License: MIT
Methodology: deterministic risk-regime engine using SPY, sector breadth, RSP/SPY, HYG/LQD, defensive/cyclical sectors, Treasuries, gold, VIX, QQQ and IWM.

This implementation reproduces the documented public rules and thresholds independently; it does not vendor the upstream project.

## Frozen external rules

Priority order:

1. BROAD_RISK_OFF
2. DEFENSIVE_ROTATION
3. BROAD_RISK_ON
4. INTERNAL_ROTATION
5. MIXED

Rules:

- BROAD_RISK_OFF:
  - SPY 5-session return < -1%
  - positive sectors <= 4 of 11
  - at least 2 confirmations:
    - GLD outperforms SPY
    - IEF or TLT outperforms SPY
    - HYG/LQD 5-session ratio return < 0
    - VIX 5-session return > 0
- DEFENSIVE_ROTATION:
  - defensive minus cyclical 5-session return spread >= +1%
  - at least 2 of XLV/XLP/XLU outperform SPY
  - QQQ or IWM underperforms SPY
- BROAD_RISK_ON:
  - SPY 5-session return > +1%
  - positive sectors >= 7 of 11
  - RSP/SPY 5-session ratio return >= 0
  - HYG/LQD 5-session ratio return >= 0
- INTERNAL_ROTATION:
  - abs(SPY 5-session return) <= 2%
  - >=3 positive sectors
  - >=3 negative sectors
  - sector return dispersion >= 2%
- otherwise MIXED.

No threshold optimization is allowed in this pass.

## Market data

Yahoo Finance / yfinance adjusted daily closes.

Universe:
- sectors: XLK XLF XLE XLV XLI XLY XLP XLU XLB XLRE XLC
- equity: SPY QQQ IWM RSP
- rates: IEF TLT
- credit: HYG LQD
- safe haven: GLD
- volatility: ^VIX

## Causality boundary

Crypto daily candles close at 00:00 UTC. A U.S. market session dated D closes later on calendar date D, so it is not considered available for the crypto close stamped D.

Therefore each market-session regime becomes eligible on D+1 calendar day before backward as-of alignment to crypto dates.

This is conservative and avoids same-date U.S.-close look-ahead.

## Frozen crypto stress definition

No change:
- ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK
- own SMA200
- enter after 3 consecutive closes breadth <=3
- exit after 3 consecutive closes breadth >=5
- next-open execution semantics

The known untouched episode 2026-04-01 -> 2026-08-23 must reproduce or the comparator run fails.

## Outputs

- daily external regime aligned causally to crypto dates
- frozen crypto stress episodes
- market-regime distribution during crypto NORMAL vs DEFENSIVE periods
- event study around crypto entry/exit signals at -30,-14,-7,0,+7,+14,+30 days
- nearest preceding BROAD_RISK_OFF before each crypto stress entry
- nearest preceding BROAD_RISK_ON before each crypto recovery
- latest fixed-end state through 2026-09-26

## Interpretation discipline

- This comparator is evidence, not a trading rule.
- Do not tune the upstream thresholds on the same sample.
- Do not convert regime labels into swaps.
- A useful association only creates a new hypothesis for a separately preregistered test.
- Preserve negative or mixed results.

## Runtime impact

Production behavior changed: NONE
Paper-live trading behavior changed: NONE
Migration required: NO
