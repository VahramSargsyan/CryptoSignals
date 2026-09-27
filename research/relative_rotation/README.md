# Relative Rotation Research Index

Family: RELATIVE_ROTATION_GRAPH_8_V1 / U8 and universe-scale extensions  
Repository: CryptoSignals Strategy Laboratory  
Last updated: 2026-09-27

This directory is the technical research record for the Relative Rotation family.

For a human-facing summary of the current U8 risk work, start here:

- [Strategy Lab: U8 Risk, Transition Ledger & Entry-Date Stress](../../docs/evidence/2026-09-27_U8_RISK_ENTRY_STRESS_STRATEGY_LAB_V1.md)

## Canonical U8

Live/paper documentation:

- [Relative Rotation Paper Live v1](../../docs/18_RELATIVE_ROTATION_PAPER_LIVE_V1.md)
- [Real Rotation Log](REAL_ROTATION_LOG.md)

Frozen U8 universe:

`ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK`

Frozen mechanics:

- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open historical execution
- 0.1% modeled cost per executed transition

## Research catalog

### U8 Origin Selection Audit

Question:

Was the historical 8-token set unusually lucky compared with other 8-token subsets?

Files:

- [Prereg](2026-09-27_U8_ORIGIN_SELECTION_AUDIT_V1_PREREG.md)
- [Evidence](2026-09-27_U8_ORIGIN_SELECTION_AUDIT_V1_EVIDENCE.md)

### Universe Scale v1

Question:

How does canonical U8 compare when the candidate universe is expanded?

Files:

- [Prereg](2026-09-27_UNIVERSE_SCALE_V1_PREREG.md)
- [Evidence](2026-09-27_UNIVERSE_SCALE_V1_EVIDENCE.md)

### U20 Deep Dive v1

Question:

What happens under a larger 20-asset relative-rotation graph, and how does canonical U8 compare?

Files:

- [Prereg](2026-09-27_U20_DEEP_DIVE_V1_PREREG.md)
- [Evidence](2026-09-27_U20_DEEP_DIVE_V1_EVIDENCE.md)

### U8 Transition Ledger v1

Question:

What exactly happens at each U8 rotation in token quantities, USDT value, and modeled transition cost?

Files:

- [Prereg](2026-09-27_U8_TRANSITION_LEDGER_V1_PREREG.md)
- [Evidence](2026-09-27_U8_TRANSITION_LEDGER_V1_EVIDENCE.md)

Key result:

- 21 executed transitions on the mature 2023-10-31 -> 2026-09-26 path;
- 0.1% modeled cost is applied on every transition;
- cumulative cost drag versus identical zero-cost route: 2.0791%.

### U8 Drawdown Entry Stress v1

Question:

How much can a **fresh original capital** lose when U8 is started near the historically strongest drawdown rather than years earlier?

Files:

- [Prereg](2026-09-27_U8_DRAWDOWN_ENTRY_STRESS_V1_PREREG.md)
- [Evidence](2026-09-27_U8_DRAWDOWN_ENTRY_STRESS_V1_EVIDENCE.md)

Key result:

- long-start `-71.23%` max drawdown was from an accumulated peak, not from original capital;
- fresh entry on 2025-09-20 reached `-66.66%` versus its own original 10,000 USDT;
- entry date materially changes principal-risk interpretation.

## Risk-reporting rule

Do not publish a Relative Rotation max drawdown without also stating the denominator/context.

Always distinguish:

1. **MAX DRAWDOWN FROM PRIOR PEAK**
2. **MINIMUM EQUITY VS INITIAL CAPITAL**

For fresh-entry work also report:

- start date;
- starting asset;
- days below original capital;
- recovery date/time;
- transition-cost assumption;
- terminal result from the same capital base.

## Next research candidate

`U8_ROLLING_ENTRY_STRESS_V1`

Proposed method:

- start a fresh 10,000 USDT portfolio every month across the mature history;
- retain historical monitor warm-up/state;
- start each portfolio in ATOM unless separately preregistered;
- measure minimum equity vs original capital, max drawdown, days below initial, recovery time and final return.

This should replace reliance on only a few hand-selected entry dates when assessing U8 principal risk.
