# OSS MARKET REGIME COMPARATOR V1 — Untouched 2026 Evidence

Date: 2026-09-27  
Branch: `research/global-macro-risk-regime-v1`  
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING  
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36298521031`
- Source commit: `0ecad7ba6dd53050afc22ddec87fffd2b78010a7`
- Artifact: `oss-market-regime-comparator-v1`
- Artifact ID: `10924377456`
- Run ID: `OSS_MARKET_REGIME_V1_0ecad7ba6dd5`
- Workflow conclusion: SUCCESS

## External methodology

Comparator methodology is independently derived from the documented deterministic regime rules in `zhuy9/market-rotation` (MIT).

No external threshold was tuned on CryptoSignals evidence.

Market regimes:
- BROAD_RISK_OFF
- DEFENSIVE_ROTATION
- BROAD_RISK_ON
- INTERNAL_ROTATION
- MIXED

U.S. market session D is made available to crypto only on D+1 calendar day to avoid same-date U.S.-close look-ahead relative to a crypto 00:00 UTC decision boundary.

## Frozen crypto gate

Frozen crypto parameters were not changed.

Untouched state-reset boundary:
- start: 2026-03-29
- expected defensive execution entry: 2026-04-01
- expected defensive execution exit: 2026-08-23

Result:
- reproduction gate: PASS
- matching rows: 1

## Main result

### Stress entry

Crypto entry signal date: 2026-03-31.

External market regime:
- 2026-03-29: BROAD_RISK_OFF / high
- 2026-03-30: BROAD_RISK_OFF / high
- 2026-03-31: BROAD_RISK_OFF / high

On 2026-03-31:
- crypto breadth: 1/8
- SPY 5-session return: about -3.57%
- positive U.S. sectors: 4/11
- HYG/LQD 5-session ratio: negative
- VIX 5-session return: about +17.1%

Nearest preceding BROAD_RISK_OFF to crypto entry signal:
- found: YES
- lead: 0 days

Interpretation:
The independent cross-asset regime strongly confirmed the start of this crypto stress episode, but this is one untouched episode and is not sufficient by itself to establish a general lead signal.

### Stress exit / recovery

Crypto exit signal date: 2026-08-22.
Crypto returned to NORMAL on 2026-08-23.

Nearest preceding BROAD_RISK_ON:
- 2026-08-14
- lead: 8 days
- confidence: high
- crypto breadth that day: 2/8
- positive U.S. sectors: 11/11

However:
- 2026-08-21: BROAD_RISK_OFF / high
- 2026-08-22: BROAD_RISK_OFF / high
- 2026-08-23: BROAD_RISK_OFF / high
- 2026-08-24: BROAD_RISK_OFF / high

Interpretation:
A raw daily BROAD_RISK_ON label cannot be used as a direct defensive-exit rule. The external regime briefly improved before crypto recovery but then reversed to BROAD_RISK_OFF exactly around the crypto recovery signal.

## Conditional regime distribution

During 144 DEFENSIVE crypto days:
- MIXED: 69 days / 47.92%
- INTERNAL_ROTATION: 44 / 30.56%
- DEFENSIVE_ROTATION: 21 / 14.58%
- BROAD_RISK_OFF: 5 / 3.47%
- BROAD_RISK_ON: 5 / 3.47%

During 38 NORMAL crypto days:
- MIXED: 25 / 65.79%
- BROAD_RISK_OFF: 7 / 18.42%
- INTERNAL_ROTATION: 4 / 10.53%
- DEFENSIVE_ROTATION: 2 / 5.26%
- BROAD_RISK_ON: 0

The raw external label therefore does not cleanly separate crypto DEFENSIVE from crypto NORMAL. BROAD_RISK_OFF is actually more frequent in the small NORMAL sample than in the DEFENSIVE sample.

## Current fixed-end state

Through 2026-09-26:
- crypto breadth: 8/8
- crypto mode: NORMAL
- external market regime: INTERNAL_ROTATION
- external confidence: medium
- last U.S. session used: 2026-09-25

## Verdict

`OSS_MARKET_REGIME_COMPARATOR_V1_UNTOUCHED_VERDICT = MIXED / ENTRY_CONFIRMATION_STRONG / RAW_EXIT_GATE_REJECTED`

What survived:
- independent cross-asset stress strongly agreed with the 2026 crypto stress entry;
- an external BROAD_RISK_ON observation appeared 8 days before the crypto recovery signal.

What did not survive:
- raw daily external regimes do not cleanly classify crypto DEFENSIVE vs NORMAL;
- a single BROAD_RISK_ON label is too transient to authorize defensive exit;
- the external market was BROAD_RISK_OFF again at the actual crypto recovery signal.

## Next research boundary

Do not tune these external thresholds on the untouched episode.

Next step is descriptive full-history replication over all causally available 8-asset crypto breadth episodes from the existing 2023-2026 dataset, using the same external OSS rules unchanged.

Only if the relationship repeats across multiple episodes should a separately preregistered macro/liquidity rule be considered.

## Runtime impact

Production behavior changed: NONE  
Paper-live trading behavior changed: NONE  
Migration required: NO

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_UNTOUCHED_REPRODUCTION_GATE + REAL_YAHOO_MARKET_DATA
