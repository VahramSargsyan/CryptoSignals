# Relative Rotation U10 Regime Robustness Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Frozen subject

Universe:

`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Canonical evidence:

`research/relative_rotation/2026-09-28_RR_U10_REGIME_ROBUSTNESS_V1_EVIDENCE.md`

Runtime:
- GitHub Actions run: `36427874155`
- Source commit: `a3a24eecc176ddc5454298679ea2366cfd0c9693`
- Artifact ID: `10971777711`
- Artifact SHA256: `6efd885db940ebcda828beec6f34a1e05cec1c325aafe6d431d8054c311560e4`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen conclusion

Classification:

`BULL_DEPENDENT`

The base concentrated U10 is not historically all-weather in absolute-return terms.

### BTC_BEAR
- 348 days
- 6 episodes
- median conditioned return: -15.4%
- worst start: -15.4%
- positive starts: 0%
- BTC same days: -30.4%
- median U10 market proxy same days: -61.9%

Interpretation: the strategy loses in BTC bear regimes, but historically loses less than BTC and the broad U10 proxy.

### BROAD_BEAR
- 590 days
- 41 episodes
- median conditioned return: -88.3%
- worst start: -88.3%
- positive starts: 0%
- median conditioned DD: -91.9%
- BTC same days: -77.2%
- median U10 market proxy same days: -98.0%

Interpretation: broad crypto weakness is a major failure regime for the base strategy.

### BROAD_BULL
- median conditioned return: +30366.8%
- worst start: +23791.5%
- positive starts: 100%

Most of the historical absolute-return engine is concentrated in broad-bull conditions.

## Calendar check

BROAD_BEAR conditioned return:
- 2024: -54.0%
- 2025: -62.5%
- 2026 through cutoff: -31.9%

BTC_BEAR:
- 2024: -15.3%
- 2025: -1.4%
- 2026 through cutoff: +1.3%

Therefore the broad-bear weakness is not isolated to one year.

## Relationship to prior bull-neutral result

The prior bull-neutral test and this result are not contradictory.

Prior test:
- removed positive strategy returns on unusually strong broad-bull days;
- showed the strategy was not dependent only on a narrow set of extreme bull days.

Current regime test:
- explicitly isolates bear-labelled days;
- shows the strategy is not absolutely profitable across bear regimes.

Frozen interpretation:

`NOT_EXTREME_BULL_DAY_DEPENDENT != ALL_WEATHER`

## Research implication

Do not respond to this result by searching for another tenth token first.

The next materially distinct research question is whether an existing defensive regime overlay can preserve the bull-regime return engine while reducing bear-regime losses.

Existing candidate:

`DEFENSIVE_LOW_VOL_CRYPTO_SMA200_BREADTH_3_5_CONFIRM3_VOL30`

That candidate is not production-approved and remains disabled.

Any next test must be preregistered as a separate defensive-overlay stress test.

## No-repeat rule

Do not rerun the identical U10 regime attribution on the same history.

A rerun requires:
- unseen data;
- changed regime definition;
- documented strategy semantic correction;
- changed universe/costs;
- or a separate defensive-overlay / forward-validation protocol.

## Production boundary

No live/paper, Telegram, universe, allocation, or exchange behavior is changed by this decision.
