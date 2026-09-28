# Relative Rotation U10 BTC/ETH Ratio SMA Cross Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical evidence

`research/relative_rotation/2026-09-28_RR_U10_BTC_ETH_RATIO_SMA_CROSS_V1_EVIDENCE.md`

Runtime:
- run: `36444297651`
- source SHA: `3ba7a9165e62f5c1117a31da5e0e38d273076741`
- artifact: `10980120912`
- artifact SHA256: `5208a49e1deaf4f0be20c8515e41b75b9942c3780b0cb82bd9f4167547012bb4`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen conclusion

`MIXED_BUT_USEFUL_AS_LAYERED_ROTATION_SIGNAL`

### ETH-strength / possible alt-rotation

BTC/ETH falling below SMA25 or SMA50 can be used as an **attention layer**, not as a formal altseason definition and not as an automatic U10 entry.

Notable observations:
- persistent BTC/ETH below SMA25: 30d median U10 +14.6%, 64% positive events;
- raw BTC/ETH below SMA50: 30d median U10 +14.9%, 75.9% positive events.

Longer ETH-strength crosses were not consistently better:
- below SMA100: 60d -10.9%, 90d -7.7%;
- below SMA200: mixed / negative on 30d and 90d.

Therefore:

`ETH_OUTPERFORMS_BTC != AUTOMATIC_ALTSEASON`

### BTC-strength / U10 risk

Fast upward BTC/ETH crosses are not reliable exits.

The cleanest warning was:

`BTC/ETH CROSS ABOVE SMA200`

Persistent 3-day subset:
- 30d U10 -3.7%
- 60d U10 -11.3%
- 90d U10 -18.4%
- only 16.7% positive at 90d

This is a strong research warning but only 6-7 full-horizon events exist.

Frozen label:

`BTC_ETH_ABOVE_SMA200 = U10_RISK_ATTENTION, NOT AUTOMATIC_EXIT`

## Relationship to altseason

Formal external altseason indices generally compare a broad basket of altcoins against BTC over a rolling horizon. BTC/ETH is only a rotation proxy.

Use the ratio as a layer:
- fast/medium ETH strength = possible alt-rotation attention;
- long BTC-strength regain = possible U10 risk attention.

Do not equate any one cross with a guaranteed altseason.

## Next hypothesis

A combined regime filter may be worth a separate preregistered test:

- TOTAL market-cap trend state;
- BTC/ETH ratio state;
- U10 forward outcome.

Do not implement this combination in production from the current evidence.

## No-repeat

Do not rerun unchanged on the same history.

A new run requires new dates, changed ratio semantics, changed U10 mechanics/universe, or a separately preregistered combined-regime / forward-validation protocol.

## Production boundary

No live/paper, Telegram, exchange, universe, allocation, or execution behavior changed.
