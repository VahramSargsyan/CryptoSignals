# Strategy Registry Standard v1

Purpose: ensure every strategy has identity, evidence, and a traceable decision.

## Required strategy identity

Each strategy should have:

- `strategy_id`
- `strategy_name`
- `strategy_version`
- `status`
- `source_commit_sha`
- `created_at`
- `supersedes_strategy_id` when applicable
- `notes`

Example:

```text
strategy_id: STRAT-0001
strategy_name: VAHRAM_ORIGINAL
strategy_version: 1.0.0
status: BACKTESTED
```

## Required strategy definition

Record:

- indicators/features;
- entry rule;
- exit rule;
- strength/scoring rule;
- timeframe;
- supported symbols/universe;
- configurable parameters;
- default parameters.

## Required evidence

Every backtest decision must link to:

- `backtest_run_id`;
- dataset/time range;
- symbols;
- timeframe;
- fees;
- slippage;
- trade count;
- metrics;
- regime breakdown;
- code SHA;
- test level.

## Allowed status values

| Status | Meaning |
|---|---|
| IDEA | hypothesis only |
| EXPERIMENTAL | implemented but not sufficiently tested |
| BACKTESTED | historical run completed |
| OUT_OF_SAMPLE_TESTED | tested on held-out data |
| WALK_FORWARD_TESTED | passed walk-forward process |
| ACCEPTED | evidence satisfies current acceptance gate |
| REJECTED | evidence fails current acceptance gate |
| NEEDS_MORE_DATA | insufficient evidence |
| REGIME_SPECIFIC | usable only under specific regime |
| OVERFIT_RISK | suspicious optimization sensitivity |
| PAPER_LIVE | observing on live data without production trust |
| LIVE_SIGNALS | approved for operational alerts |
| RETIRED | intentionally deactivated |

## Acceptance gates

Exact numeric thresholds are intentionally not frozen yet.

They must be defined **before** final evaluation of a candidate strategy to prevent moving the goalposts after seeing results.

The acceptance policy should eventually include:

- minimum trade count;
- maximum acceptable drawdown;
- positive expectancy;
- profit-factor threshold;
- benchmark comparison;
- out-of-sample requirement;
- parameter-stability requirement;
- no critical data-quality issues;
- no known look-ahead leakage.

## Rejection record

A rejected strategy must preserve:

- rejection date;
- reason;
- evidence links;
- failed metrics/gates;
- regimes where it did or did not work;
- whether re-test is allowed;
- conditions that would justify re-testing.

## Regime-specific acceptance

A strategy may be accepted only for a regime.

Example:

```text
Strategy: STRAT-0007
Status: REGIME_SPECIFIC

Allowed:
- BTC_BULL
- HIGH_VOLUME

Blocked:
- BTC_BEAR
- LOW_VOLUME
```

The runtime must not silently treat a regime-specific strategy as globally valid.

## Versioning rule

Changing any of the following creates a new strategy version:

- indicator formula;
- entry/exit logic;
- scoring logic;
- default parameters if behavior changes materially;
- market-regime gate;
- risk rule.

Historical evidence remains attached to the old version.

## Original baseline rule

`VAHRAM_ORIGINAL_V1` is immutable as a research benchmark.

Fixes to naming/documentation are allowed, but behavior changes must create a new strategy/version.
