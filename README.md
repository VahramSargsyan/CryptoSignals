# CryptoSignals Strategy Laboratory

CryptoSignals is evolving from a single crypto signal scanner into a **strategy research laboratory** with reproducible evidence.

## Current baseline

The existing repository already contains:

- Binance OHLCV ingestion;
- daily candle storage in CSV;
- Bollinger Bands;
- the current project-specific "StochRSI" calculation;
- volume filter;
- BUY / SELL signal generation;
- signal strength scoring;
- a small tracked universe of crypto pairs.

The original behavior must be preserved as a frozen benchmark named:

`VAHRAM_ORIGINAL_V1`

Do not silently "fix" or overwrite it. New indicator definitions and strategy variants must be implemented as separate versions and compared against the baseline.

## Target product

The target is a research platform that can:

1. ingest historical and live market data;
2. store data and evidence in Google Sheets / Drive;
3. run many strategy variants against the same datasets;
4. classify strategies as accepted, rejected, regime-specific, overfit-risk, or needing more data;
5. keep rejected strategies as research evidence instead of deleting them;
6. detect broad market regimes and test how regime changes affect strategy performance;
7. send only approved live/paper signals to Telegram;
8. preserve code, strategy versions, tests, and research records in GitHub;
9. later separate Research/Lab from Production/Live if the project becomes operationally mature.

## Architecture documents

Read in this order:

1. [Strategy Lab Blueprint](docs/00_STRATEGY_LAB_BLUEPRINT.md)
2. [Roadmap](docs/01_STRATEGY_LAB_ROADMAP.md)
3. [Strategy Registry Standard](docs/02_STRATEGY_REGISTRY_STANDARD.md)
4. [OSS Reuse Shortlist](docs/03_OSS_REUSE_SHORTLIST.md)
5. [Data & Backtest Standard](docs/04_DATA_BACKTEST_STANDARD.md)

## Core rule

```text
IDEA
  -> EXPERIMENTAL
  -> BACKTESTED
  -> OUT_OF_SAMPLE_TESTED
  -> WALK_FORWARD_TESTED
  -> ACCEPTED or REJECTED
  -> PAPER_LIVE
  -> LIVE_SIGNALS
```

A strategy is not "good" because a chart looks convincing. It must carry reproducible evidence.

## Research / production split

Initially keep all strategy research in this repository.

If accepted strategies later become operationally important, separate runtime responsibilities:

```text
CryptoSignals-Lab
    research / experiments / rejected strategies / backtests
                |
                v
CryptoSignals-Live
    accepted strategies / live data / Telegram / production controls
```

Do not create one repository per strategy unless there is a strong operational reason.

## Current implementation policy

This branch is documentation-only. No trading logic, Google Sheets schema, deployment, or Telegram runtime has been changed yet.
