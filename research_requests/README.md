# Strategy Lab research requests

Files in this directory are immutable request manifests used to trigger bounded GitHub Actions research runs from branches named:

research-run/**

A request file records the intended data boundary and cost assumptions before results are observed.

Current discipline:

- 2026 remains protected as out-of-sample until acceptance gates are frozen.
- Initial H7 comparison uses 2021-01-01 through the close of 2025-12-31.
- Research request JSON files are created on short-lived research-run branches.
- Large downloaded candles and detailed evidence are workflow artifacts, not committed market-history blobs.
- A workflow result does not make a strategy ACCEPTED.
