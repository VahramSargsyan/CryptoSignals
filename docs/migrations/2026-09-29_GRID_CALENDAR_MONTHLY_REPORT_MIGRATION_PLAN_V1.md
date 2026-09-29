# Grid Paper Live Calendar Monthly Report — Migration Plan V1

Date: 2026-09-29
Workflow mode: PATCH_FIX

## Objective

Add an automatic calendar month-end paper-live report to Grid Paper Live v1 without changing any grid trading rule.

The report is generated when the latest fully closed daily candle is the final UTC calendar day of a month.

## Scope

Changed:
- Grid paper-live report payload gains optional `monthly_report`;
- month-end Telegram/email text becomes a compact monthly summary;
- report.md gains full month-end profile and per-asset tables.

Unchanged:
- strategy formulas;
- H/L rules;
- entry/exit rules;
- profiles;
- symbols;
- fee/slippage assumptions;
- paper capital;
- schedule;
- broker/exchange behavior (none).

## Schema addition

Optional top-level report.json field:

`monthly_report`

Shape:

- period
- month_start
- month_end
- profile_summary[]
- asset_summary[]
- highest_month_return_profile
- lowest_month_return_profile
- paper_only

Each profile summary records:
- month start/end equity
- month return
- month max drawdown
- inception return
- inception max drawdown
- monthly BUY / SELL counts
- monthly closed trades
- current open Micro / Mid lots

Each asset summary records the same month-level diagnostics per profile/symbol.

The field is `null` on non-month-end runs.

## First month behavior

Paper observation starts 2026-09-26.

For September 2026, the month report therefore covers only the forward period from 2026-09-26 through 2026-09-30. It must not backfill September 1-25 as paper-live evidence.

For later months, the month return baseline is the last available paper equity before the first day of that month.

## Notification rule

Month-end report is notification-worthy even when there is no BUY/SELL on the latest candle.

Existing WEEK_1 and MONTH_1 milestones remain for backward compatibility and do not replace calendar month-end reporting.

## Rollback

Revert:
- `scripts/run_grid_paper_live.py`
- `scripts/send_grid_paper_report.py`
- `tests/test_grid_paper_live.py`
- documentation changes.

Old consumers remain compatible because `monthly_report` is additive and optional.

## Acceptance

- month-end detection handles 28/29/30/31-day months;
- non-month-end run has no monthly report;
- first partial September report starts at paper_start;
- month-end notification is generated without requiring a trade signal;
- existing grid tests remain green;
- live-data workflow dry run succeeds;
- no real orders are introduced.

## Residual risks

- GitHub scheduled workflows can run late; month-end detection is based on the latest closed candle, not wall-clock date, so a delayed Oct 1 run can still report Sep 30 correctly;
- the report reconstructs paper state from source data on each run, consistent with existing stateless design;
- monthly metrics are forward-paper observations, not independent out-of-sample proof of strategy profitability.
