# OFFICIAL LIQUIDITY DATA FEASIBILITY V1 — Preregistration

Date: 2026-09-27
Branch: `research/global-macro-risk-regime-v1`
Mode: DIAGNOSTIC_ONLY + ECOSYSTEM_PLANNING
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Goal

Verify that the three components needed for a causal Fed net-liquidity diagnostic can be obtained from official, keyless sources without relying on the currently timing-out FRED CSV endpoint.

Target conceptual identity (not yet a signal):

`NET_LIQUIDITY = FED_TOTAL_ASSETS - TREASURY_GENERAL_ACCOUNT - ON_RRP`

No trading rule, threshold, score, or crypto action is created in this pass.

## Official sources under test

1. Federal Reserve Board H.4.1 Data Download Program package:
   - endpoint: `https://www.federalreserve.gov/datadownload/Output.aspx?rel=H41&filetype=zip`
   - known total-assets series: `RESPPA_N.WW` (WALCL-equivalent)
   - TGA concept in H.4.1 structure: `DEPUSTG` = deposits with Federal Reserve Banks, U.S. Treasury, General Account
   - exact live H.4.1 data-series identifier for TGA MUST be discovered from the official package; do not guess it.

2. Federal Reserve Bank of New York Markets API:
   - ON RRP endpoint: `https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json`
   - test historical availability from 2023-05-05.

## Required evidence

The run must record:

- HTTP success/failure for each official source;
- names contained in the H.4.1 ZIP;
- H.4.1 Series attribute keys;
- exact total-assets series match for `RESPPA_N.WW`;
- every H.4.1 Series candidate whose attributes contain `DEPUSTG` or whose structural metadata describes the Treasury General Account;
- observation counts, first/last dates, units/frequency metadata where exposed;
- NY Fed response schema keys;
- ON RRP observation count and first/last operation dates;
- a small latest-row sample for source-shape verification.

## Decision gate

PASS only if:

- H.4.1 official package is reachable;
- exact total-assets series is present;
- a unique TGA data series can be resolved from official package metadata;
- NY Fed RRP endpoint is reachable with usable historical observations.

If TGA is ambiguous or absent, status must be `BLOCKED_TGA_IDENTITY_UNRESOLVED`; do not infer an identifier.

## Causality note

H.4.1 Wednesday observations are released later (Thursday U.S. time). A later research pass must apply release availability, not observation date, before aligning to crypto candles.

This feasibility pass does not yet align or backtest the series.

## Runtime impact

Production behavior changed: NONE
Paper-live behavior changed: NONE
Migration required: NO
