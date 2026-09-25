# Governance Source

Universal governance version: **VAHRAM_APP_GOVERNANCE v1.0.0**

Canonical universal source:
`VahramSargsyan/vbos-app/docs/governance/VAHRAM_APP_UNIVERSAL_GOVERNANCE_v1.0.0.md`

Canonical investment-specific overlay:
`VahramSargsyan/investment-cases` -> `PROJECT_GOVERNANCE.md`

Local profile: **INVESTMENT_STRATEGY_LAB**

Historical origin includes the v0.4.9 APP workflow/Golden Architecture package plus later VBOS/HMT governance improvements consolidated into v1.0.0.

## Local self-contained rule

This repository keeps a small local governance adapter so it remains safe and understandable when opened independently.

It does not require a Git submodule or mandatory cross-repository fetch for basic P0 discovery.

The Investment Lab owns investment-specific governance; this repository owns CryptoSignals-specific constraints.

## Sync rule

When universal governance changes materially:

1. update the canonical universal governance in `vbos-app/docs/governance/`;
2. bump the governance version;
3. update the central GOVERNANCE_REGISTRY;
4. synchronize affected active repositories;
5. record project-specific deviations explicitly.

Silent divergence is not allowed.
