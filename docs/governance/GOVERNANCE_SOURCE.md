# Governance Source

Local governance version: **VAHRAM_APP_GOVERNANCE v0.4.9**

Canonical investment overlay:
`VahramSargsyan/investment-cases` -> `PROJECT_GOVERNANCE.md`

Upstream origin:

- Vahram APP v0.4.9 workflow/guardrail package;
- VBOS `PROJECT_GOVERNANCE.md`;
- VBOS `AGENTS.md`;
- VBOS production schema mutation rules;
- VBOS Golden Architecture / Google Sheets performance rules.

## Local self-contained rule

This repository keeps its own local governance adapter so it remains safe and understandable when opened independently.

It does **not** require a Git submodule or mandatory cross-repository read for basic P0 discovery.

The canonical Investment Lab repository owns the investment-specific governance overlay; this repository owns CryptoSignals-specific constraints.

## Sync rule

When governance changes materially:

1. update the canonical Investment Lab governance source;
2. bump the governance version;
3. synchronize affected local adapters;
4. record project-specific deviations explicitly.

Silent divergence is not allowed.
