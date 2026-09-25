# Governance Source

Local governance version: **VAHRAM_APP_GOVERNANCE v0.4.9**

Origin:

- Vahram APP v0.4.9 workflow/guardrail package;
- VBOS `PROJECT_GOVERNANCE.md`;
- VBOS `AGENTS.md`;
- VBOS production schema mutation rules;
- VBOS Golden Architecture / Google Sheets performance rules.

## Why the rules are copied locally

Repositories must remain understandable and safe when opened independently.

Therefore this repository does **not** require a runtime dependency, Git submodule, or mandatory cross-repository read just to discover P0 rules.

A future central Investment Lab may hold the canonical investment-specific overlay, but every active repository should keep a small local governance file with:

- source version;
- P0 rules;
- project-specific constraints;
- sync note.

## Sync rule

When governance changes materially:

1. update the canonical governance source;
2. bump the governance version;
3. update local copies/adapters in affected repositories;
4. record project-specific deviations explicitly.

Silent divergence is not allowed.
