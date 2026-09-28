# Relative Rotation Forward Hypothesis Registry v1

Date: 2026-09-28
Mode: ECOSYSTEM_PLANNING
Status: PREPARED / FORWARD ACTIVATION BLOCKED UNTIL TARGET U10 FREEZE

## Purpose

Separate the accepted Relative Rotation trading core from research hypotheses that are promising but not yet accepted as strategy rules.

The registry defines what is accepted, what remains a hypothesis, what future real-market event activates observation, what evidence must be recorded, when a hypothesis may be reviewed for promotion or rejection, and what Telegram reminder should say.

This registry does not alter trading behavior.

## Universe activation gate

The current TARGET U9 is transitional. The intended steady state is a new TARGET U10 after a tenth candidate is selected.

Forward validation MUST NOT start while membership is still changing.

Activation prerequisites:

1. tenth TARGET candidate selected;
2. final TARGET U10 membership committed;
3. U10 membership version frozen;
4. forward start date recorded;
5. no backfill before that date may count as forward evidence;
6. all hypotheses below use the same frozen U10 reference until a formally versioned universe change.

Until then: FORWARD_VALIDATION_STATUS = BLOCKED_UNIVERSE_NOT_FROZEN.

## Accepted core

### RR_CORE_ROTATION_V1

Status: ACCEPTED_PAPER_LIVE / MANUAL_EXECUTION_ONLY

Frozen core mechanics:
- Binance Spot 1D closed candles;
- pair ratio = right / left;
- rolling median = 180 days;
- ARM = 15%;
- post-ARM extreme tracking;
- reversal confirmation = 3%;
- strongest confirmed max-dislocation router;
- manual execution only;
- no exchange API trading keys.

The 10% Telegram watch is an alerting layer only, not a trading rule.

## Active forward hypotheses

### HYP-RR-001 — SINGLE_MONTH_SURGE95_P1_FRACTAL

Status: NEEDS_FORWARD_EVIDENCE

Historical description: a single selected calendar month with frozen-U10 reference-equity displacement >= +95% may mark an explosive overheat regime with elevated near-term pullback risk.

Historical research candidate:
- trigger: one selected month >= +95%;
- P1 cash-out trigger: >=5% pullback from post-surge running peak;
- hypothetical cash sleeve: 30%;
- re-entry search activates after reference is >=15% below latest running peak;
- 5-bar confirmed swing-high buy-stop on currently held frozen-U10 asset;
- lower confirmed pivots ratchet stop downward;
- asset rotation resets the stop;
- no timeout fallback.

Forward activation: a completed selected calendar month in the final frozen TARGET U10 reaches >= +95%.

Telegram reminder heading: 🧪 ГИПОТЕЗА HYP-RR-001 АКТИВИРОВАНА

Telegram must state:
- this is NOT an accepted trade rule;
- selected month and move;
- current U10 reference equity;
- historical hypothesis: explosive +95% month may precede a substantial pullback;
- forward observation starts now;
- P1/5-bar is tracked only as a shadow paper overlay.

Evidence to record:
- activation date;
- selected monthly displacement;
- U10 reference equity at trigger;
- running peak after trigger;
- worst running drawdown at 31 / 62 / 93 days;
- whether -15%, -25%, -35% were reached;
- hypothetical P1 cash-out date;
- hypothetical 5-bar re-entry date;
- shadow-overlay terminal delta versus accepted U10 over completed cycle;
- cash days;
- unfinished-cycle flag;
- fees/slippage model version.

Decision gates:
- first future event: evidence only; no promotion;
- after 3 distinct forward calendar episodes: mandatory review;
- descriptive hypothesis gets FORWARD_SUPPORTED only if at least 2/3 events reach >=25% running pullback within 62 days;
- P1/5-bar may advance only to PAPER_OVERLAY_CANDIDATE if aggregate shadow-overlay equity is not below accepted-U10 baseline, no unfinished cash cycle remains at review, and no thresholds changed after forward start;
- 0/3 or 1/3 direct pullback hits -> FORWARD_REJECTED for this version;
- ambiguous evidence -> NEEDS_MORE_FORWARD_EVIDENCE;
- real-money promotion requires separate decision and at least 5 distinct forward episodes.

These are governance gates, not a statistical guarantee.

### HYP-RR-002 — MULTIMONTH_STREAK95_PULLBACK

Status: NEEDS_FORWARD_EVIDENCE / DIAGNOSTIC_ONLY

Frozen trigger:
- minimum 2 consecutive positive selected months;
- cumulative selected-equity gain >=95%;
- zero/negative selected month resets the streak;
- a single +95% month alone does not qualify;
- record overlap/non-overlap with HYP-RR-001.

No trading action is attached to this hypothesis.

Telegram heading: 🧪 ГИПОТЕЗА HYP-RR-002: МНОГОМЕСЯЧНЫЙ ПЕРЕГРЕВ

Telegram must state:
- diagnostic only;
- no sell instruction;
- streak months;
- cumulative gain;
- overlap/non-overlap with HYP-RR-001;
- start 31/62/93-day pullback observation.

Evidence to record:
- streak start/end;
- cumulative gain;
- overlap with single-month surge95;
- worst running drawdown at 31 / 62 / 93 days;
- hits at -15 / -25 / -35%;
- whether a new high occurred after an intermediate 5/10/15/20% correction.

Decision gates:
- no trading implementation can be accepted from this hypothesis alone;
- after 3 distinct NON-OVERLAP forward episodes: mandatory descriptive review;
- FORWARD_SUPPORTED_REGIME_LABEL if at least 2/3 reach >=25% running pullback within 93 days;
- 0/3 or 1/3 -> FORWARD_REJECTED as a useful -25%/93d regime label;
- any future exit/cash-out implementation requires a new preregistered hypothesis.

### HYP-RR-003 — DEFENSIVE_LOW_VOL_BREADTH

Status: NEEDS_REVALIDATION_AFTER_TARGET_U10_FREEZE

Historical candidate:
- breadth = number of TARGET tokens above causal SMA200;
- enter defensive candidate after 3 consecutive closes with breadth <=3;
- select lowest 30d realized-volatility TARGET token at entry;
- exit after 3 consecutive closes with breadth >=5.

Current state: disabled during U9 -> U10 membership migration because thresholds were not validated for the final U10 composition.

After U10 freeze:
- recompute historical diagnostics under frozen U10 membership;
- preregister unchanged or revised thresholds before forward use;
- only then may Telegram emit this hypothesis reminder.

Telegram when enabled: 🧪 ЗАЩИТНАЯ ГИПОТЕЗА: условия совпали — НЕ ПРИНЯТО КАК ТОРГОВОЕ ПРАВИЛО.

## Rejected / do-not-retry rules

HYP-RR-R01 — 65-day forced re-entry — REJECTED: reduced terminal performance when natural re-entry occurred later.
HYP-RR-R02 — old locked-peak reclaim — REJECTED: too eager.
HYP-RR-R03 — 3-bar fractal re-entry — REJECTED_AS_PRIMARY: generally too eager and regime-dependent.
HYP-RR-R04 — completed-negative-month + 5-bar lower-high exit — REJECTED: too slow; often sold after 25-35% correction.
HYP-RR-R05 — Recovery15 / 98% fallback — REJECTED: collapsed into forced ~day-16 re-entry and weakened stronger controls.

Rejected rules do not generate Telegram hypothesis alerts.

## Telegram integration contract

After the final TARGET U10 is frozen, every daily paper-live message is ordered as:
1. accepted strategy signal/status;
2. hypothesis matches, if any;
3. explicit research-only boundary.

Example:
🚨 РОТАЦИЯ ПОДТВЕРЖДЕНА
ATOM -> TWT ...

🧪 ПАРАЛЛЕЛЬНАЯ ГИПОТЕЗА HYP-RR-001
Текущий U10 также находится в режиме single-month SURGE95.
Это не меняет основной сигнал и не является дополнительной торговой командой.
Начинаем forward-наблюдение по заранее зафиксированным правилам.

If no hypothesis matches, do not add a hypothesis section.
If a hypothesis matches without an accepted trade signal, Telegram may still send the research alert because the forward event itself must be timestamped when it occurs.

## Forward evidence storage

Required future files:
- research/relative_rotation/FORWARD_HYPOTHESIS_LOG.md
- machine-readable append-only CSV/JSON artifact per daily run;
- one row per hypothesis activation;
- later outcome rows linked by stable event_id.

Event ID form: <hypothesis_id>__<frozen_universe_version>__<activation_date>.

No historical event may be inserted with an activation date before the frozen forward start.

## Promotion boundary

A Telegram hypothesis reminder is NOT a strategy signal, an exchange order, permission to override the accepted core, or proof that the hypothesis is true.

Promotion requires a documented decision after the preregistered forward evidence gate is met.

## Next action after tenth candidate selection

1. version the final universe;
2. set forward_validation_start;
3. activate HYP-RR-001 and HYP-RR-002 tracking;
4. revalidate HYP-RR-003 against frozen U10 before enabling it;
5. add Telegram hypothesis blocks;
6. create append-only forward hypothesis log;
7. do not backfill old candles as forward evidence.
