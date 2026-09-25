# OSS Reuse Shortlist v1

Purpose: record external open-source projects that can accelerate CryptoSignals without blindly importing entire architectures.

Rule:

```text
DISCOVER
 -> LICENSE CHECK
 -> ARCHITECTURE/FIT REVIEW
 -> SECURITY REVIEW
 -> EXTRACT PATTERN OR SMALL COMPONENT
 -> ADAPTER
 -> LOCAL TESTS
 -> REUSE RECORD
```

Unknown or incompatible license = no direct code adoption.

---

## 1. CryptoSignal

Repository:

https://github.com/cryptosignal/crypto-signal

License: MIT  
Language: Python  
Observed maturity at research time: ~5.6k stars, ~1.3k forks  
Last push observed: 2024-07-07

### Why relevant

Very close to the current CryptoSignals purpose:

- multi-market technical analysis;
- indicator analyzers;
- configurable signals;
- notifications;
- Binance support;
- scalable pair configuration.

### Candidate parts to study / adapt

Architecture:

- `app/analysis.py`
- `app/analyzers/`
- `app/behaviour.py`
- `app/exchange.py`
- `app/notification.py`
- `app/notifiers/`

Especially useful concepts:

- analyzer separation;
- configurable indicators;
- notifier abstraction;
- Telegram notifier;
- exchange abstraction;
- pair configuration.

### Reuse classification

`ADAPT`

MIT makes selective code reuse possible, but only with attribution/license compliance and local fit review.

Do not import the whole framework by default.

---

## 2. w1ld3r/crypto-signal

Repository:

https://github.com/w1ld3r/crypto-signal

License: MIT  
Language: Python  
Fork/extension of CryptoSignal

### Why relevant

Adds useful signal-analysis concepts including:

- StochRSI Cross;
- Increase In Volume;
- ADX;
- MACD Cross;
- MA Ribbon;
- Squeeze Momentum;
- broad all-pairs scanning;
- charts and richer notifications.

### Candidate parts to study / adapt

Focus on:

- indicator extension pattern;
- StochRSI-related implementation;
- volume-related signal logic;
- multi-pair configuration;
- notification formatting.

### Reuse classification

`ADAPT`

Do not assume all calculations are correct for our needs; validate formulas independently.

---

## 3. MoniGoMani

Repository:

https://github.com/Rikj000/MoniGoMani

License: GPL-3.0  
Archived: YES  
Observed maturity: ~1k stars

Relevant files:

- `user_data/strategies/MasterMoniGoManiHyperStrategy.py`
- `user_data/strategies/MoniGoManiHyperStrategy.py`
- `user_data/hyperopts/MGM_WeightedMultiParameterHyperOptLoss.py`
- `user_data/hyperopts/MGM_SortinoHyperOptLoss.py`
- `user_data/hyperopts/MGM_WinRatioAndProfitRatioHyperOptLoss.py`

### Why relevant

The project is especially relevant to our planned signal-strength laboratory because it works with:

- multiple signals;
- weighted contributions;
- threshold-based strategy decisions;
- parameter optimization;
- multiple optimization/loss concepts.

This is conceptually close to the current CryptoSignals `Strength 0–100` idea.

### Candidate parts to study

- weighted signal architecture;
- parameter organization;
- optimization workflow;
- loss/fitness design;
- regime/parameter experimentation patterns.

### Reuse classification

`REFERENCE_ONLY` by default.

Reason: GPL-3.0 is a stronger copyleft license. Direct code copying into a differently licensed future application must not happen casually.

We can safely learn from concepts and independently implement ideas. Any direct reuse requires a specific license decision.

---

## 4. intelligent-trading-bot

Repository:

https://github.com/asavinov/intelligent-trading-bot

License: MIT  
Language: Python  
Observed maturity: ~1.8k stars / ~400 forks  
Recent activity observed in 2026.

Relevant areas:

- `scripts/download.py`
- `scripts/features.py`
- `scripts/signals.py`
- `scripts/simulate.py`
- `scripts/predict.py`
- `scripts/predict_rolling.py`
- `common/backtesting.py`
- `common/gen_features.py`
- `common/gen_signals.py`

### Why relevant

Useful bridge from rule-based signals toward:

- feature engineering;
- backtesting;
- simulation;
- rolling prediction;
- ML experiments.

### Candidate parts to study / adapt

- separation of download/features/signals/simulation;
- reusable feature-generation layout;
- backtesting organization;
- rolling evaluation concepts;
- model/result storage patterns.

### Reuse classification

`ADAPT`

MIT permits selective reuse with compliance. Prefer small components/patterns rather than adopting the entire project.

---

## 5. Freqtrade

Repository:

https://github.com/freqtrade/freqtrade

License: GPL-3.0  
Language: Python  
Observed maturity: ~54k stars / ~11k forks  
Actively maintained.

Relevant areas:

- `freqtrade/optimize/backtesting.py`
- `freqtrade/optimize/analysis/`
- `freqtrade/optimize/hyperopt/`
- `freqtrade/optimize/hyperopt_loss/`
- `freqtrade/optimize/optimize_reports/`
- `freqtrade/data/dataprovider.py`
- `freqtrade/data/history/`
- `freqtrade/data/metrics.py`

### Why relevant

This is a mature reference for:

- backtesting;
- historical data handling;
- metrics;
- dry-run;
- strategy optimization;
- trade analysis;
- exchange abstractions.

### Candidate parts to study

Primarily methodology and architecture:

- backtest semantics;
- look-ahead protections;
- data-provider patterns;
- metric definitions;
- hyperparameter optimization concepts;
- reporting.

### Reuse classification

`REFERENCE_ONLY` by default.

Reason: GPL-3.0. Direct code reuse may impose license obligations on derivative work.

The preferred path is independent implementation of required behavior, informed by the architecture and documentation.

---

# Initial harvest priority

## Priority A — immediate research

1. CryptoSignal — analyzer/notifier architecture
2. w1ld3r/crypto-signal — indicator extensions
3. MoniGoMani — weighted-signal concepts
4. Freqtrade — backtest methodology
5. intelligent-trading-bot — feature/backtest/rolling evaluation layout

## Priority B — possible selective MIT reuse

Candidates:

- CryptoSignal notifier abstraction;
- CryptoSignal Telegram notifier patterns;
- CryptoSignal analyzer separation;
- intelligent-trading-bot feature/backtest organization.

Every copied/adapted code block must record:

- source repository;
- source path;
- source commit SHA;
- license;
- local destination;
- modifications;
- reason for adoption.

## Priority C — concept only unless license decision changes

- MoniGoMani GPL code;
- Freqtrade GPL code.

---

# Reuse principle for CryptoSignals

The target is **not** to assemble a Frankenstein application from five repositories.

The target is:

```text
keep our simple product model
+ harvest proven patterns
+ selectively adapt compatible components
+ independently verify every formula
+ preserve local tests and evidence
```
