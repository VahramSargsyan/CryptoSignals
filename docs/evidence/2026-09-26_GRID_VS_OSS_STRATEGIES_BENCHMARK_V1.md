# Grid vs Open-Source Published Strategies Benchmark V1

Date: **2026-09-26**  
Strategy under test: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **OSS BENCHMARK EXECUTED / NO PROMOTION**  
Runtime impact: **NONE**  
Migration required: **NO**

## Objective

Compare the current Grid strategy against strategy logic that was independently published by other crypto-trading projects/authors, rather than only against locally authored textbook benchmarks.

No rejected Vahram strategies were included.

## OSS source-selection rules

Only permissively licensed sources were directly transcribed/adapted in the temporary research harness.

### Source A — crypto49er/Gekko-Strategies

- repository: `crypto49er/Gekko-Strategies`
- commit: `653ec5df3a57b751716856a5f9c2c19e6cfa53c3`
- license: **MIT**
- strategy source:
  - `Fibonacci Trendlines.js`
  - `Fibonacci Trendlines.toml`
- source blob SHA:
  - JS: `75433d152f1126e138c08d4cfc6d9a08c7942557`
  - TOML: `96959beadd698eecf1ca26982db20556c0459cb9`

Code semantics used:

- EMA short = 8
- EMA medium = 21
- EMA long = 55
- buy when `EMA8 > EMA21 > EMA55` and not already long
- sell when `EMA21 < EMA55`

The source README prose is not used when it conflicts with executable source. The executable JS is the authority for this test.

### Source B — DeviaVir/zenbot

- repository: `DeviaVir/zenbot`
- branch: `unstable`
- commit: `52872fb4b5f9d10e2f891d95daec05da0b721ceb`
- license: **MIT**

Strategies used:

1. `extensions/strategies/macd/strategy.js`
   - blob SHA: `e85ee7e0f2c5296590bd86d9d23236424e9e88f2`
   - defaults: EMA 12/26, signal 9, overbought RSI 25/70
2. `extensions/strategies/srsi_macd/strategy.js`
   - blob SHA: `597f584c393e9cfded269ad0bcf7a99e5b81a517`
   - defaults used as coded: RSI14, SRSI K=5/D=3, oversold 20, overbought 80, EMA24/200, signal 9
   - note: the declared `srsi_periods=9` option is not actually passed by the source strategy into the SRSI helper; the test preserved executable behavior instead of silently repairing it.
3. `extensions/strategies/bollinger/strategy.js`
   - blob SHA: `8203f26f1e85224e6bd5e6a4783aeeaa770b61a5`
   - defaults: 20-period Bollinger, 2 standard deviations
   - buy below lower band, sell above upper band

Supporting Zenbot indicator source was also reviewed:

- `lib/ema.js`
- `lib/rsi.js`
- `lib/srsi.js`
- `lib/bollinger.js`

No GPL/AGPL strategy source was copied.

## Adaptation boundary

The source strategy **signal logic** was transcribed into a temporary local Python research harness.

The external framework execution engines were **not** reused. All external strategies were executed through one normalized comparison convention:

- long-only;
- full available capital on entry;
- full exit to cash;
- signal determined on completed candle T;
- execution at next candle open T+1;
- 10 bps fee;
- 5 bps slippage.

This is intentional so execution semantics do not advantage one external framework over another.

Important limitation:

- Gekko Fibonacci describes 8/21/55 day EMA behavior and is naturally compatible with the 1D test.
- Zenbot MACD defaults to 1h, SRSI_MACD to 30m, and Bollinger to 1h in the original project. Applying their unchanged candle-count logic to 1D is a **same-data portability benchmark**, not a native-timeframe evaluation of Zenbot.

The temporary OSS adapter is not yet repository-owned reproducible strategy code. Therefore this evidence is research-grade and cannot promote an external strategy.

## Shared dataset

Canonical corrected Binance Spot daily artifacts already used by Grid research:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Evaluation:

- 2023-09-25 through 2026-09-24
- same historical candles as the corrected Grid benchmark
- no external-strategy parameter optimization on this sample

## Aggregate results

| Strategy | 5-asset geometric return | Approx 3y CAGR | Median max DD | Worst asset | Positive assets |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Vahram Grid WIDE +6/+18** | **+253.03%** | **52.27%/yr** | **38.67%** | +82.26% | 5/5 |
| Vahram Grid BASE +1/+10 | +226.41% | 48.34%/yr | **35.98%** | +69.60% | 5/5 |
| **Gekko Fibonacci 8/21/55** | **+225.57%** | **48.21%/yr** | 39.93% | **+125.26%** | **5/5** |
| Buy & Hold | +196.94% | 43.73%/yr | 67.55% | +69.63% | 5/5 |
| Zenbot SRSI_MACD default | +122.65% | 30.58%/yr | 46.24% | +50.21% | 5/5 |
| Zenbot MACD default | +24.52% | 7.58%/yr | 54.80% | -49.34% | 3/5 |
| Zenbot Bollinger default | **-13.59%** | negative | 69.09% | -53.64% | 3/5 |

## Per-asset returns

| Strategy | LINK | ETH | SOL | BNB | BTC |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Grid WIDE** | **+516.80%** | +137.02% | **+818.79%** | +123.98% | +82.26% |
| Grid BASE | **+482.22%** | +119.81% | **+736.06%** | +104.17% | +69.60% |
| **Gekko Fibonacci** | +125.26% | **+180.15%** | +425.13% | **+330.43%** | **+156.43%** |
| Buy & Hold | +89.16% | +69.63% | +503.04% | +271.94% | +220.78% |
| Zenbot SRSI_MACD | +100.59% | +144.90% | +141.48% | +207.08% | +50.21% |
| Zenbot MACD | -49.34% | -8.41% | +82.11% | +149.37% | +42.08% |
| Zenbot Bollinger | -53.64% | -51.29% | +12.09% | +36.50% | +39.43% |

## Per-asset max drawdown

| Strategy | LINK | ETH | SOL | BNB | BTC |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Grid WIDE** | 38.67% | 39.85% | 52.60% | **27.48%** | **17.92%** |
| Grid BASE | **35.98%** | 39.00% | **51.57%** | **26.31%** | **16.26%** |
| Gekko Fibonacci | 55.31% | **37.54%** | 59.42% | 39.93% | 36.54% |
| Buy & Hold | 75.39% | 67.55% | 76.26% | 58.20% | 52.97% |
| Zenbot SRSI_MACD | 77.91% | 42.36% | 46.24% | 58.97% | 39.14% |
| Zenbot MACD | 79.22% | 54.80% | 58.70% | 31.20% | 30.54% |
| Zenbot Bollinger | 69.74% | 72.64% | 69.09% | 44.73% | 39.49% |

## Main findings

### 1. Gekko Fibonacci is a real external competitor

Its 5-asset geometric return (**+225.57%**) is nearly identical to Grid BASE (**+226.41%**) and above Buy & Hold (**+196.94%**).

It beat WIDE on three assets:

- ETH
- BNB
- BTC

Grid WIDE beat it decisively on:

- LINK
- SOL

This means the Grid's aggregate lead is concentrated in the highly oscillatory LINK/SOL behavior rather than universal per-asset superiority.

### 2. Gekko Fibonacci has a stronger worst-asset return

Worst external Fibonacci asset:

- LINK: +125.26%

Worst WIDE asset:

- BTC: +82.26%

Therefore the previously observed WIDE advantage in worst-asset return over textbook benchmarks does **not** survive this stronger external benchmark.

### 3. Grid retains the better aggregate return / risk profile

Despite Fibonacci's cross-asset consistency:

- WIDE geometric: +253.03% vs Fibonacci +225.57%
- WIDE median DD: 38.67% vs Fibonacci 39.93%

BASE is nearly tied on return and has lower median DD:

- BASE +226.41%, median DD 35.98%
- Fibonacci +225.57%, median DD 39.93%

### 4. Strategy families exploit different market structure

Fibonacci trend-following was much stronger on:

- BNB
- BTC
- ETH

Grid was dramatically stronger on:

- LINK
- SOL

This supports a strategy-selection architecture rather than forcing one strategy onto every quality asset.

### 5. Zenbot defaults are not competitive on D1 as-is

The unchanged Zenbot default candle-count rules were designed for intraday periods.

On D1:

- SRSI_MACD remained profitable on all five but below Grid/Fibonacci;
- MACD and Bollinger were weak and produced negative results on some assets.

This should not be interpreted as proof that Zenbot's strategies are poor in their native 30m/1h timeframes.

## Research interpretation

The strongest new observation is not simply "Grid wins."

A better statement is:

> A simple external 8/21/55 trend strategy nearly matches Grid BASE in five-asset aggregate return, beats WIDE on ETH/BNB/BTC, while Grid dominates on LINK/SOL and generally has lower drawdown.

This strengthens the case for future **asset/market-regime strategy routing**:

```text
quality asset
   ↓
path / regime classification
   ├─ oscillatory / wick-rich -> Grid
   ├─ persistent trend -> trend strategy
   └─ weak quality / adverse regime -> no capital
```

## Evidence limits

1. The Grid WIDE parameters were previously researched on these same five assets; this is not fresh OOS superiority evidence.
2. The Gekko/Zenbot strategies were not optimized on this sample, which makes comparison conservative in their favor/against Grid selection bias.
3. Zenbot strategies were transplanted from their native intraday candle counts onto daily data.
4. The temporary adapter is not yet repository-owned executable research code.
5. No strategy here is promoted to paper/live.

## Research decision

1. Preserve Gekko Fibonacci 8/21/55 as a **standing external benchmark**.
2. Do not tune it on the current development history before a future validation test.
3. Future Grid candidate changes should compare against:
   - Grid BASE
   - Grid WIDE
   - Buy & Hold
   - Gekko Fibonacci 8/21/55
4. A later research task may implement the exact MIT external strategies in repository-owned adapters and run native-timeframe Zenbot tests on 1h/30m data.
5. No current paper-live behavior changes.

## Paper-live boundary

No current Grid paper-live profile or symbol universe changed.

No result in this document authorizes live-money execution.
