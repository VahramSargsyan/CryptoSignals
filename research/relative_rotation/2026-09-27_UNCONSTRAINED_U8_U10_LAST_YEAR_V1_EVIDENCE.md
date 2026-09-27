# Unconstrained U8/U10 Last-Year Exhaustive Search v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/unconstrained-u8-u10-last-year-v1
GitHub Actions run: 36336738673
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Scope

Candidate pool:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

No mandatory assets.

Latest fully closed year:
2025-09-27 -> 2026-09-26

Frozen strategy:
- Binance Spot 1D
- 180d rolling median
- ARM 15%
- reversal 3%
- strongest max-dislocation router
- next-open execution
- 0.1% transition cost

Each U8 is evaluated from all 8 member assets.
Each U10 is evaluated from all 10 member assets.
Ranking metric is median terminal return across all starts.

Exhaustive spaces:
- 6,435 U8s
- 3,003 U10s

## Top 10 U8

| Rank | Assets | Median return | Worst start | Best start | Median DD |
|---:|---|---:|---:|---:|---:|
| 1 | TWT PEPE SOL AAVE AVAX FIL ETH ALGO | +241.75% | +41.04% | +293.08% | -62.43% |
| 2 | TWT PEPE AAVE AVAX FIL ETH ALGO XRP | +238.48% | +41.04% | +286.43% | -62.43% |
| 3 | TWT PEPE SOL AAVE AVAX FIL ALGO XRP | +227.53% | +36.48% | +280.36% | -62.43% |
| 4 | PEPE SOL AAVE AVAX FIL ETH ALGO XRP | +207.52% | +17.32% | +219.19% | -58.63% |
| 5 | PEPE SOL LINK AVAX FIL ETH ALGO XRP | +203.05% | +16.12% | +214.55% | -46.80% |
| 6 | PEPE BNB SOL AVAX FIL ETH ALGO XRP | +200.92% | +17.12% | +212.34% | -46.80% |
| 7 | TWT PEPE SOL AVAX FIL ETH ALGO XRP | +196.32% | +10.36% | +207.56% | -60.76% |
| 8 | PEPE SOL AVAX FIL ETH ALGO XRP HBAR | +188.78% | +12.46% | +199.73% | -46.80% |
| 9 | TWT PEPE SOL AAVE FIL ETH ALGO XRP | +187.85% | +19.95% | +234.29% | -59.67% |
| 10 | TWT PEPE SOL AAVE AVAX FIL ALGO HBAR | +187.15% | +35.61% | +277.93% | -62.43% |

Top-10 U8 common core:
PEPE + FIL + ALGO

Top-10 U8 frequencies:
- PEPE 10/10
- FIL 10/10
- ALGO 10/10
- AVAX 9/10
- SOL 9/10
- ETH 8/10
- XRP 8/10
- AAVE 6/10
- TWT 6/10
- HBAR 2/10
- BNB 1/10
- LINK 1/10
- TRX 0/10
- ATOM 0/10
- ADA 0/10

## Top 10 U10

| Rank | Assets | Median return | Worst start | Best start | Median DD |
|---:|---|---:|---:|---:|---:|
| 1 | TWT PEPE SOL AAVE LINK AVAX FIL ETH ALGO XRP | +238.48% | +41.04% | +293.08% | -62.43% |
| 2 | TWT PEPE SOL AAVE AVAX FIL ETH ALGO XRP HBAR | +222.54% | +34.40% | +274.56% | -62.43% |
| 3 | TWT PEPE BNB SOL AAVE AVAX FIL ETH ALGO XRP | +211.75% | +29.91% | +262.04% | -62.43% |
| 4 | TWT PEPE SOL TRX AAVE AVAX FIL ETH ALGO XRP | +191.49% | +21.46% | +238.51% | -62.43% |
| 5= | TWT PEPE SOL AAVE LINK AVAX FIL ETH ALGO HBAR | +184.59% | +34.40% | +274.56% | -62.43% |
| 5= | TWT PEPE AAVE LINK AVAX FIL ETH ALGO XRP HBAR | +184.59% | +34.40% | +268.23% | -62.43% |
| 7 | TWT PEPE SOL AAVE LINK AVAX FIL ALGO XRP HBAR | +175.38% | +30.05% | +262.45% | -62.43% |
| 8= | TWT PEPE BNB SOL AAVE AVAX FIL ETH ALGO HBAR | +175.07% | +29.91% | +262.04% | -62.43% |
| 8= | TWT PEPE BNB AAVE AVAX FIL ETH ALGO XRP HBAR | +175.07% | +29.91% | +255.91% | -62.43% |
| 10 | TWT PEPE SOL TRX AAVE AVAX FIL ETH ALGO HBAR | +169.38% | +26.96% | +253.82% | -62.43% |

Top-10 U10 common core:
TWT + PEPE + AAVE + AVAX + FIL + ALGO

Top-10 U10 frequencies:
- TWT 10/10
- PEPE 10/10
- AAVE 10/10
- AVAX 10/10
- FIL 10/10
- ALGO 10/10
- ETH 9/10
- SOL 8/10
- HBAR 7/10
- XRP 7/10
- LINK 4/10
- BNB 3/10
- TRX 2/10
- ATOM 0/10
- ADA 0/10

## Full distributions

U8:
- 6,435 universes
- median universe return: +5.63%
- 95th percentile: +100.26%
- best: +241.75%

U10:
- 3,003 universes
- median universe return: +30.25%
- 95th percentile: +111.47%
- best: +238.48%

Canonical U8 rank among all unconstrained U8s:
4,911 / 6,435
Return: -24.10%

Research U10 rank among all unconstrained U10s:
357 / 3,003
Return: +90.21%

U9_CLEANER context:
+90.61% over the same year.

## Top-50 robustness frequencies

U8 top 50:
- ALGO 50/50
- FIL 50/50
- AVAX 48/50
- PEPE 44/50
- ETH 38/50
- SOL 33/50
- XRP 32/50
- AAVE 24/50
- TWT 21/50
- HBAR 20/50
- LINK 17/50
- BNB 13/50
- TRX 8/50
- ATOM 2/50

U10 top 50:
- ALGO 50/50
- FIL 50/50
- AVAX 48/50
- PEPE 46/50
- AAVE 42/50
- ETH 41/50
- SOL 41/50
- XRP 40/50
- TWT 35/50
- HBAR 30/50
- TRX 24/50
- BNB 23/50
- LINK 23/50
- ATOM 5/50

## Interpretation

1. PEPE remains prominent without any mandatory-asset constraint:
   - 10/10 top U8
   - 10/10 top U10
   - 44/50 top U8
   - 46/50 top U10

2. FIL and ALGO are even more persistent:
   - both appear in every top-10 U8/U10;
   - both appear in every top-50 U8/U10 except ALGO is 50/50 and FIL 50/50 exactly.

3. ATOM is almost absent from the unconstrained winner set:
   - 0/10 top U8/U10
   - 2/50 top U8
   - 5/50 top U10.
   Therefore mandatory ATOM materially constrains the historical optimum.

4. TWT is mixed:
   - 6/10 top U8;
   - 10/10 top U10.
   It is much more compatible with strong larger graphs than ATOM in this one-year regime.

5. The unconstrained top combinations are hindsight rankings of one known year, not deployable selectors.

6. The prominence of ALGO requires a dedicated bull-run/topology audit before treating it as a candidate, because prior price-path diagnostics classified ALGO as historically explosive.

No live files changed.
