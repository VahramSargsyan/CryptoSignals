import os
import pandas as pd
from utils import calculate_bollinger_bands, calculate_stoch_rsi
from datetime import datetime, timedelta

DATA_DIR = "data"
TOKENS_FILE = "tokens.csv"
OUTPUT_FILE = "v_plus_signals_report.csv"
DAYS_BACK = 10  # Сколько дней назад считаются "актуальные" сигналы

# Загрузка токенов
tokens_df = pd.read_csv(TOKENS_FILE)
symbols = tokens_df['symbol'].tolist()

# Таблица итогов
results = []

print("📊 Анализ по стратегии Bollasto V+ (объёмный фильтр):\n")

for symbol in symbols:
    path = os.path.join(DATA_DIR, f"{symbol}.csv")
    if not os.path.exists(path):
        print(f"⚠️ {symbol}: файл не найден → пропущен")
        continue

    try:
        df = pd.read_csv(path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values(by='Date', inplace=True)
        df.set_index('Date', inplace=True)

        # Индикаторы
        df = calculate_bollinger_bands(df)
        df = calculate_stoch_rsi(df)
        df['MA_Volume'] = df['Volume'].rolling(20).mean()

        # Фильтруем только последние N дней
        cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
        recent = df[df.index >= cutoff_date]

        # Поиск последнего сигнала (по убыванию)
        for date, row in recent[::-1].iterrows():
            buy_cond = (
                row['Close'] <= row['Lower'] and
                row['StochRSI'] < 20 and
                row['Volume'] > row['MA_Volume']
            )
            sell_cond = (
                row['Close'] >= row['Upper'] and
                row['StochRSI'] > 80 and
                row['Volume'] > row['MA_Volume']
            )
            if buy_cond:
                results.append({
                    "Symbol": symbol,
                    "Signal": "BUY",
                    "Date": date.strftime("%Y-%m-%d"),
                    "Price": round(row['Close'], 6)
                })
                break
            elif sell_cond:
                results.append({
                    "Symbol": symbol,
                    "Signal": "SELL",
                    "Date": date.strftime("%Y-%m-%d"),
                    "Price": round(row['Close'], 6)
                })
                break

    except Exception as e:
        print(f"❌ Ошибка при анализе {symbol}: {str(e)}")
        continue

# Вывод
if results:
    df_result = pd.DataFrame(results)
    df_result.sort_values(by="Symbol", inplace=True)
    df_result.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Найдено {len(df_result)} сигналов. Отчёт сохранён в {OUTPUT_FILE}:\n")
    print(df_result.to_string(index=False))
else:
    print("🟡 Сигналов за последние дни не найдено.")
