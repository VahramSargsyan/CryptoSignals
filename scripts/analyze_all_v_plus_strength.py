import os
import pandas as pd
from utils import calculate_bollinger_bands, calculate_stoch_rsi
from datetime import datetime, timedelta

DATA_DIR = "data"
TOKENS_FILE = "tokens.csv"
OUTPUT_FILE = "v_plus_strength_signals.csv"
DAYS_BACK = 10

def calculate_signal_strength(row):
    strength = 0

    # Стратегия BUY
    if row['Close'] <= row['Lower'] and row['StochRSI'] < 20 and row['Volume'] > row['MA_Volume']:
        strength += (20 - row['StochRSI']) * 2  # до 40 баллов
        bb_diff = (row['Lower'] - row['Close']) / row['Lower']
        strength += min(bb_diff * 100, 30)       # до 30 баллов
        vol_boost = (row['Volume'] - row['MA_Volume']) / row['MA_Volume']
        strength += min(vol_boost * 100, 30)      # до 30 баллов
        return 'BUY', round(min(strength, 100), 1)

    # Стратегия SELL
    elif row['Close'] >= row['Upper'] and row['StochRSI'] > 80 and row['Volume'] > row['MA_Volume']:
        strength += (row['StochRSI'] - 80) * 2
        bb_diff = (row['Close'] - row['Upper']) / row['Upper']
        strength += min(bb_diff * 100, 30)
        vol_boost = (row['Volume'] - row['MA_Volume']) / row['MA_Volume']
        strength += min(vol_boost * 100, 30)
        return 'SELL', round(min(strength, 100), 1)

    return None, 0

def get_entry_percent(strength):
    if strength < 40:
        return 0
    elif strength < 60:
        return 25
    elif strength < 80:
        return 50
    else:
        return 100

# Загрузка токенов
tokens_df = pd.read_csv(TOKENS_FILE)
symbols = tokens_df['symbol'].tolist()

results = []
print("📊 Bollasto V+ with Signal Strength\n")

for symbol in symbols:
    file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    if not os.path.exists(file_path):
        print(f"⚠️ {symbol}: file not found, skipping.")
        continue

    try:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df.sort_index(inplace=True)

        df = calculate_bollinger_bands(df)
        df = calculate_stoch_rsi(df)
        df['MA_Volume'] = df['Volume'].rolling(20).mean()

        recent = df[df.index >= (datetime.now() - timedelta(days=DAYS_BACK))]

        for date, row in recent[::-1].iterrows():
            signal, strength = calculate_signal_strength(row)
            if signal:
                results.append({
                    "Symbol": symbol,
                    "Signal": signal,
                    "Date": date.strftime("%Y-%m-%d"),
                    "Price": round(row['Close'], 6),
                    "Strength": strength,
                    "Entry %": get_entry_percent(strength)
                })
                break

    except Exception as e:
        print(f"❌ {symbol}: Error — {str(e)}")

# Вывод
if results:
    df_result = pd.DataFrame(results)
    df_result.sort_values(by="Symbol", inplace=True)
    df_result.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved report to {OUTPUT_FILE}:\n")
    print(df_result.to_string(index=False))
else:
    print("🟡 No strong signals found in last days.")
