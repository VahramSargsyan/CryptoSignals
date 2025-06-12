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

    # BUY signal
    if row['Close'] <= row['Lower'] and row['StochRSI'] < 20 and row['Volume'] > row['MA_Volume']:
        strength += (20 - row['StochRSI']) * 2
        bb_diff = (row['Lower'] - row['Close']) / row['Lower']
        strength += min(bb_diff * 100, 30)
        vol_boost = (row['Volume'] - row['MA_Volume']) / row['MA_Volume']
        strength += min(vol_boost * 100, 30)
        return 'BUY', round(min(strength, 100), 1)

    # SELL signal
    elif row['Close'] >= row['Upper'] and row['StochRSI'] > 80 and row['Volume'] > row['MA_Volume']:
        strength += (row['StochRSI'] - 80) * 2
        bb_diff = (row['Close'] - row['Upper']) / row['Upper']
        strength += min(bb_diff * 100, 30)
        vol_boost = (row['Volume'] - row['MA_Volume']) / row['MA_Volume']
        strength += min(vol_boost * 100, 30)
        return 'SELL', round(min(strength, 100), 1)

    return None, 0

def get_action_and_change(signal, strength):
    if strength < 40:
        return "Ignore", 0
    elif strength < 60:
        return "Enter" if signal == "BUY" else "Reduce", 25
    elif strength < 80:
        return "Enter" if signal == "BUY" else "Reduce", 50
    else:
        return "Enter" if signal == "BUY" else "Exit", 100

# Загрузка токенов
tokens_df = pd.read_csv(TOKENS_FILE)
symbols = tokens_df['symbol'].tolist()



results = []
today_str = datetime.now().strftime("%Y-%m-%d")
signals_today = []
signals_recent = []

for symbol in symbols:
    file_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    if not os.path.exists(file_path):
        print(f"{symbol}: file not found, skipping.")
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
                action, change = get_action_and_change(signal, strength)
                result = {
                    "Symbol": symbol,
                    "Signal": signal,
                    "Action": action,
                    "Date": date.strftime("%Y-%m-%d"),
                    "Price": round(row['Close'], 6),
                    "Strength": strength,
                    "Change %": change
                }
                if date.strftime("%Y-%m-%d") == today_str:
                    signals_today.append(result)
                else:
                    signals_recent.append(result)
                break

    except Exception as e:
        print(f"{symbol}: Error — {str(e)}")

# === ВЫВОД ===


# Отдельный блок для всех сигналов (на сегодня + прошлые)
all_signals = signals_today + signals_recent

if all_signals:
    df_all = pd.DataFrame(all_signals)
    df_all.sort_values(by="Symbol", inplace=True)
    df_all.to_csv(OUTPUT_FILE, index=False)

    #  ДОБАВЛЯЕМ ЭТО: вывод полного отчёта в консоль
    print(f"\nFull signal report:\n")
    print(df_all.to_string(index=False))

    print(f"\nFull report saved to {OUTPUT_FILE}.")
else:
    print("No signals in the last 10 days.")


if signals_today:
    print(f"\nSignals found for today ({today_str}):\n")
    df_today = pd.DataFrame(signals_today)
    print(df_today.to_string(index=False))
else:
    print(f"\nNo signals found for today ({today_str}).\n")