import os
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client

# === Настройки ===
DATA_DIR = "data"
TOKENS_FILE = "tokens.csv"

INTERVAL = Client.KLINE_INTERVAL_1DAY
START_DEFAULT = "1 Jan 2021"

client = Client()

# === Загрузка списка токенов ===
tokens_df = pd.read_csv(TOKENS_FILE)
symbols = tokens_df['symbol'].tolist()

def fetch_klines(symbol, start_time):
    """Запрос свечей с Binance начиная с указанного времени"""
    return client.get_historical_klines(symbol, INTERVAL, start_time)

# === Обработка каждого токена ===
for symbol in symbols:
    file_path = os.path.join(DATA_DIR, f"{symbol}.csv")

    # Если файл существует — определяем последнюю дату
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        existing_df['Date'] = pd.to_datetime(existing_df['Date'])
        last_date = existing_df['Date'].max()
        fetch_from = (last_date + timedelta(days=1)).strftime("%d %b %Y %H:%M:%S")
    else:
        existing_df = pd.DataFrame()
        fetch_from = START_DEFAULT

    # Получаем новые свечи
    new_klines = fetch_klines(symbol, fetch_from)

    if not new_klines:
        print(f"⏭ {symbol}: No new data found.")
        continue

    # Преобразование в DataFrame
    df_new = pd.DataFrame(new_klines, columns=[
        'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
        '_', '_', '_', '_', '_', '_'
    ])
    df_new['Date'] = pd.to_datetime(df_new['timestamp'], unit='ms')
    df_new = df_new[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df_new[['Open', 'High', 'Low', 'Close', 'Volume']] = df_new[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)

    # Объединяем и сохраняем
    df_all = pd.concat([existing_df, df_new], ignore_index=True)
    df_all.drop_duplicates(subset='Date', inplace=True)
    df_all.sort_values(by='Date', inplace=True)
    df_all.to_csv(file_path, index=False)

    print(f"✅ {symbol}: added {len(df_new)} new candles.")

print("\n🏁 Update complete.")
