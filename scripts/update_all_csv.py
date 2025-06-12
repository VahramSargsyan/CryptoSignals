import os
import pandas as pd
from datetime import datetime, timedelta
from utils import fetch_klines, merge_and_save

# === Настройки ===
DATA_DIR = "data"
TOKENS_FILE = "tokens.csv"

# === Загрузка списка токенов ===
tokens_df = pd.read_csv(TOKENS_FILE)
symbols = tokens_df['symbol'].tolist()

for symbol in symbols:
    file_path = os.path.join(DATA_DIR, f"{symbol}.csv")

    # Проверяем наличие файла и дату последней свечи
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        existing_df['Date'] = pd.to_datetime(existing_df['Date'])
        last_date = existing_df['Date'].max()
        fetch_from = (last_date + timedelta(days=1)).strftime("%d %b %Y %H:%M:%S")
    else:
        existing_df = pd.DataFrame()
        fetch_from = "1 Jan 2021"

    # Получаем данные
    new_klines = fetch_klines(symbol, fetch_from)
    if not new_klines:
        print(f"⏭ {symbol}: No new data found.")
        continue

    # Обработка новых свечей
    df_new = pd.DataFrame(new_klines, columns=[
        'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
        '_', '_', '_', '_', '_', '_'
    ])
    df_new['Date'] = pd.to_datetime(df_new['timestamp'], unit='ms')
    df_new = df_new[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    df_new[['Open', 'High', 'Low', 'Close', 'Volume']] = df_new[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)

    # Объединение и сохранение
    df_all = merge_and_save(existing_df, df_new, file_path)
    print(f"✅ {symbol}: added {len(df_new)} new candles.")

print("\n🏁 Update complete.")
