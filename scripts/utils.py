import pandas as pd
from binance.client import Client
from datetime import datetime

client = Client()

def fetch_klines(symbol, start_time, interval=Client.KLINE_INTERVAL_1DAY):
    """Получает исторические свечи с Binance"""
    return client.get_historical_klines(symbol, interval, start_time)

def calculate_bollinger_bands(df, window=20):
    df['MA20'] = df['Close'].rolling(window=window).mean()
    df['STD20'] = df['Close'].rolling(window=window).std()
    df['Upper'] = df['MA20'] + 2 * df['STD20']
    df['Lower'] = df['MA20'] - 2 * df['STD20']
    return df

def calculate_stoch_rsi(df, window=14, smooth=3):
    min_val = df['Close'].rolling(window=window).min()
    max_val = df['Close'].rolling(window=window).max()
    stoch = (df['Close'] - min_val) / (max_val - min_val)
    df['StochRSI'] = stoch.rolling(smooth).mean() * 100
    return df

def merge_and_save(existing_df, new_df, save_path):
    df_all = pd.concat([existing_df, new_df], ignore_index=True)
    df_all.drop_duplicates(subset='Date', inplace=True)
    df_all.sort_values(by='Date', inplace=True)
    df_all.to_csv(save_path, index=False)
    return df_all
