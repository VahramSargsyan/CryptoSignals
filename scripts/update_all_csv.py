from binance.client import Client
import pandas as pd

# Подключение к Binance без ключей (только публичные данные)
client = Client()

# Задай параметры
symbol = 'ETHUSDT'
interval = Client.KLINE_INTERVAL_1DAY
limit = 500  # Сколько свечей (до 1000 максимум)

# Получение исторических данных
klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)

# Преобразование в DataFrame
df = pd.DataFrame(klines, columns=[
    'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base', 'taker_buy_quote', 'ignore'
])

# Оставляем только нужные колонки
df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)

# Сохраняем в CSV
df.to_csv("btc_data.csv", index=False)
print("✅ Данные успешно сохранены в twt_data.csv")
