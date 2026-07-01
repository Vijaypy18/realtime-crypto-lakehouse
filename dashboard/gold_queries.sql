-- Dashboard queries for the Gold layer.
-- Run these on a Serverless SQL warehouse, or paste each into a tile of a
-- Databricks AI/BI dashboard. Replace `workspace.crypto` if your catalog/schema differ.

-- 1) Latest price + daily return per symbol (KPI tiles)
SELECT symbol, day, day_close, day_return_pct, day_volume, vwap
FROM workspace.crypto.gold_daily_summary
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY day DESC) = 1;

-- 2) Price with 7- and 25-period moving averages (line chart, one symbol)
SELECT minute, close, ma_7, ma_25
FROM workspace.crypto.gold_moving_avgs
WHERE symbol = 'BTCUSDT'
ORDER BY minute;

-- 3) 1-minute candles for a candlestick chart
SELECT minute, open, high, low, close, volume
FROM workspace.crypto.silver_ohlc_1m
WHERE symbol = 'ETHUSDT'
ORDER BY minute;

-- 4) Traded volume by symbol (bar chart)
SELECT symbol, ROUND(SUM(day_volume), 2) AS total_volume
FROM workspace.crypto.gold_daily_summary
GROUP BY symbol
ORDER BY total_volume DESC;

-- 5) Intraday volatility ranking
SELECT symbol, day, ROUND(minute_close_volatility, 4) AS volatility
FROM workspace.crypto.gold_daily_summary
ORDER BY volatility DESC;
