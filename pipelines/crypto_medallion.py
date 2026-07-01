"""
Lakeflow Declarative Pipeline -- crypto medallion (Bronze -> Silver -> Gold).

This runs INSIDE Databricks as a Lakeflow Declarative Pipeline (formerly
Delta Live Tables / DLT). The classic `import dlt` API still works unchanged;
on newer runtimes you may instead write `from pyspark import pipelines as dp`
and use @dp.table / @dp.materialized_view. We keep `dlt` here for the widest
compatibility with existing docs and examples.

Layers:
  bronze_ticks       streaming table, Auto Loader ingest of raw JSON
  silver_trades      streaming table, cleaned + typed + quality expectations
  silver_ohlc_1m     materialized view, 1-minute OHLC candles per symbol
  gold_daily_summary materialized view, per symbol/day OHLC + volume + volatility
  gold_moving_avgs   materialized view, moving averages over the 1-min candles

The landing path is supplied via pipeline configuration key `landing_path`
(set in databricks.yml). Point it at the Volume folder your producer writes to.
"""
import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window

# Landing folder that the replay producer fills with JSON files.
LANDING_PATH = spark.conf.get("landing_path", "/Volumes/workspace/crypto/landing")


# ---------------------------------------------------------------- Bronze
@dlt.table(
    name="bronze_ticks",
    comment="Raw crypto trade ticks ingested from the landing volume via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_ticks():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )


# ---------------------------------------------------------------- Silver (trades)
@dlt.table(
    name="silver_trades",
    comment="Cleaned, typed trade ticks with a proper event timestamp.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
@dlt.expect_or_drop("has_symbol", "symbol IS NOT NULL")
def silver_trades():
    return (
        dlt.read_stream("bronze_ticks")
        .select(
            F.col("symbol").cast("string").alias("symbol"),
            F.col("trade_id").cast("long").alias("trade_id"),
            F.col("price").cast("double").alias("price"),
            F.col("quantity").cast("double").alias("quantity"),
            F.expr("timestamp_millis(trade_time)").alias("trade_ts"),
            F.col("is_buyer_maker").cast("boolean").alias("is_buyer_maker"),
        )
        .withColumn("notional", F.col("price") * F.col("quantity"))
    )


# ---------------------------------------------------------------- Silver (candles)
@dlt.table(
    name="silver_ohlc_1m",
    comment="1-minute OHLC candles per symbol built from silver_trades.",
    table_properties={"quality": "silver"},
)
def silver_ohlc_1m():
    return (
        dlt.read("silver_trades")
        .groupBy(
            F.col("symbol"),
            F.window(F.col("trade_ts"), "1 minute").alias("w"),
        )
        .agg(
            F.expr("min_by(price, trade_ts)").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.expr("max_by(price, trade_ts)").alias("close"),
            F.sum("quantity").alias("volume"),
            F.sum("notional").alias("notional"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "symbol",
            F.col("w.start").alias("minute"),
            "open", "high", "low", "close",
            "volume", "notional", "trade_count",
        )
    )


# ---------------------------------------------------------------- Gold (daily)
@dlt.table(
    name="gold_daily_summary",
    comment="Per symbol per day: OHLC, volume, VWAP and intraday volatility.",
    table_properties={"quality": "gold"},
)
def gold_daily_summary():
    return (
        dlt.read("silver_ohlc_1m")
        .withColumn("day", F.to_date("minute"))
        .groupBy("symbol", "day")
        .agg(
            F.expr("min_by(open, minute)").alias("day_open"),
            F.max("high").alias("day_high"),
            F.min("low").alias("day_low"),
            F.expr("max_by(close, minute)").alias("day_close"),
            F.sum("volume").alias("day_volume"),
            (F.sum("notional") / F.sum("volume")).alias("vwap"),
            F.stddev("close").alias("minute_close_volatility"),
            F.sum("trade_count").alias("day_trade_count"),
        )
        .withColumn(
            "day_return_pct",
            F.round((F.col("day_close") - F.col("day_open")) / F.col("day_open") * 100, 4),
        )
    )


# ---------------------------------------------------------------- Gold (moving avgs)
@dlt.table(
    name="gold_moving_avgs",
    comment="Moving averages and range over 1-minute candles (per symbol).",
    table_properties={"quality": "gold"},
)
def gold_moving_avgs():
    w7 = Window.partitionBy("symbol").orderBy("minute").rowsBetween(-6, 0)
    w25 = Window.partitionBy("symbol").orderBy("minute").rowsBetween(-24, 0)
    return (
        dlt.read("silver_ohlc_1m")
        .withColumn("ma_7", F.round(F.avg("close").over(w7), 2))
        .withColumn("ma_25", F.round(F.avg("close").over(w25), 2))
        .withColumn("range", F.col("high") - F.col("low"))
        .select("symbol", "minute", "close", "ma_7", "ma_25", "volume", "range")
    )
