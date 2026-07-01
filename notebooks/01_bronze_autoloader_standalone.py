# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest with Auto Loader (standalone Structured Streaming)
# MAGIC
# MAGIC This is an **alternative** to the Bronze table defined in the Lakeflow
# MAGIC pipeline. Use it to demonstrate raw Structured Streaming + Auto Loader
# MAGIC without the declarative framework. Run it on **Serverless** compute
# MAGIC (top-right: Connect -> Serverless).
# MAGIC
# MAGIC Free Edition tip: use `.trigger(availableNow=True)` so the stream
# MAGIC processes all currently-landed files and then stops, instead of running
# MAGIC forever and burning your daily compute quota.

# COMMAND ----------

# Adjust these to match your workspace catalog/schema and Volume.
CATALOG = "workspace"
SCHEMA = "crypto"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/_checkpoints/bronze_ticks"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

# COMMAND ----------

from pyspark.sql import functions as F

bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .load(LANDING_PATH)
    .withColumn("_ingest_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
)

# COMMAND ----------

(
    bronze.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)  # process what's landed, then stop
    .toTable(f"{CATALOG}.{SCHEMA}.bronze_ticks_standalone")
    .awaitTermination()
)

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_ticks_standalone")
    .groupBy("symbol").count().orderBy("symbol")
)
