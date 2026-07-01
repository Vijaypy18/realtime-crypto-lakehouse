# Crypto Lakehouse — real-time medallion pipeline on Databricks

An end-to-end data engineering project: real-time-style ingestion of crypto
trade ticks, incremental transformation through a Bronze → Silver → Gold
medallion, orchestration, and a serving layer — all running on **serverless**
compute on **Databricks Free Edition** (permanent, no credit card, no cloud
account).

## Architecture

```
                Ingest (into the lakehouse)
  producer  ->  landing volume  ->  Auto Loader  ->  Bronze Delta
  (your laptop)                     (streaming)       (raw ticks)
                                                          |
                Lakeflow Declarative Pipeline             v
  Silver Delta  ->  Gold Delta  ->  Serverless SQL  ->  AI/BI dashboard
  (typed +          (candles,        (warehouse)         (charts)
   candles)          aggregates)
```

| Layer | What it is | Table(s) |
|-------|-----------|----------|
| Bronze | Raw ticks, Auto Loader ingest | `bronze_ticks` |
| Silver | Cleaned/typed + quality expectations; 1-min OHLC candles | `silver_trades`, `silver_ohlc_1m` |
| Gold | Business aggregates: daily OHLC/VWAP/volatility, moving averages | `gold_daily_summary`, `gold_moving_avgs` |

## Repo layout

```
crypto-lakehouse/
├── producer/
│   ├── generate_sample.py   # make a synthetic tick dataset (stdlib only)
│   ├── replay_producer.py   # stream ticks into a landing folder as files
│   ├── capture_live.py      # OPTIONAL: capture real Binance data (needs internet)
│   └── requirements.txt
├── data/
│   └── sample_ticks.jsonl   # shipped sample so the project runs out of the box
├── pipelines/
│   └── crypto_medallion.py  # the Lakeflow Declarative Pipeline (Bronze→Silver→Gold)
├── notebooks/
│   └── 01_bronze_autoloader_standalone.py  # alt raw-streaming Bronze (learning)
├── resources/              # (kept for future extra bundle resources)
├── dashboard/
│   └── gold_queries.sql     # queries for the AI/BI dashboard
├── databricks.yml           # Asset Bundle: pipeline + orchestration job (IaC)
└── README.md
```

## Why a "replay" producer

Databricks Free Edition restricts outbound internet, so a live Binance
WebSocket can't run inside the workspace. Instead, a tiny producer runs on
**your** machine and lands JSON files that Auto Loader ingests. This keeps the
project fully reproducible — anyone can clone and run it with no API keys. The
landed message shape matches Binance's `@trade` stream, so switching to real
data later is just running `capture_live.py` instead of `generate_sample.py`.

## Setup

### 1. Create a free workspace
Sign up for **Databricks Free Edition** (email only, ~5 min):
https://www.databricks.com/learn/free-edition
Note your workspace URL, e.g. `https://dbc-xxxx.cloud.databricks.com`.

### 2. Create the catalog objects
In a notebook (Connect → Serverless), or SQL editor, run:
```sql
CREATE SCHEMA IF NOT EXISTS workspace.crypto;
CREATE VOLUME IF NOT EXISTS workspace.crypto.landing;
```
(If your default catalog isn't `workspace`, use whatever `SHOW CATALOGS`
returns and update `databricks.yml` and `dashboard/gold_queries.sql` to match.)

### 3. Land some data
Two options.

**A. Upload the sample (simplest).** Regenerate/split the sample into small
files locally, then upload them to the Volume via the UI (Catalog → Volumes →
`landing` → Upload):
```bash
python producer/generate_sample.py --minutes 120
python producer/replay_producer.py --landing ./landing --interval 0 --once
# then upload the files in ./landing to the Volume
```

**B. Stream continuously (more realistic).** Install the Databricks CLI, then
loop the producer and sync each file up:
```bash
python producer/replay_producer.py --landing ./landing --batch 50 --interval 3
# in another shell, repeatedly sync:
databricks fs cp -r ./landing dbfs:/Volumes/workspace/crypto/landing
```

### 4. Deploy the pipeline (infrastructure-as-code)
Install the CLI and authenticate:
```bash
pip install databricks-cli
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
```
Edit the `host` in `databricks.yml`, then:
```bash
databricks bundle validate
databricks bundle deploy -t dev
```
This creates the Lakeflow pipeline and the orchestration job in your workspace.

### 5. Run it
```bash
databricks bundle run crypto_pipeline_job -t dev
```
Or open the pipeline in the UI and click **Start**. You'll see the medallion
DAG build Bronze → Silver → Gold, with the data-quality expectations reported
on `silver_trades`.

### 6. Explore / dashboard
Open a SQL editor on a Serverless SQL warehouse and run the queries in
`dashboard/gold_queries.sql`, or add them as tiles to a Databricks AI/BI
dashboard.

## Free Edition notes
- **Serverless only** and **quota-limited** — if you exceed the daily quota,
  compute pauses until it resets (your data is kept). Prefer manual pipeline
  runs over the hourly schedule (the schedule ships `PAUSED`).
- **No custom storage buckets** — use the default catalog + a Volume as above.
- **Non-commercial use only.**

## Data-quality expectations
`silver_trades` drops records failing: `price > 0`, `quantity > 0`,
`symbol IS NOT NULL`. Failures are visible in the pipeline's event log and UI,
giving you observable quality metrics without extra tooling.

## Extending it
- Swap `generate_sample.py` for `capture_live.py` to use real market data.
- Add a `gold_signals` table (e.g. MA crossovers) as another materialized view.
- Add more expectations (e.g. price sanity bands per symbol).
