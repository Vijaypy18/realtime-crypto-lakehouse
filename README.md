# 🪙 Crypto Lakehouse — Real-Time Medallion Pipeline on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?logo=delta&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?logo=postgresql&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

An **end-to-end data engineering project** that ingests cryptocurrency trade
ticks and refines them through a **Bronze → Silver → Gold medallion
architecture** on the Databricks Lakehouse. It covers the full lifecycle —
streaming-style ingestion, incremental transformation, data-quality enforcement,
serverless compute, and a BI serving layer — and runs entirely on
**Databricks Free Edition** (permanent, no credit card, no cloud account, $0).

> Built with **Lakeflow Declarative Pipelines** (formerly Delta Live Tables),
> Delta Lake, PySpark Structured Streaming, Auto Loader, and Unity Catalog.

---

## 🏗️ Architecture

```
                Ingest (into the lakehouse)
  producer  ─▶  landing volume  ─▶  Auto Loader  ─▶  Bronze Delta
  (laptop)                          (streaming)       (raw ticks)
                                                          │
                Lakeflow Declarative Pipeline             ▼
  Silver Delta  ─▶  Gold Delta  ─▶  Serverless SQL  ─▶  AI/BI dashboard
  (typed +          (candles,        (warehouse)         (charts)
   candles)          aggregates)
```



| Layer | Purpose | Tables |
|-------|---------|--------|
| 🥉 **Bronze** | Raw ticks, Auto Loader ingest of JSON | `bronze_ticks` |
| 🥈 **Silver** | Cleaned & typed ticks + data-quality checks; 1-min OHLC candles | `silver_trades`, `silver_ohlc_1m` |
| 🥇 **Gold** | Business analytics: daily OHLC, VWAP, volatility, returns, moving averages | `gold_daily_summary`, `gold_moving_avgs` |

---

## ✨ Highlights

- **Streaming ingestion** with Auto Loader + Spark Structured Streaming into Delta.
- **Declarative ETL** — the entire medallion is defined once; Lakeflow builds the
  dependency DAG, orchestrates execution, and handles incremental refresh.
- **Data quality as code** — expectations validate every record and drop bad data,
  with metrics surfaced in the pipeline UI.
- **Real financial transforms** — OHLC candles, VWAP, intraday volatility, and
  7/25-period moving averages across multiple trading pairs.
- **Fully reproducible** — ships with a synthetic dataset in Binance `@trade`
  format, so anyone can clone and run it with no API keys.
- **100% serverless & free** — runs end to end on Databricks Free Edition.

---

## 📊 Sample results (`gold_daily_summary`)

| symbol | day | open | high | low | close | volume | vwap | volatility | trades | return % |
|--------|-----|------|------|-----|-------|--------|------|-----------|--------|---------|
| BTCUSDT | 2026-07-01 | 68146.81 | 69265.34 | 67404.79 | 68541.50 | 6017.02 | 68354.35 | 466.26 | 4820 | **+0.58%** |
| SOLUSDT | 2026-07-01 | 161.52 | 169.70 | 160.06 | 163.74 | 5950.72 | 164.18 | 2.36 | 4735 | **+1.37%** |
| ETHUSDT | 2026-07-01 | 3607.26 | 3635.92 | 3494.76 | 3499.98 | 5956.21 | 3546.86 | 31.35 | 4845 | **−2.97%** |

<!-- ![Dashboard](docs/dashboard.png) -->

---

## 🗂️ Repo layout

```
crypto-lakehouse/
├── producer/
│   ├── generate_sample.py   # synthetic tick generator (stdlib only)
│   ├── replay_producer.py   # streams ticks into a landing folder as files
│   ├── capture_live.py      # OPTIONAL: capture real Binance data (needs internet)
│   └── requirements.txt
├── data/
│   └── sample_ticks.jsonl   # shipped sample — runs out of the box
├── pipelines/
│   └── crypto_medallion.py  # Lakeflow Declarative Pipeline (Bronze→Silver→Gold)
├── notebooks/
│   └── 01_bronze_autoloader_standalone.py  # alt raw-streaming Bronze
├── dashboard/
│   └── gold_queries.sql     # queries for the AI/BI dashboard
├── databricks.yml           # Asset Bundle (infra-as-code; optional deploy)
└── README.md
```

---

## 📥 Data source

Ticks use the **Binance `@trade` payload format** (short keys: `s` symbol,
`p` price, `q` quantity, `T` trade time, `t` trade id, `m` buyer-maker). Since
Databricks Free Edition restricts outbound internet, the producer runs on your
laptop and lands JSON files that Auto Loader ingests — keeping the project
reproducible. Swap in `producer/capture_live.py` on a machine with internet to
use real live market data.

---

## 🚀 How to run

### 1. Create a free workspace
Sign up for **Databricks Free Edition** (email only, ~5 min):
https://www.databricks.com/learn/free-edition

### 2. Create the schema and landing volume
In a notebook (**Connect → Serverless**) or the SQL editor:
```sql
CREATE SCHEMA IF NOT EXISTS workspace.crypto;
CREATE VOLUME IF NOT EXISTS workspace.crypto.landing;
```

### 3. Generate and upload the data
On your laptop (macOS uses `python3`):
```bash
python3 producer/generate_sample.py            # writes data/sample_ticks.jsonl
cp data/sample_ticks.jsonl data/all_ticks.json # Auto Loader reads .json
```
Then in Databricks: **Catalog → workspace → crypto → Volumes → landing →
Upload** and upload `data/all_ticks.json`. Verify:
```sql
LIST '/Volumes/workspace/crypto/landing';
```

### 4. Create & run the Lakeflow pipeline
- **Jobs & Pipelines → Create → ETL pipeline.**
- Paste `pipelines/crypto_medallion.py` into the pipeline's source file.
- Set target: catalog `workspace`, schema `crypto`.
- Ensure the file is **Included** (not Excluded).
- **Connect → Serverless**, then **Run pipeline** → *Full refresh all*.

The graph builds: `bronze_ticks → silver_trades → silver_ohlc_1m →
gold_daily_summary + gold_moving_avgs`.

> **Infra-as-code alternative:** `databricks.yml` deploys the pipeline + an
> orchestration job via `databricks bundle deploy`. The UI path above is simplest
> on Free Edition.

### 5. Verify
```sql
SELECT * FROM workspace.crypto.gold_daily_summary;
SELECT * FROM workspace.crypto.silver_ohlc_1m
WHERE symbol = 'BTCUSDT' ORDER BY minute LIMIT 20;
```

### 6. Dashboard
Use `dashboard/gold_queries.sql` as tiles in a Databricks AI/BI dashboard
(e.g. a moving-averages line chart from `gold_moving_avgs`).

---

## ✅ Data quality

`silver_trades` enforces three expectations and drops any records that fail:

| Expectation | Rule |
|-------------|------|
| `valid_price` | `price > 0` |
| `valid_quantity` | `quantity > 0` |
| `has_symbol` | `symbol IS NOT NULL` |

Pass/fail metrics appear in the pipeline event log and UI — observable data
quality without extra tooling.

---

## ⚙️ Free Edition notes
- **Serverless only** and **quota-limited** — if you exceed the daily quota,
  compute pauses until reset (data is kept). Run the pipeline manually.
- **No custom storage buckets** — uses the default catalog + a Volume.
- **Non-commercial use only.**

---

## 🔮 Possible extensions
- Swap the sample for real Binance data via `capture_live.py`.
- Add a `gold_signals` table (e.g. moving-average crossover buy/sell flags).
- Add per-symbol price sanity-band expectations.
- Schedule the pipeline as a Lakeflow Job for continuous refresh.

---

## 🧰 Tech stack
Databricks · Lakeflow Declarative Pipelines (DLT) · Delta Lake · PySpark ·
Spark Structured Streaming · Auto Loader · Unity Catalog · Python · SQL · Git
