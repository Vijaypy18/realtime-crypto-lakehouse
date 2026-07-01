# Understanding This Project — A Beginner's Guide

This document explains the whole project in plain, simple language. No jargon
without a definition. By the end you'll understand *what* you built, *how* it
works from start to finish, and *why* each piece was chosen. Read it top to
bottom — each section builds on the last.

---

## 1. The one-sentence summary

> **You built a system that takes a stream of cryptocurrency price data, cleans
> it up step by step, calculates useful summaries from it, and shows the results
> on a dashboard — all running automatically on the cloud.**

That's it. Everything below is just explaining the pieces that make that happen.

---

## 2. What problem does this solve? (Why does this project exist?)

Imagine you run a crypto trading app. Every second, thousands of trades happen —
someone buys 0.5 Bitcoin, someone sells 2 Ethereum, and so on. Each trade is a
tiny message with a price, a quantity, and a timestamp.

Raw, these messages are useless to a human. Nobody can look at a million rows of
"BTC 68000.12, BTC 68000.45, BTC 67999.80..." and understand anything.

**What people actually want to know:**
- What was the highest and lowest price today?
- What's the average price weighted by volume (VWAP)?
- Is the price trending up or down (moving averages)?
- How volatile (jumpy) was it?

The job of **data engineering** is to build the "pipeline" (the assembly line)
that turns raw, messy data into clean, useful answers — reliably and
automatically. That's exactly what this project does, using crypto price data as
the example.

---

## 3. The big idea: the "Medallion Architecture"

This is the heart of the project. The word "medallion" refers to Olympic medals:
🥉 Bronze, 🥈 Silver, 🥇 Gold. The idea is that data gets **better and cleaner**
as it moves through three stages, like an assembly line in a factory.

Think of making orange juice:

| Stage | Orange juice analogy | In our project |
|-------|---------------------|----------------|
| 🥉 **Bronze** | The raw oranges, dirt and all, dumped in a crate | Raw price ticks exactly as they arrived, untouched |
| 🥈 **Silver** | Oranges washed, peeled, bad ones thrown out | Ticks cleaned, given proper column names, bad rows dropped |
| 🥇 **Gold** | The finished juice, ready to drink | Business summaries: daily highs/lows, averages, trends |

**Why three stages instead of one?** Because if something goes wrong, you can
always go back to an earlier, less-processed stage. Bronze keeps the *original*
data forever, so if you discover a bug in your cleaning logic, you can re-run it
without having lost anything. It also keeps each step simple and easy to debug —
each layer does one job.

---

## 4. Walking through the project, piece by piece

Here is every component, in the order data flows through it. For each one:
**what it is**, **what it does**, and **why we chose it**.

### 4.1 The Producer (`generate_sample.py` / `replay_producer.py`)

**What it is:** Small Python scripts that run on your laptop.

**What it does:** Creates fake-but-realistic crypto trade data and drops it into
a folder as files. In the real world, this data would come live from an exchange
like Binance. We *simulate* that so the project works without needing a live
internet feed.

**Why we did it this way:** Databricks Free Edition (our cloud platform) blocks
connections to the open internet for safety. So we can't connect live to Binance
*inside* Databricks. Instead, the producer runs on your laptop and creates the
data. This also makes the project **reproducible** — anyone can run it, no API
keys, no accounts, nothing to break.

> The fake data is shaped *exactly* like real Binance data (same field names like
> `s`=symbol, `p`=price), so switching to real data later is trivial.

### 4.2 The Landing Volume

**What it is:** A folder in the cloud (in Databricks) where the data files land.
"Volume" is just Databricks' word for a managed storage folder.

**What it does:** It's the drop-off point. Your producer uploads files here, and
the pipeline watches this folder for new files to process.

**Why:** Data pipelines almost always have a "landing zone" — a single known
place where raw data arrives before anything touches it. It separates
"getting the data in" from "processing the data."

### 4.3 Auto Loader (the ingestion tool)

**What it is:** A built-in Databricks feature (you used it in the Bronze step
with `spark.readStream.format("cloudFiles")`).

**What it does:** It automatically notices when new files appear in the landing
folder and pulls them into the pipeline — incrementally, meaning it only
processes *new* files it hasn't seen before, not everything again.

**Why we chose it (vs alternatives):** You *could* write code that lists all
files and reads them every time, but then you'd re-process the same data over and
over, and you'd have to track which files you've already handled. Auto Loader
does all that bookkeeping for you automatically and reliably. This is what makes
it "streaming-style" — it keeps up with new data as it arrives.

### 4.4 Delta Lake (the storage format)

**What it is:** The file format all your tables are stored in. Every table
(bronze_ticks, silver_trades, etc.) is a "Delta table."

**What it does:** Stores your data like a database table, but with superpowers:
it can be updated safely even while being read, it keeps a history (you can
"time travel" to older versions), and it never corrupts if a job crashes
mid-write.

**Why (vs plain CSV or Parquet files):** Plain files can get half-written and
corrupted if a job fails, and they can't handle reads and writes at the same
time. Delta guarantees "all or nothing" writes (this is called ACID
transactions). It's the foundation everything else sits on.

### 4.5 The Lakeflow Declarative Pipeline (the engine — `crypto_medallion.py`)

**What it is:** The single most important file. It's the "recipe" that defines
all your tables (Bronze, Silver, Gold) and how they connect. "Lakeflow
Declarative Pipelines" is the current name; it used to be called "Delta Live
Tables" (DLT), which is why the code says `import dlt`.

**What it does:** You *declare* what each table should look like (using
`@dlt.table`), and the pipeline figures out the rest — the order to build them
in, when to run them, how to handle failures. That's what "declarative" means:
you describe the *what*, not the *how*.

**Why we chose it (vs writing it all manually):** Without it, you'd have to write
separate scripts for each table, then write *more* code to run them in the right
order (Bronze before Silver before Gold), handle retries when something fails,
and monitor it all. Lakeflow does all of that automatically. When you clicked
"Run pipeline," it looked at your code, saw that Silver depends on Bronze and
Gold depends on Silver, and built them in the correct order by itself. That
automatic dependency graph is the "DAG" (Directed Acyclic Graph) — a fancy term
for "a flowchart of what runs before what."

### 4.6 The three layers inside the pipeline

**Bronze (`bronze_ticks`)** — reads raw files via Auto Loader and stores them
*exactly as they came in*. No cleaning. This is your safety net / source of truth.

**Silver (`silver_trades`)** — takes Bronze data and:
- Renames cryptic fields to friendly names (`s` → `symbol`, `p` → `price`).
- Converts types (text "68000.12" → an actual number).
- Turns the timestamp into a real date/time.
- **Drops bad rows** using data-quality checks (more on this below).

**Silver (`silver_ohlc_1m`)** — groups trades into 1-minute buckets and computes
the classic "candle" for each minute: **O**pen (first price), **H**igh, **L**ow,
**C**lose (last price), plus total volume. This is what candlestick charts use.

**Gold (`gold_daily_summary`)** — rolls the candles up to one row per coin per
day: the day's open/high/low/close, total volume, VWAP (volume-weighted average
price), volatility, and the daily return %.

**Gold (`gold_moving_avgs`)** — calculates 7-period and 25-period moving averages
(smoothed trend lines traders use to spot direction).

### 4.7 Data-Quality Expectations

**What it is:** Rules in the Silver layer written as `@dlt.expect_or_drop(...)`.

**What it does:** Automatically checks every single row against rules —
`price > 0`, `quantity > 0`, `symbol is not empty` — and throws away rows that
fail. It also *counts* how many passed and failed, shown in the pipeline UI.

**Why it matters:** Real data is messy — sometimes you get a price of 0, or a
missing symbol, from a glitch. If you let bad data through, your averages and
charts become wrong and nobody trusts them. Expectations are "quality control on
the assembly line." Interviewers love this because it shows you think about data
*reliability*, not just moving data around.

### 4.8 Serverless Compute

**What it is:** The actual computers (servers) that run your pipeline. "Serverless"
means *you* don't manage them — Databricks spins them up when you run something
and shuts them down when you're done.

**What it does:** Provides the processing power. When you clicked "Run pipeline,"
serverless machines started, did the work, and stopped.

**Why (vs managing your own servers):** Traditionally you'd have to set up a
cluster of machines, keep them running (paying the whole time), and manage them.
Serverless means you pay only for what you use and manage nothing. On Free
Edition it's the *only* option, which is fine for us.

### 4.9 Unity Catalog (catalog → schema → volume/table)

**What it is:** Databricks' organizing system. It has three levels, like folders:
`catalog.schema.table`. Yours is `workspace.crypto.bronze_ticks`, etc.

**What it does:** Organizes and governs your data — where tables live, who can
access them, and how they're tracked.

**Why:** Just like you organize files into folders, data platforms organize
tables into catalogs and schemas so things don't become a giant mess. `workspace`
is the catalog, `crypto` is the schema (like a folder for this project), and your
tables and the landing volume live inside it.

### 4.10 SQL Warehouse & Dashboard (the serving layer)

**What it is:** A SQL engine you query your Gold tables with, and a dashboard to
chart them.

**What it does:** This is the "front door" — where a human or a business
finally *uses* the clean data, by running queries or looking at charts.

**Why:** All the work upstream is pointless if no one can see the results. The
Gold tables are designed to be simple and fast to query, which is exactly what a
dashboard needs.

---

## 5. The complete journey of ONE trade (end to end)

Let's follow a single trade all the way through, so it clicks:

1. **It's born.** A trade happens: someone buys 0.5 BTC at $68,000. In our
   project, the producer *generates* this as a tiny JSON message:
   `{"s":"BTCUSDT","p":"68000.00","q":"0.5","T":1782890056000, ...}`

2. **It lands.** The producer writes it (with thousands of others) into a file
   and it's uploaded to the landing volume: `/Volumes/workspace/crypto/landing`.

3. **Auto Loader grabs it.** The pipeline's Bronze step notices the new file and
   reads the message into the `bronze_ticks` table — raw, untouched.

4. **Silver cleans it.** The `silver_trades` step renames `s`→`symbol`,
   `p`→`price` (as a real number), turns `T` into a proper timestamp, and checks
   it: price > 0? ✅ quantity > 0? ✅ symbol present? ✅ — it passes, so it stays.

5. **Silver buckets it.** The `silver_ohlc_1m` step puts this trade into its
   1-minute time bucket and uses it (with other trades that minute) to compute
   that minute's open/high/low/close and volume.

6. **Gold summarizes it.** The `gold_daily_summary` step rolls all the minute
   candles for BTC that day into one row: the daily high, low, VWAP, volatility,
   and return %. Our trade contributed to those numbers.

7. **A human sees it.** You run a SQL query or open the dashboard and see:
   "BTCUSDT closed at $68,541, up 0.58% today." That insight came — in part —
   from the single trade we followed.

That's the whole pipeline: **raw message → clean row → minute candle → daily
summary → chart.** Bronze → Silver → Gold.

---

## 6. Why Databricks and not other tools?

A very common way to build this same project is with separate open-source tools:
**Kafka** (for streaming), **Spark** (for processing), **Airflow** (for
scheduling/orchestration), and a separate database. That works, but you have to
install, connect, and maintain four or five different systems yourself.

**Databricks combines all of that into one platform:**

| Job | Separate-tools way | Databricks way |
|-----|-------------------|----------------|
| Ingest streaming data | Kafka | Auto Loader |
| Process/transform | Spark cluster you manage | Serverless Spark (built in) |
| Orchestrate the order | Airflow | Lakeflow (automatic DAG) |
| Store the data | A database + a data lake | Delta Lake (one place) |
| Serve results | Another BI tool | Built-in SQL + dashboards |

**Why this was the right choice for you:** less to set up, everything works
together, and — crucially — it's **free** on Databricks Free Edition with no
credit card. It's also one of the most in-demand skills in data engineering job
listings, so it's a strong thing to have on your resume.

---

## 7. Plain-English glossary (keep this handy)

- **Data pipeline** — an automated assembly line that moves and transforms data.
- **Ingestion** — getting data *into* the system.
- **Medallion architecture** — the Bronze → Silver → Gold layering pattern.
- **Bronze / Silver / Gold** — raw / cleaned / business-ready data layers.
- **Delta table / Delta Lake** — a reliable, database-like way to store data files.
- **Auto Loader** — the tool that incrementally ingests new files.
- **Lakeflow Declarative Pipelines (DLT)** — the engine that runs the medallion,
  figures out the order, and manages it automatically.
- **DAG** — a flowchart of what runs before what (Bronze → Silver → Gold).
- **Declarative** — you describe *what* you want, not the step-by-step *how*.
- **Expectations** — automatic data-quality rules that drop bad rows.
- **Serverless** — cloud computers you don't have to manage.
- **Unity Catalog** — the `catalog.schema.table` organizing system.
- **Volume** — a managed storage folder in Databricks.
- **OHLC candle** — Open/High/Low/Close price summary for a time bucket.
- **VWAP** — Volume-Weighted Average Price.
- **Moving average** — a smoothed trend line over recent prices.
- **Streaming** — processing data continuously as it arrives, vs all at once (batch).

---

## 8. How to explain this project in an interview (30 seconds)

> "I built an end-to-end data pipeline on Databricks that ingests crypto trade
> data and refines it through a Bronze–Silver–Gold medallion architecture. Raw
> ticks land in a volume, Auto Loader streams them into a Bronze Delta table, then
> a Lakeflow declarative pipeline cleans them into Silver — applying data-quality
> checks — and aggregates them into Gold tables with OHLC candles, VWAP, and
> moving averages. It runs on serverless compute and the results feed a SQL
> dashboard. I built it to learn production data-engineering patterns end to end."

Then be ready to answer three likely follow-ups:
1. **"Why a medallion architecture?"** → Each layer does one job; Bronze keeps
   raw data as a safety net; it's easy to debug and re-run.
2. **"How does the data quality work?"** → Expectations validate every row in
   Silver and drop failures, with pass/fail metrics in the pipeline UI.
3. **"What's the difference between the ingestion and the transformation?"** →
   Auto Loader streams raw files into Bronze (the "hot path"); the declarative
   pipeline transforms Bronze → Silver → Gold on serverless compute.

That's your whole project. You now understand every piece and why it's there.
