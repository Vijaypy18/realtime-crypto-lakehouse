"""
Replay producer: streams a captured tick file into a landing directory as a
sequence of small JSON files, imitating a live feed. Auto Loader (in the
Lakeflow pipeline) then picks up each new file incrementally.

Run this on YOUR machine (laptop). Point --landing at either:
  * a local folder (for the fully-local Docker path), or
  * a synced Databricks Volume path if you mount one, or
  * just generate files locally and upload them to a Volume via the UI / CLI.

Pure standard library -- no pip installs.

Examples:
    # Land 50 ticks per file, one file every 2 seconds, into ./landing
    python producer/replay_producer.py --landing ./landing --batch 50 --interval 2

    # Dump everything at once (fast, for a quick end-to-end test)
    python producer/replay_producer.py --landing ./landing --interval 0 --once
"""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def normalize(raw: dict) -> dict:
    """Map a Binance @trade payload to a clean, typed record."""
    return {
        "symbol": raw["s"],
        "trade_id": int(raw["t"]),
        "price": float(raw["p"]),
        "quantity": float(raw["q"]),
        "trade_time": int(raw["T"]),
        "is_buyer_maker": bool(raw["m"]),
        "ingest_time": datetime.now(timezone.utc).isoformat(),
    }


def read_source(path: Path):
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_batch(landing: Path, batch: list):
    landing.mkdir(parents=True, exist_ok=True)
    # Unique, sortable filename so Auto Loader treats each as a new file.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    fname = landing / f"ticks-{stamp}-{uuid.uuid4().hex[:8]}.json"
    with fname.open("w") as f:
        for rec in batch:
            f.write(json.dumps(rec) + "\n")
    return fname


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/sample_ticks.jsonl")
    ap.add_argument("--landing", default="./landing",
                    help="Directory Auto Loader watches for new files")
    ap.add_argument("--batch", type=int, default=50,
                    help="Ticks per landed file")
    ap.add_argument("--interval", type=float, default=2.0,
                    help="Seconds to wait between landed files")
    ap.add_argument("--once", action="store_true",
                    help="Land all data in a single pass then exit")
    args = ap.parse_args()

    source = Path(args.source)
    landing = Path(args.landing)
    if not source.exists():
        raise SystemExit(
            f"Source {source} not found. Run producer/generate_sample.py first."
        )

    batch, landed, total = [], 0, 0
    for raw in read_source(source):
        batch.append(normalize(raw))
        if len(batch) >= args.batch:
            fname = write_batch(landing, batch)
            landed += 1
            total += len(batch)
            print(f"landed {fname.name} ({len(batch)} ticks, {total} total)")
            batch = []
            if not args.once and args.interval > 0:
                time.sleep(args.interval)

    if batch:
        fname = write_batch(landing, batch)
        total += len(batch)
        print(f"landed {fname.name} ({len(batch)} ticks, {total} total)")

    print(f"done: {total} ticks in {landed + (1 if batch else 0)} files -> {landing}")


if __name__ == "__main__":
    main()
