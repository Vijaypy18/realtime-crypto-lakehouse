"""
Generate a realistic synthetic crypto trade-tick dataset.

Why this exists:
  Databricks Free Edition restricts outbound internet, so a live Binance
  WebSocket cannot run inside the workspace. This script produces a
  self-contained sample file (data/sample_ticks.jsonl) that the replay
  producer streams as if it were live. The message shape matches Binance's
  `<symbol>@trade` stream so the exact same code works against real data
  later (see capture_live.py).

Pure standard library only -- no pip installs required.

Usage:
    python producer/generate_sample.py --minutes 120 --out data/sample_ticks.jsonl
"""
import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Starting mid-prices per symbol (roughly realistic order of magnitude).
SYMBOLS = {
    "BTCUSDT": 68000.0,
    "ETHUSDT": 3500.0,
    "SOLUSDT": 165.0,
}

# Per-symbol volatility as a fraction of price, applied per tick (random walk).
VOL = {
    "BTCUSDT": 0.00035,
    "ETHUSDT": 0.00045,
    "SOLUSDT": 0.00070,
}


def gen(minutes: int, ticks_per_min: int, seed: int):
    rng = random.Random(seed)
    prices = dict(SYMBOLS)
    trade_id = {s: rng.randint(10_000_000, 20_000_000) for s in SYMBOLS}

    start = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    total_ms = minutes * 60 * 1000
    n_ticks = minutes * ticks_per_min * len(SYMBOLS)

    rows = []
    for _ in range(n_ticks):
        symbol = rng.choice(list(SYMBOLS))
        # Geometric random walk with a tiny mean-reversion pull to the start price.
        drift = (SYMBOLS[symbol] - prices[symbol]) * 0.0002
        shock = rng.gauss(0, VOL[symbol]) * prices[symbol]
        prices[symbol] = max(0.01, prices[symbol] + drift + shock)

        offset_ms = rng.randint(0, total_ms)
        ts = int((start.timestamp() * 1000)) + offset_ms
        trade_id[symbol] += rng.randint(1, 4)

        # Binance <symbol>@trade payload shape.
        rows.append({
            "e": "trade",
            "E": ts,
            "s": symbol,
            "t": trade_id[symbol],
            "p": f"{prices[symbol]:.2f}",
            "q": f"{rng.uniform(0.001, 2.5):.6f}",
            "T": ts,
            "m": rng.random() < 0.5,
            "M": True,
        })

    # Sort by trade time so replay is chronological.
    rows.sort(key=lambda r: r["T"])
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=120)
    ap.add_argument("--ticks-per-min", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="data/sample_ticks.jsonl")
    args = ap.parse_args()

    rows = gen(args.minutes, args.ticks_per_min, args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    print(f"Wrote {len(rows):,} ticks to {out} "
          f"({args.minutes} min, {len(SYMBOLS)} symbols)")


if __name__ == "__main__":
    main()
