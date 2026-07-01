"""
OPTIONAL: capture real Binance trade ticks to data/sample_ticks.jsonl.

Run this on your own machine (which has open internet) if you want to replace
the synthetic sample with genuinely real market data. Databricks Free Edition
cannot run this because its outbound internet is restricted.

Requires: pip install websocket-client   (see requirements.txt)

Example:
    python producer/capture_live.py --symbols btcusdt ethusdt --seconds 300
"""
import argparse
import json
import time
from pathlib import Path

try:
    import websocket  # from the websocket-client package
except ImportError:
    raise SystemExit(
        "websocket-client not installed. Run: pip install -r producer/requirements.txt"
    )

BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=["btcusdt", "ethusdt", "solusdt"])
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--out", default="data/sample_ticks.jsonl")
    args = ap.parse_args()

    streams = "/".join(f"{s.lower()}@trade" for s in args.symbols)
    url = BINANCE_WS + streams
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    ws = websocket.create_connection(url, timeout=10)
    print(f"connected: {url}")
    deadline = time.time() + args.seconds
    n = 0
    with out.open("w") as f:
        while time.time() < deadline:
            msg = json.loads(ws.recv())
            payload = msg.get("data", msg)  # combined stream wraps in "data"
            if payload.get("e") == "trade":
                f.write(json.dumps(payload) + "\n")
                n += 1
                if n % 100 == 0:
                    print(f"captured {n} ticks")
    ws.close()
    print(f"done: {n} ticks -> {out}")


if __name__ == "__main__":
    main()
