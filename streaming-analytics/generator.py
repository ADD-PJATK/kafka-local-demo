from __future__ import annotations

import argparse
import json
import random
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _producer_argv(topic: str) -> list[str]:
    bash_cmd = f'kafka-console-producer --bootstrap-server kafka:29092 --topic "{topic}" >/dev/null'
    return ["docker", "compose", "exec", "-T", "kafka", "bash", "-lc", bash_cmd]


def make_event(seq: int, sources: List[str]) -> dict:
    # Add a bit of randomness so analytics show changing distribution.
    source = random.choice(sources) if sources else "demo"
    base = random.randint(0, 100)
    # Occasionally create a spike.
    if random.random() < 0.03:
        base += random.randint(200, 600)
    return {
        "event_id": str(seq),
        "event_time": _now_iso(),
        "source": source,
        "value": base,
    }


def run_forever(topic: str, sleep_s: float, sources: List[str], show_code: bool) -> int:
    argv = _producer_argv(topic)
    if show_code:
        print("[code] producer command:")
        print("  " + " ".join(argv))

    seq = 0
    try:
        while True:
            seq += 1
            event = make_event(seq, sources=sources)
            payload = json.dumps(event, ensure_ascii=False)
            subprocess.run(argv, cwd=str(Path(__file__).resolve().parents[1]), check=True, text=True, input=payload + "\n")
            print(f"[gen] {payload}", flush=True)
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        return 0


def main() -> None:
    p = argparse.ArgumentParser(description="Continuously generate streaming events into Kafka (Ctrl+C to stop).")
    p.add_argument("--topic", default="demo-events")
    p.add_argument("--sleep", type=float, default=0.1, help="Delay between events (seconds)")
    p.add_argument(
        "--sources",
        default="demo,web,mobile,iot",
        help="Comma-separated list of source values to cycle (distribution is random).",
    )
    p.add_argument("--no-show-code", action="store_true")
    args = p.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    code = run_forever(topic=args.topic, sleep_s=args.sleep, sources=sources, show_code=not args.no_show_code)
    raise SystemExit(code)


if __name__ == "__main__":
    main()

