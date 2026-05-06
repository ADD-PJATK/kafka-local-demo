from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from kafka_demo_common import (
    Ctx,
    DEFAULT_TOPIC,
    add_common_flags,
    handle_introspection,
    print_cmd,
    repo_root_from_this_file,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def make_event(event_id: int, source: str) -> dict:
    return {
        "event_id": str(event_id),
        "event_time": _now_iso(),
        "source": source,
        "value": event_id % 100,
    }


def produce_events(ctx: Ctx, topic: str, n: int, sleep_s: float, source: str) -> None:
    print(f"[kafka-demo] Producing {n} JSON events into topic: {topic}")
    for i in range(1, n + 1):
        event = make_event(i, source=source)
        payload = json.dumps(event, ensure_ascii=False)

        bash_cmd = f'kafka-console-producer --bootstrap-server kafka:29092 --topic "{topic}" >/dev/null'
        argv = ["docker", "compose", "exec", "-T", "kafka", "bash", "-lc", bash_cmd]
        if ctx.show_code:
            print_cmd("producer command", argv)

        import subprocess

        subprocess.run(argv, cwd=str(ctx.root_dir), check=True, text=True, input=payload + "\n")
        print(f"  sent: {payload}")
        time.sleep(sleep_s)


def main() -> None:
    root_dir = repo_root_from_this_file(__file__)
    p = argparse.ArgumentParser(description="Produce JSON demo events into a Kafka topic.")
    add_common_flags(p)
    p.add_argument("topic", nargs="?", default=DEFAULT_TOPIC)
    p.add_argument("-n", "--count", type=int, default=10, help="Number of events")
    p.add_argument("--sleep", type=float, default=0.3, help="Delay between events (seconds)")
    p.add_argument("--source", default="demo", help="Event source tag")
    args = p.parse_args()

    handle_introspection(parser=p, module_file=__file__, functions=[make_event, produce_events, main], args=args)

    ctx = Ctx(root_dir=root_dir, show_code=not args.no_show_code)
    produce_events(ctx, args.topic, n=args.count, sleep_s=args.sleep, source=args.source)


if __name__ == "__main__":
    main()

