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


def _repo_root() -> Path:
    # <repo>/streaming-analytics/generator.py
    return Path(__file__).resolve().parents[1]


def _producer_argv(compose_file: Path, topic: str) -> list[str]:
    bash_cmd = f'kafka-console-producer --bootstrap-server kafka:29092 --topic "{topic}" >/dev/null'
    return ["docker", "compose", "-f", str(compose_file), "exec", "-T", "kafka", "bash", "-lc", bash_cmd]

def _topic_exists(compose_file: Path, topic: str) -> bool:
    bash_cmd = f'kafka-topics --bootstrap-server kafka:29092 --describe --topic "{topic}" >/dev/null 2>&1'
    argv = ["docker", "compose", "-f", str(compose_file), "exec", "-T", "kafka", "bash", "-lc", bash_cmd]
    cp = subprocess.run(argv, cwd=str(_repo_root()), text=True)
    return cp.returncode == 0


def _ensure_topic(compose_file: Path, topic: str, partitions: int, replication: int, show_code: bool) -> None:
    bash_cmd = (
        f'kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic "{topic}" '
        f'--partitions "{partitions}" --replication-factor "{replication}"'
    )
    argv = ["docker", "compose", "-f", str(compose_file), "exec", "-T", "kafka", "bash", "-lc", bash_cmd]
    if show_code:
        print("[code] ensure topic:")
        print("  " + " ".join(argv))
    subprocess.run(argv, cwd=str(_repo_root()), check=True, text=True)


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


def run_forever(
    topic: str,
    sleep_s: float,
    sources: List[str],
    show_code: bool,
    compose_file: Path,
    ensure_topic: bool,
    partitions: int,
    replication: int,
) -> int:
    if ensure_topic:
        _ensure_topic(compose_file, topic, partitions=partitions, replication=replication, show_code=show_code)
    else:
        # Avoid confusing producer spam in class: fail fast with a clear message.
        if not _topic_exists(compose_file, topic):
            raise SystemExit(
                f"Topic '{topic}' does not exist. Create it first, e.g.:\n"
                f"  python3 basic/topic.py create {topic}\n"
                f"Or run this generator with --ensure-topic."
            )

    argv = _producer_argv(compose_file, topic)
    if show_code:
        print("[code] producer command:")
        print("  " + " ".join(argv))

    seq = 0
    try:
        while True:
            seq += 1
            event = make_event(seq, sources=sources)
            payload = json.dumps(event, ensure_ascii=False)
            subprocess.run(argv, cwd=str(_repo_root()), check=True, text=True, input=payload + "\n")
            print(f"[gen] {payload}", flush=True)
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        return 0


def main() -> None:
    p = argparse.ArgumentParser(description="Continuously generate streaming events into Kafka (Ctrl+C to stop).")
    p.add_argument("--topic", default="demo-events")
    p.add_argument("--sleep", type=float, default=0.1, help="Delay between events (seconds)")
    p.add_argument(
        "--compose-file",
        default="basic/docker-compose.yml",
        help="Path to docker-compose.yml (relative to repo root is recommended).",
    )
    p.add_argument(
        "--ensure-topic",
        action="store_true",
        help="Create the topic if missing (auto-create is disabled in this demo).",
    )
    p.add_argument("--partitions", type=int, default=3, help="Partitions for --ensure-topic")
    p.add_argument("--replication", type=int, default=1, help="Replication factor for --ensure-topic")
    p.add_argument(
        "--sources",
        default="demo,web,mobile,iot",
        help="Comma-separated list of source values to cycle (distribution is random).",
    )
    p.add_argument("--no-show-code", action="store_true")
    args = p.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    compose_file = (_repo_root() / args.compose_file).resolve()
    code = run_forever(
        topic=args.topic,
        sleep_s=args.sleep,
        sources=sources,
        show_code=not args.no_show_code,
        compose_file=compose_file,
        ensure_topic=args.ensure_topic,
        partitions=args.partitions,
        replication=args.replication,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()

