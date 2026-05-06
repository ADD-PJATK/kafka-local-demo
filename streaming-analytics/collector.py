from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional


def _consumer_argv(topic: str, group: str, from_beginning: bool) -> list[str]:
    cmd = [
        "kafka-console-consumer",
        "--bootstrap-server",
        "kafka:29092",
        "--topic",
        topic,
        "--group",
        group,
    ]
    if from_beginning:
        cmd.append("--from-beginning")
    bash_cmd = " ".join(cmd)
    return ["docker", "compose", "exec", "-T", "kafka", "bash", "-lc", bash_cmd]


def collect(topic: str, group: str, out_path: Path, from_beginning: bool, show_code: bool) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    argv = _consumer_argv(topic=topic, group=group, from_beginning=from_beginning)
    if show_code:
        print("[code] consumer command:")
        print("  " + " ".join(argv))

    # Stream raw values (one message per line) and append to NDJSON.
    # We also validate that each line is JSON and rewrite it as compact JSON.
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None

    with out_path.open("a", encoding="utf-8") as f:
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    # Keep non-JSON lines as-is (rare, but helps debugging).
                    f.write(json.dumps({"raw": line}, ensure_ascii=False) + "\n")
                    f.flush()
                    continue
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                f.flush()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    return 0


def main() -> None:
    p = argparse.ArgumentParser(description="Collect Kafka events into an append-only NDJSON file.")
    p.add_argument("--topic", default="demo-events")
    p.add_argument("--group", default="collector-group")
    p.add_argument("--out", default="data/raw_events.ndjson")
    p.add_argument("--from-beginning", action="store_true")
    p.add_argument("--no-show-code", action="store_true")
    args = p.parse_args()

    code = collect(
        topic=args.topic,
        group=args.group,
        out_path=Path(args.out),
        from_beginning=args.from_beginning,
        show_code=not args.no_show_code,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()

