from __future__ import annotations

import argparse
import ast
import json
import subprocess
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _consumer_argv(compose_file: Path, topic: str, group: str, from_beginning: bool) -> list[str]:
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
    return ["docker", "compose", "-f", str(compose_file), "exec", "-T", "kafka", "bash", "-lc", bash_cmd]


def collect(topic: str, group: str, out_path: Path, from_beginning: bool, show_code: bool, compose_file: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    argv = _consumer_argv(compose_file=compose_file, topic=topic, group=group, from_beginning=from_beginning)
    if show_code:
        print("[code] consumer command:")
        print("  " + " ".join(argv))

    # Stream raw values (one message per line) and append to NDJSON.
    # We also validate that each line is JSON and rewrite it as compact JSON.
    proc = subprocess.Popen(argv, cwd=str(_repo_root()), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None

    def parse_obj(line: str) -> dict | None:
        try:
            obj = json.loads(line)
            return obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            pass
        if line.lstrip().startswith("{") and ("'" in line):
            try:
                obj = ast.literal_eval(line)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
        return None

    with out_path.open("a", encoding="utf-8") as f:
        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                obj = parse_obj(line)
                if obj is None:
                    # Keep non-event lines as-is (rare, but helps debugging).
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
    p = argparse.ArgumentParser(
        description="Collect Kafka events into an append-only NDJSON file.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "What this script runs under the hood\n"
            "-------------------------------\n"
            "It executes Kafka CLI in the Kafka container via docker compose:\n"
            "  docker compose -f <compose-file> exec -T kafka bash -lc \"kafka-console-consumer ...\"\n"
            "\n"
            "Kafka CLI used here: kafka-console-consumer\n"
            "  --bootstrap-server kafka:29092   address of the broker as seen *from the container*\n"
            "  --topic <name>                  topic to read from\n"
            "  --group <id>                    consumer group id (controls committed offsets)\n"
            "  --from-beginning                start from earliest offsets (only for a group with no commits)\n"
        ),
    )
    p.add_argument("--topic", default="demo-events", help='Maps to: kafka-console-consumer --topic "<topic>"')
    p.add_argument("--group", default="collector-group", help='Maps to: kafka-console-consumer --group "<id>"')
    p.add_argument("--out", default="data/raw_events.ndjson", help="Where to append NDJSON output on the host machine.")
    p.add_argument("--from-beginning", action="store_true", help="Maps to: kafka-console-consumer --from-beginning")
    p.add_argument(
        "--compose-file",
        default="basic/docker-compose.yml",
        help="Which compose file to pass to: docker compose -f <compose-file> ...",
    )
    p.add_argument("--no-show-code", action="store_true", help="Do not print the underlying docker/kafka command.")
    args = p.parse_args()

    code = collect(
        topic=args.topic,
        group=args.group,
        out_path=Path(args.out),
        from_beginning=args.from_beginning,
        show_code=not args.no_show_code,
        compose_file=(_repo_root() / args.compose_file).resolve(),
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()

