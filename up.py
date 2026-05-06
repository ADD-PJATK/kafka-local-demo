from __future__ import annotations

import argparse
import time

from kafka_demo_common import (
    Ctx,
    add_common_flags,
    docker_compose,
    docker_exec_kafka,
    handle_introspection,
    repo_root_from_this_file,
)


def start_stack(ctx: Ctx) -> None:
    print("[kafka-demo] Starting Kafka + Kafka UI...")
    docker_compose(ctx, ["up", "-d"], label="docker compose up")


def wait_ready(ctx: Ctx, timeout_s: int) -> None:
    print(f"[kafka-demo] Waiting for Kafka readiness (max {timeout_s}s)...")
    deadline = time.time() + timeout_s
    while True:
        try:
            docker_exec_kafka(
                ctx,
                'kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1',
                label="readiness check",
                capture=False,
            )
            break
        except Exception:
            if time.time() > deadline:
                raise SystemExit("[kafka-demo] ERROR: Kafka not ready in time.")
            time.sleep(2)


def main() -> None:
    root_dir = repo_root_from_this_file(__file__)

    p = argparse.ArgumentParser(description="Start local Kafka + Kafka UI (Docker Compose).")
    add_common_flags(p)
    p.add_argument("--timeout", type=int, default=60, help="Readiness timeout in seconds.")
    p.add_argument(
        "--reset",
        action="store_true",
        help="Run `docker compose down -v` before starting (recommended if Kafka fails to start).",
    )
    args = p.parse_args()

    handle_introspection(parser=p, module_file=__file__, functions=[start_stack, wait_ready, main], args=args)

    ctx = Ctx(root_dir=root_dir, show_code=not args.no_show_code)
    if args.reset:
        docker_compose(ctx, ["down", "-v"], label="docker compose down -v")
    start_stack(ctx)
    wait_ready(ctx, timeout_s=args.timeout)
    print("[kafka-demo] Kafka is up.")
    print("[kafka-demo] Kafka UI: http://localhost:8089")


if __name__ == "__main__":
    main()

