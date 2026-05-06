from __future__ import annotations

import argparse

from kafka_demo_common import (
    Ctx,
    add_common_flags,
    docker_compose,
    handle_introspection,
    repo_root_from_this_file,
)


def stop_stack(ctx: Ctx, remove_volumes: bool) -> None:
    print("[kafka-demo] Stopping Kafka + Kafka UI...")
    args = ["down"]
    if remove_volumes:
        args.append("-v")
    docker_compose(ctx, args, label="docker compose down")
    print("[kafka-demo] Done.")


def main() -> None:
    root_dir = repo_root_from_this_file(__file__)
    p = argparse.ArgumentParser(description="Stop local Kafka demo (Docker Compose).")
    add_common_flags(p)
    p.add_argument("--keep-volumes", action="store_true", help="Do not remove volumes (keeps topics/offsets).")
    args = p.parse_args()

    handle_introspection(parser=p, module_file=__file__, functions=[stop_stack, main], args=args)

    ctx = Ctx(root_dir=root_dir, show_code=not args.no_show_code)
    stop_stack(ctx, remove_volumes=not args.keep_volumes)


if __name__ == "__main__":
    main()

