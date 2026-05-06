from __future__ import annotations

import argparse
import re

from kafka_demo_common import (
    Ctx,
    DEFAULT_TOPIC,
    add_common_flags,
    docker_exec_kafka,
    handle_introspection,
    repo_root_from_this_file,
)


def topic_create(ctx: Ctx, topic: str, partitions: int, replication: int) -> None:
    docker_exec_kafka(
        ctx,
        f'kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic "{topic}" '
        f'--partitions "{partitions}" --replication-factor "{replication}"',
        label="topic create",
    )


def topic_list(ctx: Ctx) -> None:
    docker_exec_kafka(ctx, "kafka-topics --bootstrap-server kafka:29092 --list", label="topic list")


def _parse_describe(raw: str) -> tuple[str, list[tuple[str, str, str, str]]]:
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    topic_lines = [ln for ln in lines if ln.startswith("Topic:")]
    if not topic_lines:
        return "(no output)", []
    summary = topic_lines[0]
    rows: list[tuple[str, str, str, str]] = []
    for ln in topic_lines[1:]:
        m = re.search(r"Partition:\s*(\d+)\s+Leader:\s*([^\s]+)\s+Replicas:\s*([^\s]+)\s+Isr:\s*([^\s]+)", ln)
        if m:
            rows.append((m.group(1), m.group(2), m.group(3), m.group(4)))
    return summary, rows


def _print_table(summary: str, rows: list[tuple[str, str, str, str]]) -> None:
    print(summary)
    print()
    headers = ["Partition", "Leader", "Replicas", "ISR"]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))

    def fmt(cols: list[str]) -> str:
        return "  " + " | ".join(cols[i].ljust(widths[i]) for i in range(len(cols)))

    print(fmt(headers))
    print("  " + "-+-".join("-" * w for w in widths))
    for p, leader, reps, isr in rows:
        print(fmt([p, leader, reps, isr]))


def topic_describe(ctx: Ctx, topic: str, raw: bool) -> None:
    out = docker_exec_kafka(
        ctx,
        f'kafka-topics --bootstrap-server kafka:29092 --describe --topic "{topic}"',
        label="topic describe",
        capture=True,
    )
    if raw:
        print(out.rstrip())
        return
    summary, rows = _parse_describe(out)
    if summary == "(no output)":
        print(summary)
        return
    _print_table(summary, rows)


def topic_delete(ctx: Ctx, topic: str) -> None:
    docker_exec_kafka(
        ctx,
        f'kafka-topics --bootstrap-server kafka:29092 --delete --topic "{topic}"',
        label="topic delete",
    )


def main() -> None:
    root_dir = repo_root_from_this_file(__file__)
    p = argparse.ArgumentParser(description="Kafka topic helper (Docker Compose exec).")
    add_common_flags(p)

    sub = p.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="Create a topic")
    p_create.add_argument("topic", nargs="?", default=DEFAULT_TOPIC)
    p_create.add_argument("--partitions", type=int, default=3)
    p_create.add_argument("--replication", type=int, default=1)

    sub.add_parser("list", help="List topics")

    p_desc = sub.add_parser("describe", help="Describe a topic")
    p_desc.add_argument("topic", nargs="?", default=DEFAULT_TOPIC)
    p_desc.add_argument("--raw", action="store_true", help="Print Kafka's original output")

    p_del = sub.add_parser("delete", help="Delete a topic")
    p_del.add_argument("topic", nargs="?", default=DEFAULT_TOPIC)

    args = p.parse_args()

    handle_introspection(
        parser=p,
        module_file=__file__,
        functions=[topic_create, topic_list, topic_describe, topic_delete, _parse_describe, _print_table, main],
        args=args,
    )

    ctx = Ctx(root_dir=root_dir, show_code=not args.no_show_code)
    if args.cmd == "create":
        topic_create(ctx, args.topic, args.partitions, args.replication)
    elif args.cmd == "list":
        topic_list(ctx)
    elif args.cmd == "describe":
        topic_describe(ctx, args.topic, args.raw)
    elif args.cmd == "delete":
        topic_delete(ctx, args.topic)


if __name__ == "__main__":
    main()

