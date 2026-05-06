from __future__ import annotations

import argparse
import subprocess
import threading

from kafka_demo_common import (
    Ctx,
    DEFAULT_GROUP,
    DEFAULT_TOPIC,
    add_common_flags,
    handle_introspection,
    print_cmd,
    repo_root_from_this_file,
)


def _consumer_argv(
    *,
    topic: str,
    group: str,
    from_beginning: bool,
    client_id: str,
    print_meta: bool,
) -> list[str]:
    flags: list[str] = [
        "kafka-console-consumer",
        "--bootstrap-server",
        "kafka:29092",
        "--topic",
        topic,
        "--group",
        group,
        "--consumer-property",
        f"client.id={client_id}",
    ]
    if from_beginning:
        flags.append("--from-beginning")
    if print_meta:
        flags += [
            "--property",
            "print.partition=true",
            "--property",
            "print.offset=true",
            "--property",
            "print.timestamp=true",
        ]
    bash_cmd = " ".join(flags)
    return ["docker", "compose", "exec", "-T", "kafka", "bash", "-lc", bash_cmd]


def _stream_prefixed(proc: subprocess.Popen, prefix: str) -> None:
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"{prefix}{line.rstrip()}", flush=True)


def consume(
    ctx: Ctx,
    topic: str,
    group: str,
    from_beginning: bool,
    instances: int,
    print_meta: bool,
) -> int:
    if instances < 1:
        raise SystemExit("--instances must be >= 1")

    if instances == 1:
        print(f"[kafka-demo] Consuming from topic: {topic} (group: {group}). Press Ctrl+C to stop.", flush=True)
        argv = _consumer_argv(
            topic=topic,
            group=group,
            from_beginning=from_beginning,
            client_id="demo-consumer-1",
            print_meta=print_meta,
        )
        if ctx.show_code:
            print_cmd("consumer command", argv)
        try:
            return subprocess.call(argv, cwd=str(ctx.root_dir))
        except KeyboardInterrupt:
            return 0

    print(
        f"[kafka-demo] Starting {instances} consumers in the same group: {group}\n"
        f"           Topic: {topic}\n"
        f"           Expectation: each consumer receives only some partitions.\n"
        f"           Press Ctrl+C to stop all.",
        flush=True,
    )

    procs: list[subprocess.Popen] = []
    threads: list[threading.Thread] = []
    try:
        for i in range(1, instances + 1):
            argv = _consumer_argv(
                topic=topic,
                group=group,
                from_beginning=from_beginning,
                client_id=f"demo-consumer-{i}",
                print_meta=print_meta,
            )
            if ctx.show_code:
                print_cmd(f"consumer #{i}", argv)
            p = subprocess.Popen(argv, cwd=str(ctx.root_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            procs.append(p)
            t = threading.Thread(target=_stream_prefixed, args=(p, f"[c{i}] "), daemon=True)
            threads.append(t)
            t.start()

        for p in procs:
            p.wait()
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=3)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass


def main() -> None:
    root_dir = repo_root_from_this_file(__file__)
    p = argparse.ArgumentParser(description="Consume events from a Kafka topic (consumer group).")
    add_common_flags(p)
    p.add_argument("topic", nargs="?", default=DEFAULT_TOPIC)
    p.add_argument("--group", default=DEFAULT_GROUP, help="Consumer group id (controls offsets/replay).")
    p.add_argument(
        "--instances",
        type=int,
        default=1,
        help="Start N consumers in the same group (shows real consumer group partition assignment).",
    )
    p.add_argument(
        "--print-meta",
        action="store_true",
        help="Print partition/offset/timestamp (recommended for group demo).",
    )
    p.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from earliest offsets (only effective for a new group without committed offsets).",
    )
    args = p.parse_args()

    handle_introspection(
        parser=p,
        module_file=__file__,
        functions=[_consumer_argv, _stream_prefixed, consume, main],
        args=args,
    )

    ctx = Ctx(root_dir=root_dir, show_code=not args.no_show_code)
    code = consume(
        ctx,
        args.topic,
        args.group,
        args.from_beginning,
        instances=args.instances,
        print_meta=args.print_meta,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()

