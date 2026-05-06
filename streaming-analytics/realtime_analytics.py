from __future__ import annotations

import argparse
import collections
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Optional


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


def _parse_iso(ts: str) -> Optional[float]:
    try:
        # Accept both "...Z" and offset forms.
        s = (ts or "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except Exception:
        return None


@dataclass
class Event:
    received_ts: float
    event_ts: Optional[float]
    source: str
    value: Optional[float]


def _as_event(obj: dict, received_ts: float) -> Event:
    event_ts = _parse_iso(str(obj.get("event_time", "")))
    source = str(obj.get("source", "unknown"))
    v = obj.get("value", None)
    try:
        value = float(v) if v is not None else None
    except Exception:
        value = None
    return Event(received_ts=received_ts, event_ts=event_ts, source=source, value=value)


def _prune(window: Deque[Event], now: float, window_s: float) -> None:
    cutoff = now - window_s
    while window and window[0].received_ts < cutoff:
        window.popleft()


def _compute(window: Deque[Event], now: float, window_s: float) -> dict:
    _prune(window, now, window_s)

    values = [e.value for e in window if e.value is not None]
    sources: Dict[str, int] = collections.Counter(e.source for e in window)
    lateness = []
    for e in window:
        if e.event_ts is not None:
            lateness.append(e.received_ts - e.event_ts)

    out: dict = {
        "window_events": len(window),
        "window_seconds": window_s,
        "events_per_sec": (len(window) / window_s) if window_s > 0 else 0.0,
        "sources": dict(sorted(sources.items(), key=lambda kv: (-kv[1], kv[0]))),
    }

    if values:
        out.update(
            {
                "value_min": min(values),
                "value_max": max(values),
                "value_avg": sum(values) / len(values),
            }
        )
    else:
        out.update({"value_min": None, "value_max": None, "value_avg": None})

    if lateness:
        out.update(
            {
                "lateness_avg_s": sum(lateness) / len(lateness),
                "lateness_p95_s": sorted(lateness)[max(0, int(0.95 * (len(lateness) - 1)))],
            }
        )
    else:
        out.update({"lateness_avg_s": None, "lateness_p95_s": None})

    return out


def _fmt(v: Optional[float], *, digits: int = 2) -> str:
    if v is None:
        return "—"
    return f"{v:.{digits}f}"


def _format_report(stats: dict) -> str:
    # Build a compact multi-line report that is easy to read on a projector.
    sources_items = list(stats.get("sources", {}).items())
    top_sources = sources_items[:8]
    sources_line = ", ".join(f"{k}:{v}" for k, v in top_sources) if top_sources else "—"

    lines = [
        "[rt] Real-time window report",
        f"  - window: {stats.get('window_events', 0)} events / {stats.get('window_seconds', 0)}s  (eps={_fmt(stats.get('events_per_sec'))})",
        f"  - value:  avg={_fmt(stats.get('value_avg'))}  min={_fmt(stats.get('value_min'))}  max={_fmt(stats.get('value_max'))}",
        f"  - lateness (received - event_time): avg={_fmt(stats.get('lateness_avg_s'))}s  p95={_fmt(stats.get('lateness_p95_s'))}s",
        f"  - sources: {sources_line}",
    ]
    return "\n".join(lines)


def run_analytics(
    *,
    topic: str,
    group: str,
    window_s: float,
    print_every_s: float,
    from_beginning: bool,
    show_code: bool,
    compose_file: Path,
) -> int:
    argv = _consumer_argv(compose_file=compose_file, topic=topic, group=group, from_beginning=from_beginning)
    if show_code:
        print("[code] consumer command:")
        print("  " + " ".join(argv))

    proc = subprocess.Popen(argv, cwd=str(_repo_root()), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None

    window: Deque[Event] = collections.deque()
    last_print = time.time()

    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            now = time.time()
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            window.append(_as_event(obj, received_ts=now))

            if now - last_print >= print_every_s:
                stats = _compute(window, now=now, window_s=window_s)
                print(_format_report(stats), flush=True)
                last_print = now

        return 0
    except KeyboardInterrupt:
        return 0
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


def main() -> None:
    p = argparse.ArgumentParser(description="Simple real-time analytics over a Kafka topic (sliding window).")
    p.add_argument("--topic", default="demo-events")
    p.add_argument("--group", default="analytics-group")
    p.add_argument("--window-seconds", type=float, default=30.0)
    p.add_argument("--print-every", type=float, default=20.0, help="How often to print the report (seconds).")
    p.add_argument("--from-beginning", action="store_true")
    p.add_argument(
        "--compose-file",
        default="basic/docker-compose.yml",
        help="Path to docker-compose.yml (relative to repo root is recommended).",
    )
    p.add_argument("--no-show-code", action="store_true")
    args = p.parse_args()

    code = run_analytics(
        topic=args.topic,
        group=args.group,
        window_s=args.window_seconds,
        print_every_s=args.print_every,
        from_beginning=args.from_beginning,
        show_code=not args.no_show_code,
        compose_file=(_repo_root() / args.compose_file).resolve(),
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()

