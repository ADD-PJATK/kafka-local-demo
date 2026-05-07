from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


def _parse_iso(ts: str) -> Optional[datetime]:
    """
    Accepts ISO timestamps with either:
    - trailing 'Z'
    - explicit offset like '+02:00'
    """
    try:
        s = (ts or "").replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _is_number(x: Any) -> bool:
    if isinstance(x, bool):
        return False
    return isinstance(x, (int, float))


@dataclass
class Counters:
    total_lines: int = 0
    parsed_json: int = 0
    valid_events: int = 0
    invalid_events: int = 0
    skipped_empty: int = 0

    missing_fields: int = 0
    bad_event_time: int = 0
    bad_value_type: int = 0
    bad_source_type: int = 0
    bad_event_id_type: int = 0
    raw_wrapper_lines: int = 0  # {"raw": "..."} lines written by collector for non-event output


def validate_event(obj: dict) -> list[str]:
    """
    Expected schema (minimal):
    - event_id: str (or int convertible to str is allowed)
    - event_time: ISO datetime string
    - source: str
    - value: int/float (optional but recommended)
    """
    errors: list[str] = []

    # Collector may store non-JSON payload lines as {"raw": "..."} for debugging.
    if set(obj.keys()) == {"raw"}:
        return ["non-event line (raw wrapper)"]

    required = ["event_id", "event_time", "source", "value"]
    missing = [k for k in required if k not in obj]
    if missing:
        errors.append(f"missing fields: {', '.join(missing)}")
        return errors

    event_id = obj.get("event_id")
    if not isinstance(event_id, (str, int)):
        errors.append("event_id is not str/int")

    event_time = obj.get("event_time")
    if not isinstance(event_time, str) or _parse_iso(event_time) is None:
        errors.append("event_time is not valid ISO datetime")

    source = obj.get("source")
    if not isinstance(source, str) or not source.strip():
        errors.append("source is not a non-empty string")

    value = obj.get("value")
    if value is None or not _is_number(value):
        errors.append("value is not a number")

    return errors


def validate_file(
    *,
    in_path: Path,
    max_examples: int,
    max_errors: int,
    write_clean: Optional[Path],
) -> int:
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    counters = Counters()
    examples: list[dict[str, Any]] = []

    out_f = None
    if write_clean is not None:
        write_clean.parent.mkdir(parents=True, exist_ok=True)
        out_f = write_clean.open("w", encoding="utf-8")

    try:
        with in_path.open("r", encoding="utf-8") as f:
            for ln_no, line in enumerate(f, start=1):
                counters.total_lines += 1
                line = line.strip()
                if not line:
                    counters.skipped_empty += 1
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    counters.invalid_events += 1
                    if len(examples) < max_examples:
                        examples.append({"line": ln_no, "error": "invalid JSON", "raw": line[:500]})
                    if counters.invalid_events >= max_errors:
                        break
                    continue

                counters.parsed_json += 1
                if not isinstance(obj, dict):
                    counters.invalid_events += 1
                    if len(examples) < max_examples:
                        examples.append({"line": ln_no, "error": "JSON is not an object", "raw": line[:500]})
                    if counters.invalid_events >= max_errors:
                        break
                    continue

                errs = validate_event(obj)
                if not errs:
                    counters.valid_events += 1
                    if out_f is not None:
                        out_f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    continue

                counters.invalid_events += 1
                msg = "; ".join(errs)
                if msg == "non-event line (raw wrapper)":
                    counters.raw_wrapper_lines += 1
                else:
                    if "missing fields" in msg:
                        counters.missing_fields += 1
                    if "event_time" in msg:
                        counters.bad_event_time += 1
                    if "value" in msg:
                        counters.bad_value_type += 1
                    if "source" in msg:
                        counters.bad_source_type += 1
                    if "event_id" in msg:
                        counters.bad_event_id_type += 1

                if len(examples) < max_examples:
                    examples.append({"line": ln_no, "error": msg, "obj": obj})
                if counters.invalid_events >= max_errors:
                    break
    finally:
        if out_f is not None:
            out_f.close()

    # Report
    print("[validate] raw events file check")
    print(f"  - input: {in_path}")
    if write_clean is not None:
        print(f"  - clean_out: {write_clean}")
    print()
    print("[validate] summary")
    print(f"  - total_lines: {counters.total_lines}")
    print(f"  - parsed_json: {counters.parsed_json}")
    print(f"  - valid_events: {counters.valid_events}")
    print(f"  - invalid_events: {counters.invalid_events}")
    print(f"  - skipped_empty: {counters.skipped_empty}")
    print(f"  - raw_wrapper_lines: {counters.raw_wrapper_lines}")
    print()
    print("[validate] top error counters (best-effort)")
    print(f"  - missing_fields: {counters.missing_fields}")
    print(f"  - bad_event_time: {counters.bad_event_time}")
    print(f"  - bad_source_type: {counters.bad_source_type}")
    print(f"  - bad_value_type: {counters.bad_value_type}")
    print(f"  - bad_event_id_type: {counters.bad_event_id_type}")

    if examples:
        print()
        print("[validate] examples (first N)")
        for ex in examples:
            print("  ---")
            print(f"  line: {ex.get('line')}")
            print(f"  error: {ex.get('error')}")
            if "raw" in ex:
                print(f"  raw: {ex.get('raw')}")
            if "obj" in ex:
                try:
                    print("  obj: " + json.dumps(ex["obj"], ensure_ascii=False))
                except Exception:
                    print(f"  obj: {ex.get('obj')}")

    # Exit code: 0 only if nothing invalid (excluding raw-wrapper lines, which are not events)
    if counters.invalid_events == 0:
        return 0
    # If all invalids are raw wrappers, treat as success but warn (collector captured noise).
    if counters.invalid_events == counters.raw_wrapper_lines:
        print()
        print("[validate] note: only non-event 'raw wrapper' lines were found; events look valid.")
        return 0
    return 2


def main() -> None:
    p = argparse.ArgumentParser(description="Validate collected NDJSON events (schema + basic sanity checks).")
    p.add_argument("--in", dest="in_path", default="data/raw_events.ndjson", help="Input NDJSON file")
    p.add_argument("--max-examples", type=int, default=10, help="How many invalid examples to print")
    p.add_argument("--max-errors", type=int, default=10_000, help="Stop after this many invalid events")
    p.add_argument(
        "--write-clean",
        default=None,
        help="Optional path to write only valid events as NDJSON (e.g. data/raw_events_clean.ndjson)",
    )
    args = p.parse_args()

    code = validate_file(
        in_path=Path(args.in_path),
        max_examples=max(0, args.max_examples),
        max_errors=max(1, args.max_errors),
        write_clean=Path(args.write_clean) if args.write_clean else None,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()

