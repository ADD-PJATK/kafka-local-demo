# Tutorial 2: Collect + analyze streaming data in real time

Goal: show a realistic workflow for **streaming data**:

- **ingest/collect** events into a raw dataset (append-only, NDJSON)
- run **real-time analytics** (windowed metrics, per-source counts, basic anomaly signals)

## 0) Setup (from scratch)

From repo root:

```bash
python3 basic/setup.py --check-ports
```

```bash
python3 basic/up.py --reset
```

Create the topic:

```bash
python3 basic/topic.py create demo-events
```

Note:

- Run commands **from the repo root** as written above.
- If you `cd streaming-analytics/`, run scripts without the prefix, e.g. `python3 generator.py ...`.

## 1) Continuous data generation (the stream)

Run the generator (Ctrl+C to stop):

```bash
python3 streaming-analytics/generator.py --topic demo-events --sleep 0.05 --ensure-topic
```

## 2) Collect (raw event log)

In another terminal, start the collector (Ctrl+C to stop):

```bash
python3 streaming-analytics/collector.py --topic demo-events --group collector-group --out data/raw_events.ndjson
```

What to show:

- you can stop and restart the collector and it will continue appending (offsets are tracked by the group)
- the output file is an **append-only raw log** (good for replay/debugging)

## 3) Real-time analytics (sliding window)

In another terminal, run the analytics loop (Ctrl+C to stop):

```bash
python3 streaming-analytics/realtime_analytics.py --topic demo-events --group analytics-group --window-seconds 30
```

How to read the report (line by line):

- `window: <count> events / <window_seconds>s (eps=<x.xx>)`
  - **what it is**: number of events currently inside the sliding time window
  - **eps**: events per second in the window:
    \[
    eps = \frac{window\_events}{window\_seconds}
    \]
  - **why it matters**: simplest throughput/load metric

- `value: avg=<...> min=<...> max=<...>`
  - **what it is**: statistics of numeric field `value` inside the window
  - **how to interpret**:
    - rising `avg` / `max` can indicate spikes (or a real distribution shift)
    - `min/max` show range and outliers

- `lateness (received - event_time): avg=<...>s p95=<...>s`
  - **what it is**: “lateness” in seconds:
    \[
    lateness = received\_time - event\_time
    \]
  - **why it matters**: when it grows, events arrive “old” (network delay, queues, backpressure)

- `sources: web:<..>, mobile:<..>, iot:<..> ...`
  - **what it is**: per-`source` counts in the window (real-time distribution)
  - **why it matters**: quick way to notice drift (e.g., suddenly 90% `web`)

## Notes (teaching points)

- Kafka consumer groups allow you to scale **reading** (partitions bound parallelism).
- A collector often writes a raw log (NDJSON/Parquet) before heavier analytics.
- “Real-time analytics” is usually window-based. You always trade accuracy vs latency.

## Stop / cleanup

Stop scripts with Ctrl+C.

To stop Kafka:

```bash
python3 basic/down.py
```

