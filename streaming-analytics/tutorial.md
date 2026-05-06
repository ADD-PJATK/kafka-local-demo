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

### What the real-time report shows exactly

The analytics output is a **sliding-window summary**. Every ~20 seconds (default), it prints metrics computed from **the last `--window-seconds` seconds** of events that the analytics consumer has received.

#### `window: <count> events / <window_seconds>s (eps=<x.xx>)`

- **What it shows**: how many events are currently “in memory” inside the active time window (for example: last 30 seconds).
- **`eps` (events per second)** is computed as:

\[
eps = \frac{window\_events}{window\_seconds}
\]

- **Why it matters**: `eps` is the simplest throughput/load indicator. If it drops, ingestion slowed down; if it spikes, your pipeline is under higher load.

#### `value: avg=<...> min=<...> max=<...>`

- **What it shows**: windowed statistics of the numeric field `value` (only events where `value` is parseable as a number).
- **How to interpret**:
  - rising **`avg`** and/or **`max`** may indicate a spike/outlier burst (or a real distribution shift in the data source)
  - `min/max` shows the range; large `max` with stable `avg` often means rare outliers

#### `sources: web:<..>, mobile:<..>, iot:<..> ...`

- **What it shows**: per-`source` counts in the window (a “real-time distribution” snapshot).
- **Why it matters**: it lets you spot distribution drift quickly (e.g. suddenly 90% of traffic is `web`).
- **In real pipelines**: these counters often feed alerting and drift detection.

#### `lateness (received - event_time): avg=<...>s p95=<...>s`

- **What it shows**: event “lateness” (in seconds) inside the window:

\[
lateness = received\_time - event\_time
\]

- **Why it matters**: if lateness grows, events are arriving “old” (network delays, queues, backpressure, slow producers).
- **Important concept**:
  - **event time**: when the event actually happened (inside the payload)
  - **processing/ingest time**: when your system observed/processed it

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

