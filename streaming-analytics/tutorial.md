# Tutorial 2: Collect + analyze streaming data in real time

Goal: show a realistic workflow for **streaming data**:

- **ingest/collect** events into a raw dataset (append-only, NDJSON)
- run **real-time analytics** (windowed metrics, per-source counts, basic anomaly signals)

## Prerequisites

Run the basic environment first (from `../basic/`):

```bash
python3 setup.py --check-ports
```

```bash
python3 up.py --reset
```

Create the topic (if needed):

```bash
python3 topic.py create demo-events
```

## Part A — Collect (raw event log)

From repo root:

```bash
python3 streaming-analytics/collector.py --topic demo-events --group collector-group --out data/raw_events.ndjson
```

Now produce events (in another terminal, from `basic/`):

```bash
python3 produce.py demo-events --count 50 --sleep 0.1
```

What to show:

- you can stop and restart the collector and it will continue appending
- the output file is an **append-only raw log** (good for replay/debugging)

## Part B — Real-time analytics (sliding window)

Run the analytics loop:

```bash
python3 streaming-analytics/realtime_analytics.py --topic demo-events --group analytics-group --window-seconds 30 --print-every 2
```

Now produce more events:

```bash
python3 produce.py demo-events --count 200 --sleep 0.05
```

What to show:

- throughput (events/sec)
- windowed average/min/max of `value`
- per-`source` counts (distribution drift)
- basic “lateness” signal (event_time vs now)

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

