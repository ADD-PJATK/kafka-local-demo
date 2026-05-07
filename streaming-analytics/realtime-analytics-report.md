# Real-time analytics report — how to read it (detailed)

This document explains, **line by line**, what the terminal report printed by:

```bash
python3 streaming-analytics/realtime_analytics.py
```

means, what each metric is **exactly**, and how to interpret it during a live demo.

## Example output (shape)

You’ll see a repeating block like:

```text
[rt] Real-time window report
  - input: parsed=123  skipped_non_json=0
  - window: 87 events / 30.0s  (eps=2.90)
  - value:  avg=51.23  min=3.00  max=612.00
  - lateness (received - event_time): avg=1.10s  p95=2.40s
  - sources: web:40, mobile:30, iot:15, demo:2
```

Important: this is a **sliding-window summary**. The script keeps a small “in-memory” window of recent events and recomputes these stats periodically.

## `input: parsed=<n> skipped_non_json=<n>`

### What it is
- **`parsed`**: how many incoming lines (messages) were successfully parsed into an event object.
- **`skipped_non_json`**: how many incoming lines were ignored because they were not valid event payloads.

### Why this line exists
In a classroom demo, you sometimes get non-event output on the consumer stream, e.g.:
- Docker / Kafka CLI noise
- accidental non-JSON payloads
- payloads printed in a Python-dict style like `{'key': 'value'}` (single quotes)

This counter makes it obvious **whether you’re actually parsing events**.

### How to interpret quickly
- **`parsed` grows** and `window` stays non-zero → everything is working.
- **`parsed` stays at 0** and **`skipped_non_json` grows** → your consumer is reading something, but it’s not your event JSON (wrong topic, wrong producer format, or noisy output).

## `window: <count> events / <window_seconds>s (eps=<x.xx>)`

### What it is
- **`window`** is the *current in-memory sample* used for statistics:
  - the script keeps only events whose `received_time` is within the last `window_seconds`
  - everything older than the cutoff is removed (“pruned”)
- **`<count>`** is the number of events currently inside that time window.
- **`eps`** means **events per second inside the window**:

\[
eps = \frac{window\_events}{window\_seconds}
\]

### What it is *not*
- It is **not** “total events in the Kafka topic”.
- It is **not** “all historical events”.
- It is **not** “broker throughput”.

It is strictly: “how many events the analytics consumer has received *recently*”.

### How to interpret
- **Higher `count` / `eps`** → higher current load / faster production.
- **Sudden drop to 0** usually means one of:
  - the generator stopped
  - the consumer group is at the end and no new messages arrive
  - you’re reading the wrong topic
  - you’re skipping payloads (see `skipped_non_json`)

### Why windowing is used in real systems
- In streaming analytics, you rarely care about “all-time averages” in real-time.
- Windows give you **freshness** and control over **latency vs stability**:
  - short window → fast reaction but noisy stats
  - long window → smoother stats but slower reaction

## `value: avg=<...> min=<...> max=<...>`

### What it is
- These are windowed statistics of the numeric field **`value`** (only events where `value` can be interpreted as a number).
  - **`avg`**: arithmetic mean in the current window
  - **`min`** / **`max`**: extremes in the current window

### Typical interpretations during a demo
- **`max` spikes** while `avg` is stable → a few rare outliers (good moment to mention anomaly/outlier detection).
- **`avg` gradually rises** → a distribution shift (possible “drift” or a changed generator behavior).
- **`min/max` range becomes huge** → data is more variable; downstream models may degrade.

### Common pitfalls
- If you see `avg=— min=— max=—`:
  - your events may not contain `value`
  - or `value` is not numeric (string, null)
  - or you’re skipping payloads

## `sources: <name>:<count>, ...`

### What it is
- Windowed counts grouped by the categorical field **`source`**.
- This is a **real-time distribution snapshot**.

### Why it matters
- It answers: “Which sources dominate *right now*?”
- It is a simple but powerful building block for:
  - drift detection (“suddenly 90% of traffic is web”)
  - segmentation dashboards
  - capacity planning

### How to interpret
- A stable mix suggests stable behavior.
- A sudden skew can indicate:
  - a source outage
  - a traffic routing change
  - bot/spam bursts
  - an upstream bug

## `lateness (received - event_time): avg=<...>s p95=<...>s`

### What it is
For each event, the script compares:
- **`event_time`**: when the event happened (inside the payload)
- **`received_time`**: when the analytics consumer received it

and computes:

\[
lateness = received\_time - event\_time
\]

Then it reports:
- **`avg`**: average lateness in the current window
- **`p95`**: “95th percentile” lateness — a robust tail-latency indicator

### Why `p95` matters (teaching point)
Average can look fine while the “tail” is bad.
- Example: most events are 0.2s late, but some are 5–10s late → `avg` hides it, `p95` reveals it.

### How to interpret increases
If lateness grows, typical reasons are:
- network delays
- producer buffering
- broker congestion
- consumer backpressure (consumer is too slow)
- slow disks / overloaded VM

### Important streaming concept
Real systems often distinguish:
- **Event time** (timestamp in payload)
- **Processing / ingest time** (timestamp when system sees the event)

Windowing by event time vs processing time has different semantics.

## Troubleshooting: “events are printed, but window is 0”

Use these checks in order:

- **Check `input` counters**
  - If `parsed=0` and `skipped_non_json>0`, your payload isn’t being parsed as an event.
- **Check the topic**
  - ensure you produce to the same topic that analytics consumes.
- **Check the consumer group**
  - if the group already consumed everything, it will sit at the end and wait for new messages.
  - use a fresh group id for a quick demo, e.g. `--group analytics-demo-1`
- **Check that the generator is really producing into Kafka**
  - seeing generator output in terminal is good, but the definitive signal is analytics `parsed` increasing.

## Recommended “classroom-friendly” settings

- **Readable report cadence**:

```bash
python3 streaming-analytics/realtime_analytics.py --print-every 10 --window-seconds 30
```

- **If you want slower, more “dashboard-like” updates**:

```bash
python3 streaming-analytics/realtime_analytics.py --print-every 20 --window-seconds 30
```

- **If you want a faster live feeling (more movement)**:

```bash
python3 streaming-analytics/realtime_analytics.py --print-every 2 --window-seconds 10
```

