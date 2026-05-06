# Local Kafka environment (tutorial + live demo plan)

This tutorial is designed for a **live class demo** (~1.5h) and as a student reference.

## 0) Prerequisites

- Docker + Docker Compose installed
- Python 3.10+ installed
- Ports available on your machine:
  - `9092` (Kafka)
  - `8089` (Kafka UI)

Open your terminal in the **repo root**.

---

## 1) Setup check

```bash
python3 setup.py --check-ports
```

---

## 2) Start Kafka (Zookeeper) + Kafka UI

```bash
python3 up.py --reset
```

Open Kafka UI:

- `http://localhost:8089`

What to show:

- Kafka UI starts even if there are no topics yet.
- Kafka is “a log”, topics appear as logs.

---

## 3) Create and inspect a demo topic

> Note: Kafka UI may warn that topic names with `.` or `_` can collide in some metric names.  
> To avoid confusion, we use a topic name **without dots/underscores** in this tutorial.

Create:

```bash
python3 topic.py create demo-events
```

List:

```bash
python3 topic.py list
```

Describe (clean table output):

```bash
python3 topic.py describe demo-events
```

---

## 4) Demo: producer → topic → consumer

### 4.1 Start a consumer (terminal A)

```bash
python3 consume.py demo-events --group demo-group --from-beginning
```

### 4.2 Produce events (terminal B)

```bash
python3 produce.py demo-events --count 10 --sleep 0.3
```

What to highlight:

- events appear immediately at the consumer
- you can stop and start the consumer — offsets + group id matter

---

## 5) Consumer group demo (2 consumers share partitions)

Run:

```bash
python3 consume.py demo-events --group demo-group-split --instances 2 --print-meta --from-beginning
```

Then, in another terminal:

```bash
python3 produce.py demo-events --count 12 --sleep 0.1
```

What to highlight:

- output lines are prefixed with `[c1]`, `[c2]`
- each consumer receives a **subset** of partitions (see `Partition:`)

---

## 6) Stop everything

```bash
python3 down.py
```

This removes containers and volumes for a clean restart.

