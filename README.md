# Kafka Local Demo (Docker Compose) — cross-platform Python CLI

This repository contains a **local Kafka + Kafka UI demo environment** designed for classroom use and self-study.

## Quick start

```bash
python3 setup.py --check-ports
```

```bash
python3 up.py --reset
```

Open Kafka UI:

- `http://localhost:8089`

Create a demo topic:

```bash
python3 topic.py create demo-events
```

Describe it (clean table output):

```bash
python3 topic.py describe demo-events
```

Start a consumer:

```bash
python3 consume.py demo-events --group demo-group --from-beginning
```

Produce events (in another terminal):

```bash
python3 produce.py demo-events --count 10 --sleep 0.3
```

## Consumer group split demo (real scaling behavior)

Start **two consumers in the same group**:

```bash
python3 consume.py demo-events --group demo-group-split --instances 2 --print-meta --from-beginning
```

Then produce a few messages:

```bash
python3 produce.py demo-events --count 12 --sleep 0.1
```

You should see output prefixed with `[c1]`, `[c2]` and different `Partition:` values per consumer.

## Stop / reset

```bash
python3 down.py
```

## Files

- `docker-compose.yml`: Kafka + Zookeeper + Kafka UI
- `kafka-local-tutorial.md`: step-by-step tutorial (copy/paste friendly)
- `slides-tutorial.md`: tutorial slide deck source
- `presentation-tutorial.pdf`: generated tutorial slide deck (projector-ready)
- `setup.py`, `up.py`, `down.py`, `topic.py`, `produce.py`, `consume.py`: demo CLI tools

