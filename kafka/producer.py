"""Kafka producer: replays prepared historical data, one record at a time, as a simulated stream.

    python kafka/producer.py                      # streams the test set (240 events), 0.5 s apart
    python kafka/producer.py --delay 0.1          # faster
    python kafka/producer.py --inject-invalid     # also sends malformed events to exercise validation
    python kafka/producer.py --source full        # all 782 historical rows

The CSV files are bounded historical sources. Reading them row by row and waiting between rows is
only a *simulation* of data arriving over time (as in the guided lab of Unit 3).
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime
from typing import Callable

import pandas as pd
from confluent_kafka import Producer

import config
import contract
from kafka_utils import ensure_topic

CSV_COLUMNS = ["event_id", "country", "year", *contract.FACTOR_FIELDS, contract.TARGET_FIELD]

# A message to send: (description, Kafka key, function that returns the JSON text at emission time)
Message = tuple[str, str, Callable[[], str]]


def load_events(csv_path) -> list[dict]:
    """Read the prepared historical data and return one dict per record (native Python types)."""
    frame = pd.read_csv(csv_path)
    missing = [column for column in CSV_COLUMNS if column not in frame.columns]
    if missing:
        raise SystemExit(f"{csv_path} is missing columns required by the event contract: {missing}")

    events = []
    for row in frame[CSV_COLUMNS].itertuples(index=False):
        record = dict(zip(CSV_COLUMNS, row))
        record["event_id"] = str(record["event_id"])
        record["country"] = str(record["country"])
        record["year"] = int(record["year"])
        for name in contract.NUMERIC_FIELDS:
            record[name] = float(record[name])
        events.append(record)
    return events


def build_event(record: dict) -> dict:
    """Turn a historical record into a contract event; event_time is the moment of emission."""
    event = {name: record[name] for name in contract.REQUIRED_FIELDS if name != "event_time"}
    event["event_time"] = datetime.now().isoformat(timespec="seconds")
    return event


def valid_message(record: dict) -> Message:
    return (f"valid {record['event_id']}", record["country"], lambda: json.dumps(build_event(record)))


def invalid_messages(template: dict) -> list[Message]:
    """Deliberately broken messages that let the consumer demonstrate its error handling.

    Bad data is stored in the raw table with a status, skipped from prediction, and must never stop
    the stream. Some are sent as raw text so that even malformed JSON reaches Kafka untouched.
    """
    country = template["country"]

    def tweaked(event_id: str, **changes) -> Callable[[], str]:
        return lambda: json.dumps({**build_event(template), "event_id": event_id, **changes})

    def without_gdp() -> str:
        event = {**build_event(template), "event_id": "BAD01"}
        del event["gdp"]
        return json.dumps(event)

    return [
        ("INVALID: missing field (gdp)", country, without_gdp),
        ("INVALID: text instead of a number (gdp)", country, tweaked("BAD02", gdp="abc")),
        ("INVALID: negative factor (gdp)", country, tweaked("BAD03", gdp=-0.5)),
        ("INVALID: empty country", "UNKNOWN_COUNTRY", tweaked("BAD04", country="")),
        ("INVALID: bad timestamp", country, tweaked("BAD05", event_time="yesterday")),
        ("INVALID: year as text", country, tweaked("BAD06", year="2019")),
        ("INVALID: null feature (corruption)", country, tweaked("BAD07", corruption=None)),
        ("INVALID: score outside the 0-10 scale", country, tweaked("BAD08", actual_happiness_score=99)),
        ("INVALID: NaN feature (generosity)", country, tweaked("BAD09", generosity=float("nan"))),
        ("INVALID: malformed JSON", "BAD",
         lambda: '{"event_id": "BAD10", "country": "Colombia", "year": 20'),
        ("DUPLICATE: same event_id as the first valid event", country,
         lambda: json.dumps(build_event(template))),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream historical happiness records to Kafka.")
    parser.add_argument("--source", choices=sorted(config.STREAM_SOURCES), default="test",
                        help="test = held-out test set (default); full = every historical row")
    parser.add_argument("--csv", help="custom CSV path (overrides --source)")
    parser.add_argument("--delay", type=float, default=0.5, help="seconds to wait between events (default 0.5)")
    parser.add_argument("--limit", type=int, help="send only the first N valid events")
    parser.add_argument("--inject-invalid", action="store_true",
                        help="mix malformed events into the stream to exercise the consumer validation")
    args = parser.parse_args()

    csv_path = args.csv or config.STREAM_SOURCES[args.source]
    records = load_events(csv_path)
    if args.limit:
        records = records[: args.limit]

    messages: list[Message] = [valid_message(record) for record in records]
    if args.inject_invalid:
        bad = invalid_messages(records[0])
        step = max(1, len(records) // (len(bad) + 1))
        for number, message in enumerate(bad, start=1):
            # spread the bad events evenly; the duplicate (last) always lands after the original
            messages.insert(min(number * step + number - 1, len(messages)), message)

    ensure_topic(config.KAFKA_BOOTSTRAP_SERVERS, config.TOPIC,
                 config.TOPIC_PARTITIONS, config.TOPIC_REPLICATION_FACTOR)

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "happiness-producer",
        "enable.idempotence": True,   # retries cannot duplicate or reorder records inside a partition
    })

    delivered = Counter()
    failed = []

    def on_delivery(error, message):
        if error is not None:
            failed.append(str(error))
        else:
            delivered[message.partition()] += 1

    print(f"Streaming {len(messages)} messages from {csv_path} to topic '{config.TOPIC}' "
          f"(delay {args.delay}s, key = country). Press Ctrl+C to stop.\n")

    sent = 0
    try:
        for description, key, make_value in messages:
            producer.produce(topic=config.TOPIC, key=key, value=make_value(), on_delivery=on_delivery)
            producer.poll(0)          # serve delivery callbacks
            sent += 1
            print(f"Sent {sent:>4}/{len(messages)} | key={key:<28} | {description}")
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()

    print(f"\nSent: {sent} | delivered: {sum(delivered.values())} | failed: {len(failed)}")
    print("Delivered per partition:", dict(sorted(delivered.items())))
    for message in failed[:5]:
        print("  delivery error:", message)


if __name__ == "__main__":
    main()
