"""Kafka consumer: real-time inference with raw-event storage, contract validation and PostgreSQL.

    python kafka/consumer.py                    # run until Ctrl+C
    python kafka/consumer.py --idle-timeout 20  # stop after 20 s without new messages

For every message, in this order:
  1. store the original message in raw_happiness_events (status RECEIVED), before anything else
  2. parse the JSON
  3. validate the event contract (see contract.py)
  4. build the features in the SAME order used for training
  5. predict with the serialized Pipeline (models/model.pkl)
  6. store the dimensions + fact row and mark the raw row VALID (one transaction)
  7. commit the Kafka offset (only after the database work is done)

Invalid events are stored with their status, skipped from prediction, and never stop the consumer.
"""
from __future__ import annotations

import argparse
import json
import math
import pickle
import sys
import time
from collections import Counter
from dataclasses import dataclass

import pandas as pd
import psycopg2
from confluent_kafka import Consumer, KafkaError

import config
import contract
import db
from kafka_utils import ensure_topic

MAX_DB_RETRIES = 5


@dataclass
class KafkaRecord:
    """The parts of a Kafka message the pipeline needs (keeps process_record independent from Kafka)."""
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    timestamp_ms: int | None = None


@dataclass
class Outcome:
    status: str
    raw_event_id: int
    event_id: str | None = None
    detail: str | None = None
    predicted: float | None = None
    actual: float | None = None
    replayed: bool = False


def load_model():
    """Load model.pkl and its metadata; refuse to start if training and serving disagree."""
    if not config.MODEL_PATH.exists() or not config.METADATA_PATH.exists():
        sys.exit("models/model.pkl or models/model_metadata.json not found. Run notebooks/model_training.ipynb first.")

    with open(config.MODEL_PATH, "rb") as file:
        model = pickle.load(file)
    features = json.loads(config.METADATA_PATH.read_text(encoding="utf-8"))["features"]

    if list(model.feature_names_in_) != features:
        sys.exit(f"Feature order stored in model.pkl {list(model.feature_names_in_)} differs from metadata {features}.")
    unknown = [name for name in features if name not in contract.FACTOR_FIELDS]
    if unknown:
        sys.exit(f"The model needs features that the event contract does not define: {unknown}")
    if contract.TARGET_FIELD in features:
        sys.exit("actual_happiness_score must never be a model input feature (target leakage).")
    return model, features


def process_record(connection, model, features, record: KafkaRecord) -> Outcome:
    """Run the whole pipeline for one Kafka message and return what happened. Never raises for bad data."""
    # 1. raw storage first, exactly as received -------------------------------------------------
    payload_text = record.value.decode("utf-8", errors="replace")
    raw_event_id, previous_status = db.insert_raw(connection, record, payload_text)
    if previous_status not in (None, contract.STATUS_RECEIVED):
        # Same topic/partition/offset already finished in a previous run: do not process it twice.
        detail = (f"offset already stored (previous status {previous_status}); if Kafka was reset "
                  "but PostgreSQL kept its data, start clean with: docker compose down -v")
        return Outcome(previous_status, raw_event_id, detail=detail, replayed=True)

    # 2. parse ---------------------------------------------------------------------------------
    try:
        event = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        detail = f"invalid JSON: {exc.msg}"
        db.set_status(connection, raw_event_id, contract.STATUS_INVALID_SCHEMA, detail, None)
        return Outcome(contract.STATUS_INVALID_SCHEMA, raw_event_id, detail=detail)

    event_id = event.get("event_id") if isinstance(event, dict) else None
    event_id = event_id if isinstance(event_id, str) and event_id.strip() else None

    # 3. contract validation -------------------------------------------------------------------
    result = contract.validate_event(event, features)
    if not result.is_valid:
        db.set_status(connection, raw_event_id, result.status, result.detail, event_id)
        return Outcome(result.status, raw_event_id, event_id, result.detail)

    # duplicate business identity (event_id must be unique among accepted events)
    if db.has_valid_event(connection, event_id, raw_event_id):
        detail = f"event_id {event_id} was already processed"
        db.set_status(connection, raw_event_id, contract.STATUS_DUPLICATE_EVENT, detail, event_id)
        return Outcome(contract.STATUS_DUPLICATE_EVENT, raw_event_id, event_id, detail)

    # 4-5. features in training order + prediction ---------------------------------------------
    try:
        row = pd.DataFrame([{name: float(event[name]) for name in features}], columns=features)
        predicted = float(model.predict(row)[0])
        if not math.isfinite(predicted):
            raise ValueError("the model returned a non-finite value")
        actual = float(event[contract.TARGET_FIELD])
        event_time = contract.parse_event_time(event["event_time"])

        # 6. load dimensions + fact + status VALID, atomically -----------------------------------
        db.save_prediction(connection, raw_event_id, event, event_time, predicted, actual)
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        raise                                   # infrastructure problem: handled by the retry loop
    except Exception as exc:                    # a problem with THIS event: record it and move on
        connection.rollback()
        detail = f"{type(exc).__name__}: {exc}"
        db.set_status(connection, raw_event_id, contract.STATUS_PREDICTION_ERROR, detail, event_id)
        return Outcome(contract.STATUS_PREDICTION_ERROR, raw_event_id, event_id, detail)

    return Outcome(contract.STATUS_VALID, raw_event_id, event_id, predicted=predicted, actual=actual)


class RunStats:
    """Counters of one consumer run. Redelivered (already stored) messages are counted apart and never
    enter the running MAE, because they carry no new prediction."""

    def __init__(self) -> None:
        self.statuses: Counter = Counter()
        self.absolute_error_sum = 0.0

    def record(self, outcome: Outcome) -> None:
        if outcome.replayed:
            self.statuses["SKIPPED_REDELIVERED"] += 1
            return
        self.statuses[outcome.status] += 1
        if outcome.status == contract.STATUS_VALID:
            self.absolute_error_sum += abs(outcome.predicted - outcome.actual)

    @property
    def running_mae(self) -> str:
        valid = self.statuses[contract.STATUS_VALID]
        return f"{self.absolute_error_sum / valid:.3f}" if valid else "-"


def describe(outcome: Outcome, record: KafkaRecord) -> str:
    where = f"partition={record.partition} offset={record.offset}"
    if outcome.replayed:
        return f"[SKIPPED_REDELIVERED] raw={outcome.raw_event_id} | {outcome.detail} | {where}"
    label = f"[{outcome.status}]".ljust(19)
    if outcome.status == contract.STATUS_VALID:
        error = outcome.predicted - outcome.actual
        return (f"{label} {outcome.event_id} | key={record.key} | actual={outcome.actual:.3f} "
                f"predicted={outcome.predicted:.3f} error={error:+.3f} | {where}")
    return f"{label} {outcome.event_id or '-'} | {outcome.detail} | {where}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume happiness events and predict in real time.")
    parser.add_argument("--idle-timeout", type=float, default=0,
                        help="stop after this many seconds without messages (default 0 = run until Ctrl+C)")
    args = parser.parse_args()

    model, features = load_model()
    print(f"Model loaded. Features in training order: {features}")

    connection = db.connect()
    db.init_schema(connection)
    ensure_topic(config.KAFKA_BOOTSTRAP_SERVERS, config.TOPIC,
                 config.TOPIC_PARTITIONS, config.TOPIC_REPLICATION_FACTOR)

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,     # the offset is committed manually, after the database work
    })
    consumer.subscribe([config.TOPIC])
    print(f"Consumer group '{config.CONSUMER_GROUP}' waiting for events on '{config.TOPIC}'... (Ctrl+C to stop)\n")

    stats = RunStats()
    last_message_at = time.time()
    exit_code = 0

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                if args.idle_timeout and time.time() - last_message_at > args.idle_timeout:
                    print(f"No messages for {args.idle_timeout:g}s. Stopping.")
                    break
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {message.error()}")
                continue

            last_message_at = time.time()
            timestamp_type, timestamp_ms = message.timestamp()
            record = KafkaRecord(
                topic=message.topic(), partition=message.partition(), offset=message.offset(),
                key=message.key().decode("utf-8", errors="replace") if message.key() else None,
                value=message.value() or b"", timestamp_ms=timestamp_ms if timestamp_type else None,
            )

            outcome = None
            for attempt in range(1, MAX_DB_RETRIES + 1):
                try:
                    outcome = process_record(connection, model, features, record)
                    break
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    print(f"Database problem ({exc.__class__.__name__}), attempt {attempt}/{MAX_DB_RETRIES}. Reconnecting...")
                    try:
                        connection.close()
                    except Exception:
                        pass
                    time.sleep(2 * attempt)
                    connection = db.connect(retries=3)

            if outcome is None:
                # Not committing the offset means this message is delivered again after a restart.
                print("The database stayed unavailable. Stopping WITHOUT committing the offset.")
                exit_code = 1
                break

            consumer.commit(message=message, asynchronous=False)

            stats.record(outcome)
            print(describe(outcome, record) + f" | running MAE={stats.running_mae}")

    except KeyboardInterrupt:
        print("\nConsumer stopped by the user.")
    finally:
        consumer.close()
        try:
            connection.close()
        except Exception:
            pass
        print("\nSummary of this run:", dict(stats.statuses) or "no messages processed")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
