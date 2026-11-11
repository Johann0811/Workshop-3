"""PostgreSQL access layer used by the consumer.

Design rules
- Every function receives an open connection and commits its own unit of work.
- The raw message is committed BEFORE validation or prediction, so it survives any later failure.
- All timestamps are naive local datetimes created by Python (`datetime.now()`), the same convention
  used by the event_time of the producer, so they can be compared with each other.
"""
from __future__ import annotations

import time
from datetime import datetime

import psycopg2

import config

MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July",
               "August", "September", "October", "November", "December"]
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def connect(retries: int = 30, delay_seconds: float = 2.0, schema: str | None = None):
    """Open a connection, waiting for PostgreSQL to accept connections (it starts slower than Python)."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            connection = psycopg2.connect(**config.DB_PARAMS)
            if schema:
                with connection.cursor() as cursor:
                    cursor.execute(f'SET search_path TO "{schema}"')
                connection.commit()
            return connection
        except psycopg2.OperationalError as exc:
            last_error = exc
            print(f"Waiting for PostgreSQL ({attempt}/{retries})...")
            time.sleep(delay_seconds)
    raise SystemExit(f"PostgreSQL is not reachable with {config.DB_PARAMS['host']}:{config.DB_PARAMS['port']}: {last_error}")


def init_schema(connection) -> None:
    """Create tables and KPI views if they do not exist (both scripts are idempotent)."""
    with connection.cursor() as cursor:
        for script in ("create_tables.sql", "kpis.sql"):
            cursor.execute((config.SQL_DIR / script).read_text(encoding="utf-8"))
    connection.commit()


# --------------------------------------------------------------------------------------------
# raw table
# --------------------------------------------------------------------------------------------
def insert_raw(connection, record, payload_text: str):
    """Store the Kafka message exactly as received.

    Returns (raw_event_id, previous_status). `previous_status` is None for a new message; when the
    same topic/partition/offset was already stored (the consumer restarted before committing its
    Kafka offset) it is the status that row already has, which makes redelivery harmless.
    """
    kafka_timestamp = None
    if record.timestamp_ms:
        kafka_timestamp = datetime.fromtimestamp(record.timestamp_ms / 1000)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_happiness_events
                (kafka_topic, kafka_partition, kafka_offset, kafka_key, kafka_timestamp,
                 raw_payload, processing_status, received_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'RECEIVED', %s)
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
            RETURNING raw_event_id
            """,
            (record.topic, record.partition, record.offset, record.key, kafka_timestamp,
             payload_text, datetime.now()),
        )
        row = cursor.fetchone()
        if row is not None:
            connection.commit()
            return row[0], None

        cursor.execute(
            """
            SELECT raw_event_id, processing_status FROM raw_happiness_events
            WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s
            """,
            (record.topic, record.partition, record.offset),
        )
        raw_event_id, status = cursor.fetchone()
    connection.commit()
    return raw_event_id, status


def set_status(connection, raw_event_id: int, status: str, detail: str | None, event_id: str | None) -> None:
    """Finish the processing of a raw row with a final status (and the reason when it is not VALID)."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE raw_happiness_events
               SET processing_status = %s, status_detail = %s, event_id = %s, processed_at = %s
             WHERE raw_event_id = %s
            """,
            (status, detail, event_id, datetime.now(), raw_event_id),
        )
    connection.commit()


def has_valid_event(connection, event_id: str, current_raw_event_id: int) -> bool:
    """True if an event with this event_id was already processed successfully (duplicate signal)."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1 FROM raw_happiness_events
             WHERE event_id = %s AND processing_status = 'VALID' AND raw_event_id <> %s
             LIMIT 1
            """,
            (event_id, current_raw_event_id),
        )
        return cursor.fetchone() is not None


# --------------------------------------------------------------------------------------------
# dimensions + fact
# --------------------------------------------------------------------------------------------
def _upsert_country(cursor, country_name: str) -> int:
    cursor.execute(
        """
        INSERT INTO dim_country (country_name) VALUES (%s)
        ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name
        RETURNING country_id
        """,
        (country_name,),
    )
    return cursor.fetchone()[0]


def _upsert_date(cursor, moment: datetime) -> int:
    day = moment.date()
    date_id = day.year * 10000 + day.month * 100 + day.day
    iso_weekday = day.isoweekday()
    cursor.execute(
        """
        INSERT INTO dim_date (date_id, full_date, year, quarter, month, month_name,
                              day, day_of_week, day_name, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
        """,
        (date_id, day, day.year, (day.month - 1) // 3 + 1, day.month, MONTH_NAMES[day.month - 1],
         day.day, iso_weekday, DAY_NAMES[iso_weekday - 1], iso_weekday >= 6),
    )
    return date_id


def save_prediction(connection, raw_event_id: int, event: dict, event_time: datetime,
                    predicted_score: float, actual_score: float) -> None:
    """Load dimensions + fact row and mark the raw row as VALID in ONE transaction.

    Either everything is stored or nothing is: a prediction can never exist without its raw event
    being marked VALID, and a VALID raw event always has its prediction.
    """
    prediction_timestamp = datetime.now()
    try:
        with connection.cursor() as cursor:
            country_id = _upsert_country(cursor, event["country"].strip())
            date_id = _upsert_date(cursor, event_time)
            cursor.execute(
                """
                INSERT INTO fact_predictions
                    (raw_event_id, country_id, date_id, observation_year, actual_score,
                     predicted_score, prediction_error, event_time, prediction_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (raw_event_id) DO NOTHING
                """,
                (raw_event_id, country_id, date_id, event["year"], actual_score,
                 predicted_score, predicted_score - actual_score, event_time, prediction_timestamp),
            )
            cursor.execute(
                """
                UPDATE raw_happiness_events
                   SET processing_status = 'VALID', status_detail = NULL,
                       event_id = %s, processed_at = %s
                 WHERE raw_event_id = %s
                """,
                (event["event_id"], prediction_timestamp, raw_event_id),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
