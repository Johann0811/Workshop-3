-- =====================================================================================
-- Workshop 3 - Streaming ETL with Apache Kafka and Machine Learning
-- Database: PostgreSQL
-- This script is idempotent: it can be executed many times without errors or data loss.
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- RAW / STAGING TABLE (traceability, auditing, debugging, future reprocessing)
-- Stores every Kafka message exactly as it arrived, valid or not. It is NOT a dimension.
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id      BIGSERIAL PRIMARY KEY,
    kafka_topic       TEXT        NOT NULL,
    kafka_partition   INTEGER     NOT NULL,
    kafka_offset      BIGINT      NOT NULL,
    kafka_key         TEXT,
    kafka_timestamp   TIMESTAMP,
    raw_payload       TEXT        NOT NULL,            -- original message value, untouched text
    event_id          TEXT,                            -- copied from the payload when it is readable
    processing_status TEXT        NOT NULL DEFAULT 'RECEIVED',
    status_detail     TEXT,                            -- why an event was rejected / failed
    received_at       TIMESTAMP   NOT NULL,
    processed_at      TIMESTAMP,
    CONSTRAINT uq_raw_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset),
    CONSTRAINT ck_raw_status CHECK (processing_status IN (
        'RECEIVED', 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES',
        'PREDICTION_ERROR', 'DUPLICATE_EVENT'))
);

CREATE INDEX IF NOT EXISTS idx_raw_event_id ON raw_happiness_events (event_id);
CREATE INDEX IF NOT EXISTS idx_raw_status   ON raw_happiness_events (processing_status);

-- -------------------------------------------------------------------------------------
-- DIMENSIONS
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE
);

-- Calendar day of the event_time of the streamed event. date_id uses the YYYYMMDD convention.
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER  PRIMARY KEY,
    full_date   DATE     NOT NULL UNIQUE,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  TEXT     NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,                     -- ISO: 1 = Monday ... 7 = Sunday
    day_name    TEXT     NOT NULL,
    is_weekend  BOOLEAN  NOT NULL
);

-- -------------------------------------------------------------------------------------
-- FACT TABLE: one row per event that was validated and predicted successfully
-- -------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id        BIGSERIAL PRIMARY KEY,
    raw_event_id         BIGINT           NOT NULL UNIQUE
                         REFERENCES raw_happiness_events (raw_event_id),   -- trace back to the Kafka message
    country_id           INTEGER          NOT NULL REFERENCES dim_country (country_id),
    date_id              INTEGER          NOT NULL REFERENCES dim_date (date_id),
    observation_year     SMALLINT         NOT NULL,    -- historical year of the observation (2015-2019)
    actual_score         DOUBLE PRECISION NOT NULL,
    predicted_score      DOUBLE PRECISION NOT NULL,
    prediction_error     DOUBLE PRECISION NOT NULL,    -- predicted_score - actual_score
    event_time           TIMESTAMP        NOT NULL,    -- when the producer emitted the event
    prediction_timestamp TIMESTAMP        NOT NULL     -- when the consumer produced the prediction
);

CREATE INDEX IF NOT EXISTS idx_fact_country ON fact_predictions (country_id);
CREATE INDEX IF NOT EXISTS idx_fact_date    ON fact_predictions (date_id);
CREATE INDEX IF NOT EXISTS idx_fact_year    ON fact_predictions (observation_year);
