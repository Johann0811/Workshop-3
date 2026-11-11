"""Integration tests of the consumer logic against a real PostgreSQL (skipped if it is not running).

They do not need Kafka: Kafka messages are simulated with KafkaRecord objects. Every test runs in
its own temporary schema, so the real tables are never touched.

Start the database first:   docker compose up -d postgres
Run from the project root:  python -m unittest discover -s tests -v
"""
import json
import unittest
import uuid

import psycopg2

import _setup  # noqa: F401
import config
import consumer
import contract
import db

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
GOOD_EVENT = {
    "event_id": "E001", "country": "Colombia", "year": 2019,
    "gdp": 1.2, "family": 0.8, "health": 0.9, "freedom": 0.6, "generosity": 0.3, "corruption": 0.1,
    "actual_happiness_score": 6.2, "event_time": "2026-10-30T15:30:00",
}


def database_available():
    try:
        psycopg2.connect(connect_timeout=3, **config.DB_PARAMS).close()
        return True
    except psycopg2.OperationalError:
        return False


class SpyModel:
    """Delegates to the real model but remembers what it was asked to predict."""
    def __init__(self, model):
        self.model, self.seen = model, []

    def predict(self, frame):
        self.seen.append(list(frame.columns))
        return self.model.predict(frame)


class BrokenModel:
    def predict(self, frame):
        raise RuntimeError("model exploded")


@unittest.skipUnless(database_available(), "PostgreSQL is not running (docker compose up -d postgres)")
class ConsumerPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model, cls.features = consumer.load_model()

    def setUp(self):
        self.schema = f"test_{uuid.uuid4().hex[:8]}"
        admin = psycopg2.connect(**config.DB_PARAMS)
        admin.autocommit = True
        admin.cursor().execute(f'CREATE SCHEMA "{self.schema}"')
        admin.close()
        self.connection = db.connect(retries=1, schema=self.schema)
        db.init_schema(self.connection)
        self.offset = 0

    def tearDown(self):
        self.connection.close()
        admin = psycopg2.connect(**config.DB_PARAMS)
        admin.autocommit = True
        admin.cursor().execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        admin.close()

    # helpers -------------------------------------------------------------------------------
    def send(self, value, key="Colombia", model=None, offset=None, partition=0):
        if isinstance(value, dict):
            value = json.dumps(value)
        if offset is None:
            offset = self.offset
            self.offset += 1
        record = consumer.KafkaRecord(config.TOPIC, partition, offset, key, value.encode("utf-8"))
        return consumer.process_record(self.connection, model or self.model, self.features, record)

    def query(self, sql, params=()):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    # tests ---------------------------------------------------------------------------------
    def test_valid_event_is_predicted_and_traceable(self):
        outcome = self.send(GOOD_EVENT)
        self.assertEqual(outcome.status, contract.STATUS_VALID)

        raw = self.query("SELECT raw_event_id, processing_status, raw_payload, event_id FROM raw_happiness_events")
        self.assertEqual(len(raw), 1)
        self.assertEqual(raw[0][1:], ("VALID", json.dumps(GOOD_EVENT), "E001"))

        fact = self.query("""SELECT f.raw_event_id, c.country_name, f.observation_year, f.actual_score,
                                    f.predicted_score, f.prediction_error, d.date_id
                               FROM fact_predictions f
                               JOIN dim_country c USING (country_id) JOIN dim_date d USING (date_id)""")
        self.assertEqual(len(fact), 1)
        raw_event_id, country, year, actual, predicted, error, date_id = fact[0]
        self.assertEqual((raw_event_id, country, year, actual, date_id), (raw[0][0], "Colombia", 2019, 6.2, 20261030))
        self.assertAlmostEqual(error, predicted - actual)
        self.assertAlmostEqual(predicted, outcome.predicted)

    def test_actual_score_is_never_a_model_input_and_order_is_the_training_order(self):
        spy = SpyModel(self.model)
        self.send(GOOD_EVENT, model=spy)
        self.assertEqual(spy.seen, [FEATURES])
        self.assertNotIn("actual_happiness_score", spy.seen[0])

    def test_invalid_events_are_stored_skipped_and_do_not_stop_the_stream(self):
        cases = {
            "missing": ({k: v for k, v in GOOD_EVENT.items() if k != "gdp"}, "INVALID_SCHEMA"),
            "text": ({**GOOD_EVENT, "gdp": "abc"}, "INVALID_SCHEMA"),
            "negative": ({**GOOD_EVENT, "gdp": -1}, "INVALID_VALUES"),
            "bad time": ({**GOOD_EVENT, "event_time": "yesterday"}, "INVALID_SCHEMA"),
            "bad json": ('{"event_id": "BAD10", "country": ', "INVALID_SCHEMA"),
            "not an object": ("[1, 2, 3]", "INVALID_SCHEMA"),
            "nan": ('{"event_id": "N1", "country": "X", "year": 2019, "gdp": NaN, "family": 1, "health": 1, '
                    '"freedom": 1, "generosity": 1, "corruption": 1, "actual_happiness_score": 5, '
                    '"event_time": "2026-10-30T15:30:00"}', "INVALID_VALUES"),
        }
        for name, (payload, expected) in cases.items():
            self.assertEqual(self.send(payload).status, expected, name)

        # the stream keeps working after all those failures
        self.assertEqual(self.send(GOOD_EVENT).status, contract.STATUS_VALID)

        self.assertEqual(self.query("SELECT COUNT(*) FROM fact_predictions")[0][0], 1)
        stored = self.query("SELECT processing_status, status_detail FROM raw_happiness_events ORDER BY raw_event_id")
        self.assertEqual(len(stored), len(cases) + 1)                      # nothing was lost
        self.assertTrue(all(detail for status, detail in stored if status != "VALID"))

    def test_raw_payload_is_preserved_exactly_even_when_it_is_garbage(self):
        garbage = '{"weird":   "spacing" ,\n "n": 1e3 '
        self.send(garbage)
        self.assertEqual(self.query("SELECT raw_payload FROM raw_happiness_events")[0][0], garbage)

    def test_duplicate_event_id_is_detected_and_not_predicted_twice(self):
        self.assertEqual(self.send(GOOD_EVENT).status, contract.STATUS_VALID)
        self.assertEqual(self.send(GOOD_EVENT).status, contract.STATUS_DUPLICATE_EVENT)
        self.assertEqual(self.query("SELECT COUNT(*) FROM fact_predictions")[0][0], 1)
        self.assertEqual(self.query("SELECT COUNT(*) FROM raw_happiness_events")[0][0], 2)

    def test_redelivered_offset_is_ignored(self):
        self.assertEqual(self.send(GOOD_EVENT, offset=7).status, contract.STATUS_VALID)
        again = self.send(GOOD_EVENT, offset=7)
        self.assertTrue(again.replayed)
        self.assertEqual(self.query("SELECT COUNT(*) FROM raw_happiness_events")[0][0], 1)
        self.assertEqual(self.query("SELECT COUNT(*) FROM fact_predictions")[0][0], 1)

    def test_prediction_failure_is_recorded_and_does_not_crash(self):
        outcome = self.send(GOOD_EVENT, model=BrokenModel())
        self.assertEqual(outcome.status, contract.STATUS_PREDICTION_ERROR)
        row = self.query("SELECT processing_status, status_detail FROM raw_happiness_events")[0]
        self.assertEqual(row[0], "PREDICTION_ERROR")
        self.assertIn("model exploded", row[1])
        self.assertEqual(self.query("SELECT COUNT(*) FROM fact_predictions")[0][0], 0)
        self.assertEqual(self.send({**GOOD_EVENT, "event_id": "E002"}).status, contract.STATUS_VALID)

    def test_kpi_views_answer_the_four_required_kpis(self):
        for number, country in enumerate(["Colombia", "Colombia", "Chile"], start=1):
            self.send({**GOOD_EVENT, "event_id": f"E{number:03d}", "country": country})
        self.send("not json")

        self.assertEqual(self.query("SELECT predictions FROM vw_kpi_average_error")[0][0], 3)
        by_country = dict(self.query("SELECT country_name, predictions FROM vw_kpi_predictions_by_country"))
        self.assertEqual(by_country, {"Colombia": 2, "Chile": 1})
        self.assertEqual(len(self.query("SELECT * FROM vw_kpi_predicted_vs_actual")), 3)
        self.assertEqual(self.query("SELECT observation_year, predictions FROM vw_kpi_trend_by_year"), [(2019, 3)])
        self.assertEqual(len(self.query("SELECT * FROM vw_kpi_trend_by_time")), 1)
        quality = dict(self.query("SELECT processing_status, events FROM vw_event_quality"))
        self.assertEqual(quality, {"VALID": 3, "INVALID_SCHEMA": 1})


class RunStatsTests(unittest.TestCase):
    """The console bookkeeping of a run (no database needed)."""

    def test_redelivered_messages_are_counted_apart_and_never_break_the_running_mae(self):
        stats = consumer.RunStats()
        stats.record(consumer.Outcome("VALID", 1, "E001", predicted=6.0, actual=6.5))
        # a redelivered row that was VALID in a previous run has no prediction: this used to crash the consumer
        stats.record(consumer.Outcome("VALID", 2, detail="already stored", replayed=True))
        stats.record(consumer.Outcome("INVALID_SCHEMA", 3, detail="missing field: gdp"))
        self.assertEqual(dict(stats.statuses), {"VALID": 1, "SKIPPED_REDELIVERED": 1, "INVALID_SCHEMA": 1})
        self.assertEqual(stats.running_mae, "0.500")

    def test_running_mae_without_valid_events(self):
        self.assertEqual(consumer.RunStats().running_mae, "-")


if __name__ == "__main__":
    unittest.main()
