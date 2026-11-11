"""Tests of the producer building blocks (no Kafka broker required)."""
import json
import unittest
from datetime import datetime

import _setup  # noqa: F401
import config
import contract
import producer

EXPECTED_STATUS_OF_BAD_MESSAGES = {
    "missing field (gdp)": contract.STATUS_INVALID_SCHEMA,
    "text instead of a number (gdp)": contract.STATUS_INVALID_SCHEMA,
    "negative factor (gdp)": contract.STATUS_INVALID_VALUES,
    "empty country": contract.STATUS_INVALID_SCHEMA,
    "bad timestamp": contract.STATUS_INVALID_SCHEMA,
    "year as text": contract.STATUS_INVALID_SCHEMA,
    "null feature (corruption)": contract.STATUS_INVALID_SCHEMA,
    "score outside the 0-10 scale": contract.STATUS_INVALID_VALUES,
    "NaN feature (generosity)": contract.STATUS_INVALID_VALUES,
}
FEATURES = list(contract.FACTOR_FIELDS)


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.records = producer.load_events(config.STREAM_SOURCES["test"])

    def test_every_streamed_event_satisfies_the_contract(self):
        for record in self.records:
            event = producer.build_event(record)
            self.assertEqual(set(event), set(contract.REQUIRED_FIELDS))
            result = contract.validate_event(json.loads(json.dumps(event)), FEATURES)
            self.assertTrue(result.is_valid, (event, result.errors))

    def test_event_ids_are_unique_and_event_time_is_the_emission_time(self):
        self.assertEqual(len({record["event_id"] for record in self.records}), len(self.records))
        before = datetime.now().replace(microsecond=0)
        event = producer.build_event(self.records[0])
        self.assertGreaterEqual(datetime.fromisoformat(event["event_time"]), before)

    def test_injected_invalid_messages_are_classified_as_designed(self):
        for description, key, make_value in producer.invalid_messages(self.records[0]):
            text = make_value()
            if "malformed JSON" in description:
                with self.assertRaises(json.JSONDecodeError):
                    json.loads(text)
            elif description.startswith("DUPLICATE"):
                self.assertEqual(json.loads(text)["event_id"], self.records[0]["event_id"])
            else:
                label = description.split(": ", 1)[1]
                status = contract.validate_event(json.loads(text), FEATURES).status
                self.assertEqual(status, EXPECTED_STATUS_OF_BAD_MESSAGES[label], description)

    def test_streaming_files_exist_and_target_is_never_a_feature(self):
        self.assertGreater(len(self.records), 0)
        self.assertNotIn(contract.TARGET_FIELD, FEATURES)


if __name__ == "__main__":
    unittest.main()
