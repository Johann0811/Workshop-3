"""Unit tests of the event contract (no Kafka and no database required).

Run from the project root:   python -m unittest discover -s tests -v
"""
import unittest

import _setup  # noqa: F401
import contract

FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]

GOOD_EVENT = {
    "event_id": "E001", "country": "Colombia", "year": 2019,
    "gdp": 1.2, "family": 0.8, "health": 0.9, "freedom": 0.6, "generosity": 0.3, "corruption": 0.1,
    "actual_happiness_score": 6.2, "event_time": "2026-10-30T15:30:00",
}


def event_with(**changes):
    event = dict(GOOD_EVENT)
    event.update(changes)
    return event


class ContractTests(unittest.TestCase):
    def check(self, event, expected_status):
        result = contract.validate_event(event, FEATURES)
        self.assertEqual(result.status, expected_status, result.errors)
        return result

    def test_workshop_example_is_valid(self):
        self.check(GOOD_EVENT, contract.STATUS_VALID)

    def test_event_is_not_a_json_object(self):
        for value in (None, [], "text", 42):
            self.check(value, contract.STATUS_INVALID_SCHEMA)

    def test_every_required_field_is_enforced(self):
        for name in contract.REQUIRED_FIELDS:
            event = dict(GOOD_EVENT)
            del event[name]
            result = self.check(event, contract.STATUS_INVALID_SCHEMA)
            self.assertIn(f"missing field: {name}", result.errors)

    def test_null_fields_are_schema_errors(self):
        for name in contract.REQUIRED_FIELDS:
            self.check(event_with(**{name: None}), contract.STATUS_INVALID_SCHEMA)

    def test_empty_strings_are_rejected(self):
        self.check(event_with(event_id=""), contract.STATUS_INVALID_SCHEMA)
        self.check(event_with(country="   "), contract.STATUS_INVALID_SCHEMA)

    def test_wrong_types_are_schema_errors(self):
        self.check(event_with(gdp="abc"), contract.STATUS_INVALID_SCHEMA)
        self.check(event_with(gdp=True), contract.STATUS_INVALID_SCHEMA)      # bool is not a number here
        self.check(event_with(year="2019"), contract.STATUS_INVALID_SCHEMA)
        self.check(event_with(year=2019.0), contract.STATUS_INVALID_SCHEMA)
        self.check(event_with(event_id=123), contract.STATUS_INVALID_SCHEMA)

    def test_timestamp_must_be_iso_8601(self):
        for bad in ("yesterday", "2026-13-45T10:00:00", "2026-10-30", "", 20261030):
            self.check(event_with(event_time=bad), contract.STATUS_INVALID_SCHEMA)
        for good in ("2026-10-30T15:30:00", "2026-10-30T15:30:00.250", "2026-10-30T15:30:00+00:00"):
            self.check(event_with(event_time=good), contract.STATUS_VALID)

    def test_invalid_numeric_values(self):
        self.check(event_with(gdp=-0.1), contract.STATUS_INVALID_VALUES)
        self.check(event_with(gdp=float("nan")), contract.STATUS_INVALID_VALUES)
        self.check(event_with(family=float("inf")), contract.STATUS_INVALID_VALUES)
        self.check(event_with(corruption=3.5), contract.STATUS_INVALID_VALUES)
        self.check(event_with(actual_happiness_score=99), contract.STATUS_INVALID_VALUES)
        self.check(event_with(actual_happiness_score=-1), contract.STATUS_INVALID_VALUES)
        self.check(event_with(year=1999), contract.STATUS_INVALID_VALUES)

    def test_boundaries_are_valid(self):
        self.check(event_with(gdp=0, family=0.0, corruption=3.0, actual_happiness_score=10), contract.STATUS_VALID)

    def test_schema_errors_take_precedence_over_value_errors(self):
        result = self.check(event_with(gdp=-5, health="x"), contract.STATUS_INVALID_SCHEMA)
        self.assertEqual(result.errors, ["health must be numeric"])

    def test_feature_required_by_the_model_but_absent_from_the_event(self):
        result = contract.validate_event(GOOD_EVENT, FEATURES + ["region"])
        self.assertEqual(result.status, contract.STATUS_INVALID_SCHEMA)
        self.assertIn("missing feature required by the model: region", result.errors)

    def test_extra_fields_are_tolerated(self):
        self.check(event_with(note="extra"), contract.STATUS_VALID)


if __name__ == "__main__":
    unittest.main()
