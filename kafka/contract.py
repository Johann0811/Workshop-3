"""Event contract of the topic `happiness-events` (version 1).

The contract is the agreement between the producer and every downstream consumer. Kafka only
transports bytes; this module is where the application decides whether a business event can be
trusted. It is defined independently of the consumer code so that it can be read, tested and
documented on its own.

Field        Type              Rule
-----------  ----------------  ----------------------------------------------------------------
event_id     string            required, non-empty, unique among accepted events (traceability)
country      string            required, non-empty (also the Kafka event key)
year         integer           required, historical observation year (2015-2019)
gdp          number            required, finite, 0 <= value <= 3
family       number            required, finite, 0 <= value <= 3
health       number            required, finite, 0 <= value <= 3
freedom      number            required, finite, 0 <= value <= 3
generosity   number            required, finite, 0 <= value <= 3
corruption   number            required, finite, 0 <= value <= 3
actual_happiness_score  number required, finite, 0 <= value <= 10; used ONLY for evaluation,
                               never as a model input
event_time   string            required, ISO-8601 timestamp (e.g. 2026-10-30T15:30:00): the
                               moment the producer emitted the simulated event

Validation outcome -> processing status stored in raw_happiness_events
  INVALID_SCHEMA    structure problems: not JSON, not an object, missing/null field, wrong type,
                    empty string, unparseable timestamp, feature required by the model missing
  INVALID_VALUES    right structure but impossible values: NaN/infinity, negative or out-of-range
  VALID             passed the contract (and was predicted successfully)
  DUPLICATE_EVENT   a VALID event with the same event_id was already processed
  PREDICTION_ERROR  contract passed but the model/database step failed for this event
  RECEIVED          initial state, set when the raw message is stored (before any validation)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Sequence

# --- processing statuses -----------------------------------------------------------------------
STATUS_RECEIVED = "RECEIVED"
STATUS_VALID = "VALID"
STATUS_INVALID_SCHEMA = "INVALID_SCHEMA"
STATUS_INVALID_VALUES = "INVALID_VALUES"
STATUS_PREDICTION_ERROR = "PREDICTION_ERROR"
STATUS_DUPLICATE_EVENT = "DUPLICATE_EVENT"
ALL_STATUSES = (
    STATUS_RECEIVED, STATUS_VALID, STATUS_INVALID_SCHEMA,
    STATUS_INVALID_VALUES, STATUS_PREDICTION_ERROR, STATUS_DUPLICATE_EVENT,
)

# --- field definitions -------------------------------------------------------------------------
STRING_FIELDS = ("event_id", "country")
INTEGER_FIELDS = ("year",)
FACTOR_FIELDS = ("gdp", "family", "health", "freedom", "generosity", "corruption")
TARGET_FIELD = "actual_happiness_score"
NUMERIC_FIELDS = FACTOR_FIELDS + (TARGET_FIELD,)
TIMESTAMP_FIELDS = ("event_time",)

# Same order as the JSON example of the workshop.
REQUIRED_FIELDS = (
    "event_id", "country", "year",
    "gdp", "family", "health", "freedom", "generosity", "corruption",
    "actual_happiness_score", "event_time",
)

# Plausible ranges. The six factors are contributions to a 0-10 score and never exceeded 2.1 in
# 2015-2019, so 3.0 leaves head-room while still rejecting absurd values.
FACTOR_RANGE = (0.0, 3.0)
SCORE_RANGE = (0.0, 10.0)
YEAR_RANGE = (2015, 2019)


@dataclass
class ValidationResult:
    status: str
    errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return self.status == STATUS_VALID

    @property
    def detail(self) -> str:
        return "; ".join(self.errors)


def parse_event_time(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and return a naive local datetime.

    A date without a time component is rejected: the contract asks for a timestamp.
    """
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("expected an ISO-8601 timestamp such as 2026-10-30T15:30:00")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(event: Any, model_features: Sequence[str]) -> ValidationResult:
    """Validate one decoded event against the contract and the model's input requirements."""
    if not isinstance(event, dict):
        return ValidationResult(
            STATUS_INVALID_SCHEMA, [f"event must be a JSON object, got {type(event).__name__}"]
        )

    # ---- 1. structure: required fields, null values, types -------------------------------------
    schema_errors: list[str] = []

    for name in REQUIRED_FIELDS:
        if name not in event:
            schema_errors.append(f"missing field: {name}")
        elif event[name] is None:
            schema_errors.append(f"null field: {name}")

    def present(name: str) -> bool:
        return name in event and event[name] is not None

    for name in STRING_FIELDS:
        if present(name):
            if not isinstance(event[name], str):
                schema_errors.append(f"{name} must be a string")
            elif not event[name].strip():
                schema_errors.append(f"{name} must be a non-empty string")

    for name in INTEGER_FIELDS:
        if present(name) and (not isinstance(event[name], int) or isinstance(event[name], bool)):
            schema_errors.append(f"{name} must be an integer")

    for name in NUMERIC_FIELDS:
        if present(name) and not _is_number(event[name]):
            schema_errors.append(f"{name} must be numeric")

    for name in TIMESTAMP_FIELDS:
        if present(name):
            try:
                parse_event_time(event[name])
            except (TypeError, ValueError):
                schema_errors.append(f"{name} must be a valid ISO-8601 timestamp")

    # Feature required by the model but not usable in the event (explicit check, see workshop).
    for name in model_features:
        if not present(name) and f"missing field: {name}" not in schema_errors \
                and f"null field: {name}" not in schema_errors:
            schema_errors.append(f"missing feature required by the model: {name}")

    if schema_errors:
        return ValidationResult(STATUS_INVALID_SCHEMA, schema_errors)

    # ---- 2. values: finite numbers inside plausible ranges -------------------------------------
    value_errors: list[str] = []

    for name in NUMERIC_FIELDS:
        value = event[name]
        if not math.isfinite(value):
            value_errors.append(f"{name} must be a finite number, got {value}")
            continue
        low, high = SCORE_RANGE if name == TARGET_FIELD else FACTOR_RANGE
        if not low <= value <= high:
            value_errors.append(f"{name} must be between {low} and {high}, got {value}")

    low, high = YEAR_RANGE
    if not low <= event["year"] <= high:
        value_errors.append(f"year must be between {low} and {high}, got {event['year']}")

    if value_errors:
        return ValidationResult(STATUS_INVALID_VALUES, value_errors)

    return ValidationResult(STATUS_VALID)
