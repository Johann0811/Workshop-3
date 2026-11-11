"""Central configuration shared by the producer, the consumer and the database layer.

Everything has a default that matches docker-compose.yml, so the scripts run without any setup.
Each value can be overridden with an environment variable.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# --- Kafka -------------------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "happiness-events"            # required topic name
TOPIC_PARTITIONS = 3                  # required: 3 partitions
TOPIC_REPLICATION_FACTOR = 1          # required: replication factor 1 (single local broker)
CONSUMER_GROUP = "happiness-ml-team"  # required consumer group

# --- PostgreSQL --------------------------------------------------------------------------------
DB_PARAMS = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "happiness_db"),
    "user": os.getenv("POSTGRES_USER", "happiness"),
    "password": os.getenv("POSTGRES_PASSWORD", "happiness"),
}

# --- Files -------------------------------------------------------------------------------------
MODEL_PATH = ROOT / "models" / "model.pkl"
METADATA_PATH = ROOT / "models" / "model_metadata.json"
SQL_DIR = ROOT / "sql"
STREAM_SOURCES = {
    "test": ROOT / "data" / "streaming" / "stream_events.csv",
    "full": ROOT / "data" / "streaming" / "stream_events_full.csv",
}
