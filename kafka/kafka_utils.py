"""Small Kafka helper: make sure the topic exists with the required layout before using it."""
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(bootstrap_servers, topic, partitions, replication_factor, timeout_seconds=90):
    """Create `topic` if it does not exist and fail if it exists with a different partition count.

    Why this matters: if a client subscribes to a topic that does not exist, the broker may create
    it automatically with its default of ONE partition, which would silently break the required
    3-partition design. Creating it explicitly first avoids that.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    deadline = time.time() + timeout_seconds
    while True:
        try:
            metadata = admin.list_topics(timeout=5)
            break
        except KafkaException as exc:
            if time.time() > deadline:
                raise SystemExit(
                    f"Kafka is not reachable at {bootstrap_servers} ({exc}). "
                    "Is the container running? Try: docker compose ps"
                ) from exc
            print("Waiting for Kafka to be ready...")
            time.sleep(2)

    existing = metadata.topics.get(topic)
    if existing is not None and existing.error is None:
        found = len(existing.partitions)
        if found != partitions:
            raise SystemExit(
                f"Topic '{topic}' exists with {found} partitions but {partitions} are required. "
                "Reset the environment with: docker compose down -v"
            )
        return

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
    )
    for created_topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{created_topic}' created ({partitions} partitions, replication factor {replication_factor}).")
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
