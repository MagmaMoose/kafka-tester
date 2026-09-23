"""Stand-ins for kafka-python's three clients, so the unit tests need no broker."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from kafka_tester import kafka_ops


@dataclass
class FakeRecord:
    partition: int
    offset: int
    timestamp: int
    key: bytes | None = None
    value: bytes | None = None


@dataclass
class FakeMetadata:
    topic: str
    partition: int
    offset: int


@dataclass
class FakeCluster:
    """The state every fake client shares: topics, their records, and what was asked of it."""

    topics: dict[str, dict[int, list[FakeRecord]]] = field(default_factory=dict)
    configs: list[dict] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    admin_error: Exception | None = None
    create_error: Exception | None = None
    # When set, poll() returns nothing, as a consumer that cannot reach the leader would.
    stall: bool = False

    def add_topic(self, name: str, partitions: int = 1) -> None:
        self.topics[name] = {p: [] for p in range(partitions)}

    def add_record(self, topic: str, partition: int, timestamp: int, value: bytes) -> None:
        records = self.topics[topic][partition]
        records.append(FakeRecord(partition, len(records), timestamp, None, value))


class FakeAdmin:
    def __init__(self, cluster: FakeCluster, **config):
        cluster.configs.append({"client": "admin", **config})
        if cluster.admin_error is not None:
            raise cluster.admin_error
        self.cluster = cluster

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def describe_cluster(self):
        return {
            "cluster_id": "fake-cluster",
            "controller_id": 1,
            "brokers": [{"broker_id": 1, "host": "broker-1.internal", "port": 9092, "rack": None}],
        }

    def list_topics(self):
        return list(self.cluster.topics)

    def create_topics(self, new_topics, timeout_ms=None):
        if self.cluster.create_error is not None:
            raise self.cluster.create_error
        for name in new_topics:
            self.cluster.add_topic(name)
            self.cluster.created.append(name)


class FakeFuture:
    def __init__(self, metadata):
        self.metadata = metadata

    def get(self, timeout=None):
        return self.metadata


class FakeProducer:
    def __init__(self, cluster: FakeCluster, **config):
        cluster.configs.append({"client": "producer", **config})
        self.cluster = cluster
        self.closed = False

    def send(self, topic, value):
        records = self.cluster.topics.setdefault(topic, {0: []})[0]
        records.append(FakeRecord(0, len(records), 1_700_000_000_000, None, value))
        return FakeFuture(FakeMetadata(topic, 0, len(records) - 1))

    def close(self, timeout=None):
        self.closed = True


class FakeConsumer:
    def __init__(self, cluster: FakeCluster, **config):
        cluster.configs.append({"client": "consumer", **config})
        self.cluster = cluster
        self.positions = {}
        self.closed_with = None

    def partitions_for_topic(self, topic):
        return set(self.cluster.topics.get(topic, {}))

    def assign(self, partitions):
        self.assigned = list(partitions)

    def _records(self, tp):
        return self.cluster.topics[tp.topic][tp.partition]

    def beginning_offsets(self, partitions, timeout_ms=None):
        return {tp: 0 for tp in partitions}

    def end_offsets(self, partitions, timeout_ms=None):
        return {tp: len(self._records(tp)) for tp in partitions}

    def seek(self, tp, offset):
        self.positions[tp] = offset

    def position(self, tp, timeout_ms=None):
        return self.positions[tp]

    def poll(self, timeout_ms=0):
        if self.cluster.stall:
            return {}
        batches = {}
        for tp in self.assigned:
            batch = self._records(tp)[self.positions[tp] :]
            if batch:
                batches[tp] = batch
                self.positions[tp] += len(batch)
        return batches

    def close(self, autocommit=True, timeout_ms=None):
        self.closed_with = autocommit


@pytest.fixture
def cluster(monkeypatch) -> FakeCluster:
    """A fake cluster, with kafka_ops wired to it instead of kafka-python."""
    state = FakeCluster()
    monkeypatch.setattr(kafka_ops, "KafkaAdminClient", lambda **c: FakeAdmin(state, **c))
    monkeypatch.setattr(kafka_ops, "KafkaProducer", lambda **c: FakeProducer(state, **c))
    monkeypatch.setattr(kafka_ops, "KafkaConsumer", lambda **c: FakeConsumer(state, **c))
    return state
