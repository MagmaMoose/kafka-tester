import json
import logging
import ssl

import pytest
from kafka.errors import (
    KafkaConnectionError,
    KafkaTimeoutError,
    TopicAlreadyExistsError,
    TopicAuthorizationFailedError,
)

from kafka_tester import kafka_ops
from kafka_tester.kafka_ops import Connection, TopicNotFoundError


def test_plaintext_config():
    config = kafka_ops.client_config(Connection("a:9092,b:9092", timeout_ms=5000))
    assert config["bootstrap_servers"] == ["a:9092", "b:9092"]
    assert config["request_timeout_ms"] == 5000
    assert config["bootstrap_timeout_ms"] == 5000
    assert "security_protocol" not in config


def test_tls_verifies_against_the_system_store_by_default():
    config = kafka_ops.client_config(Connection("a:9093", tls=True))
    assert config["security_protocol"] == "SSL"
    assert "ssl_context" not in config
    assert "ssl_cafile" not in config


def test_tls_with_a_private_ca():
    config = kafka_ops.client_config(Connection("a:9093", tls=True, ca_file="/etc/ca.pem"))
    assert config["ssl_cafile"] == "/etc/ca.pem"


def test_tls_without_verification_accepts_any_certificate():
    config = kafka_ops.client_config(
        Connection("a:9093", tls=True, verify=False, ca_file="/etc/ca.pem")
    )
    context = config["ssl_context"]
    assert context.verify_mode == ssl.CERT_NONE
    assert context.check_hostname is False
    assert "ssl_cafile" not in config


def test_check_reports_brokers_as_advertised(cluster):
    cluster.add_topic("orders")
    cluster.add_topic("audit")
    result = kafka_ops.check(Connection("seed:9092"))
    assert result["cluster_id"] == "fake-cluster"
    assert result["brokers"] == [{"id": 1, "host": "broker-1.internal", "port": 9092, "rack": None}]
    assert result["topics"] == ["audit", "orders"]
    assert result["topic_count"] == 2
    assert result["elapsed_ms"] >= 0


def test_ensure_topic_creates_a_missing_topic(cluster):
    assert kafka_ops.ensure_topic(Connection("seed:9092"), "new-topic") is True
    assert cluster.created == ["new-topic"]


def test_ensure_topic_leaves_an_existing_topic(cluster):
    cluster.add_topic("existing")
    assert kafka_ops.ensure_topic(Connection("seed:9092"), "existing") is False
    assert cluster.created == []


def test_ensure_topic_accepts_losing_a_race(cluster):
    cluster.create_error = TopicAlreadyExistsError()
    assert kafka_ops.ensure_topic(Connection("seed:9092"), "raced") is False


def test_produce_creates_the_topic_and_reports_the_offset(cluster):
    result = kafka_ops.produce(Connection("seed:9092"), "fresh", "hello")
    assert result["topic"] == "fresh"
    assert result["offset"] == 0
    assert result["topic_created"] is True
    assert result["warnings"] == []
    assert result["sent"]["message"] == "hello"
    stored = cluster.topics["fresh"][0][0].value
    assert json.loads(stored)["message"] == "hello"
    producer = next(c for c in cluster.configs if c["client"] == "producer")
    assert producer["retries"] == 0
    assert producer["enable_idempotence"] is False


def test_produce_without_creating_the_topic(cluster):
    result = kafka_ops.produce(Connection("seed:9092"), "t", "x", create_topic=False)
    assert result["topic_created"] is False
    assert not any(c["client"] == "admin" for c in cluster.configs)


def test_produce_carries_on_when_it_may_not_create_topics(cluster):
    cluster.create_error = TopicAuthorizationFailedError()
    result = kafka_ops.produce(Connection("seed:9092"), "locked", "x")
    assert result["topic_created"] is False
    assert "TopicAuthorizationFailedError" in result["warnings"][0]


def test_produce_stops_when_the_cluster_does_not_answer(cluster):
    cluster.admin_error = KafkaTimeoutError("Unable to bootstrap from ['seed:9092']")
    with pytest.raises(KafkaTimeoutError):
        kafka_ops.produce(Connection("seed:9092"), "t", "x")
    assert not any(c["client"] == "producer" for c in cluster.configs)


def test_consume_returns_the_newest_records_first(cluster):
    cluster.add_topic("events", partitions=2)
    for i in range(4):
        cluster.add_record("events", 0, 1000 + 2 * i, f'{{"n": {2 * i}}}'.encode())
        cluster.add_record("events", 1, 1001 + 2 * i, f'{{"n": {2 * i + 1}}}'.encode())
    result = kafka_ops.consume(Connection("seed:9092"), "events", limit=3)
    assert [r["value"]["n"] for r in result["records"]] == [7, 6, 5]
    assert result["partitions"] == 2
    assert result["incomplete"] is False
    consumer = next(c for c in cluster.configs if c["client"] == "consumer")
    assert consumer["group_id"] is None
    assert consumer["allow_auto_create_topics"] is False


def test_consume_an_empty_topic(cluster):
    cluster.add_topic("quiet")
    result = kafka_ops.consume(Connection("seed:9092"), "quiet")
    assert result["records"] == []
    assert result["incomplete"] is False


def test_consume_a_missing_topic(cluster):
    with pytest.raises(TopicNotFoundError):
        kafka_ops.consume(Connection("seed:9092"), "nope")


def test_consume_gives_up_when_the_wait_runs_out(cluster):
    cluster.add_topic("slow")
    cluster.add_record("slow", 0, 1000, b"x")
    cluster.stall = True
    result = kafka_ops.consume(Connection("seed:9092", timeout_ms=0), "slow")
    assert result["records"] == []
    assert result["incomplete"] is True


@pytest.mark.parametrize(
    ("raw", "shown"),
    [
        (None, None),
        (b'{"a": 1}', {"a": 1}),
        (b"plain text", "plain text"),
        (b"\xff\xfe", {"base64": "//4=", "bytes": 2}),
    ],
)
def test_decode(raw, shown):
    assert kafka_ops._decode(raw) == shown


def test_decode_cuts_long_values():
    shown = kafka_ops._decode(b"x" * (kafka_ops.MAX_SHOWN_CHARS + 10))
    assert shown.endswith("(10 more characters)")


def test_record_without_a_timestamp():
    record = type(
        "R", (), {"partition": 0, "offset": 3, "timestamp": -1, "key": b"k", "value": b"v"}
    )
    assert kafka_ops._record(record) == {
        "partition": 0,
        "offset": 3,
        "timestamp": None,
        "key": "k",
        "value": "v",
    }


def test_iso():
    assert kafka_ops._iso(1_700_000_000_123) == "2023-11-14T22:13:20.123Z"


@pytest.mark.parametrize(
    ("exc", "text"),
    [
        (KafkaTimeoutError("no answer"), "KafkaTimeoutError: no answer"),
        (KafkaConnectionError(), "KafkaConnectionError"),
        (OSError("refused"), "OSError: refused"),
        (TopicNotFoundError(), "TopicNotFoundError"),
    ],
)
def test_describe_error(exc, text):
    assert kafka_ops.describe_error(exc) == text


def test_failures_carry_what_the_client_logged(monkeypatch):
    def failing_admin(**config):
        logger = logging.getLogger("kafka.net.manager")
        logger.warning("Bootstrap connection to bootstrap-0 failed: refused")
        logger.warning("Bootstrap connection to bootstrap-0 failed: refused")
        logger.info("not a warning, so not kept")
        raise KafkaTimeoutError("Unable to bootstrap")

    monkeypatch.setattr(kafka_ops, "KafkaAdminClient", failing_admin)
    with pytest.raises(KafkaTimeoutError) as caught:
        kafka_ops.check(Connection("seed:9092"))
    assert kafka_ops.client_log(caught.value) == [
        "Bootstrap connection to bootstrap-0 failed: refused"
    ]
    assert not any(isinstance(h, kafka_ops._ClientLog) for h in logging.getLogger("kafka").handlers)


def test_client_log_of_an_exception_that_has_none():
    assert kafka_ops.client_log(ValueError()) == []
