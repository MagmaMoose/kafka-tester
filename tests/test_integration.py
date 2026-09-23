"""Against real kafka-python clients.

The unreachable-endpoint test runs everywhere: it needs no broker, and it is the only
test that hands client_config to the real clients, which reject any key they do not
know. The rest need a broker, and run when KAFKA_TESTER_IT_BOOTSTRAP names one (CI
starts one).
"""

import os
import time
import uuid

import pytest
from kafka.errors import KafkaError

from kafka_tester import kafka_ops
from kafka_tester.app import create_app
from kafka_tester.config import Settings, Target
from kafka_tester.kafka_ops import Connection, TopicNotFoundError

BOOTSTRAP = os.environ.get("KAFKA_TESTER_IT_BOOTSTRAP", "")
needs_broker = pytest.mark.skipif(
    not BOOTSTRAP, reason="set KAFKA_TESTER_IT_BOOTSTRAP=host:port to run against a broker"
)


@pytest.fixture
def topic():
    return f"kafka-tester-it-{uuid.uuid4().hex[:8]}"


@pytest.mark.parametrize("action", ["check", "produce", "consume"])
def test_an_unreachable_endpoint_fails_within_the_timeout(action):
    conn = Connection("127.0.0.1:1", timeout_ms=1000)
    run = {
        "check": lambda: kafka_ops.check(conn),
        "produce": lambda: kafka_ops.produce(conn, "t", "x"),
        "consume": lambda: kafka_ops.consume(conn, "t"),
    }[action]
    started = time.monotonic()
    with pytest.raises(KafkaError) as caught:
        run()
    assert time.monotonic() - started < 5
    assert kafka_ops.client_log(caught.value)


@needs_broker
def test_check():
    result = kafka_ops.check(Connection(BOOTSTRAP))
    assert result["brokers"]


@needs_broker
def test_produce_then_consume(topic):
    conn = Connection(BOOTSTRAP)
    first = kafka_ops.produce(conn, topic, "one")
    second = kafka_ops.produce(conn, topic, "two")
    assert first["topic_created"] is True
    assert second["topic_created"] is False
    records = kafka_ops.consume(conn, topic)["records"]
    assert [r["value"]["message"] for r in records] == ["two", "one"]


@needs_broker
def test_consume_a_missing_topic_creates_nothing(topic):
    conn = Connection(BOOTSTRAP)
    with pytest.raises(TopicNotFoundError):
        kafka_ops.consume(conn, topic)
    assert topic not in kafka_ops.check(conn)["topics"]


@needs_broker
def test_the_web_api_end_to_end(topic):
    web = create_app(Settings(targets=(Target("it", BOOTSTRAP),))).test_client()
    produced = web.post(
        "/test", json={"target": "it", "action": "produce", "topic": topic, "message": "via web"}
    )
    assert produced.get_json()["success"] is True
    consumed = web.post("/test", json={"target": "it", "action": "consume", "topic": topic})
    assert consumed.get_json()["records"][0]["value"]["message"] == "via web"
