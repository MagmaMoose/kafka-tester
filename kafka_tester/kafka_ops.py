"""What the tester does to a Kafka cluster: describe it, write to it, read from it.

Each call builds its own client and closes it before returning. A tester that held
a connection open would go on reporting a cluster healthy on the strength of a
connection made before it broke.
"""

from __future__ import annotations

import base64
import functools
import json
import logging
import ssl
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError, KafkaTimeoutError, TopicAlreadyExistsError

CLIENT_ID = "kafka-tester"
# Longest key or value echoed back per record. Anything longer is cut, not dropped.
MAX_SHOWN_CHARS = 2000
# How many of kafka-python's own log lines travel with a failure.
CLIENT_LOG_LINES = 5


@dataclass(frozen=True)
class Connection:
    bootstrap: str
    tls: bool = False
    verify: bool = True
    ca_file: str | None = None
    timeout_ms: int = 10_000


class TopicNotFoundError(LookupError):
    """The topic does not exist, or this client is not allowed to see it."""


def describe_error(exc: BaseException) -> str:
    # KafkaError already leads with its class name; OSError and ssl errors do not.
    if isinstance(exc, KafkaError):
        return str(exc)
    detail = str(exc)
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__


def client_log(exc: BaseException) -> list[str]:
    """What kafka-python logged while the operation that raised ``exc`` ran."""
    return list(getattr(exc, "client_log", []))


class _ClientLog(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        line = record.getMessage()
        if line in self.lines:
            self.lines.remove(line)
        self.lines.append(line)


def _with_client_log(operation):
    """Attach kafka-python's warnings to any exception ``operation`` raises.

    kafka-python raises the same "Unable to bootstrap" for a refused connection, a
    name that does not resolve and a failed TLS handshake, and only logs which it
    was. That line is the diagnosis, so it travels with the error. The handler sits
    on a shared logger: two tests running at once can see each other's lines.
    """

    @functools.wraps(operation)
    def run(*args, **kwargs):
        handler = _ClientLog()
        logger = logging.getLogger("kafka")
        logger.addHandler(handler)
        try:
            return operation(*args, **kwargs)
        except Exception as exc:
            exc.client_log = handler.lines[-CLIENT_LOG_LINES:]
            raise
        finally:
            logger.removeHandler(handler)

    return run


def _unverified_tls_context() -> ssl.SSLContext:
    """TLS that accepts any certificate.

    Only used when the operator turns verification off, so a test can tell "the
    broker is down" apart from "the broker's certificate is not one we trust".
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def client_config(conn: Connection) -> dict:
    """Settings shared by the admin client, the producer and the consumer."""
    config = {
        "bootstrap_servers": conn.bootstrap.split(","),
        "client_id": CLIENT_ID,
        "request_timeout_ms": conn.timeout_ms,
        "bootstrap_timeout_ms": conn.timeout_ms,
        # kafka-python backs off for up to 30s between reconnects by default, which
        # turns one unreachable broker into a request that outlives the browser's wait.
        "reconnect_backoff_ms": 200,
        "reconnect_backoff_max_ms": 1000,
    }
    if conn.tls:
        config["security_protocol"] = "SSL"
        if not conn.verify:
            config["ssl_context"] = _unverified_tls_context()
        elif conn.ca_file:
            config["ssl_cafile"] = conn.ca_file
    return config


@_with_client_log
def check(conn: Connection) -> dict:
    """Describe the cluster without writing anything.

    The brokers come back as they advertise themselves. When this works and a
    produce then times out, those advertised addresses are what this client cannot
    reach, which is the most common Kafka connectivity fault there is.
    """
    started = time.monotonic()
    with KafkaAdminClient(**client_config(conn)) as admin:
        cluster = admin.describe_cluster()
        topics = sorted(admin.list_topics())
    return {
        "cluster_id": cluster.get("cluster_id"),
        "controller_id": cluster.get("controller_id"),
        "brokers": [
            {
                "id": broker.get("broker_id"),
                "host": broker.get("host"),
                "port": broker.get("port"),
                "rack": broker.get("rack"),
            }
            for broker in cluster.get("brokers", [])
        ],
        "topic_count": len(topics),
        "topics": topics[:100],
        "elapsed_ms": _elapsed_ms(started),
    }


def ensure_topic(conn: Connection, topic: str) -> bool:
    """Create ``topic`` with the broker's default partitions and replication if missing.

    Returns True when this call created it.
    """
    with KafkaAdminClient(**client_config(conn)) as admin:
        if topic in admin.list_topics():
            return False
        try:
            admin.create_topics({topic: {}}, timeout_ms=conn.timeout_ms)
        except TopicAlreadyExistsError:
            # Created by someone else between the list and the create.
            return False
    return True


@_with_client_log
def produce(conn: Connection, topic: str, message: str, create_topic: bool = True) -> dict:
    """Write one JSON message to ``topic`` and wait for the broker to acknowledge it."""
    started = time.monotonic()
    warnings = []
    created = False
    if create_topic:
        try:
            created = ensure_topic(conn, topic)
        except KafkaTimeoutError:
            # The cluster did not answer, so the producer would only wait as long again.
            raise
        except (KafkaError, OSError) as exc:
            # Not fatal: a client may produce to topics it cannot create, and the
            # broker may auto-create it. The send below is the test that matters.
            warnings.append(f"could not create the topic first ({describe_error(exc)})")
    payload = {"message": message, "source": CLIENT_ID, "sent_at": _iso(time.time() * 1000)}
    producer = KafkaProducer(
        **client_config(conn),
        max_block_ms=conn.timeout_ms,
        # One attempt. A retry would turn a failing endpoint into a slow pass.
        retries=0,
        enable_idempotence=False,
    )
    try:
        value = json.dumps(payload).encode("utf-8")
        record = producer.send(topic, value).get(timeout=conn.timeout_ms / 1000)
    finally:
        producer.close(timeout=conn.timeout_ms / 1000)
    return {
        "topic": record.topic,
        "partition": record.partition,
        "offset": record.offset,
        "topic_created": created,
        "sent": payload,
        "warnings": warnings,
        "elapsed_ms": _elapsed_ms(started),
    }


@_with_client_log
def consume(conn: Connection, topic: str, limit: int = 5) -> dict:
    """Read the newest ``limit`` records on ``topic``, across all its partitions.

    Reads by assignment rather than as a consumer group, so nothing is committed and
    no group is left behind on the cluster. An empty topic is a success with no records.
    """
    started = time.monotonic()
    consumer = KafkaConsumer(
        **client_config(conn),
        group_id=None,
        enable_auto_commit=False,
        # Reading must never create the topic it was asked to read.
        allow_auto_create_topics=False,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise TopicNotFoundError(
                f"topic {topic!r} does not exist, or this client is not allowed to see it"
            )
        assigned = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(assigned)
        first = consumer.beginning_offsets(assigned, timeout_ms=conn.timeout_ms)
        end = consumer.end_offsets(assigned, timeout_ms=conn.timeout_ms)
        pending = set()
        for tp in assigned:
            start = max(first[tp], end[tp] - limit)
            consumer.seek(tp, start)
            if start < end[tp]:
                pending.add(tp)
        records = []
        deadline = time.monotonic() + conn.timeout_ms / 1000
        while pending and time.monotonic() < deadline:
            for tp, batch in consumer.poll(timeout_ms=500).items():
                records.extend(r for r in batch if r.offset < end[tp])
                if tp in pending and consumer.position(tp) >= end[tp]:
                    pending.discard(tp)
    finally:
        consumer.close(autocommit=False)
    records.sort(key=lambda r: (r.timestamp, r.partition, r.offset))
    newest = records[-limit:]
    return {
        "topic": topic,
        "partitions": len(assigned),
        "records": [_record(r) for r in reversed(newest)],
        # True when the wait ran out before every partition was read to its end.
        "incomplete": bool(pending),
        "elapsed_ms": _elapsed_ms(started),
    }


def _record(record) -> dict:
    return {
        "partition": record.partition,
        "offset": record.offset,
        "timestamp": _iso(record.timestamp) if record.timestamp and record.timestamp > 0 else None,
        "key": _decode(record.key),
        "value": _decode(record.value),
    }


def _decode(raw: bytes | None):
    """Bytes as the most readable thing they are: JSON, then text, then base64."""
    if raw is None:
        return None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        encoded = base64.b64encode(raw).decode("ascii")
        return {"base64": _cut(encoded), "bytes": len(raw)}
    if len(text) > MAX_SHOWN_CHARS:
        return _cut(text)
    try:
        return json.loads(text)
    except ValueError:
        return text


def _cut(text: str) -> str:
    if len(text) <= MAX_SHOWN_CHARS:
        return text
    return f"{text[:MAX_SHOWN_CHARS]}... ({len(text) - MAX_SHOWN_CHARS} more characters)"


def _iso(epoch_ms: float) -> str:
    moment = datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _elapsed_ms(started: float) -> int:
    return round((time.monotonic() - started) * 1000)
