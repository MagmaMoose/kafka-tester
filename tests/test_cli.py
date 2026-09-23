import json
import logging

import pytest
from kafka.errors import KafkaTimeoutError

from kafka_tester import __main__ as cli
from kafka_tester import kafka_ops


@pytest.fixture
def calls(monkeypatch):
    seen = []

    def fake(action):
        def run(conn, *args, **kwargs):
            seen.append((action, conn, args, kwargs))
            return {"action": action}

        return run

    for action in ("check", "produce", "consume"):
        monkeypatch.setattr(kafka_ops, action, fake(action))
    return seen


def test_check_prints_json(calls, capsys):
    assert cli.main(["check", "--bootstrap", "a:1,b:2", "--timeout-ms", "3000"]) == 0
    assert json.loads(capsys.readouterr().out) == {"action": "check"}
    conn = calls[0][1]
    assert (conn.bootstrap, conn.tls, conn.verify, conn.timeout_ms) == (
        "a:1,b:2",
        False,
        True,
        3000,
    )


def test_tls_options(calls):
    cli.main(["check", "--bootstrap", "a:1", "--tls", "--insecure", "--ca-file", "/ca.pem"])
    conn = calls[0][1]
    assert (conn.tls, conn.verify, conn.ca_file) == (True, False, "/ca.pem")


def test_produce(calls):
    cli.main(
        ["produce", "--bootstrap", "a:1", "--topic", "t", "--message", "m", "--no-create-topic"]
    )
    assert calls[0][0] == "produce"
    assert calls[0][2] == ("t", "m")
    assert calls[0][3] == {"create_topic": False}


def test_consume(calls):
    cli.main(["consume", "--bootstrap", "a:1", "--topic", "t", "--limit", "20"])
    assert calls[0][2] == ("t",)
    assert calls[0][3] == {"limit": 20}


def test_a_failure_exits_1_with_the_client_log(monkeypatch, capsys):
    def fail(conn):
        exc = KafkaTimeoutError("Unable to bootstrap")
        exc.client_log = ["Bootstrap connection to bootstrap-0 failed: refused"]
        raise exc

    monkeypatch.setattr(kafka_ops, "check", fail)
    assert cli.main(["check", "--bootstrap", "a:1"]) == 1
    err = capsys.readouterr().err
    assert "KafkaTimeoutError: Unable to bootstrap" in err
    assert "client: Bootstrap connection to bootstrap-0 failed: refused" in err
    assert any(isinstance(h, logging.NullHandler) for h in logging.getLogger("kafka").handlers)


def test_bad_arguments_exit_2(calls, capsys):
    assert cli.main(["check", "--bootstrap", "no-port"]) == 2
    assert "not a host:port" in capsys.readouterr().err
    assert cli.main(["produce", "--bootstrap", "a:1", "--topic", "bad topic"]) == 2
    assert calls == []


@pytest.mark.parametrize("limit", ["0", "1001", "many"])
def test_limit_is_bounded(limit):
    with pytest.raises(SystemExit):
        cli.main(["consume", "--bootstrap", "a:1", "--limit", limit])
