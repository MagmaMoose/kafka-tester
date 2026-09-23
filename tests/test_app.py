import pytest
from kafka.errors import KafkaTimeoutError

from kafka_tester import app as app_module
from kafka_tester import kafka_ops
from kafka_tester.app import CUSTOM, create_app
from kafka_tester.config import Settings, Target

SETTINGS = Settings(
    targets=(
        Target("local", "localhost:9092"),
        Target("public", "kafka.example.com:443", tls=True, verify=False),
    ),
    ip_echo_url="https://echo.example.com",
)


@pytest.fixture
def calls(monkeypatch):
    """Record what the routes ask of kafka_ops, and answer for it."""
    seen = []

    def fake(action):
        def run(conn, *args, **kwargs):
            seen.append((action, conn, args, kwargs))
            return {"elapsed_ms": 1}

        return run

    for action in ("check", "produce", "consume"):
        monkeypatch.setattr(kafka_ops, action, fake(action))
    return seen


def client(settings=SETTINGS):
    return create_app(settings).test_client()


def test_index_lists_the_targets_and_the_custom_option():
    page = client().get("/").get_data(as_text=True)
    assert "local (localhost:9092)" in page
    assert "public (kafka.example.com:443, TLS)" in page
    assert f'<option value="{CUSTOM}">' in page


def test_index_without_custom_endpoints():
    page = client(Settings(targets=SETTINGS.targets, allow_custom=False)).get("/")
    assert f'<option value="{CUSTOM}">' not in page.get_data(as_text=True)


def test_index_says_when_there_is_nothing_to_test():
    page = client(Settings(allow_custom=False)).get("/").get_data(as_text=True)
    assert "No targets are configured" in page


def test_security_headers():
    response = client().get("/")
    assert "default-src 'self'" in response.headers["Content-Security-Policy"]
    assert response.headers["X-Content-Type-Options"] == "nosniff"


def test_static_files_are_served():
    assert client().get("/static/app.js").status_code == 200


def test_healthz():
    assert client().get("/healthz").get_json() == {"status": "ok"}


@pytest.mark.parametrize(
    ("body", "status", "error"),
    [
        (None, 400, "JSON object"),
        ({"action": "delete"}, 400, "action must be one of"),
        ({"target": "elsewhere"}, 400, "no preset target"),
        ({"target": CUSTOM, "bootstrap": "no-port"}, 400, "not a host:port"),
        ({"target": "local", "topic": "bad topic"}, 400, "topic names"),
        ({"target": "local", "message": "x" * 10_001}, 400, "limited to"),
        ({"target": CUSTOM, "bootstrap": "a:1", "tls": "sometimes"}, 400, "tls must be"),
    ],
)
def test_rejects_bad_requests(calls, body, status, error):
    response = client().post("/test", json=body) if body else client().post("/test", data="x")
    assert response.status_code == status
    assert error in response.get_json()["error"]
    assert calls == []


def test_custom_endpoints_can_be_turned_off(calls):
    settings = Settings(targets=SETTINGS.targets, allow_custom=False)
    response = client(settings).post("/test", json={"target": CUSTOM, "bootstrap": "a:1"})
    assert response.status_code == 403
    assert calls == []


def test_a_preset_keeps_its_own_tls_settings(calls):
    response = client().post("/test", json={"target": "public", "tls": False, "verify": True})
    assert response.get_json()["success"] is True
    action, conn, _, _ = calls[0]
    assert action == "check"
    assert (conn.bootstrap, conn.tls, conn.verify) == ("kafka.example.com:443", True, False)


def test_a_custom_endpoint(calls):
    body = {"target": CUSTOM, "bootstrap": " a:1 ", "tls": True, "verify": False}
    response = client().post("/test", json=body)
    assert response.get_json()["bootstrap"] == "a:1"
    conn = calls[0][1]
    assert (conn.bootstrap, conn.tls, conn.verify) == ("a:1", True, False)


def test_with_no_presets_the_form_address_is_used(calls):
    client(Settings()).post("/test", json={"bootstrap": "b:2"})
    assert calls[0][1].bootstrap == "b:2"


def test_produce(calls):
    body = {"target": "local", "action": "produce", "topic": "t1", "message": "hi"}
    response = client(Settings(targets=SETTINGS.targets, create_topics=False)).post(
        "/test", json=body
    )
    assert response.get_json()["action"] == "produce"
    assert calls[0][2] == ("t1", "hi")
    assert calls[0][3] == {"create_topic": False}


def test_produce_fills_in_the_topic_and_message(calls):
    client().post("/test", json={"target": "local", "action": "produce", "message": ""})
    assert calls[0][2] == ("kafka-tester", "Test message from kafka-tester")


def test_consume(calls):
    client().post("/test", json={"target": "local", "action": "consume", "topic": "t2"})
    assert calls[0][0] == "consume"
    assert calls[0][2] == ("t2",)
    assert calls[0][3] == {"limit": app_module.CONSUME_LIMIT}


def test_a_kafka_failure_is_reported_with_its_detail(monkeypatch):
    def fail(conn):
        exc = KafkaTimeoutError("Unable to bootstrap from ['localhost:9092']")
        exc.client_log = ["Bootstrap connection to bootstrap-0 failed: refused"]
        raise exc

    monkeypatch.setattr(kafka_ops, "check", fail)
    response = client().post("/test", json={"target": "local"})
    assert response.status_code == 502
    assert response.get_json() == {
        "success": False,
        "error": "KafkaTimeoutError: Unable to bootstrap from ['localhost:9092']",
        "action": "check",
        "bootstrap": "localhost:9092",
        "client_log": ["Bootstrap connection to bootstrap-0 failed: refused"],
    }


def test_an_unexpected_failure_is_logged_not_returned(monkeypatch):
    def fail(conn):
        raise RuntimeError("internal detail")

    monkeypatch.setattr(kafka_ops, "check", fail)
    response = client().post("/test", json={"target": "local"})
    assert response.status_code == 500
    assert "internal detail" not in response.get_data(as_text=True)


def test_debug_ip(monkeypatch):
    monkeypatch.setattr(app_module.network, "outbound_ip", lambda url: "203.0.113.7")
    monkeypatch.setattr(app_module.network, "resolve", lambda bootstrap: {bootstrap: ["192.0.2.1"]})
    body = client().get("/debug/ip").get_json()
    assert body["outbound_ip"] == "203.0.113.7"
    assert body["dns"]["local"] == {"localhost:9092": ["192.0.2.1"]}


def test_debug_ip_when_the_echo_service_fails(monkeypatch):
    def fail(url):
        raise OSError("timed out")

    monkeypatch.setattr(app_module.network, "outbound_ip", fail)
    monkeypatch.setattr(app_module.network, "resolve", lambda bootstrap: {})
    assert client().get("/debug/ip").get_json()["outbound_ip_error"] == "OSError: timed out"


def test_debug_ip_with_the_echo_service_off():
    body = client(Settings(ip_echo_url="")).get("/debug/ip").get_json()
    assert body == {"dns": {}}


def test_create_app_reads_the_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_TESTER_TARGETS", '[{"name": "from-env", "bootstrap": "e:1"}]')
    page = create_app().test_client().get("/").get_data(as_text=True)
    assert "from-env (e:1)" in page
