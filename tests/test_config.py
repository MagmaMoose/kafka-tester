import json

import pytest

from kafka_tester.config import (
    ConfigError,
    Settings,
    Target,
    parse_bool,
    validate_bootstrap,
    validate_topic,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, True), ("", True), (True, True), (False, False), ("yes", True), (" OFF ", False)],
)
def test_parse_bool(value, expected):
    assert parse_bool(value, "flag", True) is expected


def test_parse_bool_rejects_anything_else():
    with pytest.raises(ConfigError, match="flag must be true or false"):
        parse_bool("maybe", "flag", True)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("broker:9092", "broker:9092"),
        (" a:1 , b.example.com:9093 ", "a:1,b.example.com:9093"),
        ("10.0.0.5:9092", "10.0.0.5:9092"),
        ("[::1]:9092", "[::1]:9092"),
    ],
)
def test_validate_bootstrap(value, expected):
    assert validate_bootstrap(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "   ",
        "broker",
        "broker:",
        "broker:0",
        "broker:70000",
        "PLAINTEXT://broker:9092",
        "a:9092,,b:9092",
        "[]:9092",
        "[zz]:9092",
        "bro ker:9092",
    ],
)
def test_validate_bootstrap_rejects(value):
    with pytest.raises(ConfigError):
        validate_bootstrap(value)


def test_validate_topic():
    assert validate_topic("orders.v1_retry-2") == "orders.v1_retry-2"
    for bad in ("", "has space", "x" * 250, None):
        with pytest.raises(ConfigError):
            validate_topic(bad)


def test_defaults():
    settings = Settings.from_env({})
    assert settings == Settings()
    assert settings.allow_custom is True
    assert settings.timeout_ms == 10_000
    assert settings.ip_echo_url == "https://api.ipify.org"


def test_everything_set():
    targets = [
        {"name": "local", "bootstrap": "localhost:9092"},
        {
            "name": "public",
            "bootstrap": "kafka.example.com:443",
            "tls": True,
            "verify": "false",
            "description": "Through the ingress",
        },
    ]
    settings = Settings.from_env(
        {
            "KAFKA_TESTER_TARGETS": json.dumps(targets),
            "KAFKA_TESTER_ALLOW_CUSTOM": "false",
            "KAFKA_TESTER_DEFAULT_TOPIC": "health",
            "KAFKA_TESTER_TIMEOUT_MS": "5000",
            "KAFKA_TESTER_CREATE_TOPICS": "no",
            "KAFKA_TESTER_SSL_CAFILE": "/etc/kafka/ca.pem",
            "KAFKA_TESTER_IP_ECHO_URL": "",
        }
    )
    assert settings.targets == (
        Target("local", "localhost:9092"),
        Target(
            "public",
            "kafka.example.com:443",
            tls=True,
            verify=False,
            description="Through the ingress",
        ),
    )
    assert settings.allow_custom is False
    assert settings.default_topic == "health"
    assert settings.timeout_ms == 5000
    assert settings.create_topics is False
    assert settings.ca_file == "/etc/kafka/ca.pem"
    assert settings.ip_echo_url == ""
    assert settings.target("public").tls is True
    assert settings.target("missing") is None


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"KAFKA_TESTER_TIMEOUT_MS": "10s"}, "KAFKA_TESTER_TIMEOUT_MS"),
        ({"KAFKA_TESTER_TIMEOUT_MS": "500"}, "KAFKA_TESTER_TIMEOUT_MS"),
        ({"KAFKA_TESTER_DEFAULT_TOPIC": "bad topic"}, "KAFKA_TESTER_DEFAULT_TOPIC"),
        ({"KAFKA_TESTER_IP_ECHO_URL": "http://example.com"}, "https://"),
        ({"KAFKA_TESTER_ALLOW_CUSTOM": "sometimes"}, "KAFKA_TESTER_ALLOW_CUSTOM"),
        ({"KAFKA_TESTER_TARGETS": "{not json"}, "not valid JSON"),
        ({"KAFKA_TESTER_TARGETS": '{"name": "a"}'}, "JSON list"),
        ({"KAFKA_TESTER_TARGETS": '["a"]'}, "JSON object"),
        ({"KAFKA_TESTER_TARGETS": '[{"bootstrap": "a:1"}]'}, "needs a name"),
        (
            {"KAFKA_TESTER_TARGETS": json.dumps([{"name": "a", "bootstrap": "a:1"}] * 2)},
            "more than once",
        ),
        (
            {"KAFKA_TESTER_TARGETS": '[{"name": "a", "bootstrap": "a:1", "sasl": "x"}]'},
            "unknown keys: sasl",
        ),
        (
            {"KAFKA_TESTER_TARGETS": '[{"name": "a", "bootstrap": "a"}]'},
            "target 'a': not a host:port",
        ),
        ({"KAFKA_TESTER_TARGETS": '[{"name": "a", "bootstrap": "a:1", "tls": "sure"}]'}, "tls"),
    ],
)
def test_bad_settings(env, message):
    with pytest.raises(ConfigError, match=message):
        Settings.from_env(env)


def test_reads_the_process_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_TESTER_DEFAULT_TOPIC", "from-env")
    assert Settings.from_env().default_topic == "from-env"
