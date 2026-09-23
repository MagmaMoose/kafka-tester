import socket
import ssl

import pytest

from kafka_tester import network


class FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self.body = body

    def read(self, size):
        return self.body[:size]


def fake_https(status=200, body=b"203.0.113.7\n"):
    requests = []

    class Connection:
        def __init__(self, host, port, timeout, context):
            assert context.verify_mode == ssl.CERT_REQUIRED
            assert context.check_hostname is True
            requests.append({"host": host, "port": port, "timeout": timeout})

        def request(self, method, path):
            requests[-1].update(method=method, path=path)

        def getresponse(self):
            return FakeResponse(status, body)

        def close(self):
            requests[-1]["closed"] = True

    return Connection, requests


def test_outbound_ip(monkeypatch):
    connection, requests = fake_https()
    monkeypatch.setattr(network.http.client, "HTTPSConnection", connection)
    assert network.outbound_ip("https://echo.example.com") == "203.0.113.7"
    assert requests == [
        {
            "host": "echo.example.com",
            "port": None,
            "timeout": 5.0,
            "method": "GET",
            "path": "/",
            "closed": True,
        }
    ]


def test_outbound_ip_keeps_the_path_and_query(monkeypatch):
    connection, requests = fake_https()
    monkeypatch.setattr(network.http.client, "HTTPSConnection", connection)
    network.outbound_ip("https://echo.example.com:8443/ip?format=text")
    assert (requests[0]["port"], requests[0]["path"]) == (8443, "/ip?format=text")


def test_outbound_ip_refuses_plain_http():
    with pytest.raises(ValueError, match="https"):
        network.outbound_ip("http://echo.example.com")  # DevSkim: ignore DS137138


def test_outbound_ip_on_an_error_status(monkeypatch):
    connection, requests = fake_https(status=503)
    monkeypatch.setattr(network.http.client, "HTTPSConnection", connection)
    with pytest.raises(OSError, match="HTTP 503"):
        network.outbound_ip("https://echo.example.com")
    assert requests[0]["closed"] is True


def test_resolve(monkeypatch):
    def getaddrinfo(host, port, type):
        if host == "missing.example.com":
            raise socket.gaierror("Name or service not known")
        return [
            (None, None, None, "", ("192.0.2.1", 0)),
            (None, None, None, "", ("192.0.2.1", 0)),
            (None, None, None, "", ("2001:db8::1", 0, 0, 0)),
        ]

    monkeypatch.setattr(network.socket, "getaddrinfo", getaddrinfo)
    assert network.resolve("broker.example.com:9092,missing.example.com:9092,[::1]:9092") == {
        "broker.example.com": ["192.0.2.1", "2001:db8::1"],
        "missing.example.com": "does not resolve: Name or service not known",
        "::1": ["192.0.2.1", "2001:db8::1"],
    }
