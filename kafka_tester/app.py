"""The web UI and the JSON API behind it."""

from __future__ import annotations

import logging

from flask import Flask, jsonify, render_template, request
from kafka.errors import KafkaError

from kafka_tester import kafka_ops, network
from kafka_tester.config import (
    ConfigError,
    Settings,
    parse_bool,
    validate_bootstrap,
    validate_topic,
)

log = logging.getLogger(__name__)

ACTIONS = ("check", "produce", "consume")
# The target-list value that means "use the bootstrap address typed in the form".
CUSTOM = "__custom__"
MAX_MESSAGE_CHARS = 10_000
CONSUME_LIMIT = 5

# Failures that describe the cluster or the network, which is the answer the operator
# came for, so their text goes back to the browser. Anything else is a fault in this
# app: it is logged in full and reported without detail.
_EXPECTED = (KafkaError, kafka_ops.TopicNotFoundError, OSError)


class RequestError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def _connection(body: dict, settings: Settings) -> kafka_ops.Connection:
    name = body.get("target")
    if name == CUSTOM or (not name and not settings.targets):
        if not settings.allow_custom:
            raise RequestError("this tester only connects to its preset targets", 403)
        tls = parse_bool(body.get("tls"), "tls", False)
        return kafka_ops.Connection(
            bootstrap=validate_bootstrap(body.get("bootstrap")),
            tls=tls,
            verify=parse_bool(body.get("verify"), "verify", True),
            ca_file=settings.ca_file,
            timeout_ms=settings.timeout_ms,
        )
    target = settings.target(name) if isinstance(name, str) else None
    if target is None:
        raise RequestError(f"no preset target called {name!r}")
    # A preset's TLS settings are part of the preset; the form cannot override them.
    return kafka_ops.Connection(
        bootstrap=target.bootstrap,
        tls=target.tls,
        verify=target.verify,
        ca_file=settings.ca_file,
        timeout_ms=settings.timeout_ms,
    )


def create_app(settings: Settings | None = None) -> Flask:
    settings = settings if settings is not None else Settings.from_env()
    # A no-op when the server already configured logging.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # kafka-python logs every connection at INFO, which buries the one line per test.
    logging.getLogger("kafka").setLevel(logging.WARNING)
    app = Flask(__name__)
    app.json.sort_keys = False

    @app.after_request
    def _security_headers(response):
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
        )
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        return response

    @app.get("/")
    def index():
        return render_template(
            "index.html", settings=settings, custom=CUSTOM, consume_limit=CONSUME_LIMIT
        )

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/test")
    def run_test():
        body = request.get_json(silent=True)
        try:
            if not isinstance(body, dict):
                raise RequestError("send a JSON object")
            action = body.get("action", "check")
            if action not in ACTIONS:
                raise RequestError(f"action must be one of {', '.join(ACTIONS)}")
            conn = _connection(body, settings)
            topic = validate_topic(body.get("topic") or settings.default_topic)
            message = body.get("message")
            if not isinstance(message, str) or not message:
                message = "Test message from kafka-tester"
            if len(message) > MAX_MESSAGE_CHARS:
                raise RequestError(f"messages are limited to {MAX_MESSAGE_CHARS} characters")
        except ConfigError as exc:
            return _failure(str(exc), 400)
        except RequestError as exc:
            return _failure(str(exc), exc.status)

        log.info("%s against %s (tls=%s)", action, conn.bootstrap, conn.tls)
        try:
            if action == "check":
                result = kafka_ops.check(conn)
            elif action == "produce":
                result = kafka_ops.produce(
                    conn, topic, message, create_topic=settings.create_topics
                )
            else:
                result = kafka_ops.consume(conn, topic, limit=CONSUME_LIMIT)
        except _EXPECTED as exc:
            log.warning("%s against %s failed: %s", action, conn.bootstrap, exc)
            return _failure(
                kafka_ops.describe_error(exc),
                502,
                action=action,
                bootstrap=conn.bootstrap,
                client_log=kafka_ops.client_log(exc),
            )
        except Exception:
            log.exception("%s against %s failed unexpectedly", action, conn.bootstrap)
            return _failure("kafka-tester hit an internal error; its log has the details", 500)
        return jsonify(
            {
                "success": True,
                "action": action,
                "bootstrap": conn.bootstrap,
                "tls": conn.tls,
                **result,
            }
        )

    @app.get("/debug/ip")
    def debug_ip():
        result: dict = {
            "dns": {target.name: network.resolve(target.bootstrap) for target in settings.targets}
        }
        if settings.ip_echo_url:
            try:
                result["outbound_ip"] = network.outbound_ip(settings.ip_echo_url)
            except (OSError, ValueError) as exc:
                result["outbound_ip_error"] = kafka_ops.describe_error(exc)
        return result

    return app


def _failure(message: str, status: int, **context):
    body = {"success": False, "error": message}
    body.update({key: value for key, value in context.items() if value})
    return jsonify(body), status
