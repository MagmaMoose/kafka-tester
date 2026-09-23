"""Settings, read once from the environment at startup, and the checks on user input.

Nothing is required. With no settings the tester starts with no preset targets and
lets whoever opens it type a bootstrap address.
"""

from __future__ import annotations

import json
import os
import re
import string
from collections.abc import Mapping
from dataclasses import dataclass

# Kafka's own rule for topic names.
_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
# Hostnames and IPv4 addresses. IPv6 goes in brackets and is checked separately.
_HOST_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}
_TARGET_KEYS = {"name", "bootstrap", "tls", "verify", "description"}


class ConfigError(ValueError):
    """A setting or a request value that cannot be used as given."""


def parse_bool(value: object, name: str, default: bool) -> bool:
    """Read a flag from an environment string or a JSON boolean."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ConfigError(f"{name} must be true or false, not {value!r}")


def validate_bootstrap(value: object) -> str:
    """Return ``host:port[,host:port...]`` with whitespace removed, or raise ConfigError.

    Checked up front because kafka-python's answer to a malformed address is a
    timeout many seconds later, which reads like the broker being down.
    """
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("bootstrap servers are required, as host:port")
    servers = []
    for raw in value.split(","):
        entry = raw.strip()
        host, sep, port = entry.rpartition(":")
        if host.startswith("[") and host.endswith("]"):
            bare = host[1:-1]
            host_ok = bool(bare) and all(c in string.hexdigits + ":." for c in bare)
        else:
            host_ok = bool(_HOST_RE.match(host))
        if not (sep and host_ok and port.isdigit() and 0 < int(port) < 65536):
            raise ConfigError(f"not a host:port address: {entry!r}")
        servers.append(f"{host}:{int(port)}")
    return ",".join(servers)


def validate_topic(value: object) -> str:
    if not isinstance(value, str) or not _TOPIC_RE.match(value):
        raise ConfigError(
            "topic names are 1 to 249 characters of letters, digits, '.', '_' and '-'"
        )
    return value


@dataclass(frozen=True)
class Target:
    """A preset endpoint, offered in the UI's target list."""

    name: str
    bootstrap: str
    tls: bool = False
    verify: bool = True
    description: str = ""


def _parse_targets(raw: str) -> tuple[Target, ...]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"KAFKA_TESTER_TARGETS is not valid JSON: {exc}") from exc
    if not isinstance(data, list):
        raise ConfigError("KAFKA_TESTER_TARGETS must be a JSON list of targets")
    targets: list[Target] = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise ConfigError(f"target {index} must be a JSON object")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(f"target {index} needs a name")
        name = name.strip()
        if any(t.name == name for t in targets):
            raise ConfigError(f"target name {name!r} is used more than once")
        unknown = sorted(set(item) - _TARGET_KEYS)
        if unknown:
            raise ConfigError(f"target {name!r} has unknown keys: {', '.join(unknown)}")
        try:
            bootstrap = validate_bootstrap(item.get("bootstrap"))
        except ConfigError as exc:
            raise ConfigError(f"target {name!r}: {exc}") from exc
        targets.append(
            Target(
                name=name,
                bootstrap=bootstrap,
                tls=parse_bool(item.get("tls"), f"target {name!r} tls", False),
                verify=parse_bool(item.get("verify"), f"target {name!r} verify", True),
                description=str(item.get("description") or ""),
            )
        )
    return tuple(targets)


@dataclass(frozen=True)
class Settings:
    targets: tuple[Target, ...] = ()
    # Whether the UI accepts any bootstrap address, or only the preset targets.
    allow_custom: bool = True
    default_topic: str = "kafka-tester"
    # Bounds each Kafka request, and the wait for a produced message or read records.
    timeout_ms: int = 10_000
    # Create a missing topic before producing to it, with the broker's defaults.
    create_topics: bool = True
    # CA bundle for TLS endpoints signed by a private CA. The system store otherwise.
    ca_file: str | None = None
    # Where /debug/ip asks for this server's public address. Empty turns that off.
    ip_echo_url: str = "https://api.ipify.org"

    def target(self, name: str) -> Target | None:
        return next((t for t in self.targets if t.name == name), None)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        timeout = env.get("KAFKA_TESTER_TIMEOUT_MS", "").strip() or "10000"
        if not timeout.isdigit() or not 1000 <= int(timeout) <= 120_000:
            raise ConfigError("KAFKA_TESTER_TIMEOUT_MS must be a number from 1000 to 120000")
        default_topic = env.get("KAFKA_TESTER_DEFAULT_TOPIC", "").strip() or cls.default_topic
        try:
            validate_topic(default_topic)
        except ConfigError as exc:
            raise ConfigError(f"KAFKA_TESTER_DEFAULT_TOPIC: {exc}") from exc
        echo_url = env.get("KAFKA_TESTER_IP_ECHO_URL", cls.ip_echo_url).strip()
        if echo_url and not echo_url.startswith("https://"):
            raise ConfigError("KAFKA_TESTER_IP_ECHO_URL must be an https:// URL, or empty")
        raw_targets = env.get("KAFKA_TESTER_TARGETS", "").strip()
        return cls(
            targets=_parse_targets(raw_targets) if raw_targets else (),
            allow_custom=parse_bool(
                env.get("KAFKA_TESTER_ALLOW_CUSTOM"), "KAFKA_TESTER_ALLOW_CUSTOM", True
            ),
            default_topic=default_topic,
            timeout_ms=int(timeout),
            create_topics=parse_bool(
                env.get("KAFKA_TESTER_CREATE_TOPICS"), "KAFKA_TESTER_CREATE_TOPICS", True
            ),
            ca_file=env.get("KAFKA_TESTER_SSL_CAFILE", "").strip() or None,
            ip_echo_url=echo_url,
        )
