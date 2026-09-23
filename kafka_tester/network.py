"""Where this server's traffic comes from, and what the targets resolve to.

The two questions behind most "it works from my laptop" Kafka reports: which address
does the broker's allowlist see, and does the name resolve to the same place from
here as from there.
"""

from __future__ import annotations

import http.client
import socket
from urllib.parse import urlsplit


def outbound_ip(url: str, timeout: float = 5.0) -> str:
    """This server's address as seen by ``url``, a service that echoes it back."""
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        raise ValueError("the IP echo URL must be https://")
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    connection = http.client.HTTPSConnection(parts.hostname, parts.port, timeout=timeout)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        if response.status != 200:
            raise OSError(f"{parts.hostname} answered HTTP {response.status}")
        return response.read(64).decode("utf-8", "replace").strip()
    finally:
        connection.close()


def resolve(bootstrap: str) -> dict[str, list[str] | str]:
    """Each bootstrap host, and the addresses it resolves to from here."""
    results: dict[str, list[str] | str] = {}
    for server in bootstrap.split(","):
        host = server.rpartition(":")[0].strip("[]")
        try:
            infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except OSError as exc:
            results[host] = f"does not resolve: {exc}"
        else:
            results[host] = sorted({info[4][0] for info in infos})
    return results
