# kafka-tester

Test any Kafka endpoint's health from a small web UI, or from the command line.

It answers the questions that come up when a client cannot talk to Kafka: can this
network reach the bootstrap address, what do the brokers advertise, can a message be
written and read back, and does the TLS certificate verify.

- **Check** connects and lists the brokers as they advertise themselves. It writes nothing.
- **Produce** writes one JSON message, creating the topic first if it is missing.
- **Consume** reads the newest messages on a topic without joining a consumer group, so
  it commits nothing and leaves no group behind.

When a test fails, the reply carries kafka-python's own account of why (connection
refused, a name that does not resolve, a TLS handshake that failed), which the client
library otherwise reports as a bare "Unable to bootstrap".

`/debug/ip` shows the server's outbound IP address, which is the one a broker's
allowlist sees, and what each preset target resolves to from where the tester runs.

## Run it

```bash
docker run --rm -p 8000:8000 \
  -e KAFKA_TESTER_TARGETS='[{"name": "local", "bootstrap": "host.docker.internal:9092"}]' \
  ghcr.io/magmamoose/kafka-tester:latest
```

Then open <http://localhost:8000>. Images are tagged `vX.Y.Z` per release; pin one
outside a quick try.

On Kubernetes, use the Helm chart:

```bash
helm install kafka-tester oci://ghcr.io/magmamoose/charts/kafka-tester
```

See [MagmaMoose/charts](https://github.com/MagmaMoose/charts/tree/main/charts/kafka-tester)
for its values.

## Configuration

All settings are environment variables, and all are optional.

| Variable | Default | |
|---|---|---|
| `KAFKA_TESTER_TARGETS` | none | JSON list of preset targets. Each has `name` and `bootstrap` (`host:port[,host:port...]`), and optionally `tls` (false), `verify` (true) and `description`. |
| `KAFKA_TESTER_ALLOW_CUSTOM` | `true` | `false` limits the UI to the preset targets. |
| `KAFKA_TESTER_DEFAULT_TOPIC` | `kafka-tester` | Topic the form starts with. |
| `KAFKA_TESTER_TIMEOUT_MS` | `10000` | Bounds each Kafka request, from 1000 to 120000. |
| `KAFKA_TESTER_CREATE_TOPICS` | `true` | Create a missing topic, with the broker's default partitions and replication, before producing. |
| `KAFKA_TESTER_SSL_CAFILE` | none | CA bundle for TLS endpoints under a private CA. The system store is used otherwise. |
| `KAFKA_TESTER_IP_ECHO_URL` | `https://api.ipify.org` | Where `/debug/ip` asks for the outbound address. Empty turns that off. |

A target with `"tls": true` connects with TLS and verifies the certificate and hostname;
`"verify": false` accepts any certificate. SASL authentication is not supported.

## Security

The tester has no login. It connects wherever it is asked to, from wherever it runs,
and can write to any topic it can reach. Keep it off the internet or behind your own
authentication, and set `KAFKA_TESTER_ALLOW_CUSTOM=false` where it should only reach
its presets.

## Command line

The image also carries a CLI with the same three actions. It prints JSON, and exits 1
when the endpoint fails the test and 2 on bad arguments.

```bash
docker run --rm ghcr.io/magmamoose/kafka-tester:latest \
  python -m kafka_tester check --bootstrap broker-1:9092,broker-2:9092

python -m kafka_tester produce --bootstrap kafka.example.com:9093 --tls --topic health
python -m kafka_tester consume --bootstrap kafka.example.com:9093 --tls --topic health --limit 10
```

`--insecure` accepts any TLS certificate and `--ca-file` trusts a private CA.

## Development

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest
ruff check . && ruff format --check .
```

The broker tests run when `KAFKA_TESTER_IT_BOOTSTRAP` names a broker. The stock Kafka
image is enough:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.1
KAFKA_TESTER_IT_BOOTSTRAP=localhost:9092 pytest
```

To run the UI from the checkout:

```bash
flask --app 'kafka_tester.app:create_app()' run --port 8000
```

## License

[Apache-2.0](LICENSE)
