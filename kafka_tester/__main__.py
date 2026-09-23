"""Command-line use: ``python -m kafka_tester check --bootstrap broker:9092``.

The same three actions as the web UI, with the result printed as JSON. Exits 1 when
the endpoint fails the test and 2 on bad arguments, so it drops into a shell loop, a
CI step or a Kubernetes Job unchanged.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from kafka.errors import KafkaError

from kafka_tester import kafka_ops
from kafka_tester.config import ConfigError, validate_bootstrap, validate_topic


def _limit(value: str) -> int:
    number = int(value)
    if not 1 <= number <= 1000:
        raise argparse.ArgumentTypeError("must be from 1 to 1000")
    return number


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--bootstrap", required=True, help="host:port[,host:port...]")
    common.add_argument("--tls", action="store_true", help="connect with TLS")
    common.add_argument(
        "--insecure", action="store_true", help="with --tls, accept any certificate"
    )
    common.add_argument("--ca-file", help="with --tls, trust this CA bundle")
    common.add_argument("--timeout-ms", type=int, default=10_000, help="default: 10000")

    parser = argparse.ArgumentParser(
        prog="kafka-tester", description="Check a Kafka endpoint from the command line."
    )
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", parents=[common], help="describe the cluster, write nothing")
    produce = actions.add_parser("produce", parents=[common], help="write one message")
    produce.add_argument("--topic", default="kafka-tester")
    produce.add_argument("--message", default="Test message from kafka-tester")
    produce.add_argument(
        "--no-create-topic", action="store_true", help="do not create a missing topic first"
    )
    consume = actions.add_parser("consume", parents=[common], help="read the newest messages")
    consume.add_argument("--topic", default="kafka-tester")
    consume.add_argument("--limit", type=_limit, default=5, help="1 to 1000, default: 5")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # kafka-python's warnings come back with a failure (below); unhandled, Python
    # would also print every one of them to stderr as they happen.
    logging.getLogger("kafka").addHandler(logging.NullHandler())
    try:
        conn = kafka_ops.Connection(
            bootstrap=validate_bootstrap(args.bootstrap),
            tls=args.tls,
            verify=not args.insecure,
            ca_file=args.ca_file,
            timeout_ms=args.timeout_ms,
        )
        if args.action == "check":
            result = kafka_ops.check(conn)
        elif args.action == "produce":
            result = kafka_ops.produce(
                conn,
                validate_topic(args.topic),
                args.message,
                create_topic=not args.no_create_topic,
            )
        else:
            result = kafka_ops.consume(conn, validate_topic(args.topic), limit=args.limit)
    except ConfigError as exc:
        print(f"kafka-tester: {exc}", file=sys.stderr)
        return 2
    except (KafkaError, kafka_ops.TopicNotFoundError, OSError) as exc:
        print(f"kafka-tester: {kafka_ops.describe_error(exc)}", file=sys.stderr)
        for line in kafka_ops.client_log(exc):
            print(f"  client: {line}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
