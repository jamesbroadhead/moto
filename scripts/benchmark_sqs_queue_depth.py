"""Measure SQS HTTP sends at increasing queue depths.

Run from the repository: python -m scripts.benchmark_sqs_queue_depth --label baseline
Each sample uses a fresh server process. Backlog fixtures are inserted directly into
the backend before measurement; every measured operation uses the Boto3 HTTP client.
"""

import argparse
import hashlib
import json
import logging
import multiprocessing
import platform
import statistics
import subprocess
from importlib.metadata import version
from time import perf_counter
from uuid import UUID

import boto3
from botocore.config import Config

import moto


def serve(ready, stop, case, depth):
    from moto.core import DEFAULT_ACCOUNT_ID
    from moto.server import ThreadedMotoServer
    from moto.sqs.models import Message, sqs_backends

    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    fifo = case != "standard"
    name = "benchmark.fifo" if fifo else "benchmark"
    attributes = {"FifoQueue": "true"} if fifo else {}
    if case in ["fifo_content", "events_fifo"]:
        attributes["ContentBasedDeduplication"] = "true"
    queue = sqs_backends[DEFAULT_ACCOUNT_ID]["us-east-1"].create_queue(
        name, **attributes
    )
    for i in range(depth):
        body = f"seed-{i}"
        message = Message(str(UUID(int=i + 1)), body)
        message.mark_sent()
        if fifo:
            message.group_id = "seed"
            message.deduplication_id = (
                hashlib.sha256(body.encode()).hexdigest()
                if case in ["fifo_content", "events_fifo"]
                else body
            )
        queue._messages.append(message)
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    ready.put((server.get_host_and_port()[1], name, queue.queue_arn))
    try:
        stop.wait()
    finally:
        server.stop()


def measure(case, depth, operations):
    context = multiprocessing.get_context("spawn")
    ready, stop = context.Queue(), context.Event()
    process = context.Process(target=serve, args=(ready, stop, case, depth))
    process.start()
    clients = []
    try:
        port, name, arn = ready.get(timeout=30)
        config = {
            "region_name": "us-east-1",
            "aws_access_key_id": "testing",
            "aws_secret_access_key": "testing",
            "endpoint_url": f"http://127.0.0.1:{port}",
            "config": Config(retries={"total_max_attempts": 1}, read_timeout=30),
        }
        sqs = boto3.client("sqs", **config)
        clients.append(sqs)
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        if case == "events_fifo":
            events = boto3.client("events", **config)
            clients.append(events)
            events.put_rule(
                Name="benchmark", EventPattern=json.dumps({"source": ["bench"]})
            )
            result = events.put_targets(
                Rule="benchmark",
                Targets=[
                    {
                        "Id": "queue",
                        "Arn": arn,
                        "SqsParameters": {"MessageGroupId": "bench"},
                    }
                ],
            )
            assert result["FailedEntryCount"] == 0

        def send(i):
            if case == "events_fifo":
                result = events.put_events(
                    Entries=[
                        {
                            "Source": "bench",
                            "DetailType": "benchmark",
                            "Detail": json.dumps({"i": i}),
                        }
                    ]
                )
                assert result["FailedEntryCount"] == 0
            else:
                args = {"QueueUrl": url, "MessageBody": f"measured-{i}"}
                if case != "standard":
                    args["MessageGroupId"] = "bench"
                if case == "fifo_explicit":
                    args["MessageDeduplicationId"] = f"measured-{i}"
                assert sqs.send_message(**args)["MessageId"]

        for i in range(-5, 0):
            send(i)
        latencies = []
        start = perf_counter()
        for i in range(operations):
            before = perf_counter()
            send(i)
            latencies.append((perf_counter() - before) * 1000)
        elapsed = perf_counter() - start
        count = int(
            sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
            )["Attributes"]["ApproximateNumberOfMessages"]
        )
        assert count == depth + operations + 5, count
        return {
            "case": case,
            "initial_depth": depth,
            "final_depth": count,
            "operations": operations,
            "requests_per_second": operations / elapsed,
            "median_ms": statistics.median(latencies),
            "p95_ms": sorted(latencies)[int((len(latencies) - 1) * 0.95)],
        }
    finally:
        for client in clients:
            client.close()
        stop.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--operations", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--depths", type=int, nargs="+", default=[0, 1000, 8000])
    parser.add_argument(
        "--cases",
        nargs="+",
        choices=["standard", "fifo_explicit", "fifo_content", "events_fifo"],
        default=["standard", "fifo_explicit", "fifo_content", "events_fifo"],
    )
    args = parser.parse_args()
    if min(args.operations, args.rounds) < 1 or min(args.depths) < 0:
        parser.error(
            "operations and rounds must be positive; depths must be nonnegative"
        )
    print(  # noqa: T201
        json.dumps(
            {
                "label": args.label,
                "moto": moto.__version__,
                "source": moto.__file__,
                "head": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
                "tracked_diff": subprocess.check_output(
                    ["git", "diff", "--stat"], text=True
                ).strip(),
                "python": platform.python_version(),
                "platform": platform.platform(),
                "versions": {p: version(p) for p in ["boto3", "botocore", "werkzeug"]},
            }
        ),
        flush=True,
    )
    for round_number in range(args.rounds):
        depths = args.depths if round_number % 2 == 0 else list(reversed(args.depths))
        for case in args.cases:
            for depth in depths:
                print(  # noqa: T201
                    json.dumps(
                        {
                            "label": args.label,
                            "round": round_number + 1,
                            **measure(case, depth, args.operations),
                        }
                    ),
                    flush=True,
                )


if __name__ == "__main__":
    main()
