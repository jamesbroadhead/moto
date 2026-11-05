"""Compare DynamoDB serializers over localhost HTTP in fresh server processes.

Run with the repository installed: python scripts/benchmark_dynamodb_serialization.py
"""

import argparse
import json
import logging
import multiprocessing
import platform
import statistics
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version
from time import perf_counter

import boto3
from botocore.config import Config


def serve(ready, stop, serializer):
    from moto.core.serialize import SERVICE_SPECIFIC_SERIALIZERS, JSONSerializer
    from moto.server import ThreadedMotoServer

    if serializer == "generic":
        SERVICE_SPECIFIC_SERIALIZERS["dynamodb"]["json"] = JSONSerializer
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    ready.put(server.get_host_and_port()[1])
    try:
        stop.wait()
    finally:
        server.stop()


def measure(serializer, operations, workers, attributes):
    context = multiprocessing.get_context("spawn")
    ready, stop = context.Queue(), context.Event()
    process = context.Process(target=serve, args=(ready, stop, serializer))
    process.start()
    client = None
    try:
        port = ready.get(timeout=30)
        client = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            endpoint_url=f"http://127.0.0.1:{port}",
            config=Config(
                retries={"total_max_attempts": 1},
                max_pool_connections=workers,
                connect_timeout=5,
                read_timeout=30,
            ),
        )
        client.create_table(
            TableName="benchmark",
            BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        )
        item = {
            "id": {"S": "one"},
            **{f"attribute_{i}": {"S": "x" * 32} for i in range(attributes)},
        }
        client.put_item(TableName="benchmark", Item=item)

        def read(_):
            started = perf_counter()
            actual = client.get_item(TableName="benchmark", Key={"id": {"S": "one"}})[
                "Item"
            ]
            assert actual == item
            return (perf_counter() - started) * 1000

        for i in range(5):
            read(i)
        started = perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            latencies = list(pool.map(read, range(operations)))
        elapsed = perf_counter() - started
        return {
            "serializer": serializer,
            "workers": workers,
            "attributes": attributes,
            "operations": operations,
            "elapsed_seconds": elapsed,
            "requests_per_second": operations / elapsed,
            "median_ms": statistics.median(latencies),
            "p95_ms": sorted(latencies)[int(0.95 * (operations - 1))],
        }
    finally:
        if client is not None:
            client.close()
        stop.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operations", type=int, default=50)
    parser.add_argument("--attributes", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if min(args.operations, args.attributes, args.rounds) < 1:
        parser.error("operations, attributes and rounds must be positive")
    print(  # noqa: T201
        json.dumps(
            {
                "versions": {
                    p: version(p) for p in ["moto", "boto3", "botocore", "werkzeug"]
                },
                "python": platform.python_version(),
                "platform": platform.platform(),
            }
        ),
        flush=True,
    )
    for workers in [1, 10]:
        for round_number in range(args.rounds):
            # Alternate order to reduce systematic warm-up and background-load bias.
            modes = (
                ["generic", "optimized"]
                if round_number % 2 == 0
                else ["optimized", "generic"]
            )
            for serializer in modes:
                result = measure(serializer, args.operations, workers, args.attributes)
                print(json.dumps({"round": round_number + 1, **result}), flush=True)  # noqa: T201


if __name__ == "__main__":
    main()
