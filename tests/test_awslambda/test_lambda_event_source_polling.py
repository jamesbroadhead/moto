"""MOTO_LAMBDA_EVENT_SOURCE_POLLING: event source mappings poll their source and invoke through MOTO_LAMBDA_INVOKE_ENDPOINT."""

import base64
import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest import SkipTest

import boto3
import pytest

from moto import mock_aws, settings
from moto.awslambda.event_source_pollers import EventSourceMappingPoller

from .utilities import get_role_name, get_test_zip_file1

REGION = "us-east-1"


class Endpoint:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        # Returns (function failed, response payload) for an event
        self.respond: Callable[[dict[str, Any]], tuple[bool, Any]] = lambda e: (
            False,
            None,
        )

    def records(self) -> list[dict[str, Any]]:
        with self.lock:
            return [r for c in self.calls for r in c["event"]["Records"]]


@pytest.fixture(name="endpoint")
def fixture_endpoint(monkeypatch):
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The settings are read by the server process")
    endpoint = Endpoint()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            event = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with endpoint.lock:
                endpoint.calls.append({"event": event, "headers": dict(self.headers)})
            failed, payload = endpoint.respond(event)
            body = json.dumps(payload).encode()
            self.send_response(200)
            if failed:
                self.send_header("X-Amz-Function-Error", "Unhandled")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv(
        "MOTO_LAMBDA_INVOKE_ENDPOINT", f"http://127.0.0.1:{server.server_address[1]}"
    )
    monkeypatch.setenv("MOTO_LAMBDA_EVENT_SOURCE_POLLING", "true")
    monkeypatch.setattr(EventSourceMappingPoller, "POLL_INTERVAL", 0.05)
    monkeypatch.setattr(EventSourceMappingPoller, "RETRY_DELAY", 0.1)
    yield endpoint
    server.shutdown()


def wait_for(condition: Callable[[], Any], timeout: float = 10) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError("timed out")


def create_function(name: str = "fn") -> str:
    return boto3.client("lambda", region_name=REGION).create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=get_role_name(),
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": get_test_zip_file1()},
    )["FunctionArn"]


def create_queue(name: str = "q", **attributes: str) -> tuple[str, str]:
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    return url, arn


def queue_counts(url: str) -> tuple[int, int]:
    attributes = boto3.client("sqs", region_name=REGION).get_queue_attributes(
        QueueUrl=url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    return (
        int(attributes["ApproximateNumberOfMessages"]),
        int(attributes["ApproximateNumberOfMessagesNotVisible"]),
    )


@mock_aws
def test_sqs_batches_records_and_deletes_them(endpoint):
    function_arn = create_function()
    url, queue_arn = create_queue()
    lam = boto3.client("lambda", region_name=REGION)
    esm = lam.create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=function_arn,
        BatchSize=5,
        MaximumBatchingWindowInSeconds=1,
    )
    sqs = boto3.client("sqs", region_name=REGION)
    sqs.send_message_batch(
        QueueUrl=url,
        Entries=[
            {
                "Id": str(i),
                "MessageBody": f"m{i}",
                "MessageAttributes": {
                    "kind": {"DataType": "String", "StringValue": "test"}
                },
            }
            for i in range(3)
        ],
    )

    wait_for(lambda: len(endpoint.records()) == 3)
    (call,) = endpoint.calls
    assert (
        call["headers"]["X-Moto-Event-Source-Mapping-Arn"]
        == (esm["EventSourceMappingArn"])
    )
    record = call["event"]["Records"][0]
    assert record["eventSource"] == "aws:sqs"
    assert record["eventSourceARN"] == queue_arn
    assert record["body"] == "m0"
    assert record["attributes"]["ApproximateReceiveCount"] == "1"
    assert record["messageAttributes"]["kind"]["stringValue"] == "test"
    wait_for(lambda: queue_counts(url) == (0, 0))
    assert lam.get_event_source_mapping(UUID=esm["UUID"])["LastProcessingResult"] == (
        "OK"
    )


@mock_aws
def test_sqs_splits_into_batch_size(endpoint):
    function_arn = create_function()
    url, queue_arn = create_queue()
    sqs = boto3.client("sqs", region_name=REGION)
    for i in range(5):
        sqs.send_message(QueueUrl=url, MessageBody=f"m{i}")
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=function_arn, BatchSize=2
    )

    wait_for(lambda: len(endpoint.records()) == 5)
    assert all(len(c["event"]["Records"]) <= 2 for c in endpoint.calls)
    assert sorted(r["body"] for r in endpoint.records()) == [f"m{i}" for i in range(5)]


@mock_aws
def test_sqs_filter_criteria_match_the_json_body(endpoint):
    function_arn = create_function()
    url, queue_arn = create_queue()
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=function_arn,
        FilterCriteria={
            "Filters": [{"Pattern": json.dumps({"body": {"kind": ["keep"]}})}]
        },
    )
    sqs = boto3.client("sqs", region_name=REGION)
    for body in [
        json.dumps({"kind": "drop"}),
        "not json",
        json.dumps({"kind": "keep"}),
    ]:
        sqs.send_message(QueueUrl=url, MessageBody=body)

    wait_for(lambda: endpoint.records())
    # Messages that match no filter are deleted without invoking the function
    wait_for(lambda: queue_counts(url) == (0, 0))
    assert [json.loads(r["body"]) for r in endpoint.records()] == [{"kind": "keep"}]


@mock_aws
def test_sqs_partial_batch_response_retries_only_the_failed_messages(endpoint):
    function_arn = create_function()
    url, queue_arn = create_queue(VisibilityTimeout="1")
    attempts: dict[str, int] = {}

    def respond(event):
        failures = []
        for record in event["Records"]:
            attempts[record["body"]] = attempts.get(record["body"], 0) + 1
            if record["body"] == "bad" and attempts["bad"] == 1:
                failures.append({"itemIdentifier": record["messageId"]})
        return False, {"batchItemFailures": failures}

    endpoint.respond = respond
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=function_arn,
        BatchSize=10,
        MaximumBatchingWindowInSeconds=1,
        FunctionResponseTypes=["ReportBatchItemFailures"],
    )
    sqs = boto3.client("sqs", region_name=REGION)
    sqs.send_message_batch(
        QueueUrl=url,
        Entries=[{"Id": "1", "MessageBody": "good"}, {"Id": "2", "MessageBody": "bad"}],
    )

    wait_for(lambda: attempts.get("bad") == 2)
    wait_for(lambda: queue_counts(url) == (0, 0))
    assert attempts == {"good": 1, "bad": 2}
    retried = endpoint.calls[-1]["event"]["Records"][0]
    assert retried["attributes"]["ApproximateReceiveCount"] == "2"


@mock_aws
def test_sqs_function_error_leaves_messages_until_the_visibility_timeout(endpoint):
    function_arn = create_function()
    url, queue_arn = create_queue(VisibilityTimeout="1")
    endpoint.respond = lambda e: (len(endpoint.calls) == 1, None)
    lam = boto3.client("lambda", region_name=REGION)
    esm = lam.create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=function_arn
    )
    boto3.client("sqs", region_name=REGION).send_message(QueueUrl=url, MessageBody="x")

    wait_for(lambda: len(endpoint.calls) == 1)
    assert lam.get_event_source_mapping(UUID=esm["UUID"])["LastProcessingResult"] == (
        "PROBLEM: Function call failed"
    )
    time.sleep(0.5)
    assert len(endpoint.calls) == 1
    assert queue_counts(url) == (0, 1)
    wait_for(lambda: len(endpoint.calls) == 2)
    wait_for(lambda: queue_counts(url) == (0, 0))


@mock_aws
def test_disabled_and_deleted_mappings_do_not_deliver(endpoint):
    function_arn = create_function()
    url, queue_arn = create_queue()
    lam = boto3.client("lambda", region_name=REGION)
    esm = lam.create_event_source_mapping(
        EventSourceArn=queue_arn, FunctionName=function_arn, Enabled=False
    )
    sqs = boto3.client("sqs", region_name=REGION)
    sqs.send_message(QueueUrl=url, MessageBody="first")
    time.sleep(0.5)
    assert endpoint.calls == []
    assert queue_counts(url) == (1, 0)

    lam.update_event_source_mapping(UUID=esm["UUID"], Enabled=True)
    wait_for(lambda: [r["body"] for r in endpoint.records()] == ["first"])
    wait_for(lambda: queue_counts(url) == (0, 0))

    lam.delete_event_source_mapping(UUID=esm["UUID"])
    sqs.send_message(QueueUrl=url, MessageBody="second")
    time.sleep(0.5)
    assert [r["body"] for r in endpoint.records()] == ["first"]
    assert queue_counts(url) == (1, 0)


def put_kinesis(name: str, *payloads: Any) -> None:
    kinesis = boto3.client("kinesis", region_name=REGION)
    for payload in payloads:
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        kinesis.put_record(StreamName=name, Data=data, PartitionKey="k")


def create_stream(name: str = "s") -> str:
    kinesis = boto3.client("kinesis", region_name=REGION)
    kinesis.create_stream(StreamName=name, ShardCount=1)
    return kinesis.describe_stream(StreamName=name)["StreamDescription"]["StreamARN"]


@mock_aws
def test_kinesis_delivers_from_trim_horizon_and_filters_on_json_data(endpoint):
    function_arn = create_function()
    stream_arn = create_stream()
    put_kinesis("s", {"order": {"type": "buy"}}, {"order": {"type": "sell"}}, b"raw")
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_arn,
        StartingPosition="TRIM_HORIZON",
        FilterCriteria={
            "Filters": [{"Pattern": json.dumps({"data": {"order": {"type": ["buy"]}}})}]
        },
    )

    (record,) = wait_for(endpoint.records)
    assert record["eventSource"] == "aws:kinesis"
    assert record["eventSourceARN"] == stream_arn
    assert record["eventID"].startswith("shardId-000000000000:")
    assert record["kinesis"]["partitionKey"] == "k"
    assert json.loads(base64.b64decode(record["kinesis"]["data"])) == {
        "order": {"type": "buy"}
    }
    time.sleep(0.3)
    assert len(endpoint.calls) == 1


@mock_aws
def test_kinesis_latest_skips_earlier_records(endpoint):
    function_arn = create_function()
    stream_arn = create_stream()
    put_kinesis("s", {"n": 1})
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=stream_arn, FunctionName=function_arn, StartingPosition="LATEST"
    )
    put_kinesis("s", {"n": 2})

    (record,) = wait_for(endpoint.records)
    assert json.loads(base64.b64decode(record["kinesis"]["data"])) == {"n": 2}


@mock_aws
def test_kinesis_partial_failure_resumes_from_the_lowest_failed_record(endpoint):
    function_arn = create_function()
    stream_arn = create_stream()
    put_kinesis("s", {"n": 1}, {"n": 2}, {"n": 3})

    def respond(event):
        records = event["Records"]
        if len(endpoint.calls) == 1:
            return False, {
                "batchItemFailures": [
                    {"itemIdentifier": records[1]["kinesis"]["sequenceNumber"]}
                ]
            }
        return False, {"batchItemFailures": []}

    endpoint.respond = respond
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_arn,
        StartingPosition="TRIM_HORIZON",
        FunctionResponseTypes=["ReportBatchItemFailures"],
    )

    wait_for(lambda: len(endpoint.calls) == 2)

    def numbers(call):
        return [
            json.loads(base64.b64decode(r["kinesis"]["data"]))["n"]
            for r in call["event"]["Records"]
        ]

    assert numbers(endpoint.calls[0]) == [1, 2, 3]
    assert numbers(endpoint.calls[1]) == [2, 3]
    put_kinesis("s", {"n": 4})
    wait_for(lambda: len(endpoint.calls) == 3)
    assert numbers(endpoint.calls[2]) == [4]


@mock_aws
def test_kinesis_exhausted_retries_go_to_the_on_failure_destination(endpoint):
    function_arn = create_function()
    stream_arn = create_stream()
    dlq_url, dlq_arn = create_queue("dlq")
    endpoint.respond = lambda e: (True, {"errorMessage": "boom"})
    lam = boto3.client("lambda", region_name=REGION)
    lam.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_arn,
        StartingPosition="TRIM_HORIZON",
        MaximumRetryAttempts=1,
        DestinationConfig={"OnFailure": {"Destination": dlq_arn}},
    )
    put_kinesis("s", {"n": 1})

    sqs = boto3.client("sqs", region_name=REGION)
    (message,) = wait_for(
        lambda: sqs.receive_message(QueueUrl=dlq_url).get("Messages", [])
    )
    body = json.loads(message["Body"])
    assert body["requestContext"]["condition"] == "RetryAttemptsExhausted"
    assert body["requestContext"]["approximateInvokeCount"] == 2
    assert body["KinesisBatchInfo"]["batchSize"] == 1
    assert body["KinesisBatchInfo"]["streamArn"] == stream_arn
    assert len(endpoint.calls) == 2


@mock_aws
def test_dynamodb_stream_records_are_delivered_and_filtered(endpoint):
    function_arn = create_function()
    ddb = boto3.client("dynamodb", region_name=REGION)
    stream_arn = ddb.create_table(
        TableName="t",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )["TableDescription"]["LatestStreamArn"]
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_arn,
        StartingPosition="LATEST",
        BatchSize=10,
        FilterCriteria={
            "Filters": [{"Pattern": json.dumps({"eventName": ["MODIFY"]})}]
        },
    )
    ddb.put_item(TableName="t", Item={"id": {"S": "a"}, "v": {"N": "1"}})
    ddb.put_item(TableName="t", Item={"id": {"S": "a"}, "v": {"N": "2"}})

    (record,) = wait_for(endpoint.records)
    assert record["eventName"] == "MODIFY"
    assert record["eventSource"] == "aws:dynamodb"
    assert record["eventSourceARN"] == stream_arn
    assert record["dynamodb"]["NewImage"]["v"] == {"N": "2"}
    assert record["dynamodb"]["OldImage"]["v"] == {"N": "1"}
    assert isinstance(record["dynamodb"]["ApproximateCreationDateTime"], float)


@mock_aws
def test_dynamodb_stream_retries_a_failed_batch(endpoint):
    function_arn = create_function()
    ddb = boto3.client("dynamodb", region_name=REGION)
    stream_arn = ddb.create_table(
        TableName="t",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "KEYS_ONLY"},
    )["TableDescription"]["LatestStreamArn"]
    endpoint.respond = lambda e: (len(endpoint.calls) < 3, None)
    boto3.client("lambda", region_name=REGION).create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_arn,
        StartingPosition="TRIM_HORIZON",
    )
    ddb.put_item(TableName="t", Item={"id": {"S": "a"}})

    wait_for(lambda: len(endpoint.calls) == 3)
    ids = {c["event"]["Records"][0]["eventID"] for c in endpoint.calls}
    assert len(ids) == 1
    ddb.put_item(TableName="t", Item={"id": {"S": "b"}})
    wait_for(lambda: len(endpoint.calls) == 4)
    assert endpoint.calls[3]["event"]["Records"][0]["dynamodb"]["Keys"] == {
        "id": {"S": "b"}
    }
