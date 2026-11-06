import json
from contextlib import ExitStack
from unittest.mock import PropertyMock, patch

import boto3
import pytest

from moto import mock_aws, settings
from moto.sqs.models import Queue

COUNTERS = [
    "approximate_number_of_messages",
    "approximate_number_of_messages_delayed",
    "approximate_number_of_messages_not_visible",
]

pytestmark = pytest.mark.skipif(
    settings.TEST_SERVER_MODE, reason="Patches queue properties in the local backend"
)


@mock_aws
@pytest.mark.parametrize("fifo", [False, True])
def test_static_attributes_do_not_count_messages(fifo):
    sqs = boto3.client("sqs", region_name="us-east-1")
    attributes = {"KmsMasterKeyId": "alias/benchmark"}
    if fifo:
        attributes.update(FifoQueue="true", ContentBasedDeduplication="true")
    url = sqs.create_queue(
        QueueName="benchmark.fifo" if fifo else "benchmark", Attributes=attributes
    )["QueueUrl"]
    full = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
    names = [name for name in full if not name.startswith("ApproximateNumberOf")]
    with patch.object(Queue, COUNTERS[0], new_callable=PropertyMock) as counter:
        counter.side_effect = AssertionError("Static attributes must not scan messages")
        selected = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=names)[
            "Attributes"
        ]
    assert selected == {name: full[name] for name in names}


@mock_aws
@pytest.mark.parametrize(
    "requested, expected",
    [
        ("ApproximateNumberOfMessages", "1"),
        ("ApproximateNumberOfMessagesDelayed", "1"),
        ("ApproximateNumberOfMessagesNotVisible", "0"),
    ],
)
def test_only_requested_message_counter_is_evaluated(requested, expected):
    sqs = boto3.client("sqs", region_name="us-east-1")
    url = sqs.create_queue(QueueName="benchmark")["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="visible")
    sqs.send_message(QueueUrl=url, MessageBody="delayed", DelaySeconds=900)
    with ExitStack() as patches:
        for name, attribute in zip(
            COUNTERS,
            [
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesDelayed",
                "ApproximateNumberOfMessagesNotVisible",
            ],
            strict=True,
        ):
            if attribute != requested:
                counter = patches.enter_context(
                    patch.object(Queue, name, new_callable=PropertyMock)
                )
                counter.side_effect = AssertionError(
                    "An unrequested counter was evaluated"
                )
        response = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[requested])
    assert response["Attributes"] == {requested: expected}


@mock_aws
def test_eventbridge_fifo_delivery_does_not_count_messages():
    sqs = boto3.client("sqs", region_name="us-east-1")
    events = boto3.client("events", region_name="us-east-1")
    url = sqs.create_queue(
        QueueName="target.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    events.put_rule(Name="deliver", EventPattern=json.dumps({"source": ["test"]}))
    events.put_targets(
        Rule="deliver",
        Targets=[
            {
                "Id": "queue",
                "Arn": arn,
                "SqsParameters": {"MessageGroupId": "group"},
            }
        ],
    )
    with patch.object(Queue, COUNTERS[0], new_callable=PropertyMock) as counter:
        counter.side_effect = AssertionError(
            "Event delivery must not count the backlog"
        )
        response = events.put_events(
            Entries=[
                {
                    "Source": "test",
                    "DetailType": "test",
                    "Detail": json.dumps({"value": 1}),
                }
            ]
        )
        assert response["FailedEntryCount"] == 0
    assert sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"] == {"ApproximateNumberOfMessages": "1"}
