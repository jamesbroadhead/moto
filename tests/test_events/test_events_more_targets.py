import json

import boto3

from moto import mock_aws
from tests import DEFAULT_ACCOUNT_ID as ACCOUNT_ID

REGION = "us-east-1"


@mock_aws
def test_put_events_starts_a_state_machine_execution():
    sfn = boto3.client("stepfunctions", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    definition = {"StartAt": "Done", "States": {"Done": {"Type": "Succeed"}}}
    machine_arn = sfn.create_state_machine(
        name="job",
        definition=json.dumps(definition),
        roleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/sfn",
    )["stateMachineArn"]
    events.put_rule(Name="jobs", EventPattern=json.dumps({"source": ["app"]}))
    events.put_targets(Rule="jobs", Targets=[{"Id": "sfn", "Arn": machine_arn}])

    events.put_events(
        Entries=[{"Source": "app", "DetailType": "job", "Detail": '{"id": 1}'}]
    )

    executions = sfn.list_executions(stateMachineArn=machine_arn)["executions"]
    assert len(executions) == 1
    execution = sfn.describe_execution(executionArn=executions[0]["executionArn"])
    assert json.loads(execution["input"])["detail"] == {"id": 1}


@mock_aws
def test_put_events_delivers_to_firehose_and_keeps_going_after_a_failing_target():
    s3 = boto3.client("s3", region_name=REGION)
    firehose = boto3.client("firehose", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3.create_bucket(Bucket="history")
    stream_arn = firehose.create_delivery_stream(
        DeliveryStreamName="history",
        ExtendedS3DestinationConfiguration={
            "RoleARN": f"arn:aws:iam::{ACCOUNT_ID}:role/firehose",
            "BucketARN": "arn:aws:s3:::history",
        },
    )["DeliveryStreamARN"]
    queue_url = sqs.create_queue(QueueName="after")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    events.put_rule(Name="history", EventPattern=json.dumps({"source": ["app"]}))
    events.put_targets(
        Rule="history",
        Targets=[
            {
                "Id": "missing",
                "Arn": f"arn:aws:firehose:{REGION}:{ACCOUNT_ID}:deliverystream/missing",
            },
            {"Id": "firehose", "Arn": stream_arn},
            {"Id": "queue", "Arn": queue_arn},
        ],
    )

    events.put_events(Entries=[{"Source": "app", "DetailType": "d", "Detail": "{}"}])

    objects = s3.list_objects_v2(Bucket="history").get("Contents", [])
    assert len(objects) == 1
    assert sqs.receive_message(QueueUrl=queue_url).get("Messages")
