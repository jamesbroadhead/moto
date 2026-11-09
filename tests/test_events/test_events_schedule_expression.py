import json
from datetime import datetime, timedelta, timezone
from unittest import SkipTest

import boto3

from moto import mock_aws, settings
from moto.scheduler.runner import ScheduleRunner

REGION = "us-east-1"


def queue() -> tuple[object, str, str]:
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName="targets")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    return sqs, url, arn


def bodies(sqs: object, url: str) -> list[dict]:
    messages = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)  # type: ignore[attr-defined]
    return [json.loads(m["Body"]) for m in messages.get("Messages", [])]


@mock_aws
def test_scheduled_rule_sends_scheduled_events() -> None:
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("Runs the schedule runner in this process")
    sqs, url, arn = queue()
    events = boto3.client("events", region_name=REGION)
    rule_arn = events.put_rule(Name="scan", ScheduleExpression="rate(1 minute)")[
        "RuleArn"
    ]
    events.put_targets(
        Rule="scan",
        Targets=[
            {"Id": "event", "Arn": arn},
            {"Id": "constant", "Arn": arn, "Input": '{"scan": true}'},
            {"Id": "path", "Arn": arn, "InputPath": "$.resources"},
        ],
    )
    events.put_rule(
        Name="paused", ScheduleExpression="rate(1 minute)", State="DISABLED"
    )
    events.put_targets(Rule="paused", Targets=[{"Id": "q", "Arn": arn}])
    runner = ScheduleRunner()
    now = datetime.now(timezone.utc)

    runner.run_due(now)
    assert bodies(sqs, url) == []
    runner.run_due(now + timedelta(seconds=61))

    received = bodies(sqs, url)
    assert len(received) == 3
    assert {"scan": True} in received
    assert [rule_arn] in received
    event = next(b for b in received if isinstance(b, dict) and "source" in b)
    assert event["source"] == "aws.events"
    assert event["detail-type"] == "Scheduled Event"
    assert event["resources"] == [rule_arn]
    assert event["detail"] == {}


@mock_aws
def test_put_events_skip_disabled_and_schedule_only_rules() -> None:
    sqs, url, arn = queue()
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="scheduled", ScheduleExpression="rate(5 minutes)")
    events.put_rule(
        Name="disabled", EventPattern='{"source": ["app"]}', State="DISABLED"
    )
    events.put_rule(Name="enabled", EventPattern='{"source": ["app"]}')
    for rule in ["scheduled", "disabled", "enabled"]:
        events.put_targets(
            Rule=rule, Targets=[{"Id": "q", "Arn": arn, "Input": f'"{rule}"'}]
        )

    events.put_events(
        Entries=[{"Source": "app", "DetailType": "d", "Detail": '{"a": 1}'}]
    )

    assert bodies(sqs, url) == ["enabled"]
