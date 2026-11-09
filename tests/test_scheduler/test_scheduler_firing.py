import json
import time
from datetime import datetime, timedelta, timezone
from unittest import SkipTest
from zoneinfo import ZoneInfo

import boto3
import pytest

from moto import mock_aws, settings
from moto.core import DEFAULT_ACCOUNT_ID
from moto.scheduler.runner import Cron, ScheduleRunner

REGION = "eu-west-1"
ROLE = f"arn:aws:iam::{DEFAULT_ACCOUNT_ID}:role/scheduler"


@pytest.fixture(autouse=True)
def decorator_mode_only() -> None:
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("Runs the schedule runner in this process")


def queue() -> tuple[object, str, str]:
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(QueueName="fired")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    return sqs, url, arn


def bodies(sqs: object, url: str) -> list[str]:
    messages = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)  # type: ignore[attr-defined]
    return [m["Body"] for m in messages.get("Messages", [])]


def create(name: str, expression: str, target: dict, **kwargs: object) -> str:
    client = boto3.client("scheduler", region_name=REGION)
    return client.create_schedule(
        Name=name,
        ScheduleExpression=expression,
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={"RoleArn": ROLE, **target},
        **kwargs,
    )["ScheduleArn"]


@pytest.mark.parametrize(
    "expression,after,expected",
    [
        ("0 12 * * ? *", "2026-11-09T12:00", "2026-11-10T12:00"),
        ("0/15 * ? * MON-FRI *", "2026-11-13T23:50", "2026-11-16T00:00"),
        ("30 9 L * ? *", "2026-02-01T00:00", "2026-02-28T09:30"),
        ("0 8 ? * 6L *", "2026-11-01T00:00", "2026-11-27T08:00"),
        ("0 9 ? * 2#1 *", "2026-11-03T00:00", "2026-12-07T09:00"),
        ("0 9 15W * ? *", "2026-11-01T00:00", "2026-11-16T09:00"),
        ("0 0 1 JAN ? 2027", "2026-11-09T00:00", "2027-01-01T00:00"),
    ],
)
def test_cron_next_after(expression: str, after: str, expected: str) -> None:
    start = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
    assert Cron(expression).next_after(start) == datetime.fromisoformat(
        expected
    ).replace(tzinfo=timezone.utc)


@mock_aws
def test_rate_schedule_delivers_input_to_sqs() -> None:
    sqs, url, arn = queue()
    schedule_arn = create(
        "every-minute",
        "rate(1 minute)",
        {"Arn": arn, "Input": '{"arn": "<aws.scheduler.schedule-arn>", "n": 1}'},
    )
    runner = ScheduleRunner()
    now = datetime.now(timezone.utc)

    runner.run_due(now)
    assert bodies(sqs, url) == []

    runner.run_due(now + timedelta(seconds=61))
    assert [json.loads(b) for b in bodies(sqs, url)] == [{"arn": schedule_arn, "n": 1}]

    runner.run_due(now + timedelta(seconds=90))
    assert bodies(sqs, url) == []
    runner.run_due(now + timedelta(seconds=121))
    assert len(bodies(sqs, url)) == 1


@mock_aws
def test_disabled_schedule_is_not_fired_until_enabled() -> None:
    sqs, url, arn = queue()
    target = {"Arn": arn, "Input": "hello", "RoleArn": ROLE}
    create("off", "rate(1 minute)", {"Arn": arn, "Input": "hello"}, State="DISABLED")
    runner = ScheduleRunner()
    now = datetime.now(timezone.utc)

    runner.run_due(now)
    runner.run_due(now + timedelta(minutes=5))
    assert bodies(sqs, url) == []

    boto3.client("scheduler", region_name=REGION).update_schedule(
        Name="off",
        ScheduleExpression="rate(1 minute)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target=target,
        State="ENABLED",
    )
    runner.run_due(now + timedelta(minutes=5))
    runner.run_due(now + timedelta(minutes=6, seconds=1))
    assert bodies(sqs, url) == ["hello"]


@mock_aws
def test_at_schedule_fires_once_and_is_deleted_after_completion() -> None:
    sqs, url, arn = queue()
    when = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=10)
    local = when.astimezone(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%dT%H:%M:%S")
    create(
        "once",
        f"at({local})",
        {"Arn": arn, "Input": '{"at": "<aws.scheduler.scheduled-time>"}'},
        ScheduleExpressionTimezone="Europe/Berlin",
        ActionAfterCompletion="DELETE",
    )
    create(
        "kept", f"at({local})", {"Arn": arn}, ScheduleExpressionTimezone="Europe/Berlin"
    )
    runner = ScheduleRunner()

    runner.run_due(when - timedelta(seconds=1))
    assert bodies(sqs, url) == []
    runner.run_due(when)
    runner.run_due(when + timedelta(hours=1))
    assert sorted(bodies(sqs, url)) == sorted(
        [json.dumps({"at": when.strftime("%Y-%m-%dT%H:%M:%SZ")}), "{}"]
    )

    client = boto3.client("scheduler", region_name=REGION)
    names = [s["Name"] for s in client.list_schedules()["Schedules"]]
    assert names == ["kept"]


@mock_aws
def test_schedule_stops_at_end_date() -> None:
    sqs, url, arn = queue()
    now = datetime.now(timezone.utc)
    create(
        "short",
        "rate(1 minute)",
        {"Arn": arn, "Input": "tick"},
        EndDate=now + timedelta(seconds=90),
    )
    runner = ScheduleRunner()
    runner.run_due(now)
    for minute in range(1, 5):
        runner.run_due(now + timedelta(minutes=minute, seconds=1))
    assert bodies(sqs, url) == ["tick"]


@mock_aws
def test_eventbridge_and_universal_targets() -> None:
    sqs, url, arn = queue()
    events = boto3.client("events", region_name=REGION)
    events.put_rule(Name="from-scheduler", EventPattern='{"source": ["my.app"]}')
    events.put_targets(Rule="from-scheduler", Targets=[{"Id": "q", "Arn": arn}])
    create(
        "put-events",
        "rate(1 minute)",
        {
            "Arn": f"arn:aws:events:{REGION}:{DEFAULT_ACCOUNT_ID}:event-bus/default",
            "Input": '{"k": "v"}',
            "EventBridgeParameters": {"Source": "my.app", "DetailType": "Tick"},
        },
    )
    create(
        "universal",
        "rate(1 minute)",
        {
            "Arn": "arn:aws:scheduler:::aws-sdk:sqs:sendMessage",
            "Input": json.dumps({"QueueUrl": url, "MessageBody": "direct"}),
        },
    )
    runner = ScheduleRunner()
    now = datetime.now(timezone.utc)
    runner.run_due(now)
    runner.run_due(now + timedelta(seconds=61))

    received = bodies(sqs, url)
    assert "direct" in received
    event = json.loads(next(b for b in received if b != "direct"))
    assert event["source"] == "my.app"
    assert event["detail-type"] == "Tick"
    assert event["detail"] == {"k": "v"}


@mock_aws
def test_lambda_target_with_cron_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    from moto.awslambda.models import LambdaFunction

    invocations: list[str] = []
    monkeypatch.setattr(
        LambdaFunction,
        "invoke",
        lambda self, body, *args, **kwargs: invocations.append(body),
    )
    iam = boto3.client("iam", region_name=REGION)
    role = iam.create_role(RoleName="fn", AssumeRolePolicyDocument="{}")["Role"]
    fn = boto3.client("lambda", region_name=REGION).create_function(
        FunctionName="nightly",
        Runtime="python3.12",
        Role=role["Arn"],
        Handler="index.handler",
        Code={"ZipFile": b"x"},
    )
    create(
        "nightly",
        "cron(0 3 * * ? *)",
        {"Arn": fn["FunctionArn"], "Input": '{"job": "vacuum"}'},
        ScheduleExpressionTimezone="Europe/Berlin",
    )
    runner = ScheduleRunner()
    now = datetime.now(timezone.utc)
    runner.run_due(now)
    runner.run_due(now + timedelta(days=1, minutes=1))
    assert invocations == ['{"job": "vacuum"}']


@mock_aws
def test_background_runner_fires_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOTO_SCHEDULER_FIRE", "true")
    monkeypatch.setenv("MOTO_SCHEDULER_TICK_SECONDS", "0.1")
    sqs, url, arn = queue()
    when = datetime.now(timezone.utc) + timedelta(seconds=2)
    create(
        "soon", f"at({when.strftime('%Y-%m-%dT%H:%M:%S')})", {"Arn": arn, "Input": "x"}
    )

    deadline = time.time() + 15
    received: list[str] = []
    while not received and time.time() < deadline:
        received = bodies(sqs, url)
        time.sleep(0.2)
    assert received == ["x"]
