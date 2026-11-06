import boto3
import pytest

from moto import mock_aws


@mock_aws
def test_composite_alarm_lifecycle_and_child_state_changes():
    client = boto3.client("cloudwatch", region_name="us-east-1")
    client.put_metric_alarm(
        AlarmName="child",
        Namespace="test",
        MetricName="errors",
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=1,
        Period=60,
        Threshold=1,
        Statistic="Sum",
    )
    child_arn = client.describe_alarms()["MetricAlarms"][0]["AlarmArn"]
    rule = f'ALARM("{child_arn}") AND NOT INSUFFICIENT_DATA("child")'
    client.put_composite_alarm(
        AlarmName="parent", AlarmRule=rule, Tags=[{"Key": "owner", "Value": "original"}]
    )
    client.put_composite_alarm(
        AlarmName="grandparent", AlarmRule='ALARM("parent") OR FALSE'
    )
    assert (
        "CompositeAlarms" not in client.describe_alarms()
        or not client.describe_alarms()["CompositeAlarms"]
    )
    composite = client.describe_alarms(AlarmTypes=["CompositeAlarm"])["CompositeAlarms"]
    assert {a["AlarmName"]: a["StateValue"] for a in composite} == {
        "parent": "OK",
        "grandparent": "OK",
    }
    assert composite[0]["AlarmRule"] == rule
    client.set_alarm_state(AlarmName="child", StateValue="ALARM", StateReason="test")
    composite = client.describe_alarms(AlarmTypes=["CompositeAlarm"])["CompositeAlarms"]
    assert all(a["StateValue"] == "ALARM" for a in composite)
    parent = next(a for a in composite if a["AlarmName"] == "parent")
    client.put_composite_alarm(
        AlarmName="parent",
        AlarmRule="FALSE",
        ActionsEnabled=False,
        Tags=[{"Key": "owner", "Value": "ignored"}],
    )
    updated = client.describe_alarms(
        AlarmNames=["parent"], AlarmTypes=["CompositeAlarm"]
    )["CompositeAlarms"][0]
    assert updated["StateValue"] == "ALARM"
    assert updated["StateUpdatedTimestamp"] == parent["StateUpdatedTimestamp"]
    assert updated["ActionsEnabled"] is False
    assert client.list_tags_for_resource(ResourceARN=parent["AlarmArn"])["Tags"] == [
        {"Key": "owner", "Value": "original"}
    ]
    client.delete_alarms(AlarmNames=["grandparent"])
    assert [
        a["AlarmName"]
        for a in client.describe_alarms(AlarmTypes=["CompositeAlarm"])[
            "CompositeAlarms"
        ]
    ] == ["parent"]


@mock_aws
def test_invalid_composite_rule_does_not_replace_existing_alarm():
    client = boto3.client("cloudwatch", region_name="us-east-1")
    client.put_composite_alarm(AlarmName="parent", AlarmRule="TRUE")
    with pytest.raises(client.exceptions.InvalidParameterValueException):
        client.put_composite_alarm(AlarmName="parent", AlarmRule='ALARM("child") AND')
    assert (
        client.describe_alarms(AlarmTypes=["CompositeAlarm"])["CompositeAlarms"][0][
            "AlarmRule"
        ]
        == "TRUE"
    )


@mock_aws
def test_composite_alarm_cycle_stops_evaluation():
    client = boto3.client("cloudwatch", region_name="us-east-1")
    client.put_composite_alarm(AlarmName="first", AlarmRule='NOT ALARM("second")')
    client.put_composite_alarm(AlarmName="second", AlarmRule='NOT ALARM("first")')
    before = {
        a["AlarmName"]: a["StateValue"]
        for a in client.describe_alarms(AlarmTypes=["CompositeAlarm"])[
            "CompositeAlarms"
        ]
    }
    client.set_alarm_state(AlarmName="first", StateValue="OK", StateReason="test")
    after = {
        a["AlarmName"]: a["StateValue"]
        for a in client.describe_alarms(AlarmTypes=["CompositeAlarm"])[
            "CompositeAlarms"
        ]
    }
    assert after == {**before, "first": "OK"}
    with pytest.raises(client.exceptions.ResourceConflict) as exc:
        client.delete_alarms(AlarmNames=["first"])
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    client.put_composite_alarm(AlarmName="second", AlarmRule="FALSE")
    client.delete_alarms(AlarmNames=["first"])
    assert [
        a["AlarmName"]
        for a in client.describe_alarms(AlarmTypes=["CompositeAlarm"])[
            "CompositeAlarms"
        ]
    ] == ["second"]


@pytest.mark.parametrize(
    "rule,state",
    [
        ("TRUE OR FALSE AND FALSE", "ALARM"),
        ("(TRUE OR FALSE) AND FALSE", "OK"),
        ("NOT (TRUE AND FALSE)", "ALARM"),
        ('OK("missing")', "OK"),
    ],
)
@mock_aws
def test_composite_alarm_rule_precedence(rule, state):
    client = boto3.client("cloudwatch", region_name="us-east-1")
    client.put_composite_alarm(AlarmName="parent", AlarmRule=rule)
    assert (
        client.describe_alarms(AlarmTypes=["CompositeAlarm"])["CompositeAlarms"][0][
            "StateValue"
        ]
        == state
    )
