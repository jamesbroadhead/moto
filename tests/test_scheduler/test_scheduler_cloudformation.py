import json

import boto3

from moto import mock_aws

TEMPLATE = {
    "Resources": {
        "Group": {
            "Type": "AWS::Scheduler::ScheduleGroup",
            "Properties": {"Name": "jobs"},
        },
        "Tick": {
            "Type": "AWS::Scheduler::Schedule",
            "Properties": {
                "GroupName": {"Ref": "Group"},
                "ScheduleExpression": "rate(5 minutes)",
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "Target": {
                    "Arn": "arn:aws:lambda:us-east-1:123456789012:function:f",
                    "RoleArn": "arn:aws:iam::123456789012:role/r",
                },
            },
        },
    },
    "Outputs": {
        "GroupArn": {"Value": {"Fn::GetAtt": ["Group", "Arn"]}},
        "ScheduleArn": {"Value": {"Fn::GetAtt": ["Tick", "Arn"]}},
    },
}


@mock_aws
def test_stack_creates_schedule_group_and_schedule():
    cfn = boto3.client("cloudformation", region_name="us-east-1")
    cfn.create_stack(StackName="s", TemplateBody=json.dumps(TEMPLATE))
    outputs = {
        o["OutputKey"]: o["OutputValue"]
        for o in cfn.describe_stacks(StackName="s")["Stacks"][0]["Outputs"]
    }
    scheduler = boto3.client("scheduler", region_name="us-east-1")

    group = scheduler.get_schedule_group(Name="jobs")
    assert outputs["GroupArn"] == group["Arn"]
    (schedule,) = scheduler.list_schedules(GroupName="jobs")["Schedules"]
    assert outputs["ScheduleArn"] == schedule["Arn"]
    assert schedule["Arn"].startswith(
        "arn:aws:scheduler:us-east-1:123456789012:schedule/jobs/"
    )

    # Schedules can be added to the stack's group through the API
    scheduler.create_schedule(
        Name="extra",
        GroupName="jobs",
        ScheduleExpression="rate(1 hour)",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target=TEMPLATE["Resources"]["Tick"]["Properties"]["Target"],
    )

    cfn.delete_stack(StackName="s")
    assert "jobs" not in [
        g["Name"] for g in scheduler.list_schedule_groups()["ScheduleGroups"]
    ]
