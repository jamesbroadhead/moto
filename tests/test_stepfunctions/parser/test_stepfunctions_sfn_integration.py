import json
from time import sleep
from unittest import SkipTest

import boto3

from moto import settings

from . import aws_verified

ROLE = "arn:aws:iam::123456789012:role/sfn-role"


def _wait(client, execution_arn):
    for _ in range(50):
        execution = client.describe_execution(executionArn=execution_arn)
        if execution["status"] != "RUNNING":
            return execution
        sleep(0.2)
    raise AssertionError("the execution did not finish")


@aws_verified
def test_start_execution_sync_2_runs_the_child_and_returns_its_output():
    if settings.TEST_SERVER_MODE:
        raise SkipTest("Don't need to test this in ServerMode")
    client = boto3.client("stepfunctions", region_name="us-east-1")
    child = client.create_state_machine(
        name="child",
        roleArn=ROLE,
        definition=json.dumps(
            {
                "StartAt": "Echo",
                "States": {
                    "Echo": {
                        "Type": "Pass",
                        "Parameters": {"seen.$": "$.x"},
                        "End": True,
                    }
                },
            }
        ),
    )["stateMachineArn"]
    parent = client.create_state_machine(
        name="parent",
        roleArn=ROLE,
        definition=json.dumps(
            {
                "StartAt": "Child",
                "States": {
                    "Child": {
                        "Type": "Task",
                        "Resource": "arn:aws:states:::states:startExecution.sync:2",
                        "Parameters": {"StateMachineArn": child, "Input": {"x": 1}},
                        "Next": "Done",
                    },
                    "Done": {
                        "Type": "Pass",
                        "Parameters": {
                            "output.$": "$.Output",
                            "status.$": "$.Status",
                            "name.$": "$$.Execution.Name",
                        },
                        "End": True,
                    },
                },
            }
        ),
    )["stateMachineArn"]

    # Without a name, the execution gets a generated one, as on AWS
    execution_arn = client.start_execution(stateMachineArn=parent)["executionArn"]
    execution = _wait(client, execution_arn)

    assert execution["status"] == "SUCCEEDED", execution.get("cause")
    output = json.loads(execution["output"])
    assert output["output"] == {"seen": 1}
    assert output["status"] == "SUCCEEDED"
    assert output["name"] and output["name"] != "None"
    assert execution_arn.endswith(f":{output['name']}")
