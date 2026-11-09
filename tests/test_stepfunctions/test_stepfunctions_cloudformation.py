import json
import time

import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws


@mock_aws
def test_state_machine_cloudformation():
    sf = boto3.client("stepfunctions", region_name="us-east-1")
    cf = boto3.resource("cloudformation", region_name="us-east-1")
    definition = (
        '{"StartAt": "HelloWorld", '
        '"States": {"HelloWorld": {"Type": "Task", '
        '"Resource": "arn:aws:lambda:us-east-1:111122223333;:function:HelloFunction", '
        '"End": true}}}'
    )
    role_arn = (
        "arn:aws:iam::111122223333:role/service-role/StatesExecutionRole-us-east-1;"
    )
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "An example template for a Step Functions state machine.",
        "Resources": {
            "MyStateMachine": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {
                    "StateMachineName": "HelloWorld-StateMachine",
                    "StateMachineType": "STANDARD",
                    "DefinitionString": definition,
                    "RoleArn": role_arn,
                    "TracingConfiguration": {"Enabled": False},
                    "EncryptionConfiguration": {"Type": "AWS_OWNED_KEY"},
                    "LoggingConfiguration": {"Level": "OFF"},
                    "Tags": [
                        {"Key": "key1", "Value": "value1"},
                        {"Key": "key2", "Value": "value2"},
                    ],
                },
            }
        },
        "Outputs": {
            "StateMachineArn": {"Value": {"Ref": "MyStateMachine"}},
            "StateMachineName": {"Value": {"Fn::GetAtt": ["MyStateMachine", "Name"]}},
        },
    }
    cf.create_stack(StackName="test_stack", TemplateBody=json.dumps(template))
    outputs_list = cf.Stack("test_stack").outputs
    output = {item["OutputKey"]: item["OutputValue"] for item in outputs_list}
    state_machine = sf.describe_state_machine(stateMachineArn=output["StateMachineArn"])
    assert state_machine["stateMachineArn"] == output["StateMachineArn"]
    assert state_machine["name"] == output["StateMachineName"]
    assert state_machine["roleArn"] == role_arn
    assert state_machine["definition"] == definition
    assert state_machine["tracingConfiguration"] == {"enabled": False}
    assert state_machine["encryptionConfiguration"]["type"] == "AWS_OWNED_KEY"
    assert state_machine["loggingConfiguration"]["level"] == "OFF"
    tags = sf.list_tags_for_resource(resourceArn=output["StateMachineArn"]).get("tags")
    for i, tag in enumerate(tags, 1):
        assert tag["key"] == f"key{i}"
        assert tag["value"] == f"value{i}"

    cf.Stack("test_stack").delete()
    with pytest.raises(ClientError) as ex:
        sf.describe_state_machine(stateMachineArn=output["StateMachineArn"])
    assert ex.value.response["Error"]["Code"] == "StateMachineDoesNotExist"
    assert "Does Not Exist" in ex.value.response["Error"]["Message"]


@mock_aws
def test_state_machine_cloudformation_update_with_replacement():
    sf = boto3.client("stepfunctions", region_name="us-east-1")
    cf = boto3.resource("cloudformation", region_name="us-east-1")
    definition = (
        '{"StartAt": "HelloWorld", '
        '"States": {"HelloWorld": {"Type": "Task", '
        '"Resource": "arn:aws:lambda:us-east-1:111122223333;:function:HelloFunction", '
        '"End": true}}}'
    )
    role_arn = (
        "arn:aws:iam::111122223333:role/service-role/StatesExecutionRole-us-east-1"
    )
    properties = {
        "StateMachineName": "HelloWorld-StateMachine",
        "DefinitionString": definition,
        "RoleArn": role_arn,
        "Tags": [
            {"Key": "key1", "Value": "value1"},
            {"Key": "key2", "Value": "value2"},
        ],
    }
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "An example template for a Step Functions state machine.",
        "Resources": {
            "MyStateMachine": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {},
            }
        },
        "Outputs": {
            "StateMachineArn": {"Value": {"Ref": "MyStateMachine"}},
            "StateMachineName": {"Value": {"Fn::GetAtt": ["MyStateMachine", "Name"]}},
        },
    }
    template["Resources"]["MyStateMachine"]["Properties"] = properties
    cf.create_stack(StackName="test_stack", TemplateBody=json.dumps(template))
    outputs_list = cf.Stack("test_stack").outputs
    output = {item["OutputKey"]: item["OutputValue"] for item in outputs_list}
    state_machine = sf.describe_state_machine(stateMachineArn=output["StateMachineArn"])
    original_machine_arn = state_machine["stateMachineArn"]
    original_creation_date = state_machine["creationDate"]

    # Update State Machine, with replacement.
    updated_role = role_arn + "-updated"
    updated_definition = definition.replace("HelloWorld", "HelloWorld2")
    updated_properties = {
        "StateMachineName": "New-StateMachine-Name",
        "DefinitionString": updated_definition,
        "RoleArn": updated_role,
        "Tags": [
            {"Key": "key3", "Value": "value3"},
            {"Key": "key1", "Value": "updated_value"},
        ],
    }
    template["Resources"]["MyStateMachine"]["Properties"] = updated_properties
    cf.Stack("test_stack").update(TemplateBody=json.dumps(template))
    outputs_list = cf.Stack("test_stack").outputs
    output = {item["OutputKey"]: item["OutputValue"] for item in outputs_list}
    state_machine = sf.describe_state_machine(stateMachineArn=output["StateMachineArn"])
    assert state_machine["stateMachineArn"] != original_machine_arn
    assert state_machine["name"] == "New-StateMachine-Name"
    assert state_machine["creationDate"] > original_creation_date
    assert state_machine["roleArn"] == updated_role
    assert state_machine["definition"] == updated_definition
    tags = sf.list_tags_for_resource(resourceArn=output["StateMachineArn"]).get("tags")
    assert len(tags) == 3
    for tag in tags:
        if tag["key"] == "key1":
            assert tag["value"] == "updated_value"

    with pytest.raises(ClientError) as ex:
        sf.describe_state_machine(stateMachineArn=original_machine_arn)
    assert ex.value.response["Error"]["Code"] == "StateMachineDoesNotExist"
    assert "State Machine Does Not Exist" in ex.value.response["Error"]["Message"]


@mock_aws
def test_state_machine_cloudformation_update_with_no_interruption():
    sf = boto3.client("stepfunctions", region_name="us-east-1")
    cf = boto3.resource("cloudformation", region_name="us-east-1")
    definition = (
        '{"StartAt": "HelloWorld", '
        '"States": {"HelloWorld": {"Type": '
        '"Task", "Resource": "arn:aws:lambda:us-east-1:111122223333;:function:HelloFunction", '
        '"End": true}}}'
    )
    role_arn = (
        "arn:aws:iam::111122223333:role/service-role/StatesExecutionRole-us-east-1"
    )
    properties = {
        "StateMachineName": "HelloWorld-StateMachine",
        "DefinitionString": definition,
        "RoleArn": role_arn,
        "Tags": [
            {"Key": "key1", "Value": "value1"},
            {"Key": "key2", "Value": "value2"},
        ],
    }
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "An example template for a Step Functions state machine.",
        "Resources": {
            "MyStateMachine": {
                "Type": "AWS::StepFunctions::StateMachine",
                "Properties": {},
            }
        },
        "Outputs": {
            "StateMachineArn": {"Value": {"Ref": "MyStateMachine"}},
            "StateMachineName": {"Value": {"Fn::GetAtt": ["MyStateMachine", "Name"]}},
        },
    }
    template["Resources"]["MyStateMachine"]["Properties"] = properties
    cf.create_stack(StackName="test_stack", TemplateBody=json.dumps(template))
    outputs_list = cf.Stack("test_stack").outputs
    output = {item["OutputKey"]: item["OutputValue"] for item in outputs_list}
    state_machine = sf.describe_state_machine(stateMachineArn=output["StateMachineArn"])
    machine_arn = state_machine["stateMachineArn"]
    creation_date = state_machine["creationDate"]

    # Update State Machine in-place, no replacement.
    updated_role = role_arn + "-updated"
    updated_definition = definition.replace("HelloWorld", "HelloWorldUpdated")
    updated_properties = {
        "DefinitionString": updated_definition,
        "RoleArn": updated_role,
        "Tags": [
            {"Key": "key3", "Value": "value3"},
            {"Key": "key1", "Value": "updated_value"},
        ],
    }
    template["Resources"]["MyStateMachine"]["Properties"] = updated_properties
    cf.Stack("test_stack").update(TemplateBody=json.dumps(template))

    state_machine = sf.describe_state_machine(stateMachineArn=machine_arn)
    assert state_machine["name"] == "HelloWorld-StateMachine"
    assert state_machine["creationDate"] == creation_date
    assert state_machine["roleArn"] == updated_role
    assert state_machine["definition"] == updated_definition
    tags = sf.list_tags_for_resource(resourceArn=machine_arn).get("tags")
    assert len(tags) == 3
    for tag in tags:
        if tag["key"] == "key1":
            assert tag["value"] == "updated_value"


@mock_aws
def test_state_machine_definition_object_with_substitutions_runs_when_execution_is_on():
    from moto.core.config import default_user_config

    default_user_config["stepfunctions"]["execute_state_machine"] = True
    try:
        sf = boto3.client("stepfunctions", region_name="us-east-1")
        cf = boto3.client("cloudformation", region_name="us-east-1")
        template = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "Machine": {
                    "Type": "AWS::StepFunctions::StateMachine",
                    "Properties": {
                        "StateMachineName": "subst",
                        "RoleArn": "arn:aws:iam::123456789012:role/sfn",
                        "Definition": {
                            "StartAt": "Pass",
                            "States": {
                                "Pass": {
                                    "Type": "Pass",
                                    "Result": "${Value}",
                                    "End": True,
                                }
                            },
                        },
                        "DefinitionSubstitutions": {"Value": "substituted"},
                    },
                }
            },
        }
        cf.create_stack(StackName="sfn", TemplateBody=json.dumps(template))
        arn = sf.list_state_machines()["stateMachines"][0]["stateMachineArn"]
        assert (
            "substituted"
            in sf.describe_state_machine(stateMachineArn=arn)["definition"]
        )
        execution_arn = sf.start_execution(stateMachineArn=arn)["executionArn"]
        for _ in range(50):
            execution = sf.describe_execution(executionArn=execution_arn)
            if execution["status"] != "RUNNING":
                break
            time.sleep(0.1)
        assert execution["status"] == "SUCCEEDED"
        assert (
            "substituted" in execution.get("output", "")
            or "substituted"
            in sf.describe_state_machine(stateMachineArn=arn)["definition"]
        )
    finally:
        default_user_config["stepfunctions"]["execute_state_machine"] = False
