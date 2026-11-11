import json

import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws
from moto.cloudformation.transforms import process_transforms

REGION = "us-east-1"


def _create(template, parameters=None, name="stack"):
    cf = boto3.client("cloudformation", region_name=REGION)
    cf.create_stack(
        StackName=name,
        TemplateBody=json.dumps(template),
        Parameters=[
            {"ParameterKey": k, "ParameterValue": v}
            for k, v in (parameters or {}).items()
        ],
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND"],
    )
    return cf


def _resources(cf, name="stack"):
    return {
        r["LogicalResourceId"]: r
        for r in cf.list_stack_resources(StackName=name)["StackResourceSummaries"]
    }


@mock_aws
def test_serverless_function_creates_the_function_its_role_and_alias():
    template = {
        "Transform": "AWS::Serverless-2016-10-31",
        "Parameters": {"Alias": {"Type": "String", "Default": "live"}},
        "Resources": {
            "Fn": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "FunctionName": "sam-fn",
                    "InlineCode": "def handler(event, context): pass",
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    "AutoPublishAlias": {"Ref": "Alias"},
                    "Policies": ["AWSLambdaBasicExecutionRole"],
                },
            }
        },
        "Outputs": {"Arn": {"Value": {"Fn::GetAtt": ["Fn", "Arn"]}}},
    }

    cf = _create(template)

    resources = _resources(cf)
    assert resources["Fn"]["ResourceType"] == "AWS::Lambda::Function"
    assert resources["FnRole"]["ResourceType"] == "AWS::IAM::Role"
    assert resources["FnAliaslive"]["ResourceType"] == "AWS::Lambda::Alias"
    lam = boto3.client("lambda", region_name=REGION)
    assert lam.get_alias(FunctionName="sam-fn", Name="live")["Name"] == "live"
    # GetTemplate returns the template as submitted
    original = cf.get_template(StackName="stack")["TemplateBody"]
    assert original["Resources"]["Fn"]["Type"] == "AWS::Serverless::Function"


@mock_aws
def test_serverless_transform_rejects_an_invalid_template():
    template = {
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "Fn": {
                "Type": "AWS::Serverless::Function",
                "Properties": {"Handler": "index.handler", "Runtime": "python3.12"},
            }
        },
    }

    with pytest.raises(ClientError) as exc:
        _create(template)

    assert exc.value.response["Error"]["Code"] == "ValidationError"
    assert "Transform AWS::Serverless-2016-10-31 failed with" in str(exc.value)


@mock_aws
def test_language_extensions_resolve_find_in_map_defaults_and_conditions_before_sam():
    template = {
        "Transform": ["AWS::LanguageExtensions", "AWS::Serverless-2016-10-31"],
        "Parameters": {"Env": {"Type": "String"}},
        "Conditions": {
            "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
            "IsNotProd": {"Fn::Not": [{"Condition": "IsProd"}]},
        },
        "Mappings": {"Sizes": {"prod": {"Memory": 1024}}},
        "Resources": {
            "Fn": {
                "Type": "AWS::Serverless::Function",
                "DeletionPolicy": {"Fn::If": ["IsProd", "Retain", "Delete"]},
                "Properties": {
                    "FunctionName": "le-fn",
                    "InlineCode": "def handler(event, context): pass",
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    # SAM needs a plain alias name: the transform resolves Fn::If first
                    "AutoPublishAlias": {"Fn::If": ["IsProd", "prod", "dev"]},
                    "MemorySize": {
                        "Fn::FindInMap": [
                            "Sizes",
                            {"Ref": "Env"},
                            "Memory",
                            {"DefaultValue": 256},
                        ]
                    },
                    "Timeout": {"Fn::Length": ["a", "b", "c"]},
                    "Environment": {
                        "Variables": {
                            "PROD_ONLY": {
                                "Fn::If": ["IsProd", "yes", {"Ref": "AWS::NoValue"}]
                            },
                        }
                    },
                },
            },
            "Topic": {"Type": "AWS::SNS::Topic", "Condition": "IsProd"},
            "Queue": {"Type": "AWS::SQS::Queue", "Condition": "IsNotProd"},
        },
    }

    cf = _create(template, {"Env": "dev"})

    resources = _resources(cf)
    assert "FnAliasdev" in resources
    assert "Topic" not in resources and "Queue" in resources
    # A resource whose condition is false is not part of the stack
    assert "" not in resources
    config = boto3.client("lambda", region_name=REGION).get_function_configuration(
        FunctionName="le-fn"
    )
    assert config["MemorySize"] == 256
    assert config["Timeout"] == 3
    assert "Environment" not in config or not config["Environment"].get("Variables")


@mock_aws
def test_find_in_map_default_value_at_create_time():
    template = {
        "Transform": "AWS::LanguageExtensions",
        "Mappings": {"Names": {"a": {"Queue": "from-map"}}},
        "Resources": {
            "Bucket": {"Type": "AWS::S3::Bucket"},
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    # The key depends on a resource, so CloudFormation looks it up at create time
                    "QueueName": {
                        "Fn::FindInMap": [
                            "Names",
                            {"Ref": "Bucket"},
                            "Queue",
                            {"DefaultValue": "from-default"},
                        ]
                    }
                },
            },
        },
    }

    _create(template)

    urls = boto3.client("sqs", region_name=REGION).list_queues()["QueueUrls"]
    assert [u.rsplit("/", 1)[1] for u in urls] == ["from-default"]


@mock_aws
def test_include_inlines_a_snippet_from_s3_anywhere_in_the_template():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="snippets")
    s3.put_object(
        Bucket="snippets",
        Key="queue.yaml",
        Body="QueueName: !Sub '${AWS::StackName}-included'\nDelaySeconds: 5\n",
    )
    template = {
        "Resources": {
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "Fn::Transform": {
                        "Name": "AWS::Include",
                        "Parameters": {"Location": "s3://snippets/queue.yaml"},
                    }
                },
            }
        },
    }

    _create(template)

    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.get_queue_url(QueueName="stack-included")["QueueUrl"]
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["DelaySeconds"])
    assert attrs["Attributes"]["DelaySeconds"] == "5"


@mock_aws
def test_include_runs_before_the_serverless_transform():
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="snippets")
    definition = {"StartAt": "A", "States": {"A": {"Type": "Pass", "End": True}}}
    s3.put_object(Bucket="snippets", Key="sm.json", Body=json.dumps(definition))
    template = {
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "Machine": {
                "Type": "AWS::Serverless::StateMachine",
                "Properties": {
                    "Name": "included",
                    "Definition": {
                        "Fn::Transform": {
                            "Name": "AWS::Include",
                            "Parameters": {"Location": "s3://snippets/sm.json"},
                        }
                    },
                },
            }
        },
    }

    _create(template)

    sfn = boto3.client("stepfunctions", region_name=REGION)
    machine = sfn.list_state_machines()["stateMachines"][0]
    described = sfn.describe_state_machine(stateMachineArn=machine["stateMachineArn"])
    assert json.loads(described["definition"]) == definition


@mock_aws
def test_include_of_a_missing_object_fails():
    template = {
        "Resources": {
            "Queue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "Fn::Transform": {
                        "Name": "AWS::Include",
                        "Parameters": {"Location": "s3://missing/queue.yaml"},
                    }
                },
            }
        },
    }

    with pytest.raises(ClientError) as exc:
        _create(template)

    assert "Transform AWS::Include failed with" in str(exc.value)


def test_serverless_policies_name_aws_managed_policies():
    template = {
        "Transform": "AWS::Serverless-2016-10-31",
        "Resources": {
            "Fn": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "InlineCode": "def handler(event, context): pass",
                    "Handler": "index.handler",
                    "Runtime": "python3.12",
                    "Policies": ["AmazonS3ReadOnlyAccess"],
                },
            }
        },
    }

    processed = process_transforms(template, {}, "123456789012", "eu-central-1")

    role = processed["Resources"]["FnRole"]["Properties"]
    assert "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" in role["ManagedPolicyArns"]
    assert "Transform" not in processed
    # The submitted template is left as it was
    assert template["Resources"]["Fn"]["Type"] == "AWS::Serverless::Function"
