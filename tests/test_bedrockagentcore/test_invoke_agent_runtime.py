import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import SkipTest

import boto3
import pytest

from moto import mock_aws, settings

REGION = "us-east-1"
TEMPLATE = {
    "Resources": {
        "Runner": {
            "Type": "AWS::BedrockAgentCore::Runtime",
            "Properties": {
                "AgentRuntimeName": "runner",
                "AgentRuntimeArtifact": {
                    "CodeConfiguration": {
                        "Code": {"S3": {"Bucket": "code", "Prefix": "runner.zip"}},
                        "EntryPoint": ["app.py"],
                        "Runtime": "PYTHON_3_12",
                    }
                },
                "RoleArn": "arn:aws:iam::123456789012:role/runner",
                "NetworkConfiguration": {"NetworkMode": "PUBLIC"},
                "EnvironmentVariables": {"GREETING": "hello"},
            },
        },
        "Endpoint": {
            "Type": "AWS::BedrockAgentCore::RuntimeEndpoint",
            "Properties": {
                "AgentRuntimeId": {"Fn::GetAtt": ["Runner", "AgentRuntimeId"]},
                "AgentRuntimeVersion": {
                    "Fn::GetAtt": ["Runner", "AgentRuntimeVersion"]
                },
                "Name": "live",
            },
        },
        "Interpreter": {
            "Type": "AWS::BedrockAgentCore::CodeInterpreterCustom",
            "Properties": {
                "Name": "interpreter",
                "NetworkConfiguration": {"NetworkMode": "SANDBOX"},
            },
        },
    },
    "Outputs": {
        "RunnerArn": {"Value": {"Fn::GetAtt": ["Runner", "AgentRuntimeArn"]}},
        "RunnerId": {"Value": {"Fn::GetAtt": ["Runner", "AgentRuntimeId"]}},
        "EndpointArn": {
            "Value": {"Fn::GetAtt": ["Endpoint", "AgentRuntimeEndpointArn"]}
        },
        "InterpreterId": {
            "Value": {"Fn::GetAtt": ["Interpreter", "CodeInterpreterId"]}
        },
    },
}


def _outputs() -> dict[str, str]:
    cfn = boto3.client("cloudformation", region_name=REGION)
    cfn.create_stack(StackName="agents", TemplateBody=json.dumps(TEMPLATE))
    return {
        o["OutputKey"]: o["OutputValue"]
        for o in cfn.describe_stacks(StackName="agents")["Stacks"][0]["Outputs"]
    }


@mock_aws
def test_stack_creates_runtime_endpoint_and_code_interpreter():
    outputs = _outputs()
    control = boto3.client("bedrock-agentcore-control", region_name=REGION)
    runtime = control.get_agent_runtime(agentRuntimeId=outputs["RunnerId"])
    assert runtime["agentRuntimeArn"] == outputs["RunnerArn"]
    assert runtime["environmentVariables"] == {"GREETING": "hello"}
    code = runtime["agentRuntimeArtifact"]["codeConfiguration"]
    assert code["code"]["s3"] == {"bucket": "code", "prefix": "runner.zip"}
    endpoints = control.list_agent_runtime_endpoints(agentRuntimeId=outputs["RunnerId"])
    assert [e["name"] for e in endpoints["runtimeEndpoints"]] == ["live"]
    assert (
        outputs["EndpointArn"]
        == endpoints["runtimeEndpoints"][0]["agentRuntimeEndpointArn"]
    )
    assert outputs["InterpreterId"].startswith("interpreter-")


@pytest.fixture(name="runtime_server")
def fixture_runtime_server(monkeypatch: pytest.MonkeyPatch):
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The endpoint setting is read by the server process")
    seen: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            seen.append(
                {
                    "path": self.path,
                    "session": self.headers[
                        "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id"
                    ],
                    "body": json.loads(body),
                }
            )
            answer = json.dumps({"echo": json.loads(body)}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(answer)))
            self.end_headers()
            self.wfile.write(answer)

        def log_message(self, *args: object) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv(
        "MOTO_BEDROCK_AGENTCORE_INVOKE_ENDPOINT",
        f"http://127.0.0.1:{server.server_address[1]}",
    )
    yield seen
    server.shutdown()


@mock_aws
def test_invoke_agent_runtime_forwards_to_the_endpoint(runtime_server):
    outputs = _outputs()
    client = boto3.client("bedrock-agentcore", region_name=REGION)
    session = "s" * 40
    response = client.invoke_agent_runtime(
        agentRuntimeArn=outputs["RunnerArn"],
        qualifier="live",
        runtimeSessionId=session,
        payload=json.dumps({"prompt": "hi"}).encode(),
    )

    assert json.loads(response["response"].read()) == {"echo": {"prompt": "hi"}}
    assert response["contentType"] == "application/json"
    assert response["runtimeSessionId"] == session
    (call,) = runtime_server
    assert call["session"] == session
    assert call["body"] == {"prompt": "hi"}
    assert call["path"].endswith("/invocations?qualifier=live")
    assert call["path"].startswith("/runtimes/arn%3Aaws%3Abedrock-agentcore%3A")


@mock_aws
def test_invoke_unknown_runtime_is_not_found(runtime_server):
    client = boto3.client("bedrock-agentcore", region_name=REGION)
    with pytest.raises(client.exceptions.ResourceNotFoundException):
        client.invoke_agent_runtime(
            agentRuntimeArn="arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/nope",
            runtimeSessionId="s" * 40,
            payload=b"{}",
        )
