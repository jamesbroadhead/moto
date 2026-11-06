import boto3
import pytest
import requests

from moto import mock_aws


@mock_aws
def test_converse_returns_text_and_configured_tool_use_responses():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    params = {
        "modelId": "test-model",
        "messages": [{"role": "user", "content": [{"text": "Hello"}]}],
    }
    response = client.converse(**params)
    assert response["output"]["message"]["role"] == "assistant"
    assert response["output"]["message"]["content"][0]["text"]
    assert response["stopReason"] == "end_turn"
    assert (
        response["usage"]["totalTokens"]
        == response["usage"]["inputTokens"] + response["usage"]["outputTokens"]
    )
    expected = {
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "tool-1",
                            "name": "lookup",
                            "input": {"key": "value"},
                        }
                    }
                ],
            }
        },
        "stopReason": "tool_use",
        "usage": {"inputTokens": 5, "outputTokens": 3, "totalTokens": 8},
        "metrics": {"latencyMs": 12},
    }
    assert (
        requests.post(
            "http://motoapi.amazonaws.com/moto-api/static/bedrock-runtime/converse-results",
            json={"results": [expected]},
        ).status_code
        == 201
    )
    other_region = boto3.client("bedrock-runtime", region_name="eu-west-1")
    assert other_region.converse(**params)["stopReason"] == "end_turn"
    response = client.converse(
        **params,
        toolConfig={
            "tools": [
                {
                    "toolSpec": {
                        "name": "lookup",
                        "inputSchema": {"json": {"type": "object"}},
                    }
                }
            ]
        },
    )
    response.pop("ResponseMetadata")
    assert response == expected
    assert client.converse(**params)["stopReason"] == "end_turn"


@mock_aws
def test_converse_rejects_invalid_roles_and_prompt_management_overrides():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    with pytest.raises(client.exceptions.ValidationException):
        client.converse(
            modelId="test-model",
            messages=[{"role": "system", "content": [{"text": "Hello"}]}],
        )
    with pytest.raises(client.exceptions.ValidationException):
        client.converse(
            modelId="arn:aws:bedrock:us-east-1:123456789012:prompt/ABCDEFGHIJ:1",
            inferenceConfig={"maxTokens": 10},
        )
