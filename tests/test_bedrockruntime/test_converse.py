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


TOOL_USE_RESULT = {
    "output": {
        "message": {
            "role": "assistant",
            "content": [
                {"text": "Let me look that up."},
                {
                    "toolUse": {
                        "toolUseId": "tool-1",
                        "name": "lookup",
                        "input": {"key": "value"},
                    }
                },
            ],
        }
    },
    "stopReason": "tool_use",
    "usage": {"inputTokens": 5, "outputTokens": 3, "totalTokens": 8},
    "metrics": {"latencyMs": 12},
}


@mock_aws
def test_converse_stream_returns_the_queued_result_as_events():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    requests.post(
        "http://motoapi.amazonaws.com/moto-api/static/bedrock-runtime/converse-results",
        json={"results": [TOOL_USE_RESULT]},
    )
    response = client.converse_stream(
        modelId="test-model",
        messages=[{"role": "user", "content": [{"text": "Hello"}]}],
    )
    assert list(response["stream"]) == [
        {"messageStart": {"role": "assistant"}},
        {
            "contentBlockDelta": {
                "contentBlockIndex": 0,
                "delta": {"text": "Let me look that up."},
            }
        },
        {"contentBlockStop": {"contentBlockIndex": 0}},
        {
            "contentBlockStart": {
                "contentBlockIndex": 1,
                "start": {"toolUse": {"toolUseId": "tool-1", "name": "lookup"}},
            }
        },
        {
            "contentBlockDelta": {
                "contentBlockIndex": 1,
                "delta": {"toolUse": {"input": '{"key": "value"}'}},
            }
        },
        {"contentBlockStop": {"contentBlockIndex": 1}},
        {"messageStop": {"stopReason": "tool_use"}},
        {
            "metadata": {
                "usage": {"inputTokens": 5, "outputTokens": 3, "totalTokens": 8},
                "metrics": {"latencyMs": 12},
            }
        },
    ]


@mock_aws
def test_converse_stream_without_queued_results_streams_the_default_text():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    response = client.converse_stream(
        modelId="test-model",
        messages=[{"role": "user", "content": [{"text": "Hello"}]}],
    )
    events = list(response["stream"])
    assert events[0] == {"messageStart": {"role": "assistant"}}
    assert events[1]["contentBlockDelta"]["delta"]["text"] == "Moto mock response"
    assert events[-2] == {"messageStop": {"stopReason": "end_turn"}}
    assert "usage" in events[-1]["metadata"]


@mock_aws
def test_converse_stream_validates_the_request():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    with pytest.raises(client.exceptions.ValidationException):
        client.converse_stream(
            modelId="test-model",
            messages=[{"role": "system", "content": [{"text": "Hello"}]}],
        )


def _scripted_model(request):
    """Calls the tool on the first turn, and answers once the tool result is in."""
    last = request["messages"][-1]["content"]
    if any("toolResult" in block for block in last):
        return {
            "output": {"message": {"role": "assistant", "content": [{"text": "42"}]}}
        }
    tool = request["toolConfig"]["tools"][0]["toolSpec"]["name"]
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "tool-1",
                            "name": tool,
                            "input": {"model": request["modelId"]},
                        }
                    }
                ],
            }
        }
    }


TOOLS = {
    "tools": [
        {"toolSpec": {"name": "lookup", "inputSchema": {"json": {"type": "object"}}}}
    ]
}


@mock_aws(config={"bedrock-runtime": {"converse_responder": _scripted_model}})
def test_a_responder_answers_each_turn_from_the_request():
    client = boto3.client("bedrock-runtime", region_name="eu-central-1")
    messages = [{"role": "user", "content": [{"text": "What is the answer?"}]}]
    first = client.converse(modelId="my-model", messages=messages, toolConfig=TOOLS)
    assert first["stopReason"] == "tool_use"
    assert first["output"]["message"]["content"][0]["toolUse"]["input"] == {
        "model": "my-model"
    }
    assert set(first["usage"]) == {"inputTokens", "outputTokens", "totalTokens"}
    assert "latencyMs" in first["metrics"]

    messages += [
        first["output"]["message"],
        {
            "role": "user",
            "content": [
                {
                    "toolResult": {
                        "toolUseId": "tool-1",
                        "content": [{"json": {"answer": 42}}],
                    }
                }
            ],
        },
    ]
    second = client.converse_stream(
        modelId="my-model", messages=messages, toolConfig=TOOLS
    )
    events = list(second["stream"])
    assert events[1]["contentBlockDelta"]["delta"] == {"text": "42"}
    assert {"messageStop": {"stopReason": "end_turn"}} in events


@mock_aws(config={"bedrock-runtime": {"converse_responder": _scripted_model}})
def test_queued_results_come_before_the_responder():
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    requests.post(
        "http://motoapi.amazonaws.com/moto-api/static/bedrock-runtime/converse-results",
        json={"results": [TOOL_USE_RESULT]},
    )
    messages = [{"role": "user", "content": [{"text": "Hello"}]}]
    queued = client.converse(modelId="m", messages=messages, toolConfig=TOOLS)
    assert queued["output"] == TOOL_USE_RESULT["output"]
    answered = client.converse(modelId="m", messages=messages, toolConfig=TOOLS)
    assert answered["output"]["message"]["content"][0]["toolUse"]["input"] == {
        "model": "m"
    }
