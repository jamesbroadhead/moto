"""BedrockRuntimeBackend class with methods for supported APIs."""

import json
import re
from collections import deque
from collections.abc import Callable
from typing import Any

from moto.core.base_backend import BackendDict, BaseBackend
from moto.core.config import default_user_config
from moto.core.exceptions import JsonRESTError


class BedrockRuntimeBackend(BaseBackend):
    """Implementation of BedrockRuntime APIs."""

    def __init__(self, region_name: str, account_id: str) -> None:
        super().__init__(region_name, account_id)
        self.converse_results: deque[dict[str, Any]] = deque()

    def converse(
        self, model_id: str, parameters: dict[str, Any], request: dict[str, Any]
    ) -> dict[str, Any]:
        """Return a queued result, the configured responder's answer, or a deterministic default response.

        Model inference, tokenization, guardrails and model-specific validation
        are not simulated. Results can be queued through the Moto API at
        /moto-api/static/bedrock-runtime/converse-results with a JSON body
        containing results, account_id (optional), and region (optional).
        Each result is a complete Converse response, consumed in FIFO order.

        .. sourcecode:: python

            requests.post(
                "http://motoapi.amazonaws.com/moto-api/static/bedrock-runtime/converse-results",
                json={"results": [{"output": {"message": {"role": "assistant", "content": [{"text": "Hi"}]}}}]},
            )

        When the queue is empty, a responder can answer from the request, for example
        to script a multi-turn conversation with tool calls. Configure it with
        ``mock_aws(config={"bedrock-runtime": {"converse_responder": function}})``,
        where the function takes the request (the Converse request body plus
        ``modelId``) and returns a Converse response. In ServerMode, POST
        ``{"bedrock-runtime": {"converse_responder": "http://host:port/path"}}`` to
        /moto-api/config: Moto POSTs each request there as JSON and reads the
        response from the reply.

        A result may leave out stopReason, usage and metrics; Moto fills them in.
        ConverseStream uses the same results and streams them as events.
        """

        def invalid(message: str) -> None:
            raise JsonRESTError("ValidationException", message)

        managed_prompt = model_id.startswith("arn:") and ":prompt/" in model_id
        if managed_prompt and any(
            key in parameters
            for key in (
                "additionalModelRequestFields",
                "inferenceConfig",
                "system",
                "toolConfig",
            )
        ):
            invalid(
                "Prompt management requests cannot override inference configuration, system or tools"
            )
        messages = parameters.get("messages", [])
        if not messages and not managed_prompt:
            invalid("messages must contain at least one message")
        for message in messages:
            if message["role"] not in ("user", "assistant"):
                invalid("Message role must be user or assistant")
            if not message["content"]:
                invalid("Message content must not be empty")
        for pointer in parameters.get("additionalModelResponseFieldPaths", []):
            if not pointer.startswith("/") or re.search(r"~(?![01])", pointer):
                invalid("Additional model response field paths must be JSON pointers")
        responder = default_user_config.get("bedrock-runtime", {}).get(
            "converse_responder"
        )
        if self.converse_results:
            result = self.converse_results.popleft()
        elif responder is not None:
            result = _call_responder(responder, {"modelId": model_id, **request})
        else:
            result = {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [{"text": "Moto mock response"}],
                    }
                }
            }
        content = result.get("output", {}).get("message", {}).get("content", [])
        stop_reason = "tool_use" if any("toolUse" in b for b in content) else "end_turn"
        return {
            "stopReason": stop_reason,
            "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0},
            "metrics": {"latencyMs": 0},
            **result,
        }

    def converse_stream(
        self, model_id: str, parameters: dict[str, Any], request: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Streams the result that Converse would return, as ConverseStream events.

        Each text block is one delta, and a tool call's input is one JSON delta.
        """
        result = self.converse(model_id, parameters, request)
        message = result["output"]["message"]
        events: list[dict[str, Any]] = [{"messageStart": {"role": message["role"]}}]
        for index, block in enumerate(message["content"]):
            if "toolUse" in block:
                tool = block["toolUse"]
                start = {"toolUseId": tool["toolUseId"], "name": tool["name"]}
                events.append(
                    {
                        "contentBlockStart": {
                            "contentBlockIndex": index,
                            "start": {"toolUse": start},
                        }
                    }
                )
                delta: dict[str, Any] = {
                    "toolUse": {"input": json.dumps(tool["input"])}
                }
            elif "reasoningContent" in block:
                delta = {
                    "reasoningContent": block["reasoningContent"].get(
                        "reasoningText", {}
                    )
                }
            else:
                delta = {"text": block["text"]}
            events.append(
                {"contentBlockDelta": {"contentBlockIndex": index, "delta": delta}}
            )
            events.append({"contentBlockStop": {"contentBlockIndex": index}})
        stop = {"stopReason": result["stopReason"]}
        if "additionalModelResponseFields" in result:
            stop["additionalModelResponseFields"] = result[
                "additionalModelResponseFields"
            ]
        events.append({"messageStop": stop})
        metadata = {"usage": result["usage"], "metrics": result["metrics"]}
        if "trace" in result:
            metadata["trace"] = result["trace"]
        events.append({"metadata": metadata})
        return events

    def invoke_model(
        self,
        payload: dict[str, Any],
        model_id: str,
    ) -> dict[str, Any]:
        assert payload is not None
        assert model_id is not None
        inference_result: dict[str, Any] = {}
        return inference_result


def _call_responder(
    responder: "str | Callable[[dict[str, Any]], dict[str, Any]]",
    request: dict[str, Any],
) -> dict[str, Any]:
    if callable(responder):
        return responder(request)
    import requests

    response = requests.post(responder, json=request, timeout=300)
    response.raise_for_status()
    return response.json()


# Using `ec2` for the service name to work around lack of regions for bedrock-runtime in Botocore.
# https://github.com/getmoto/moto/issues/7745
bedrockruntime_backends = BackendDict(BedrockRuntimeBackend, "ec2")
