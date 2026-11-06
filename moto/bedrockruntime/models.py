"""BedrockRuntimeBackend class with methods for supported APIs."""

import re
from collections import deque
from typing import Any

from moto.core.base_backend import BackendDict, BaseBackend
from moto.core.exceptions import JsonRESTError


class BedrockRuntimeBackend(BaseBackend):
    """Implementation of BedrockRuntime APIs."""

    def __init__(self, region_name: str, account_id: str) -> None:
        super().__init__(region_name, account_id)
        self.converse_results: deque[dict[str, Any]] = deque()

    def converse(self, model_id: str, parameters: dict[str, Any]) -> dict[str, Any]:
        """Return queued inference results, or a deterministic default response.

        Model inference, tokenization, guardrails and model-specific validation
        are not simulated. Results can be queued through the Moto API at
        /moto-api/static/bedrock-runtime/converse-results with a JSON body
        containing results, account_id (optional), and region (optional).
        Each result is a complete Converse response, consumed in FIFO order.
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
        try:
            return self.converse_results.popleft()
        except IndexError:
            return {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [{"text": "Moto mock response"}],
                    }
                },
                "stopReason": "end_turn",
                "usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0},
                "metrics": {"latencyMs": 0},
            }

    def invoke_model(
        self,
        payload: dict[str, Any],
        model_id: str,
    ) -> dict[str, Any]:
        assert payload is not None
        assert model_id is not None
        inference_result: dict[str, Any] = {}
        return inference_result


# Using `ec2` for the service name to work around lack of regions for bedrock-runtime in Botocore.
# https://github.com/getmoto/moto/issues/7745
bedrockruntime_backends = BackendDict(BedrockRuntimeBackend, "ec2")
