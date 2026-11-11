"""BedrockAgentCore models."""

from typing import Any
from urllib.parse import quote

from moto import settings
from moto.core.base_backend import BackendDict, BaseBackend
from moto.core.common_models import BaseModel
from moto.core.utils import unix_time, utcnow
from moto.moto_api._internal import mock_random

from .exceptions import ResourceNotFoundException, ValidationException


class Event(BaseModel):
    def __init__(
        self,
        memory_id: str,
        actor_id: str,
        session_id: str,
        event_timestamp: Any,
        payload: list[dict[str, Any]],
        branch: dict[str, Any] | None,
        metadata: dict[str, Any] | None,
    ):
        self.memory_id = memory_id
        self.actor_id = actor_id
        self.session_id = session_id
        # real AWS event ids look like "<epoch-millis>#<hex>"
        self.event_id = f"{int(unix_time() * 1000)}#{mock_random.get_random_hex(16)}"
        self.event_timestamp = event_timestamp or utcnow()
        self.payload = payload
        self.branch = branch or {"name": "main"}
        self.metadata = metadata or {}

    def to_dict(self, include_payload: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "memoryId": self.memory_id,
            "actorId": self.actor_id,
            "sessionId": self.session_id,
            "eventId": self.event_id,
            "eventTimestamp": self.event_timestamp,
            "branch": self.branch,
        }
        if include_payload:
            result["payload"] = self.payload
        if self.metadata:
            result["metadata"] = self.metadata
        return result


class BedrockAgentCoreBackend(BaseBackend):
    """Implementation of BedrockAgentCore APIs."""

    def __init__(self, region_name: str, account_id: str) -> None:
        super().__init__(region_name, account_id)
        # keyed by (memoryId, actorId, sessionId), values keyed by eventId,
        # insertion order == chronological order
        self.events: dict[tuple[str, str, str], dict[str, Event]] = {}

    def create_event(
        self,
        memory_id: str,
        actor_id: str | None,
        session_id: str | None,
        event_timestamp: Any,
        payload: list[dict[str, Any]] | None,
        branch: dict[str, Any] | None,
        metadata: dict[str, Any] | None,
    ) -> Event:
        if not actor_id:
            raise ValidationException(
                "1 validation error detected: Value at 'actorId' failed to satisfy constraint: Member must not be null"
            )
        if not session_id:
            raise ValidationException(
                "1 validation error detected: Value at 'sessionId' failed to satisfy constraint: Member must not be null"
            )
        if not payload:
            raise ValidationException(
                "1 validation error detected: Value at 'payload' failed to satisfy constraint: Member must not be empty"
            )

        event = Event(
            memory_id=memory_id,
            actor_id=actor_id,
            session_id=session_id,
            event_timestamp=event_timestamp,
            payload=payload,
            branch=branch,
            metadata=metadata,
        )
        key = (memory_id, actor_id, session_id)
        self.events.setdefault(key, {})[event.event_id] = event
        return event

    def get_event(
        self, memory_id: str, actor_id: str, session_id: str, event_id: str
    ) -> Event:
        event = self.events.get((memory_id, actor_id, session_id), {}).get(event_id)
        if not event:
            raise ResourceNotFoundException(f"Event {event_id} not found")
        return event

    def list_events(
        self,
        memory_id: str,
        actor_id: str,
        session_id: str,
        max_results: int | None,
    ) -> list[Event]:
        events = list(self.events.get((memory_id, actor_id, session_id), {}).values())
        if max_results:
            events = events[:max_results]
        return events

    def delete_event(
        self, memory_id: str, actor_id: str, session_id: str, event_id: str
    ) -> str:
        key = (memory_id, actor_id, session_id)
        if event_id not in self.events.get(key, {}):
            raise ResourceNotFoundException(f"Event {event_id} not found")
        del self.events[key][event_id]
        return event_id

    def invoke_agent_runtime(
        self,
        agent_runtime_arn: str,
        qualifier: str | None,
        session_id: str,
        payload: bytes,
        headers: dict[str, str],
    ) -> tuple[int, dict[str, str], bytes]:
        """Runs the invocation on the runtime's code, which MOTO_BEDROCK_AGENTCORE_INVOKE_ENDPOINT or
        MOTO_BEDROCK_AGENTCORE_EXECUTOR=process provides. Without either, this is not implemented.

        Returns the runtime's status, content type and body.
        """
        from moto.bedrockagentcorecontrol.models import bedrockagentcorecontrol_backends

        control = bedrockagentcorecontrol_backends[self.account_id][self.region_name]
        runtime = next(
            (
                r
                for r in control.agent_runtimes.values()
                if r.agent_runtime_arn == agent_runtime_arn
            ),
            None,
        )
        if runtime is None:
            raise ResourceNotFoundException(f"Runtime {agent_runtime_arn} not found")
        endpoint = settings.bedrock_agentcore_invoke_endpoint()
        if endpoint is None:
            if settings.bedrock_agentcore_executor() == "process":
                from . import executor

                return executor.invoke(runtime, payload, headers)
            raise NotImplementedError(
                "InvokeAgentRuntime needs MOTO_BEDROCK_AGENTCORE_EXECUTOR=process or "
                "MOTO_BEDROCK_AGENTCORE_INVOKE_ENDPOINT to run the runtime"
            )
        import requests

        response = requests.post(
            f"{endpoint.rstrip('/')}/runtimes/{quote(agent_runtime_arn, safe='')}/invocations",
            params={"qualifier": qualifier} if qualifier else None,
            data=payload,
            headers=headers,
            timeout=900,
        )
        return (
            response.status_code,
            {
                "Content-Type": response.headers.get(
                    "Content-Type", "application/octet-stream"
                )
            },
            response.content,
        )


bedrockagentcore_backends = BackendDict(
    BedrockAgentCoreBackend,
    "bedrock-agentcore",
    # matches the workaround already used by the sibling
    # bedrock-agentcore-control service (see its models.py) -
    # moto's own region list for this service doesn't include it yet
    use_boto3_regions=False,
    additional_regions=[
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "ap-south-1",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "ca-central-1",
        "eu-central-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-north-1",
        "sa-east-1",
    ],
)
