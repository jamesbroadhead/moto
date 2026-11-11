"""Runs AgentCore runtimes as local processes (MOTO_BEDROCK_AGENTCORE_EXECUTOR=process).

AgentCore runs a runtime's code in a microVM per session, where the code serves the runtime contract on port 8080:
GET /ping answers once it is ready, and POST /invocations takes each InvokeAgentRuntime payload. Here each runtime
version runs as one local process of its entry point, started on first use and kept running, which serves every
session; runtime_launcher.py maps its port 8080 to a free loopback port. Only code configurations of the Python
runtimes are supported, not container images.
"""

from __future__ import annotations

import atexit
import fnmatch
import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from moto import settings
from moto.awslambda.executor import _kill, _materialize

from .exceptions import RuntimeClientError

if TYPE_CHECKING:
    from moto.bedrockagentcorecontrol.models import AgentRuntime

logger = logging.getLogger(__name__)

# How long a runtime has to answer /ping after it starts
INIT_TIMEOUT = 120.0
INVOKE_TIMEOUT = 900.0
LAUNCHER = str(Path(__file__).with_name("runtime_launcher.py"))


@dataclass
class RuntimeSpec:
    account_id: str
    region: str
    arn: str
    name: str
    runtime: str
    entry_point: list[str]
    environment: dict[str, str]
    code_dir: str


def _code_dir(runtime: AgentRuntime) -> str:
    code = runtime.agent_runtime_artifact.get("codeConfiguration")
    if not code:
        raise NotImplementedError(
            "The process executor runs code configurations only, not container images"
        )
    s3 = code["code"]["s3"]
    bucket, key = s3["bucket"], s3["prefix"]
    if bucket == settings.lambda_local_code_bucket():
        local = _materialize(str(Path(key).expanduser().absolute()), b"", "agentcore")
        return local  # type: ignore[return-value]
    from moto.s3.models import s3_backends
    from moto.utilities.utils import get_partition

    backend = s3_backends[runtime.account_id][get_partition(runtime.region_name)]
    try:
        obj = backend.get_object(bucket, key)
    except Exception:
        obj = None
    if obj is None:
        raise RuntimeClientError(f"The runtime's code s3://{bucket}/{key} is missing")
    return _materialize(None, obj.value, "agentcore")  # type: ignore[return-value]


def _spec(runtime: AgentRuntime) -> RuntimeSpec:
    code = runtime.agent_runtime_artifact.get("codeConfiguration") or {}
    return RuntimeSpec(
        account_id=runtime.account_id,
        region=runtime.region_name,
        arn=runtime.agent_runtime_arn,
        name=runtime.agent_runtime_name,
        runtime=code.get("runtime", ""),
        entry_point=list(code.get("entryPoint") or []),
        environment={
            k: str(v) for k, v in (runtime.environment_variables or {}).items()
        },
        code_dir=_code_dir(runtime),
    )


def _fingerprint(runtime: AgentRuntime) -> str:
    return json.dumps(
        [
            runtime.agent_runtime_version,
            runtime.agent_runtime_artifact,
            runtime.environment_variables,
        ],
        sort_keys=True,
        default=str,
    )


def python_command(runtime: str) -> list[str]:
    for pattern, command in settings.bedrock_agentcore_runtime_commands().items():
        if fnmatch.fnmatch(runtime, pattern):
            return command
    return [sys.executable]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class RuntimeHost:
    """One runtime version's process, started on first use."""

    def __init__(self, spec: RuntimeSpec, fingerprint: str):
        self.spec = spec
        self.fingerprint = fingerprint
        self.port = _free_port()
        self.process: subprocess.Popen[bytes] | None = None
        # Held while the process starts; other runtimes start at the same time.
        self.lock = threading.Lock()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _env(self) -> dict[str, str]:
        # Moto's own settings, which can hold secrets such as database DSNs, are not the runtime's business.
        env = {k: v for k, v in os.environ.items() if not k.startswith("MOTO_")}
        env.setdefault("AWS_ACCESS_KEY_ID", "role-account-id")
        env.setdefault("AWS_SECRET_ACCESS_KEY", "role-secret-key")
        env.setdefault("AWS_SESSION_TOKEN", "session-token")
        endpoint_url = settings.bedrock_agentcore_aws_endpoint_url()
        if endpoint_url is None and not settings.is_test_proxy_mode():
            host = os.environ.get("MOTO_HOST") or "http://localhost"
            endpoint_url = f"{host}:{settings.moto_server_port()}"
        if endpoint_url:
            env.setdefault("AWS_ENDPOINT_URL", endpoint_url)
        python_paths = [self.spec.code_dir]
        if self.spec.environment.get("PYTHONPATH"):
            python_paths.append(self.spec.environment["PYTHONPATH"])
        env.update(self.spec.environment)
        env.update(
            {
                "AWS_REGION": self.spec.region,
                "AWS_DEFAULT_REGION": self.spec.region,
                "PYTHONPATH": os.pathsep.join(python_paths),
                "PYTHONUNBUFFERED": "1",
            }
        )
        return env

    def _pump(self, proc: subprocess.Popen[bytes]) -> None:
        assert proc.stdout
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", "replace").rstrip("\n")
            sys.stderr.write(f"[agentcore {self.spec.name}] {line}\n")
            sys.stderr.flush()

    def _ping(self) -> bool:
        try:
            with urllib.request.urlopen(f"{self.url}/ping", timeout=2) as resp:
                return resp.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def ensure_started(self) -> None:
        with self.lock:
            if self.alive:
                return
            if not self.spec.entry_point:
                raise RuntimeClientError(f"Runtime {self.spec.name} has no entry point")
            command = [
                *python_command(self.spec.runtime),
                LAUNCHER,
                str(self.port),
                *self.spec.entry_point,
            ]
            self.process = proc = subprocess.Popen(
                command,
                cwd=self.spec.code_dir,
                env=self._env(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            threading.Thread(target=self._pump, args=(proc,), daemon=True).start()
            deadline = time.time() + INIT_TIMEOUT
            while time.time() < deadline:
                if proc.poll() is not None:
                    raise RuntimeClientError(
                        f"Runtime {self.spec.name} exited with {proc.returncode} during initialization"
                    )
                if self._ping():
                    return
                time.sleep(0.2)
            self.stop()
            raise RuntimeClientError(
                f"Runtime initialization time exceeded. Please make sure that initialization completes in {INIT_TIMEOUT:.0f}s."
            )

    def invoke(
        self, payload: bytes, headers: dict[str, str]
    ) -> tuple[int, dict[str, str], bytes]:
        self.ensure_started()
        request = urllib.request.Request(
            f"{self.url}/invocations", data=payload, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=INVOKE_TIMEOUT) as resp:
                status, content_type, body = (
                    resp.status,
                    resp.headers.get("Content-Type"),
                    resp.read(),
                )
        except urllib.error.HTTPError as e:
            status, content_type, body = e.code, e.headers.get("Content-Type"), e.read()
        except (urllib.error.URLError, OSError) as e:
            raise RuntimeClientError(
                f"Runtime {self.spec.name} did not answer the invocation: {e}"
            )
        return (
            status,
            {"Content-Type": content_type or "application/octet-stream"},
            body,
        )

    def stop(self) -> None:
        proc = self.process
        if proc is not None and proc.poll() is None:
            _kill(proc)
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


_hosts: dict[tuple[str, str], RuntimeHost] = {}
_hosts_lock = threading.Lock()


def get_host(runtime: AgentRuntime) -> RuntimeHost:
    key = (runtime.account_id, runtime.agent_runtime_arn)
    fingerprint = _fingerprint(runtime)
    with _hosts_lock:
        host = _hosts.get(key)
    if host is not None and host.fingerprint == fingerprint:
        return host
    spec = _spec(runtime)
    with _hosts_lock:
        current = _hosts.get(key)
        if current is not None and current.fingerprint == fingerprint:
            return current
        new = _hosts[key] = RuntimeHost(spec, fingerprint)
    if current is not None:
        current.stop()
    return new


def invoke(
    runtime: AgentRuntime, payload: bytes, headers: dict[str, str]
) -> tuple[int, dict[str, str], bytes]:
    return get_host(runtime).invoke(payload, headers)


def shutdown() -> None:
    """Stops every runtime's process."""
    with _hosts_lock:
        hosts = list(_hosts.values())
        _hosts.clear()
    for host in hosts:
        host.stop()


atexit.register(shutdown)
