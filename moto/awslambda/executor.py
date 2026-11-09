"""Runs Lambda functions in warm local processes (MOTO_LAMBDA_EXECUTOR=process).

Each function version gets a pool of workers. A worker is one execution environment: moto serves the Lambda
Runtime API and Extensions API for it on a local port, and starts the runtime's bootstrap and the layers'
external extensions as processes that poll that API, as they do in Lambda. Workers are started on first use,
reused across invocations, and replaced after a timeout, a crash or a change to the function.
"""

from __future__ import annotations

import atexit
import base64
import fnmatch
import glob
import hashlib
import io
import json
import logging
import os
import queue
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any

from moto import settings

if TYPE_CHECKING:
    from .models import LambdaFunction

logger = logging.getLogger(__name__)

# Lambda's init phase budget; a slower init gets the function's timeout, as Lambda retries it then.
INIT_TIMEOUT = 10.0
LOG_TAIL_BYTES = 4096
RUNTIME_CLIENT = str(Path(__file__).with_name("runtime_client.py"))
LAMBDA_BASE_IMAGE_BOOTSTRAP = "/var/runtime/bootstrap"


@dataclass
class InvocationResult:
    payload: bytes
    error: bool
    logs: str = ""


@dataclass
class FunctionSpec:
    account_id: str
    region: str
    name: str
    arn: str
    version: str
    runtime: str
    handler: str
    memory: int
    timeout: float
    environment: dict[str, str]
    code_dir: str
    layer_dirs: list[str]
    tracing_active: bool
    reserved_concurrency: int | None
    extensions: list[str] = field(default_factory=list)


def local_code_path(location: dict[str, Any]) -> str | None:
    """The local path that Code or Content names in MOTO_LAMBDA_LOCAL_CODE_BUCKET, if it does."""
    bucket = settings.lambda_local_code_bucket()
    if bucket and location.get("S3Bucket") == bucket and location.get("S3Key"):
        return str(Path(location["S3Key"]).expanduser().absolute())
    return None


def _extract(data: bytes, target: Path) -> str:
    if not target.is_dir():
        tmp = target.with_name(f"{target.name}.{uuid.uuid4().hex}.tmp")
        tmp.mkdir(parents=True)
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                path = zf.extract(info, tmp)
                mode = info.external_attr >> 16
                if mode and not info.is_dir():
                    os.chmod(path, mode & 0o7777)
        try:
            tmp.rename(target)
        except OSError:  # extracted concurrently by another invocation
            shutil.rmtree(tmp, ignore_errors=True)
    return str(target)


def _materialize(local_path: str | None, data: bytes, kind: str) -> str | None:
    root = Path(settings.lambda_work_dir()) / kind
    if local_path:
        p = Path(local_path)
        if p.is_file() and zipfile.is_zipfile(p):
            digest = hashlib.sha256(f"{p}:{p.stat().st_mtime_ns}".encode()).hexdigest()
            return _extract(p.read_bytes(), root / digest)
        return str(p)
    if data:
        return _extract(data, root / hashlib.sha256(data).hexdigest())
    return None


def _layer_dirs(fn: LambdaFunction) -> list[str]:
    from .utils import get_backend

    backend = get_backend(fn.account_id, fn.region)
    patterns = settings.lambda_layer_paths()
    dirs = []
    for layer in fn.layers:
        arn = layer["Arn"]
        path = next((d for p, d in patterns.items() if fnmatch.fnmatch(arn, p)), None)
        if path is None:
            lv = backend.layers_versions_by_arn(arn)
            if lv is not None:
                path = _materialize(
                    local_code_path(lv.content), lv.code_bytes, "layers"
                )
        if path:
            dirs.append(path)
        else:
            logger.warning("Lambda %s: layer %s has no content", fn.function_name, arn)
    return dirs


def _extension_commands(layer_dirs: list[str]) -> list[str]:
    """External extensions: executables in a layer's extensions/ directory, which Lambda mounts at /opt/extensions."""
    found: dict[str, str] = {}
    for layer in layer_dirs:
        ext_dir = Path(layer, "extensions")
        if ext_dir.is_dir():
            for entry in sorted(ext_dir.iterdir()):
                if entry.is_file() and os.access(entry, os.X_OK):
                    found.setdefault(entry.name, str(entry))
    return [found[name] for name in sorted(found)]


def _link_into_opt(opt: str, layer_dirs: list[str]) -> None:
    """Lambda mounts the layers at /opt; here they share one directory, and the first layer to provide a path wins."""
    for layer in layer_dirs:
        for entry in Path(layer).iterdir():
            target = Path(opt, entry.name)
            if entry.is_dir() and target.is_dir() and not target.is_symlink():
                _link_into_opt(str(target), [str(entry)])
            elif not target.exists() and not target.is_symlink():
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    target.symlink_to(entry)
                except OSError:
                    pass


def _fingerprint(fn: LambdaFunction) -> str:
    return json.dumps(
        [
            fn.run_time,
            fn.handler,
            fn.code_sha_256,
            fn.code.get("S3Bucket"),
            fn.code.get("S3Key"),
            fn.environment_vars,
            [layer["Arn"] for layer in fn.layers],
            fn.memory_size,
            fn.timeout,
            fn.tracing_config,
            fn.reserved_concurrency,
        ],
        sort_keys=True,
        default=str,
    )


def _spec(fn: LambdaFunction) -> FunctionSpec:
    code_dir = _materialize(local_code_path(fn.code), fn.code_bytes, "code")
    layer_dirs = _layer_dirs(fn)
    if settings.lambda_opt_dir():
        _link_into_opt(settings.lambda_opt_dir(), layer_dirs)  # type: ignore[arg-type]
    if code_dir is None:
        code_dir = os.path.join(settings.lambda_work_dir(), "code", "empty")
        os.makedirs(code_dir, exist_ok=True)
    rc = fn.reserved_concurrency
    return FunctionSpec(
        account_id=fn.account_id,
        region=fn.region,
        name=fn.function_name,
        arn=fn.function_arn,
        version=str(fn.version),
        runtime=fn.run_time or "",
        handler=fn.handler or "",
        memory=int(fn.memory_size),
        timeout=float(fn.timeout),
        environment={k: str(v) for k, v in (fn.environment_vars or {}).items()},
        code_dir=code_dir,
        layer_dirs=layer_dirs,
        tracing_active=(fn.tracing_config or {}).get("Mode") == "Active",
        reserved_concurrency=None if rc is None else int(rc),
        extensions=_extension_commands(layer_dirs),
    )


def bootstrap_command(spec: FunctionSpec) -> list[str] | None:
    for pattern, command in settings.lambda_runtime_commands().items():
        if fnmatch.fnmatch(spec.runtime, pattern):
            return command
    if spec.runtime.startswith("provided") or not spec.runtime:
        for root in [spec.code_dir, *spec.layer_dirs]:
            if os.access(os.path.join(root, "bootstrap"), os.X_OK):
                return [os.path.join(root, "bootstrap")]
        return None
    if spec.runtime.startswith("python"):
        if os.access(LAMBDA_BASE_IMAGE_BOOTSTRAP, os.X_OK):
            return [LAMBDA_BASE_IMAGE_BOOTSTRAP]
        try:
            import awslambdaric  # noqa: F401  # type: ignore[import-not-found,import-untyped,unused-ignore]

            return [sys.executable, "-m", "awslambdaric", spec.handler]
        except ImportError:
            # -c leaves the working directory, the function's code, at the front of sys.path
            return [
                sys.executable,
                "-c",
                f"import runpy; runpy.run_path({RUNTIME_CLIENT!r}, run_name='__main__')",
            ]
    return None


def trace_header(sampled: bool) -> str:
    root = f"1-{int(time.time()):08x}-{secrets.token_hex(12)}"
    return f"Root={root};Parent={secrets.token_hex(8)};Sampled={int(sampled)}"


def _error(error_type: str, message: str) -> bytes:
    return json.dumps({"errorMessage": message, "errorType": error_type}).encode()


def _kill(proc: subprocess.Popen[bytes]) -> None:
    try:
        if hasattr(os, "killpg"):
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError):
        pass


class _Invocation:
    def __init__(
        self,
        payload: bytes,
        deadline_ms: int,
        trace_id: str,
        client_context: str | None,
    ):
        self.request_id = str(uuid.uuid4())
        self.payload = payload
        self.deadline_ms = deadline_ms
        self.trace_id = trace_id
        self.client_context = client_context
        self.done = threading.Event()
        self.response = b""
        self.error = False


class _Extension:
    def __init__(self, name: str, events: list[str]):
        self.name = name
        self.events = set(events)
        self.queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self.nexts = 0


class _ApiServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, worker: Worker):
        self.worker = worker
        super().__init__(("127.0.0.1", 0), _ApiHandler)

    def handle_error(self, request: Any, client_address: Any) -> None:
        # Killed runtimes drop their connections mid-request
        logger.debug("Runtime API request failed", exc_info=True)


class _ApiHandler(BaseHTTPRequestHandler):
    server: _ApiServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _reply(
        self, status: int, body: bytes = b"{}", headers: dict[str, str] | None = None
    ) -> None:
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> bytes:
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def do_GET(self) -> None:
        worker = self.server.worker
        if self.path == "/2018-06-01/runtime/invocation/next":
            inv = worker.next_invocation()
            if inv is None:
                self._reply(500, _error("Runtime.Shutdown", "shutting down"))
                return
            headers = {
                "Lambda-Runtime-Aws-Request-Id": inv.request_id,
                "Lambda-Runtime-Deadline-Ms": str(inv.deadline_ms),
                "Lambda-Runtime-Invoked-Function-Arn": worker.spec.arn,
                "Lambda-Runtime-Trace-Id": inv.trace_id,
            }
            if inv.client_context:
                headers["Lambda-Runtime-Client-Context"] = inv.client_context
            self._reply(200, inv.payload, headers)
        elif self.path == "/2020-01-01/extension/event/next":
            ext_id = self.headers.get("Lambda-Extension-Identifier", "")
            event = worker.next_extension_event(ext_id)
            if event is None:
                self._reply(403, _error("Extension.Unknown", "unknown extension"))
                return
            self._reply(
                200,
                json.dumps(event).encode(),
                {"Lambda-Extension-Event-Identifier": str(uuid.uuid4())},
            )
        else:
            self._reply(404, _error("InvalidRequest", self.path))

    def do_POST(self) -> None:
        worker = self.server.worker
        body = self._body()
        parts = self.path.strip("/").split("/")
        if parts[:3] == ["2018-06-01", "runtime", "invocation"] and len(parts) == 5:
            error = parts[4] == "error"
            if error and not body:
                error_type = self.headers.get(
                    "Lambda-Runtime-Function-Error-Type", "Runtime.Unknown"
                )
                body = _error(error_type, "Unknown error")
            worker.complete(parts[3], body, error)
            self._reply(202, b'{"status":"OK"}')
        elif self.path == "/2018-06-01/runtime/init/error":
            worker.init_failed(body or _error("Runtime.Unknown", "init error"))
            self._reply(202, b'{"status":"OK"}')
        elif self.path == "/2020-01-01/extension/register":
            name = self.headers.get("Lambda-Extension-Name", "")
            events = json.loads(body or b"{}").get("events", [])
            ext_id = worker.register_extension(name, events)
            info = {
                "functionName": worker.spec.name,
                "functionVersion": worker.spec.version,
                "handler": worker.spec.handler,
            }
            self._reply(
                200, json.dumps(info).encode(), {"Lambda-Extension-Identifier": ext_id}
            )
        elif self.path == "/2020-01-01/extension/init/error":
            worker.init_failed(body or _error("Extension.Unknown", "init error"))
            self._reply(202)
        else:
            # extension exit errors; the Logs and Telemetry APIs accept subscriptions and deliver nothing
            self._reply(202 if "extension" in self.path else 200)

    do_PUT = do_POST


class Worker:
    def __init__(self, pool: FunctionPool, number: int):
        self.pool = pool
        self.spec = pool.spec
        self.number = number
        self.server = _ApiServer(self)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.invocations: queue.Queue[_Invocation | None] = queue.Queue()
        self.current: _Invocation | None = None
        self.initialized = threading.Event()
        self.init_error: bytes | None = None
        self.extensions: dict[str, _Extension] = {}
        self.ext_condition = threading.Condition()
        self.ext_invocations = 0
        self.processes: list[subprocess.Popen[bytes]] = []
        self.bootstrap: subprocess.Popen[bytes] | None = None
        self.lines: list[str] = []
        self.lines_lock = threading.Lock()
        self.dead = False
        now = datetime.now(timezone.utc)
        self.log_stream = f"{now:%Y/%m/%d}/[{self.spec.version}]{uuid.uuid4().hex}"

    # Runtime and Extensions API callbacks

    def next_invocation(self) -> _Invocation | None:
        self.initialized.set()
        return self.invocations.get()

    def complete(self, request_id: str, body: bytes, error: bool) -> None:
        inv = self.current
        if inv is not None and inv.request_id == request_id and not inv.done.is_set():
            inv.response, inv.error = body, error
            inv.done.set()

    def init_failed(self, body: bytes) -> None:
        self.init_error = body
        self.initialized.set()

    def register_extension(self, name: str, events: list[str]) -> str:
        ext_id = str(uuid.uuid4())
        with self.ext_condition:
            self.extensions[ext_id] = _Extension(name, events)
            self.ext_condition.notify_all()
        self._log(f"EXTENSION\tName: {name}\tState: Registered\tEvents: {events}")
        return ext_id

    def next_extension_event(self, ext_id: str) -> dict[str, Any] | None:
        ext = self.extensions.get(ext_id)
        if ext is None:
            return None
        with self.ext_condition:
            ext.nexts += 1
            self.ext_condition.notify_all()
        return ext.queue.get()

    # Lifecycle

    @property
    def alive(self) -> bool:
        return (
            not self.dead
            and self.bootstrap is not None
            and self.bootstrap.poll() is None
        )

    def _log(self, line: str) -> None:
        with self.lines_lock:
            self.lines.append(line)

    def _pump(self, proc: subprocess.Popen[bytes]) -> None:
        assert proc.stdout
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", "replace").rstrip("\n")
            self._log(line)
            # Function output, as CloudWatch Logs would show it
            sys.stderr.write(f"[lambda {self.spec.name}] {line}\n")
            sys.stderr.flush()

    def _spawn(
        self, command: list[str], env: dict[str, str]
    ) -> subprocess.Popen[bytes]:
        proc = subprocess.Popen(
            command,
            env=env,
            cwd=self.spec.code_dir,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.processes.append(proc)
        threading.Thread(target=self._pump, args=(proc,), daemon=True).start()
        return proc

    def _wait_extensions(self, done: Any, deadline: float) -> bool:
        with self.ext_condition:
            while not done() and time.time() < deadline:
                ext_procs = self.processes[: len(self.spec.extensions)]
                if ext_procs and all(p.poll() is not None for p in ext_procs):
                    break
                self.ext_condition.wait(0.05)
            return bool(done())

    def start(self, command: list[str]) -> None:
        """Runs the init phase; afterwards init_error is set if it failed."""
        tmp = Path(settings.lambda_work_dir(), "tmp", self.spec.name, uuid.uuid4().hex)
        tmp.mkdir(parents=True, exist_ok=True)
        env = {
            **self.pool.env,
            "AWS_LAMBDA_RUNTIME_API": f"127.0.0.1:{self.server.server_address[1]}",
            "AWS_LAMBDA_LOG_STREAM_NAME": self.log_stream,
            "TMPDIR": str(tmp),
        }
        deadline = time.time() + max(INIT_TIMEOUT, self.spec.timeout)
        for ext in self.spec.extensions:
            self._spawn([ext], env)
        if self.spec.extensions and not self._wait_extensions(
            lambda: len(self.extensions) >= len(self.spec.extensions)
            and all(e.nexts for e in self.extensions.values()),
            deadline,
        ):
            if not self.init_error:
                self.init_error = _error(
                    "Extension.Crash" if time.time() < deadline else "Sandbox.Timedout",
                    "extensions did not finish their init phase",
                )
            return
        if self.init_error:
            return
        self.bootstrap = self._spawn(command, env)
        while not self.initialized.wait(0.05):
            if self.bootstrap.poll() is not None:
                self.init_error = _error(
                    "Runtime.ExitError",
                    f"Runtime exited with error: exit status {self.bootstrap.returncode}",
                )
                return
            if time.time() > deadline:
                self.init_error = _error("Sandbox.Timedout", "init phase timed out")
                return

    def invoke(
        self, payload: bytes, trace_id: str, client_context: str | None
    ) -> InvocationResult:
        assert self.bootstrap is not None
        start = time.time()
        deadline = start + self.spec.timeout
        inv = _Invocation(payload, int(deadline * 1000), trace_id, client_context)
        with self.lines_lock:
            first_line = len(self.lines)
        self._log(f"START RequestId: {inv.request_id} Version: {self.spec.version}")
        self.current = inv
        self.ext_invocations += 1
        for ext in self.extensions.values():
            if "INVOKE" in ext.events:
                ext.queue.put(
                    {
                        "eventType": "INVOKE",
                        "deadlineMs": inv.deadline_ms,
                        "requestId": inv.request_id,
                        "invokedFunctionArn": self.spec.arn,
                        "tracing": {"type": "X-Amzn-Trace-Id", "value": trace_id},
                    }
                )
        self.invocations.put(inv)
        result: InvocationResult | None = None
        while not inv.done.wait(0.02):
            if self.bootstrap.poll() is not None:
                code = self.bootstrap.returncode
                status = f"exit status {code}" if code >= 0 else f"signal {-code}"
                message = f"RequestId: {inv.request_id} Error: Runtime exited with error: {status}"
                result = InvocationResult(_error("Runtime.ExitError", message), True)
                break
            if time.time() > deadline:
                stamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                message = f"{stamp} {inv.request_id} Task timed out after {self.spec.timeout:.2f} seconds"
                result = InvocationResult(_error("Sandbox.Timedout", message), True)
                break
        if result is None:
            result = InvocationResult(inv.response, inv.error)
            invoking = [e for e in self.extensions.values() if "INVOKE" in e.events]
            target = self.ext_invocations + 1
            if invoking:
                self._wait_extensions(
                    lambda: all(e.nexts >= target for e in invoking), deadline
                )
        else:
            self.kill()
        self.current = None
        duration = (time.time() - start) * 1000
        self._log(f"END RequestId: {inv.request_id}")
        self._log(
            f"REPORT RequestId: {inv.request_id}\tDuration: {duration:.2f} ms\t"
            f"Memory Size: {self.spec.memory} MB"
        )
        with self.lines_lock:
            lines = self.lines[first_line:]
        self._save_logs(lines)
        logs = "\n".join(lines) + "\n"
        result.logs = logs.encode()[-LOG_TAIL_BYTES:].decode("utf-8", "ignore")
        return result

    def _save_logs(self, lines: list[str]) -> None:
        from moto.logs.models import logs_backends

        try:
            backend = logs_backends[self.spec.account_id][self.spec.region]
            group = f"/aws/lambda/{self.spec.name}"
            backend.ensure_log_group(group)
            if self.log_stream not in backend.groups[group].streams:
                backend.create_log_stream(group, self.log_stream)
            now = int(time.time() * 1000)
            backend.put_log_events(
                group,
                self.log_stream,
                [{"timestamp": now, "message": line} for line in lines],
            )
        except Exception:  # logs must never fail an invocation
            logger.debug("could not save Lambda logs", exc_info=True)

    def kill(self) -> None:
        """Ends the execution environment: the runtime, its extensions and everything they started."""
        if self.dead:
            return
        self.dead = True
        for ext in self.extensions.values():
            ext.queue.put(
                {"eventType": "SHUTDOWN", "shutdownReason": "spindown", "deadlineMs": 0}
            )
        for proc in self.processes:
            _kill(proc)
        for _ in range(4):
            self.invocations.put(None)
        for ext in self.extensions.values():
            ext.queue.put(None)
        threading.Thread(target=self._close_server, daemon=True).start()

    def _close_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        for proc in self.processes:
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


class FunctionPool:
    def __init__(self, spec: FunctionSpec, fingerprint: str):
        self.spec = spec
        self.fingerprint = fingerprint
        size = settings.lambda_process_pool_size()
        if spec.reserved_concurrency is not None:
            size = min(size, spec.reserved_concurrency)
        if spec.extensions:
            # Extensions often listen on fixed ports, and workers share the host's network.
            size = 1
        size = max(size, 1)
        self.size = size
        self.slots = threading.BoundedSemaphore(size)
        self.event_source_slots = threading.BoundedSemaphore(
            max(1, size - settings.lambda_event_source_free_workers())
        )
        # Workers share the host's /tmp, which some code unpacks files into during init.
        self.starting = threading.Lock()
        self.lock = threading.Lock()
        self.idle: list[Worker] = []
        self.workers: set[Worker] = set()
        self.started = 0
        self.retired = False
        self.env = self._env()

    def _env(self) -> dict[str, str]:
        spec = self.spec
        env = dict(os.environ)
        env.setdefault("AWS_ACCESS_KEY_ID", "role-account-id")
        env.setdefault("AWS_SECRET_ACCESS_KEY", "role-secret-key")
        env.setdefault("AWS_SESSION_TOKEN", "session-token")
        host = os.environ.get("MOTO_HOST") or "http://localhost"
        port = settings.moto_server_port()
        env.update(
            {
                "MOTO_HOST": host,
                "MOTO_PORT": port,
                "MOTO_HTTP_ENDPOINT": f"{host}:{port}",
            }
        )
        if not settings.is_test_proxy_mode():
            env.setdefault("AWS_ENDPOINT_URL", f"{host}:{port}")
        python_paths = [spec.code_dir]
        bin_paths = []
        for layer in spec.layer_dirs:
            python_paths.append(os.path.join(layer, "python"))
            python_paths += sorted(
                glob.glob(
                    os.path.join(layer, "python", "lib", "python3*", "site-packages")
                )
            )
            bin_paths.append(os.path.join(layer, "bin"))
        if spec.environment.get("PYTHONPATH"):
            python_paths.append(spec.environment["PYTHONPATH"])
        env.update(spec.environment)
        env.update(
            {
                "_HANDLER": spec.handler,
                "AWS_REGION": spec.region,
                "AWS_DEFAULT_REGION": spec.region,
                "AWS_EXECUTION_ENV": f"AWS_Lambda_{spec.runtime}",
                "AWS_LAMBDA_FUNCTION_NAME": spec.name,
                "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": str(spec.memory),
                "AWS_LAMBDA_FUNCTION_VERSION": spec.version,
                "AWS_LAMBDA_INITIALIZATION_TYPE": "on-demand",
                "AWS_LAMBDA_LOG_GROUP_NAME": f"/aws/lambda/{spec.name}",
                "LAMBDA_TASK_ROOT": spec.code_dir,
                "PATH": os.pathsep.join([*bin_paths, os.environ.get("PATH", "")]),
                "PYTHONPATH": os.pathsep.join(python_paths),
                "PYTHONUNBUFFERED": "1",
                "TZ": ":UTC",
            }
        )
        return env

    def _idle_worker(self) -> Worker | None:
        with self.lock:
            while self.idle:
                worker = self.idle.pop()
                if worker.alive:
                    return worker
                worker.kill()
        return None

    def _start_worker(self) -> tuple[Worker, list[str]]:
        with self.lock:
            self.started += 1
            worker = Worker(self, self.started)
            self.workers.add(worker)
        command = bootstrap_command(self.spec)
        if command is None:
            worker.init_error = _error(
                "Runtime.InvalidEntrypoint",
                f"moto has no bootstrap for runtime {self.spec.runtime!r}: set MOTO_LAMBDA_RUNTIME_COMMANDS",
            )
        else:
            worker.start(command)
        with worker.lines_lock:
            lines = list(worker.lines)
        return worker, lines

    def invoke(
        self,
        payload: bytes,
        event_source: bool = False,
        trace_id: str | None = None,
        client_context: str | None = None,
    ) -> InvocationResult:
        if event_source:
            with self.event_source_slots:
                return self._invoke(payload, trace_id, client_context)
        return self._invoke(payload, trace_id, client_context)

    def _invoke(
        self, payload: bytes, trace_id: str | None, client_context: str | None
    ) -> InvocationResult:
        trace_id = trace_id or trace_header(self.spec.tracing_active)
        with self.slots:
            worker = self._idle_worker()
            if worker is None:
                with self.starting:
                    worker = self._idle_worker()
                    if worker is None:
                        worker, init_lines = self._start_worker()
                        if worker.init_error:
                            # Lambda resets an environment whose init failed; the next call runs init again.
                            self._discard(worker)
                            logs = "\n".join(init_lines) + "\n"
                            return InvocationResult(
                                worker.init_error,
                                True,
                                logs.encode()[-LOG_TAIL_BYTES:].decode(
                                    "utf-8", "ignore"
                                ),
                            )
            result = worker.invoke(payload, trace_id, client_context)
            with self.lock:
                if worker.alive and not self.retired:
                    self.idle.append(worker)
                    return result
            self._discard(worker)
            return result

    def _discard(self, worker: Worker) -> None:
        worker.kill()
        with self.lock:
            self.workers.discard(worker)

    def retire(self) -> None:
        """Stops idle workers now and busy ones when their invocation ends."""
        with self.lock:
            self.retired = True
            idle, self.idle = self.idle, []
        for worker in idle:
            self._discard(worker)

    def shutdown(self) -> None:
        self.retire()
        with self.lock:
            workers = list(self.workers)
        for worker in workers:
            self._discard(worker)


_pools: dict[tuple[str, str], FunctionPool] = {}
_pools_lock = threading.Lock()


def get_pool(fn: LambdaFunction) -> FunctionPool:
    key = (fn.account_id, fn.function_arn)
    fingerprint = _fingerprint(fn)
    with _pools_lock:
        pool = _pools.get(key)
    if pool is not None and pool.fingerprint == fingerprint:
        return pool
    spec = _spec(fn)
    with _pools_lock:
        current = _pools.get(key)
        if current is not None and current.fingerprint == fingerprint:
            return current
        new = _pools[key] = FunctionPool(spec, fingerprint)
    if current is not None:
        current.retire()
    return new


def invoke(
    fn: LambdaFunction,
    payload: bytes,
    event_source: bool = False,
    trace_id: str | None = None,
    client_context: str | None = None,
) -> InvocationResult:
    if client_context:
        try:
            client_context = base64.b64decode(client_context).decode()
        except Exception:
            pass
    return get_pool(fn).invoke(payload, event_source, trace_id, client_context)


def shutdown(
    account_id: str | None = None,
    region: str | None = None,
    function_name: str | None = None,
) -> None:
    """Stops the workers of the matching functions: all of them by default."""
    with _pools_lock:
        matching = [
            key
            for key, pool in _pools.items()
            if account_id in (None, pool.spec.account_id)
            and region in (None, pool.spec.region)
            and function_name in (None, pool.spec.name)
        ]
        pools = [_pools.pop(key) for key in matching]
    for pool in pools:
        pool.shutdown()


atexit.register(shutdown)
