"""InvokeAgentRuntime with MOTO_BEDROCK_AGENTCORE_EXECUTOR=process: moto runs the runtime's code locally."""

import io
import json
import threading
import time
import zipfile
from pathlib import Path
from unittest import SkipTest

import boto3
import pytest

from moto import mock_aws, settings
from moto.bedrockagentcore import executor

REGION = "us-east-1"
SESSION = "s" * 40

# A runtime that serves AgentCore's contract on port 8080 with only the standard library.
APP = """import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.answer(200, {"status": "Healthy"} if self.path == "/ping" else {})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.answer(
            200,
            {
                "path": self.path,
                "body": body,
                "pid": os.getpid(),
                "greeting": os.environ.get("GREETING"),
                "session": self.headers.get("X-Amzn-Bedrock-AgentCore-Runtime-Session-Id"),
                "moto_settings": sorted(k for k in os.environ if k.startswith("MOTO_")),
            },
        )

    def answer(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
"""
NEVER_SERVES = "import time\ntime.sleep(60)\n"
EXITS = "import sys\nsys.exit(3)\n"


@pytest.fixture(autouse=True)
def process_executor(monkeypatch: pytest.MonkeyPatch):
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The executor setting is read by the server process")
    monkeypatch.setenv("MOTO_BEDROCK_AGENTCORE_EXECUTOR", "process")
    monkeypatch.setenv("MOTO_TEST_ONLY_SETTING", "secret")
    yield
    executor.shutdown()


def _zip(source: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("app.py", source)
    return buf.getvalue()


def _create_runtime(name: str, bucket: str, prefix: str) -> str:
    control = boto3.client("bedrock-agentcore-control", region_name=REGION)
    return control.create_agent_runtime(
        agentRuntimeName=name,
        agentRuntimeArtifact={
            "codeConfiguration": {
                "code": {"s3": {"bucket": bucket, "prefix": prefix}},
                "runtime": "PYTHON_3_12",
                "entryPoint": ["app.py"],
            }
        },
        roleArn="arn:aws:iam::123456789012:role/runner",
        networkConfiguration={"networkMode": "PUBLIC"},
        environmentVariables={"GREETING": "hello"},
    )["agentRuntimeArn"]


def _runtime_in_s3(name: str, source: str) -> str:
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="code")
    s3.put_object(Bucket="code", Key=f"{name}.zip", Body=_zip(source))
    return _create_runtime(name, "code", f"{name}.zip")


def _invoke(arn: str, payload: dict) -> dict:
    client = boto3.client("bedrock-agentcore", region_name=REGION)
    response = client.invoke_agent_runtime(
        agentRuntimeArn=arn,
        runtimeSessionId=SESSION,
        payload=json.dumps(payload).encode(),
        contentType="application/json",
    )
    assert response["contentType"] == "application/json"
    assert response["runtimeSessionId"] == SESSION
    return json.loads(response["response"].read())


@mock_aws
def test_invoke_runs_the_runtime_code_from_s3_on_port_8080():
    arn = _runtime_in_s3("runner", APP)

    first = _invoke(arn, {"prompt": "hi"})
    second = _invoke(arn, {"prompt": "again"})

    assert first["path"] == "/invocations"
    assert first["body"] == {"prompt": "hi"}
    assert first["session"] == SESSION
    assert first["greeting"] == "hello"
    # moto's own settings are not the runtime's business
    assert first["moto_settings"] == []
    # the runtime keeps running between invocations
    assert second["pid"] == first["pid"]
    assert second["body"] == {"prompt": "again"}


@mock_aws
def test_code_in_the_local_code_bucket_is_read_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("MOTO_LAMBDA_LOCAL_CODE_BUCKET", "local-code")
    (tmp_path / "app.py").write_text(APP)
    arn = _create_runtime("local", "local-code", str(tmp_path))

    assert _invoke(arn, {"prompt": "hi"})["body"] == {"prompt": "hi"}


@mock_aws
def test_a_runtime_that_exits_while_starting_is_a_runtime_client_error():
    arn = _runtime_in_s3("exits", EXITS)
    client = boto3.client("bedrock-agentcore", region_name=REGION)

    with pytest.raises(client.exceptions.RuntimeClientError):
        client.invoke_agent_runtime(
            agentRuntimeArn=arn, runtimeSessionId=SESSION, payload=b"{}"
        )


@mock_aws
def test_an_unanswered_ping_fails_the_start_and_stops_the_runtime(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(executor, "INIT_TIMEOUT", 1.0)
    arn = _runtime_in_s3("silent", NEVER_SERVES)
    client = boto3.client("bedrock-agentcore", region_name=REGION)

    with pytest.raises(client.exceptions.RuntimeClientError) as exc:
        client.invoke_agent_runtime(
            agentRuntimeArn=arn, runtimeSessionId=SESSION, payload=b"{}"
        )

    assert "initialization" in exc.value.response["Error"]["Message"]
    (host,) = [h for h in executor._hosts.values() if h.spec.arn == arn]
    assert host.process is not None and host.process.poll() is not None


@mock_aws
def test_a_starting_runtime_does_not_hold_up_another(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(executor, "INIT_TIMEOUT", 30.0)
    slow = _runtime_in_s3("slow", "import time\ntime.sleep(10)\n" + APP)
    fast = _runtime_in_s3("fast", APP)
    threading.Thread(target=_invoke, args=(slow, {}), daemon=True).start()
    deadline = time.time() + 5
    while not any(h.spec.arn == slow for h in executor._hosts.values()):
        assert time.time() < deadline, "the slow runtime did not start"
        time.sleep(0.05)

    started = time.time()
    assert _invoke(fast, {"prompt": "hi"})["body"] == {"prompt": "hi"}
    assert time.time() - started < 8
