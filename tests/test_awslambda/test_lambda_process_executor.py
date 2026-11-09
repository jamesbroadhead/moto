import base64
import io
import json
import os
import sys
import threading
import time
import zipfile
from pathlib import Path
from unittest import SkipTest
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError

from moto import mock_aws, settings
from moto.awslambda import executor
from moto.awslambda.models import EVENT_SOURCE_HEADER
from moto.awslambda.utils import get_backend

from .utilities import get_role_name

REGION = "us-west-2"
LOCAL_CODE_BUCKET = "local-code"

HANDLER = """import os


def handler(event, context):
    return {"event": event, "pid": os.getpid()}
"""
CONTEXT_HANDLER = """import os


def handler(event, context):
    print("handling", event)
    return {
        "event": event,
        "pid": os.getpid(),
        "request_id": context.aws_request_id,
        "remaining": context.get_remaining_time_in_millis(),
        "trace": os.environ.get("_X_AMZN_TRACE_ID"),
        "env": {k: os.environ.get(k) for k in ("GREETING", "AWS_LAMBDA_FUNCTION_NAME", "AWS_REGION")},
    }
"""
FAILING_IMPORT = 'raise RuntimeError("no config")\n'
FAILING_HANDLER = """def handler(event, context):
    raise ValueError("bad event")
"""
SLOW_HANDLER = """import os
import time


def handler(event, context):
    time.sleep(1)
    open(os.path.join(os.environ["LAMBDA_TASK_ROOT"], "finished"), "w").close()
"""
SLEEPING_HANDLER = """import time


def handler(event, context):
    time.sleep(event["seconds"])
"""
WRITING_HANDLER = """def handler(event, context):
    open(event["path"], "w").write("done")
"""
LAYER_HANDLER = """import greeting


def handler(event, context):
    return greeting.greet()
"""
# A custom runtime that serves `$_HANDLER` through the Lambda Runtime API.
BOOTSTRAP = f"""#!{sys.executable}
import importlib
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])
api = f"http://{{os.environ['AWS_LAMBDA_RUNTIME_API']}}/2018-06-01/runtime"
module, name = os.environ["_HANDLER"].rsplit(".", 1)
try:
    handler = getattr(importlib.import_module(module), name)
except Exception as e:
    error = {{"errorMessage": str(e), "errorType": type(e).__name__}}
    urllib.request.urlopen(f"{{api}}/init/error", json.dumps(error).encode())
    raise SystemExit(1)
while True:
    with urllib.request.urlopen(f"{{api}}/invocation/next") as r:
        request_id, event = r.headers["Lambda-Runtime-Aws-Request-Id"], json.load(r)
    urllib.request.urlopen(f"{{api}}/invocation/{{request_id}}/response", json.dumps(handler(event, None)).encode())
"""
# An external extension that records each event it receives.
EXTENSION = f"""#!{sys.executable}
import json
import os
import urllib.request

api = f"http://{{os.environ['AWS_LAMBDA_RUNTIME_API']}}/2020-01-01/extension"
req = urllib.request.Request(
    f"{{api}}/register",
    json.dumps({{"events": ["INVOKE", "SHUTDOWN"]}}).encode(),
    {{"Lambda-Extension-Name": "recorder"}},
)
ext_id = urllib.request.urlopen(req).headers["Lambda-Extension-Identifier"]
while True:
    req = urllib.request.Request(f"{{api}}/event/next", headers={{"Lambda-Extension-Identifier": ext_id}})
    event = json.load(urllib.request.urlopen(req))
    with open(os.environ["EVENTS_FILE"], "a") as f:
        f.write(event["eventType"] + "\\n")
    if event["eventType"] == "SHUTDOWN":
        break
"""
RUNTIMES = ["python3.12", "provided.al2023"]


@pytest.fixture(autouse=True)
def process_executor(tmp_path, monkeypatch):
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The executor's settings apply to the server process")
    monkeypatch.setenv("MOTO_LAMBDA_EXECUTOR", "process")
    monkeypatch.setenv("MOTO_LAMBDA_WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("MOTO_LAMBDA_LOCAL_CODE_BUCKET", LOCAL_CODE_BUCKET)
    with mock_aws():
        yield
        executor.shutdown()


@pytest.fixture
def client():
    return boto3.client("lambda", REGION)


def write_code(directory: Path, source: str, bootstrap: str = BOOTSTRAP) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "app.py").write_text(source)
    (directory / "bootstrap").write_text(bootstrap)
    (directory / "bootstrap").chmod(0o755)
    return directory


def create(client, code_dir, runtime="python3.12", timeout=10, **kwargs):
    name = kwargs.pop("FunctionName", str(uuid4())[:8])
    client.create_function(
        FunctionName=name,
        Runtime=runtime,
        Role=get_role_name(),
        Handler="app.handler",
        Code={"S3Bucket": LOCAL_CODE_BUCKET, "S3Key": str(code_dir)},
        Timeout=timeout,
        **kwargs,
    )
    return name


def invoke(client, name, event=None, **kwargs):
    resp = client.invoke(
        FunctionName=name, Payload=json.dumps(event or {}), LogType="Tail", **kwargs
    )
    return resp, json.loads(resp["Payload"].read() or b"null")


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_warm_worker_returns_each_result(client, tmp_path, runtime):
    name = create(client, write_code(tmp_path / "code", HANDLER), runtime)

    _, first = invoke(client, name, {"n": 1})
    _, second = invoke(client, name, {"n": 2})

    assert first["event"] == {"n": 1}
    assert second["event"] == {"n": 2}
    assert second["pid"] == first["pid"]


def test_context_environment_and_trace_header(client, tmp_path):
    name = create(
        client,
        write_code(tmp_path / "code", CONTEXT_HANDLER),
        Environment={"Variables": {"GREETING": "hello"}},
        TracingConfig={"Mode": "Active"},
    )

    resp, result = invoke(client, name)

    assert result["env"] == {
        "GREETING": "hello",
        "AWS_LAMBDA_FUNCTION_NAME": name,
        "AWS_REGION": REGION,
    }
    assert 0 < result["remaining"] <= 10_000
    assert result["trace"].startswith("Root=1-") and "Sampled=1" in result["trace"]
    assert resp["ExecutedVersion"] == "$LATEST"
    logs = base64.b64decode(resp["LogResult"]).decode()
    assert f"START RequestId: {result['request_id']}" in logs
    assert "handling {}" in logs
    assert f"REPORT RequestId: {result['request_id']}" in logs


def test_caller_trace_header_is_passed_on(tmp_path):
    client = boto3.client("lambda", REGION)
    name = create(client, write_code(tmp_path / "code", CONTEXT_HANDLER))
    header = (
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    )
    fn = get_backend("123456789012", REGION).get_function(name)

    payload = fn.invoke("{}", {"x-amzn-trace-id": header}, {})

    assert json.loads(payload)["trace"] == header


def test_uploaded_zip(client):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("app.py", HANDLER)
    name = str(uuid4())[:8]
    client.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=get_role_name(),
        Handler="app.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    _, result = invoke(client, name, {"zip": True})

    assert result["event"] == {"zip": True}


def test_handler_error_is_unhandled_function_error(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", FAILING_HANDLER))

    resp, error = invoke(client, name)

    assert resp["FunctionError"] == "Unhandled"
    assert error["errorType"] == "ValueError"
    assert error["errorMessage"] == "bad event"


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_init_error_is_returned_and_retried_on_each_invoke(client, tmp_path, runtime):
    name = create(client, write_code(tmp_path / "code", FAILING_IMPORT), runtime)

    results = [invoke(client, name) for _ in range(2)]

    assert [r["FunctionError"] for r, _ in results] == ["Unhandled"] * 2
    assert [(e["errorType"], e["errorMessage"]) for _, e in results] == [
        ("RuntimeError", "no config")
    ] * 2
    pool = executor.get_pool(get_backend("123456789012", REGION).get_function(name))
    assert pool.started == 2


def test_bootstrap_exit_during_init_is_runtime_exit_error(client, tmp_path):
    code = write_code(tmp_path / "code", HANDLER, bootstrap="#!/bin/sh\nexit 3\n")
    name = create(client, code, "provided.al2023")

    errors = [invoke(client, name)[1] for _ in range(2)]

    assert (
        errors
        == [
            {
                "errorMessage": "Runtime exited with error: exit status 3",
                "errorType": "Runtime.ExitError",
            }
        ]
        * 2
    )


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_timed_out_handler_never_finishes(client, tmp_path, runtime):
    code = write_code(tmp_path / "code", SLOW_HANDLER)
    name = create(client, code, runtime, timeout=1)
    fn = get_backend("123456789012", REGION).get_function(name)
    fn.timeout = 0.5  # below the API's minimum of one second

    errors = [invoke(client, name)[1] for _ in range(2)]
    time.sleep(1)

    assert [e["errorType"] for e in errors] == ["Sandbox.Timedout"] * 2
    assert all("Task timed out after 0.50 seconds" in e["errorMessage"] for e in errors)
    assert not (code / "finished").exists()


def test_unknown_runtime_needs_a_command(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", HANDLER), "nodejs22.x")

    resp, error = invoke(client, name)

    assert resp["FunctionError"] == "Unhandled"
    assert error["errorType"] == "Runtime.InvalidEntrypoint"
    assert "MOTO_LAMBDA_RUNTIME_COMMANDS" in error["errorMessage"]


def test_runtime_commands_setting(client, tmp_path, monkeypatch):
    runtime = write_code(tmp_path / "runtime", "", bootstrap=BOOTSTRAP)
    monkeypatch.setenv(
        "MOTO_LAMBDA_RUNTIME_COMMANDS",
        json.dumps({"nodejs*": [str(runtime / "bootstrap")]}),
    )
    code = tmp_path / "code"
    code.mkdir()
    (code / "app.py").write_text(HANDLER)
    name = create(client, code, "nodejs22.x")

    _, result = invoke(client, name, {"custom": 1})

    assert result["event"] == {"custom": 1}


def test_layers_are_importable(client, tmp_path):
    local_layer = tmp_path / "layer" / "python"
    local_layer.mkdir(parents=True)
    (local_layer / "greeting.py").write_text("def greet():\n    return 'local'\n")
    local = client.publish_layer_version(
        LayerName="local",
        Content={"S3Bucket": LOCAL_CODE_BUCKET, "S3Key": str(tmp_path / "layer")},
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("python/greeting.py", "def greet():\n    return 'zipped'\n")
    zipped = client.publish_layer_version(
        LayerName="zipped", Content={"ZipFile": buf.getvalue()}
    )
    code = write_code(tmp_path / "code", LAYER_HANDLER)

    first = create(client, code, Layers=[local["LayerVersionArn"]])
    second = create(client, code, Layers=[zipped["LayerVersionArn"]])

    assert invoke(client, first)[1] == "local"
    assert invoke(client, second)[1] == "zipped"


def test_layer_paths_setting_provides_public_layers(client, tmp_path, monkeypatch):
    public = tmp_path / "powertools" / "python"
    public.mkdir(parents=True)
    (public / "greeting.py").write_text("def greet():\n    return 'public'\n")
    monkeypatch.setenv(
        "MOTO_LAMBDA_LAYER_PATHS",
        json.dumps({"*:layer:AWSLambdaPowertools*": str(tmp_path / "powertools")}),
    )
    arn = "arn:aws:lambda:us-west-2:017000801446:layer:AWSLambdaPowertoolsPythonV3:1"
    name = create(client, write_code(tmp_path / "code", LAYER_HANDLER), Layers=[arn])

    assert invoke(client, name)[1] == "public"


def test_extensions_receive_invoke_events(client, tmp_path):
    layer = tmp_path / "layer"
    (layer / "extensions").mkdir(parents=True)
    (layer / "extensions" / "recorder").write_text(EXTENSION)
    (layer / "extensions" / "recorder").chmod(0o755)
    version = client.publish_layer_version(
        LayerName="ext", Content={"S3Bucket": LOCAL_CODE_BUCKET, "S3Key": str(layer)}
    )
    events = tmp_path / "events"
    name = create(
        client,
        write_code(tmp_path / "code", HANDLER),
        Layers=[version["LayerVersionArn"]],
        Environment={"Variables": {"EVENTS_FILE": str(events)}},
    )

    invoke(client, name)
    _, result = invoke(client, name)

    assert result["event"] == {}
    assert events.read_text().splitlines() == ["INVOKE", "INVOKE"]


def test_asynchronous_invocation(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", WRITING_HANDLER))
    target = tmp_path / "written"

    resp = client.invoke(
        FunctionName=name,
        InvocationType="Event",
        Payload=json.dumps({"path": str(target)}),
    )

    assert resp["StatusCode"] == 202
    for _ in range(100):
        if target.exists():
            break
        time.sleep(0.1)
    assert target.read_text() == "done"


def test_dry_run_does_not_execute(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", WRITING_HANDLER))

    resp = client.invoke(FunctionName=name, InvocationType="DryRun")

    assert resp["StatusCode"] == 204
    assert not list(Path(os.environ["MOTO_LAMBDA_WORK_DIR"]).glob("tmp/*"))


def test_reserved_concurrency_zero_throttles(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", HANDLER))
    client.put_function_concurrency(FunctionName=name, ReservedConcurrentExecutions=0)

    with pytest.raises(ClientError) as exc:
        invoke(client, name)

    assert exc.value.response["Error"]["Code"] == "TooManyRequestsException"


def test_reserved_concurrency_limits_the_pool(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", HANDLER))
    client.put_function_concurrency(FunctionName=name, ReservedConcurrentExecutions=1)

    pool = executor.get_pool(get_backend("123456789012", REGION).get_function(name))

    assert pool.size == 1


def test_configuration_change_starts_new_workers(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", CONTEXT_HANDLER))
    _, before = invoke(client, name)

    client.update_function_configuration(
        FunctionName=name, Environment={"Variables": {"GREETING": "changed"}}
    )
    _, after = invoke(client, name)

    assert after["env"]["GREETING"] == "changed"
    assert after["pid"] != before["pid"]


def test_published_version_runs_its_own_code(client, tmp_path):
    name = create(
        client,
        write_code(tmp_path / "code", CONTEXT_HANDLER),
        Environment={"Variables": {"GREETING": "v1"}},
    )
    client.publish_version(FunctionName=name)
    client.update_function_configuration(
        FunctionName=name, Environment={"Variables": {"GREETING": "latest"}}
    )

    resp, v1 = invoke(client, name, Qualifier="1")
    _, latest = invoke(client, name)

    assert v1["env"]["GREETING"] == "v1"
    assert resp["ExecutedVersion"] == "1"
    assert latest["env"]["GREETING"] == "latest"


def test_event_sources_leave_a_worker_for_other_calls(tmp_path, monkeypatch):
    monkeypatch.setenv("MOTO_LAMBDA_PROCESS_POOL_SIZE", "2")
    client = boto3.client("lambda", REGION)
    name = create(client, write_code(tmp_path / "code", SLEEPING_HANDLER))
    fn = get_backend("123456789012", REGION).get_function(name)
    finished = []

    def call(label: str, seconds: float, event_source: bool) -> None:
        headers = {EVENT_SOURCE_HEADER: "true"} if event_source else {}
        response_headers: dict[str, str] = {}
        fn.invoke(json.dumps({"seconds": seconds}), headers, response_headers)
        assert "x-amz-function-error" not in response_headers
        finished.append(label)

    event_sources = [
        threading.Thread(target=call, args=(f"event source {n}", 2, True))
        for n in (1, 2)
    ]
    for t in event_sources:
        t.start()
    time.sleep(0.5)
    call("synchronous", 0, False)
    for t in event_sources:
        t.join()

    assert finished[0] == "synchronous"
    assert sorted(finished[1:]) == ["event source 1", "event source 2"]


def test_logs_are_written_to_cloudwatch(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", CONTEXT_HANDLER))
    invoke(client, name, {"logged": True})

    logs = boto3.client("logs", REGION)
    events = logs.filter_log_events(logGroupName=f"/aws/lambda/{name}")["events"]

    assert any("handling {'logged': True}" in e["message"] for e in events)


def test_delete_function_stops_its_workers(client, tmp_path):
    name = create(client, write_code(tmp_path / "code", HANDLER))
    invoke(client, name)
    pool = executor.get_pool(get_backend("123456789012", REGION).get_function(name))
    [worker] = pool.idle

    client.delete_function(FunctionName=name)

    assert not worker.alive


ENV_HANDLER = """import os


def handler(event, context):
    return {k: os.environ.get(k) for k in event["names"]}
"""


def test_moto_settings_stay_out_of_the_function(client, tmp_path, monkeypatch):
    monkeypatch.setenv("MOTO_ATHENA_FEDERATED_CATALOGS", '{"x": "password=secret"}')
    monkeypatch.setenv("MOTO_LAMBDA_AWS_ENDPOINT_URL", "")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    name = create(client, write_code(tmp_path / "code", ENV_HANDLER))

    _, result = invoke(
        client,
        name,
        {
            "names": [
                "MOTO_ATHENA_FEDERATED_CATALOGS",
                "MOTO_LAMBDA_EXECUTOR",
                "AWS_ENDPOINT_URL",
                "MOTO_PORT",
            ]
        },
    )

    assert result == {
        "MOTO_ATHENA_FEDERATED_CATALOGS": None,
        "MOTO_LAMBDA_EXECUTOR": None,
        "AWS_ENDPOINT_URL": None,
        "MOTO_PORT": "5000",
    }
