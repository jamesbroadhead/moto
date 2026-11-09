import json
import sys
import time
from unittest import SkipTest

import boto3
import pytest

from moto import mock_aws, settings
from moto.glue import job_runner

REGION = "us-east-1"
SCRIPT = """import json
import sys

args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
print("output", json.dumps(args))
with open(args["--OUT"], "w") as f:
    json.dump(args, f)
if args.get("--FAIL"):
    raise RuntimeError(args["--FAIL"])
"""


@pytest.fixture(autouse=True)
def process_executor(monkeypatch):
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The executor's settings apply to the server process")
    monkeypatch.setenv("MOTO_GLUE_JOB_EXECUTOR", "process")
    with mock_aws():
        yield


@pytest.fixture
def glue():
    return boto3.client("glue", REGION)


def create_job(glue, script_location, name="job", command="pythonshell", **kwargs):
    glue.create_job(
        Name=name,
        Role="role",
        Command={"Name": command, "ScriptLocation": script_location},
        **kwargs,
    )
    return name


def wait(glue, name, run_id):
    for _ in range(200):
        run = glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]
        if run["JobRunState"] not in ("STARTING", "RUNNING"):
            return run
        time.sleep(0.05)
    raise AssertionError("job run did not finish")


def test_python_shell_job_runs_script_from_s3(glue, tmp_path):
    s3 = boto3.client("s3", REGION)
    s3.create_bucket(Bucket="scripts")
    s3.put_object(Bucket="scripts", Key="jobs/script.py", Body=SCRIPT.encode())
    out = tmp_path / "out.json"
    name = create_job(
        glue,
        "s3://scripts/jobs/script.py",
        DefaultArguments={"--OUT": str(out), "--A": "default"},
    )

    run_id = glue.start_job_run(JobName=name, Arguments={"--A": "run"})["JobRunId"]
    run = wait(glue, name, run_id)

    assert run["JobRunState"] == "SUCCEEDED"
    assert json.loads(out.read_text()) == {
        "--JOB_NAME": name,
        "--JOB_RUN_ID": run_id,
        "--OUT": str(out),
        "--A": "run",
    }
    logs = boto3.client("logs", REGION).get_log_events(
        logGroupName=job_runner.OUTPUT_LOG_GROUP, logStreamName=run_id
    )
    assert logs["events"][0]["message"].startswith("output ")


def test_failed_script_records_the_error(glue, tmp_path):
    script = tmp_path / "script.py"
    script.write_text(SCRIPT)
    name = create_job(glue, str(script))

    run_id = glue.start_job_run(
        JobName=name, Arguments={"--OUT": str(tmp_path / "out"), "--FAIL": "boom"}
    )["JobRunId"]
    run = wait(glue, name, run_id)

    assert run["JobRunState"] == "FAILED"
    assert run["ErrorMessage"] == "RuntimeError: boom"


def test_script_in_local_code_bucket(glue, tmp_path, monkeypatch):
    monkeypatch.setenv("MOTO_LAMBDA_LOCAL_CODE_BUCKET", "local-code")
    script = tmp_path / "script.py"
    script.write_text(SCRIPT)
    out = tmp_path / "out.json"
    name = create_job(glue, f"s3://local-code/{script}")

    run_id = glue.start_job_run(JobName=name, Arguments={"--OUT": str(out)})["JobRunId"]

    assert wait(glue, name, run_id)["JobRunState"] == "SUCCEEDED"
    assert out.exists()


def test_timeout(glue, tmp_path, monkeypatch):
    monkeypatch.setattr(job_runner, "SECONDS_PER_MINUTE", 0.2)
    script = tmp_path / "script.py"
    script.write_text("import time\ntime.sleep(5)\n")
    name = create_job(glue, str(script), Timeout=1)

    run_id = glue.start_job_run(JobName=name)["JobRunId"]

    assert wait(glue, name, run_id)["JobRunState"] == "TIMEOUT"


def test_command_setting_maps_other_job_types(glue, tmp_path, monkeypatch):
    runner = tmp_path / "runner.py"
    runner.write_text(
        "import sys, json\njson.dump(sys.argv[1:], open(sys.argv[-1], 'w'))\n"
    )
    monkeypatch.setenv(
        "MOTO_GLUE_JOB_COMMANDS", json.dumps({"glueetl": [sys.executable, str(runner)]})
    )
    out = tmp_path / "out.json"
    name = create_job(glue, "s3://missing/run_sql.py", command="glueetl")

    run_id = glue.start_job_run(JobName=name, Arguments={"--OUT": str(out)})["JobRunId"]

    assert wait(glue, name, run_id)["JobRunState"] == "SUCCEEDED"
    assert json.loads(out.read_text()) == [
        "s3://missing/run_sql.py",
        "--JOB_NAME",
        name,
        "--JOB_RUN_ID",
        run_id,
        "--OUT",
        str(out),
    ]


def test_unmapped_job_types_keep_default_behaviour(glue):
    name = create_job(glue, "s3://missing/script.py", command="glueetl")

    run_id = glue.start_job_run(JobName=name)["JobRunId"]

    run = glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]
    assert run["JobRunState"] in ("STARTING", "RUNNING", "SUCCEEDED")
    assert "ErrorMessage" not in run
