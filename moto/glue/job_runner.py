"""Runs Glue job runs as local processes (MOTO_GLUE_JOB_EXECUTOR=process).

The command for the job's Command.Name (MOTO_GLUE_JOB_COMMANDS) runs the job's script with the run's
arguments, as Glue passes them: ``--JOB_NAME <name> --JOB_RUN_ID <id> --<key> <value> ...``.
Output goes to the CloudWatch log groups Glue uses for Python shell jobs.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from moto import settings

OUTPUT_LOG_GROUP = "/aws-glue/python-jobs/output"
ERROR_LOG_GROUP = "/aws-glue/python-jobs/error"
# Glue's default timeout, in minutes
DEFAULT_TIMEOUT = 2880
SECONDS_PER_MINUTE = 60


def command_for(command: dict[str, Any] | None) -> list[str] | None:
    if settings.glue_job_executor() != "process":
        return None
    return settings.glue_job_commands().get((command or {}).get("Name") or "")


def script_path(location: str, account_id: str, partition: str) -> str:
    """A local copy of the script; a location moto cannot resolve is passed on unchanged."""
    if not location.startswith("s3://"):
        return location.removeprefix("file://")
    bucket, _, key = location[len("s3://") :].partition("/")
    if bucket == settings.lambda_local_code_bucket():
        return key
    from moto.s3.models import s3_backends

    try:
        obj = s3_backends[account_id][partition].get_object(bucket, key)
    except Exception:
        return location
    directory = Path(tempfile.mkdtemp(prefix="moto-glue-"))
    path = directory / (Path(key).name or "script.py")
    path.write_bytes(obj.value)  # type: ignore[union-attr]
    return str(path)


def arguments(job_name: str, run_id: str, args: dict[str, str]) -> list[str]:
    argv = ["--JOB_NAME", job_name, "--JOB_RUN_ID", run_id]
    for key, value in args.items():
        argv += [key if key.startswith("--") else f"--{key}", str(value)]
    return argv


def _save_logs(
    account_id: str, region: str, group: str, stream: str, output: str
) -> None:
    from moto.core.utils import unix_time_millis
    from moto.logs.models import logs_backends

    if not output:
        return
    backend = logs_backends[account_id][region]
    backend.ensure_log_group(group)
    if stream not in backend.groups[group].streams:
        backend.create_log_stream(group, stream)
    now = unix_time_millis()
    backend.put_log_events(
        group,
        stream,
        [{"timestamp": now, "message": line} for line in output.splitlines()],
    )


def run(
    command: list[str],
    job_command: dict[str, Any],
    job_name: str,
    run_id: str,
    args: dict[str, str],
    timeout_minutes: int | None,
    account_id: str,
    region: str,
    partition: str,
) -> tuple[str, str | None]:
    """Runs the job to completion; returns its JobRunState and ErrorMessage."""
    script = script_path(job_command.get("ScriptLocation") or "", account_id, partition)
    env = dict(os.environ)
    env.update({"AWS_REGION": region, "AWS_DEFAULT_REGION": region})
    if not settings.is_test_proxy_mode():
        host = os.environ.get("MOTO_HOST") or "http://localhost"
        env.setdefault("AWS_ENDPOINT_URL", f"{host}:{settings.moto_server_port()}")
    timeout = (timeout_minutes or DEFAULT_TIMEOUT) * SECONDS_PER_MINUTE
    try:
        proc = subprocess.run(
            [*command, script, *arguments(job_name, run_id, args)],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        _save_logs(account_id, region, ERROR_LOG_GROUP, run_id, _text(e.stderr))
        return "TIMEOUT", f"Job run timed out after {timeout_minutes} minutes"
    except OSError as e:
        return "FAILED", str(e)
    _save_logs(account_id, region, OUTPUT_LOG_GROUP, run_id, proc.stdout)
    _save_logs(account_id, region, ERROR_LOG_GROUP, run_id, proc.stderr)
    if proc.returncode == 0:
        return "SUCCEEDED", None
    lines = [line for line in proc.stderr.splitlines() if line.strip()]
    return "FAILED", (lines[-1] if lines else f"exit status {proc.returncode}")[:2000]


def _text(value: Any) -> str:
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else value or ""
