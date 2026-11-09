"""A minimal Lambda runtime interface client for Python functions.

The process executor starts it when neither the Lambda base image's bootstrap nor awslambdaric is available.
It polls the Runtime API at AWS_LAMBDA_RUNTIME_API and calls the handler named by _HANDLER, as the official
client does. It runs as a standalone script and must not import moto.
"""

import importlib
import json
import os
import sys
import time
import traceback
import urllib.request
from typing import Any


class LambdaContext:
    def __init__(self, headers: Any) -> None:
        self.aws_request_id = headers["Lambda-Runtime-Aws-Request-Id"]
        self.invoked_function_arn = headers.get(
            "Lambda-Runtime-Invoked-Function-Arn", ""
        )
        self.function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
        self.function_version = os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        self.memory_limit_in_mb = os.environ.get(
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128"
        )
        self.log_group_name = os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME", "")
        self.log_stream_name = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME", "")
        self._deadline_ms = int(headers.get("Lambda-Runtime-Deadline-Ms") or 0)
        client_context = headers.get("Lambda-Runtime-Client-Context")
        self.client_context = json.loads(client_context) if client_context else None
        identity = headers.get("Lambda-Runtime-Cognito-Identity")
        self.identity = json.loads(identity) if identity else None

    def get_remaining_time_in_millis(self) -> int:
        return max(0, self._deadline_ms - int(time.time() * 1000))


def error_body(e: BaseException) -> bytes:
    return json.dumps(
        {
            "errorMessage": str(e),
            "errorType": type(e).__name__,
            "stackTrace": traceback.format_exception(type(e), e, e.__traceback__),
        }
    ).encode()


def post(url: str, body: bytes, error_type: str | None = None) -> None:
    headers = {"Lambda-Runtime-Function-Error-Type": error_type} if error_type else {}
    urllib.request.urlopen(urllib.request.Request(url, body, headers, method="POST"))


def main() -> None:
    api = f"http://{os.environ['AWS_LAMBDA_RUNTIME_API']}/2018-06-01/runtime"
    task_root = os.environ.get("LAMBDA_TASK_ROOT", ".")
    if task_root not in sys.path:
        sys.path.insert(0, task_root)
    module_name, _, func_name = os.environ["_HANDLER"].rpartition(".")
    try:
        handler = getattr(
            importlib.import_module(module_name.replace("/", ".")), func_name
        )
    except Exception as e:
        traceback.print_exc()
        post(f"{api}/init/error", error_body(e), type(e).__name__)
        sys.exit(1)
    while True:
        with urllib.request.urlopen(f"{api}/invocation/next") as r:
            headers, payload = r.headers, r.read()
        request_id = headers["Lambda-Runtime-Aws-Request-Id"]
        if headers.get("Lambda-Runtime-Trace-Id"):
            os.environ["_X_AMZN_TRACE_ID"] = headers["Lambda-Runtime-Trace-Id"]
        try:
            event = json.loads(payload) if payload else {}
            result = handler(event, LambdaContext(headers))
            body = json.dumps(result).encode()
        except Exception as e:
            traceback.print_exc()
            post(
                f"{api}/invocation/{request_id}/error", error_body(e), type(e).__name__
            )
            continue
        post(f"{api}/invocation/{request_id}/response", body)


if __name__ == "__main__":
    main()
