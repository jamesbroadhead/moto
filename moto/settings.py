import json
import os
import pathlib
from functools import lru_cache

from moto.core.config import default_user_config


def is_test_proxy_mode() -> bool:
    return os.environ.get("TEST_PROXY_MODE", "0").lower() == "true"


TEST_SERVER_MODE = os.environ.get("TEST_SERVER_MODE", "0").lower() == "true"
TEST_DECORATOR_MODE = not TEST_SERVER_MODE and not is_test_proxy_mode()

INITIAL_NO_AUTH_ACTION_COUNT = float(
    os.environ.get("INITIAL_NO_AUTH_ACTION_COUNT", float("inf"))
)
DEFAULT_CONTAINER_REGISTRY = os.environ.get("DEFAULT_CONTAINER_REGISTRY", "docker.io")

S3_IGNORE_SUBDOMAIN_BUCKETNAME = os.environ.get(
    "S3_IGNORE_SUBDOMAIN_BUCKETNAME", ""
) in ["1", "true"]

# How many seconds to wait before we "validate" a new certificate in ACM.
ACM_VALIDATION_WAIT = int(os.environ.get("MOTO_ACM_VALIDATION_WAIT", "60"))

EC2_ENABLE_INSTANCE_TYPE_VALIDATION = bool(
    os.environ.get("MOTO_EC2_ENABLE_INSTANCE_TYPE_VALIDATION", False)
)

ENABLE_KEYPAIR_VALIDATION = bool(
    os.environ.get("MOTO_ENABLE_KEYPAIR_VALIDATION", False)
)

ENABLE_AMI_VALIDATION = bool(os.environ.get("MOTO_ENABLE_AMI_VALIDATION", False))

MAX_FORM_MEMORY_SIZE = int(
    os.environ.get("MOTO_MAX_FORM_MEMORY_SIZE", 1024 * 1024 * 50)
)

PRETTIFY_RESPONSES = bool(os.environ.get("MOTO_PRETTIFY_RESPONSES", False))

DISABLE_GLOBAL_CORS = bool(os.environ.get("MOTO_DISABLE_GLOBAL_CORS", False))

# Fully skip test that require docker
SKIP_REQUIRES_DOCKER = bool(os.environ.get("TESTS_SKIP_REQUIRES_DOCKER", False))

LAMBDA_DATA_DIR = os.environ.get("MOTO_LAMBDA_DATA_DIR", "/tmp/data")


def get_sf_execution_history_type() -> str:
    """
    Determines which execution history events `get_execution_history` returns
    :returns: str representing the type of Step Function Execution Type events should be
        returned. Default value is SUCCESS, currently supports (SUCCESS || FAILURE)
    """
    return os.environ.get("SF_EXECUTION_HISTORY_TYPE", "SUCCESS")


def get_s3_custom_endpoints() -> list[str]:
    endpoints = os.environ.get("MOTO_S3_CUSTOM_ENDPOINTS")
    if endpoints:
        return endpoints.split(",")
    return []


def s3_presigned_url_secrets() -> dict[str, str] | None:
    """MOTO_S3_VERIFY_PRESIGNED_URLS: S3 verifies SigV4 presigned URLs when set.

    The value is a JSON object of access key IDs to secret access keys that moto did not issue itself, such as
    static credentials of the clients; `{}` verifies only keys that IAM or STS in moto issued.
    """
    secrets = os.environ.get("MOTO_S3_VERIFY_PRESIGNED_URLS")
    return json.loads(secrets) if secrets is not None else None


S3_UPLOAD_PART_MIN_SIZE = int(os.environ.get("S3_UPLOAD_PART_MIN_SIZE", "5242880"))


def get_s3_default_key_buffer_size() -> int:
    return int(
        os.environ.get(
            "MOTO_S3_DEFAULT_KEY_BUFFER_SIZE", S3_UPLOAD_PART_MIN_SIZE - 1024
        )
    )


def get_s3_default_max_keys() -> int:
    return int(os.environ.get("MOTO_S3_DEFAULT_MAX_KEYS", 1000))


def s3_allow_crossdomain_access() -> bool:
    return os.environ.get("MOTO_S3_ALLOW_CROSSACCOUNT_ACCESS", "true").lower() == "true"


def sqs_enforce_strict_queue_purge_behaviour() -> bool:
    return (
        os.environ.get("MOTO_SQS_ENFORCE_STRICT_QUEUE_PURGE_BEHAVIOUR", "true").lower()
        == "true"
    )


def ec2_load_default_amis() -> bool:
    # True by default - only the value 'false' will return false
    return os.environ.get("MOTO_EC2_LOAD_DEFAULT_AMIS", "true").lower() != "false"


def ecs_new_arn_format() -> bool:
    # True by default - only the value 'false' will return false
    return os.environ.get("MOTO_ECS_NEW_ARN", "true").lower() != "false"


def events_invoke_http() -> bool:
    return os.environ.get("MOTO_EVENTS_INVOKE_HTTP", "false").lower() == "true"


def scheduler_fire() -> bool:
    """Run EventBridge Scheduler schedules and scheduled EventBridge rules: moto invokes their targets when they are due."""
    return os.environ.get("MOTO_SCHEDULER_FIRE", "false").lower() == "true"


def scheduler_tick_seconds() -> float:
    """How often the schedule runner checks for due schedules."""
    return float(os.environ.get("MOTO_SCHEDULER_TICK_SECONDS") or "1")


def allow_unknown_region() -> bool:
    return os.environ.get("MOTO_ALLOW_NONEXISTENT_REGION", "false").lower() == "true"


def lambda_invoke_endpoint() -> str | None:
    """Base URL of a Lambda Invoke API that runs function code, used in place of Docker."""
    return os.environ.get("MOTO_LAMBDA_INVOKE_ENDPOINT") or None


def bedrock_agentcore_invoke_endpoint() -> str | None:
    """Base URL of a server that runs AgentCore runtimes; InvokeAgentRuntime forwards to it.

    Requests go to ``{endpoint}/runtimes/{url-encoded runtime ARN}/invocations?qualifier=...``
    with the payload, content type and session id headers, as AgentCore sends them to the runtime.
    """
    return os.environ.get("MOTO_BEDROCK_AGENTCORE_INVOKE_ENDPOINT") or None


def bedrock_agentcore_executor() -> str | None:
    """MOTO_BEDROCK_AGENTCORE_EXECUTOR: "process" runs each runtime's code as a local process that serves the
    AgentCore runtime contract (/ping, /invocations on port 8080, mapped to a free local port).
    Code in MOTO_LAMBDA_LOCAL_CODE_BUCKET is read in place."""
    return (os.environ.get("MOTO_BEDROCK_AGENTCORE_EXECUTOR") or "").lower() or None


def bedrock_agentcore_runtime_commands() -> dict[str, list[str]]:
    """MOTO_BEDROCK_AGENTCORE_RUNTIME_COMMANDS: JSON mapping runtime patterns (fnmatch) to the Python that runs
    the entry point, as a list or a shell-style string, e.g. {"PYTHON_3_13": "/usr/bin/python3.13"}.
    Default: moto's own Python."""
    import shlex

    commands = json.loads(
        os.environ.get("MOTO_BEDROCK_AGENTCORE_RUNTIME_COMMANDS") or "{}"
    )
    return {
        pattern: shlex.split(cmd) if isinstance(cmd, str) else list(cmd)
        for pattern, cmd in commands.items()
    }


def bedrock_agentcore_aws_endpoint_url() -> str | None:
    """MOTO_BEDROCK_AGENTCORE_AWS_ENDPOINT_URL: the AWS_ENDPOINT_URL the process executor gives runtimes.
    Unset: Moto's own URL. Empty: none, for hosts that route AWS host names to Moto themselves."""
    return os.environ.get("MOTO_BEDROCK_AGENTCORE_AWS_ENDPOINT_URL")


def lambda_event_source_mappings_disabled() -> bool:
    """Leave event source mappings to an external poller: moto then does not invoke functions for them."""
    return (
        os.environ.get("MOTO_LAMBDA_EVENT_SOURCE_MAPPINGS_DISABLED", "false").lower()
        == "true"
    )


def lambda_event_source_polling() -> bool:
    """Poll SQS, Kinesis and DynamoDB Streams event sources in background threads, as Lambda does.

    Without it, moto invokes the function inside the SendMessage/PutRecord/PutItem call that produced the record.
    """
    return (
        os.environ.get("MOTO_LAMBDA_EVENT_SOURCE_POLLING", "false").lower() == "true"
        and not lambda_event_source_mappings_disabled()
    )


def lambda_event_source_mappings_synchronous() -> bool:
    """Whether moto delivers event source mapping records to functions inside the call that produced them."""
    return not (
        lambda_event_source_mappings_disabled() or lambda_event_source_polling()
    )


def glue_job_run_endpoint() -> str | None:
    """URL that runs Glue job runs: moto POSTs each new run there and records the outcome."""
    return os.environ.get("MOTO_GLUE_JOB_RUN_ENDPOINT") or None


def lambda_executor() -> str:
    """MOTO_LAMBDA_EXECUTOR: "docker" (default) runs each invocation in a container;
    "process" runs functions in warm local processes that speak the Lambda Runtime API."""
    return os.environ.get("MOTO_LAMBDA_EXECUTOR", "docker").lower()


def lambda_runtime_commands() -> dict[str, list[str]]:
    """MOTO_LAMBDA_RUNTIME_COMMANDS: JSON mapping runtime patterns (fnmatch) to the bootstrap command
    that starts the runtime, as a list or a shell-style string, e.g. {"java21": "/opt/java21/bootstrap"}."""
    import shlex

    commands = json.loads(os.environ.get("MOTO_LAMBDA_RUNTIME_COMMANDS") or "{}")
    return {
        pattern: shlex.split(cmd) if isinstance(cmd, str) else list(cmd)
        for pattern, cmd in commands.items()
    }


def lambda_process_pool_size() -> int:
    """MOTO_LAMBDA_PROCESS_POOL_SIZE: the most warm workers per function version (default 4).
    A function's reserved concurrency lowers it."""
    return int(os.environ.get("MOTO_LAMBDA_PROCESS_POOL_SIZE", "4"))


def lambda_event_source_free_workers() -> int:
    """MOTO_LAMBDA_EVENT_SOURCE_FREE_WORKERS: workers per function that event source mapping
    invocations leave for other invocations (default 1), so polling cannot starve direct calls."""
    return int(os.environ.get("MOTO_LAMBDA_EVENT_SOURCE_FREE_WORKERS", "1"))


def lambda_local_code_bucket() -> str | None:
    """MOTO_LAMBDA_LOCAL_CODE_BUCKET: code and layers whose S3Bucket has this name are read from
    the local directory (or zip file) named by S3Key, without uploading them.
    Glue job scripts in this bucket are read the same way."""
    return os.environ.get("MOTO_LAMBDA_LOCAL_CODE_BUCKET") or None


def lambda_layer_paths() -> dict[str, str]:
    """MOTO_LAMBDA_LAYER_PATHS: JSON mapping layer version ARN patterns (fnmatch) to local directories
    with their content, for layers moto does not hold, e.g. public AWS layers."""
    return json.loads(os.environ.get("MOTO_LAMBDA_LAYER_PATHS") or "{}")


def lambda_opt_dir() -> str | None:
    """MOTO_LAMBDA_OPT_DIR: a directory, typically /opt, into which the process executor links the
    contents of each function's layers (first layer to provide a path wins), for code that reads /opt."""
    return os.environ.get("MOTO_LAMBDA_OPT_DIR") or None


def lambda_aws_endpoint_url() -> str | None:
    """MOTO_LAMBDA_AWS_ENDPOINT_URL: the AWS_ENDPOINT_URL the process executor gives functions.
    Unset: Moto's own URL. Empty: none, for hosts that route AWS host names to Moto themselves."""
    return os.environ.get("MOTO_LAMBDA_AWS_ENDPOINT_URL")


def lambda_work_dir() -> str:
    """MOTO_LAMBDA_WORK_DIR: where the process executor unpacks code and gives each worker its TMPDIR."""
    import tempfile

    return os.environ.get("MOTO_LAMBDA_WORK_DIR") or os.path.join(
        tempfile.gettempdir(), "moto-lambda"
    )


def glue_job_executor() -> str | None:
    """MOTO_GLUE_JOB_EXECUTOR: "process" runs job runs as local processes, see glue_job_commands."""
    return (os.environ.get("MOTO_GLUE_JOB_EXECUTOR") or "").lower() or None


def glue_job_commands() -> dict[str, list[str]]:
    """MOTO_GLUE_JOB_COMMANDS: JSON mapping a job's Command.Name to the command that runs its script,
    as a list or a shell-style string. Default: pythonshell jobs run with the current interpreter.
    Jobs with other command names keep moto's default behaviour."""
    import shlex
    import sys

    commands = json.loads(
        os.environ.get("MOTO_GLUE_JOB_COMMANDS")
        or json.dumps({"pythonshell": [sys.executable]})
    )
    return {
        name: shlex.split(cmd) if isinstance(cmd, str) else list(cmd)
        for name, cmd in commands.items()
    }


def lambda_stub_ecr() -> bool:
    # Whether to stub or mock ecr backend when deploying image based lambdas.
    # True => don't requiring image presence in moto ecr backend for `create_function`.
    # False => require image presence in moto ecr backend for `create_function`
    return os.environ.get("MOTO_LAMBDA_STUB_ECR", "TRUE").lower() != "false"


def athena_federated_catalogs() -> dict[str, str]:
    """MOTO_ATHENA_FEDERATED_CATALOGS: JSON mapping catalog-name patterns (fnmatch) to PostgreSQL DSNs.

    The SQL engine reads tables of a matching catalog from that database, as a federated query would.
    """
    import json

    return json.loads(os.environ.get("MOTO_ATHENA_FEDERATED_CATALOGS") or "{}")


def athena_sql_engine() -> str | None:
    # "iceberg" runs Athena queries over Glue/Iceberg tables; unset keeps canned results
    engine = os.environ.get("MOTO_ATHENA_SQL_ENGINE", "").lower()
    return engine if engine == "iceberg" else None


def moto_server_port() -> str:
    return os.environ.get("MOTO_PORT") or "5000"


def moto_proxy_port() -> str:
    return os.environ.get("MOTO_PROXY_PORT") or "5005"


@lru_cache
def moto_server_host() -> str:
    # Where functions reach moto, e.g. for custom resource responses
    if os.environ.get("MOTO_HOST"):
        return os.environ["MOTO_HOST"]
    if is_docker():
        return get_docker_host()
    else:
        return "http://host.docker.internal"


def moto_lambda_image() -> str | None:
    return os.environ.get("MOTO_DOCKER_LAMBDA_IMAGE")


def moto_network_name() -> str | None:
    return os.environ.get("MOTO_DOCKER_NETWORK_NAME")


def moto_network_mode() -> str | None:
    return os.environ.get("MOTO_DOCKER_NETWORK_MODE")


def test_server_mode_endpoint() -> str:
    return os.environ.get(
        "TEST_SERVER_MODE_ENDPOINT", f"http://localhost:{moto_server_port()}"
    )


def test_proxy_mode_endpoint() -> str:
    return os.environ.get(
        "TEST_PROXY_MODE_ENDPOINT", f"http://localhost:{moto_proxy_port()}"
    )


def is_docker() -> bool:
    path = pathlib.Path("/proc/self/cgroup")
    return (
        os.path.exists("/.dockerenv")
        or path.is_file()
        and any("docker" in line for line in path.read_text())
    )


def get_docker_host() -> str:
    try:
        cmd = "curl -s --unix-socket /run/docker.sock http://docker/containers/$HOSTNAME/json"
        container_info = os.popen(cmd).read()
        network_settings = json.loads(container_info)["NetworkSettings"]
        network_name = moto_network_name()
        if network_name and network_name in network_settings["Networks"]:
            _ip = network_settings["Networks"][network_name]["IPAddress"]
        else:
            _ip = network_settings["IPAddress"]
            if network_name:
                print(  # noqa
                    f"WARNING - Moto couldn't find network '{network_name}' - defaulting to {_ip}"
                )
        return f"http://{_ip}"
    except Exception as e:  # noqa
        print(  # noqa
            "WARNING - Unable to parse Docker API response. Defaulting to 'host.docker.internal'"
        )
        print(f"{type(e)}::{e}")  # noqa
        return "http://host.docker.internal"


def get_cognito_idp_user_pool_id_strategy() -> str | None:
    return os.environ.get("MOTO_COGNITO_IDP_USER_POOL_ID_STRATEGY")


def get_cognito_idp_user_pool_client_id_strategy() -> str | None:
    return os.environ.get("MOTO_COGNITO_IDP_USER_POOL_CLIENT_ID_STRATEGY")


def get_cognito_idp_user_pool_enable_totp() -> bool:
    return (
        os.environ.get("MOTO_COGNITO_IDP_USER_POOL_ENABLE_TOTP", "false").lower()
        == "true"
    )


def enable_iso_regions() -> bool:
    return os.environ.get("MOTO_ENABLE_ISO_REGIONS", "false").lower() == "true"


def load_iam_aws_managed_policies() -> bool:
    return (
        default_user_config.get("iam", {}).get("load_aws_managed_policies", False)
        is True
        or os.environ.get("MOTO_IAM_LOAD_MANAGED_POLICIES", "").lower() == "true"
    )


#
# NOTE:  Recommend the next major release to set this to True for proper
# default behavior
#
def iot_use_valid_cert() -> bool:
    return default_user_config.get("iot", {}).get("use_valid_cert", False)
