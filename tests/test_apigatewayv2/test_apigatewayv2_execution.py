import base64
import json
from collections.abc import Callable
from typing import Any
from unittest import SkipTest, mock

import boto3
import pytest
import requests
from werkzeug.test import Client

from moto import mock_aws, settings
from moto.awslambda_simple.models import LambdaSimpleBackend
from moto.moto_server.werkzeug_app import (
    DomainDispatcherApplication,
    create_backend_app,
)
from tests import DEFAULT_ACCOUNT_ID

REGION = "us-east-1"
FUNCTION = f"arn:aws:lambda:{REGION}:{DEFAULT_ACCOUNT_ID}:function:handler"
AUTHORIZER = f"arn:aws:lambda:{REGION}:{DEFAULT_ACCOUNT_ID}:function:authorizer"


def invocation_uri(function_arn: str) -> str:
    return f"arn:aws:apigateway:{REGION}:lambda:path/2015-03-31/functions/{function_arn}/invocations"


class Lambdas:
    """Stands in for the functions behind Moto's simple Lambda backend, and records their events."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.handlers: dict[str, Callable[[dict[str, Any]], Any]] = {}

    def invoke(
        self,
        function_name: str,
        qualifier: Any,
        body: str,
        headers: Any,
        response_headers: dict[str, str],
    ) -> bytes:
        name = function_name.split(":")[-1]
        event = json.loads(body)
        self.events.append((name, event))
        handler = self.handlers.get(name, lambda e: {"statusCode": 200, "body": ""})
        try:
            return json.dumps(handler(event)).encode()
        except Exception as e:
            response_headers["x-amz-function-error"] = "Unhandled"
            return json.dumps({"errorMessage": str(e)}).encode()

    def event(self, name: str = "handler") -> dict[str, Any]:
        return [e for n, e in self.events if n == name][-1]


@pytest.fixture
def lambdas():
    if settings.TEST_SERVER_MODE:
        raise SkipTest(
            "Sends requests to execute-api hosts, which only the decorator intercepts"
        )
    functions = Lambdas()
    with mock_aws(config={"lambda": {"use_docker": False}}):
        with mock.patch.object(LambdaSimpleBackend, "invoke", functions.invoke):
            yield functions


@pytest.fixture
def client():
    return boto3.client("apigatewayv2", region_name=REGION)


def create_api(
    client: Any,
    route_keys: list[str],
    version: str = "2.0",
    stage: str = "$default",
    stage_variables: dict[str, str] | None = None,
    **route_kwargs: Any,
) -> str:
    api_id = client.create_api(Name="api", ProtocolType="HTTP")["ApiId"]
    integration_id = client.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=invocation_uri(FUNCTION),
        PayloadFormatVersion=version,
    )["IntegrationId"]
    for key in route_keys:
        client.create_route(
            ApiId=api_id,
            RouteKey=key,
            Target=f"integrations/{integration_id}",
            **route_kwargs,
        )
    client.create_stage(
        ApiId=api_id, StageName=stage, StageVariables=stage_variables or {}
    )
    return api_id


def url(api_id: str, path: str) -> str:
    return f"https://{api_id}.execute-api.{REGION}.amazonaws.com{path}"


ROUTES = [
    "GET /items/latest",
    "GET /items/{id}",
    "ANY /items/{proxy+}",
    "ANY /orders/{id}",
    "DELETE /orders/{id}",
    "$default",
]


@pytest.mark.parametrize(
    ("method", "path", "route_key", "path_parameters"),
    [
        ("GET", "/items/latest", "GET /items/latest", None),
        ("GET", "/items/42", "GET /items/{id}", {"id": "42"}),
        ("GET", "/items/42/history", "ANY /items/{proxy+}", {"proxy": "42/history"}),
        ("GET", "/items/", "ANY /items/{proxy+}", {"proxy": ""}),
        ("DELETE", "/orders/7", "DELETE /orders/{id}", {"id": "7"}),
        ("GET", "/orders/7", "ANY /orders/{id}", {"id": "7"}),
        ("GET", "/unknown", "$default", None),
    ],
    ids=[
        "literal-over-parameter",
        "parameter-over-greedy",
        "greedy-for-deeper-path",
        "greedy-for-empty-tail",
        "method-over-any",
        "any-for-other-method",
        "default-route-for-no-match",
    ],
)
def test_request_reaches_most_specific_route(
    lambdas, client, method, path, route_key, path_parameters
):
    api_id = create_api(client, ROUTES)

    resp = requests.request(method, url(api_id, path))

    assert resp.status_code == 200
    event = lambdas.event()
    assert event["routeKey"] == route_key
    assert event["rawPath"] == path
    assert event.get("pathParameters") == path_parameters
    assert event["requestContext"]["apiId"] == api_id
    assert event["requestContext"]["http"]["method"] == method


def test_no_matching_route_is_not_found(lambdas, client):
    api_id = create_api(client, ["GET /items"])

    resp = requests.get(url(api_id, "/other"))

    assert resp.status_code == 404
    assert resp.json() == {"message": "Not Found"}
    assert "Apigw-Requestid" in resp.headers
    assert lambdas.events == []


def test_unknown_api_is_not_found(lambdas):
    resp = requests.get(url("unknown1", "/items"))

    assert resp.status_code == 404
    assert resp.json() == {"message": "Not Found"}


def test_payload_2_0(lambdas, client):
    api_id = create_api(client, ["POST /items/{id}"], stage_variables={"env": "test"})

    requests.post(
        url(api_id, "/items/1?status=open&status=closed&page=2"),
        json={"a": 1},
        headers={"Cookie": "a=1; b=2", "X-Thing": "x"},
    )

    event = lambdas.event()
    assert event["version"] == "2.0"
    assert event["rawQueryString"] == "status=open&status=closed&page=2"
    assert event["queryStringParameters"] == {"status": "open,closed", "page": "2"}
    assert event["cookies"] == ["a=1", "b=2"]
    assert "cookie" not in event["headers"]
    assert event["headers"]["x-thing"] == "x"
    assert event["headers"]["content-type"] == "application/json"
    assert event["body"] == '{"a": 1}'
    assert event["isBase64Encoded"] is False
    assert event["stageVariables"] == {"env": "test"}
    context = event["requestContext"]
    assert context["accountId"] == DEFAULT_ACCOUNT_ID
    assert context["domainName"] == f"{api_id}.execute-api.{REGION}.amazonaws.com"
    assert context["domainPrefix"] == api_id
    assert context["stage"] == "$default"
    assert context["routeKey"] == "POST /items/{id}"
    assert context["http"]["path"] == "/items/1"
    assert isinstance(context["timeEpoch"], int)


def test_binary_body_is_base64_encoded(lambdas, client):
    api_id = create_api(client, ["POST /upload"])

    requests.post(
        url(api_id, "/upload"),
        data=b"\x00\x01",
        headers={"Content-Type": "application/octet-stream"},
    )

    event = lambdas.event()
    assert event["isBase64Encoded"] is True
    assert base64.b64decode(event["body"]) == b"\x00\x01"


def test_payload_1_0(lambdas, client):
    api_id = create_api(client, ["GET /items/{id}"], version="1.0")

    resp = requests.get(
        url(api_id, "/items/1?status=open&status=closed"), headers={"X-Thing": "x"}
    )

    assert resp.status_code == 200
    event = lambdas.event()
    assert event["version"] == "1.0"
    assert event["resource"] == "/items/{id}"
    assert event["path"] == "/items/1"
    assert event["httpMethod"] == "GET"
    assert event["headers"]["X-Thing"] == "x"
    assert event["multiValueHeaders"]["X-Thing"] == ["x"]
    assert event["queryStringParameters"] == {"status": "closed"}
    assert event["multiValueQueryStringParameters"] == {"status": ["open", "closed"]}
    assert event["pathParameters"] == {"id": "1"}
    assert event["stageVariables"] is None
    assert event["body"] is None
    assert event["requestContext"]["resourcePath"] == "/items/{id}"
    assert event["requestContext"]["httpMethod"] == "GET"


def test_lambda_response_2_0(lambdas, client):
    api_id = create_api(client, ["GET /r"])
    lambdas.handlers["handler"] = lambda e: {
        "statusCode": 201,
        "headers": {"X-One": "1", "Content-Type": "text/plain"},
        "multiValueHeaders": {"X-Many": ["a", "b"]},
        "cookies": ["c1=1", "c2=2"],
        "body": base64.b64encode(b"hello").decode(),
        "isBase64Encoded": True,
    }

    resp = requests.get(url(api_id, "/r"))

    assert resp.status_code == 201
    assert resp.content == b"hello"
    assert resp.headers["X-One"] == "1"
    assert resp.headers["Content-Type"] == "text/plain"
    assert resp.headers["X-Many"] == "a, b"
    assert "c1=1" in resp.headers["Set-Cookie"]


def test_lambda_response_2_0_without_status_code_is_the_body(lambdas, client):
    api_id = create_api(client, ["GET /r"])
    lambdas.handlers["handler"] = lambda e: {"hello": "world"}

    resp = requests.get(url(api_id, "/r"))

    assert resp.status_code == 200
    assert resp.headers["Content-Type"] == "application/json"
    assert resp.json() == {"hello": "world"}


def test_lambda_response_1_0_needs_a_status_code(lambdas, client):
    api_id = create_api(client, ["GET /r"], version="1.0")
    lambdas.handlers["handler"] = lambda e: {"hello": "world"}

    resp = requests.get(url(api_id, "/r"))

    assert resp.status_code == 500
    assert resp.json() == {"message": "Internal Server Error"}


def test_lambda_response_1_0(lambdas, client):
    api_id = create_api(client, ["GET /r"], version="1.0")
    lambdas.handlers["handler"] = lambda e: {
        "statusCode": 404,
        "headers": {"X-One": "1"},
        "body": "missing",
    }

    resp = requests.get(url(api_id, "/r"))

    assert resp.status_code == 404
    assert resp.text == "missing"
    assert resp.headers["X-One"] == "1"


def test_function_error_is_internal_server_error(lambdas, client):
    api_id = create_api(client, ["GET /r"])

    def fail(event: Any) -> Any:
        raise ValueError("boom")

    lambdas.handlers["handler"] = fail

    resp = requests.get(url(api_id, "/r"))

    assert resp.status_code == 500
    assert resp.json() == {"message": "Internal Server Error"}


def test_simple_lambda_backend_is_invoked():
    if settings.TEST_SERVER_MODE:
        raise SkipTest(
            "Sends requests to execute-api hosts, which only the decorator intercepts"
        )
    with mock_aws(config={"lambda": {"use_docker": False}}):
        client = boto3.client("apigatewayv2", region_name=REGION)
        api_id = create_api(client, ["GET /r"])
        requests.post(
            "http://motoapi.amazonaws.com/moto-api/static/lambda-simple/response",
            json={"results": [json.dumps({"statusCode": 202, "body": "queued"})]},
        )

        resp = requests.get(url(api_id, "/r"))
        assert (resp.status_code, resp.text) == (202, "queued")

        # Without a queued result the simple backend answers with the event itself
        resp = requests.get(url(api_id, "/r"))
        assert resp.status_code == 200
        assert resp.json()["routeKey"] == "GET /r"


def test_named_stage_is_the_first_path_segment(lambdas, client):
    api_id = create_api(
        client, ["GET /items"], stage="dev", stage_variables={"fn": "named"}
    )

    assert requests.get(url(api_id, "/items")).status_code == 404

    resp = requests.get(url(api_id, "/dev/items"))

    assert resp.status_code == 200
    event = lambdas.event()
    assert event["requestContext"]["stage"] == "dev"
    assert event["rawPath"] == "/dev/items"
    assert event["stageVariables"] == {"fn": "named"}


def test_stage_variables_in_integration_uri(lambdas, client):
    api_id = client.create_api(Name="api", ProtocolType="HTTP")["ApiId"]
    integration_id = client.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=invocation_uri(
            f"arn:aws:lambda:{REGION}:{DEFAULT_ACCOUNT_ID}:function:${{stageVariables.fn}}"
        ),
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    client.create_route(
        ApiId=api_id, RouteKey="GET /r", Target=f"integrations/{integration_id}"
    )
    client.create_stage(
        ApiId=api_id, StageName="$default", StageVariables={"fn": "other"}
    )

    requests.get(url(api_id, "/r"))

    assert [name for name, _ in lambdas.events] == ["other"]


def test_request_parameter_mapping(lambdas, client):
    api_id = create_api(client, ["GET /items/{id}"])
    integration_id = client.get_integrations(ApiId=api_id)["Items"][0]["IntegrationId"]
    client.update_integration(
        ApiId=api_id,
        IntegrationId=integration_id,
        RequestParameters={
            "overwrite:header.x-item": "item-$request.path.id",
            "remove:header.x-secret": "''",
            "append:querystring.source": "$context.stage",
        },
    )

    requests.get(url(api_id, "/items/9"), headers={"X-Secret": "s"})

    event = lambdas.event()
    assert event["headers"]["x-item"] == "item-9"
    assert "x-secret" not in event["headers"]
    assert event["queryStringParameters"] == {"source": "$default"}


def create_lambda_authorizer(client: Any, api_id: str, **kwargs: Any) -> str:
    params: dict[str, Any] = {
        "ApiId": api_id,
        "AuthorizerType": "REQUEST",
        "AuthorizerUri": invocation_uri(AUTHORIZER),
        "IdentitySource": ["$request.header.Authorization"],
        "Name": "auth",
        "AuthorizerPayloadFormatVersion": "2.0",
    }
    params.update(kwargs)
    return client.create_authorizer(**params)["AuthorizerId"]


def create_authorized_api(client: Any, version: str = "2.0", **kwargs: Any) -> str:
    api_id = create_api(client, [], version=version)
    authorizer_id = create_lambda_authorizer(client, api_id, **kwargs)
    integration_id = client.get_integrations(ApiId=api_id)["Items"][0]["IntegrationId"]
    client.create_route(
        ApiId=api_id,
        RouteKey="GET /items/{id}",
        Target=f"integrations/{integration_id}",
        AuthorizationType="CUSTOM",
        AuthorizerId=authorizer_id,
    )
    return api_id


def test_simple_lambda_authorizer(lambdas, client):
    api_id = create_authorized_api(client, EnableSimpleResponses=True)
    lambdas.handlers["authorizer"] = lambda e: {
        "isAuthorized": e["headers"]["authorization"] == "good",
        "context": {"user": "u1"},
    }

    denied = requests.get(url(api_id, "/items/1"), headers={"Authorization": "bad"})
    allowed = requests.get(url(api_id, "/items/1"), headers={"Authorization": "good"})

    assert denied.status_code == 403
    assert denied.json() == {"message": "Forbidden"}
    assert allowed.status_code == 200
    auth_event = lambdas.event("authorizer")
    assert auth_event["type"] == "REQUEST"
    assert auth_event["identitySource"] == ["good"]
    assert auth_event["routeArn"] == (
        f"arn:aws:execute-api:{REGION}:{DEFAULT_ACCOUNT_ID}:{api_id}/$default/GET/items/1"
    )
    assert auth_event["routeKey"] == "GET /items/{id}"
    assert lambdas.event()["requestContext"]["authorizer"] == {"lambda": {"user": "u1"}}


def test_lambda_authorizer_needs_its_identity_source(lambdas, client):
    api_id = create_authorized_api(client, EnableSimpleResponses=True)

    resp = requests.get(url(api_id, "/items/1"))

    assert resp.status_code == 401
    assert resp.json() == {"message": "Unauthorized"}
    assert lambdas.events == []


def test_iam_policy_lambda_authorizer(lambdas, client):
    api_id = create_authorized_api(client, AuthorizerPayloadFormatVersion="1.0")
    lambdas.handlers["authorizer"] = lambda e: {
        "principalId": "user-1",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": "Allow",
                    "Resource": f"arn:aws:execute-api:*:*:{api_id}/*/GET/items/1",
                }
            ],
        },
        "context": {"tenant": "t1"},
    }

    allowed = requests.get(url(api_id, "/items/1"), headers={"Authorization": "t"})
    denied = requests.get(url(api_id, "/items/2"), headers={"Authorization": "t"})

    assert allowed.status_code == 200
    assert denied.status_code == 403
    auth_event = lambdas.event("authorizer")
    assert auth_event["version"] == "1.0"
    assert auth_event["identitySource"] == "t"
    assert auth_event["methodArn"].endswith(f"{api_id}/$default/GET/items/2")
    assert lambdas.event()["requestContext"]["authorizer"] == {
        "lambda": {"tenant": "t1"},
        "principalId": "user-1",
    }


def test_lambda_authorizer_context_in_payload_1_0(lambdas, client):
    api_id = create_authorized_api(client, version="1.0", EnableSimpleResponses=True)
    lambdas.handlers["authorizer"] = lambda e: {
        "isAuthorized": True,
        "context": {"user": "u1"},
    }

    requests.get(url(api_id, "/items/1"), headers={"Authorization": "t"})

    authorizer = lambdas.event()["requestContext"]["authorizer"]
    assert authorizer["user"] == "u1"


def test_lambda_authorizer_results_are_cached(lambdas, client):
    api_id = create_authorized_api(
        client, EnableSimpleResponses=True, AuthorizerResultTtlInSeconds=300
    )
    lambdas.handlers["authorizer"] = lambda e: {"isAuthorized": True}

    for _ in range(3):
        resp = requests.get(url(api_id, "/items/1"), headers={"Authorization": "t"})
        assert resp.status_code == 200
    requests.get(url(api_id, "/items/1"), headers={"Authorization": "other"})

    assert [n for n, _ in lambdas.events].count("authorizer") == 2


def test_failing_lambda_authorizer_is_internal_server_error(lambdas, client):
    api_id = create_authorized_api(client, EnableSimpleResponses=True)
    lambdas.handlers["authorizer"] = lambda e: "not a response"

    resp = requests.get(url(api_id, "/items/1"), headers={"Authorization": "t"})

    assert resp.status_code == 500
    assert resp.json() == {"message": "Internal Server Error"}


def jwt(claims: dict[str, Any]) -> str:
    def part(value: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part(claims)}.sig"


def create_jwt_api(client: Any, scopes: list[str] | None = None) -> str:
    api_id = create_api(client, [])
    authorizer_id = client.create_authorizer(
        ApiId=api_id,
        AuthorizerType="JWT",
        IdentitySource=["$request.header.Authorization"],
        JwtConfiguration={"Audience": ["app"], "Issuer": "https://issuer"},
        Name="jwt",
    )["AuthorizerId"]
    integration_id = client.get_integrations(ApiId=api_id)["Items"][0]["IntegrationId"]
    client.create_route(
        ApiId=api_id,
        RouteKey="GET /me",
        Target=f"integrations/{integration_id}",
        AuthorizationType="JWT",
        AuthorizerId=authorizer_id,
        AuthorizationScopes=scopes or [],
    )
    return api_id


def test_jwt_authorizer(lambdas, client):
    api_id = create_jwt_api(client)
    token = jwt({"iss": "https://issuer", "aud": "app", "sub": "u1", "scope": "read"})

    missing = requests.get(url(api_id, "/me"))
    wrong = requests.get(
        url(api_id, "/me"),
        headers={"Authorization": jwt({"iss": "https://issuer", "aud": "other"})},
    )
    expired = requests.get(
        url(api_id, "/me"),
        headers={
            "Authorization": jwt({"iss": "https://issuer", "aud": "app", "exp": 1})
        },
    )
    resp = requests.get(
        url(api_id, "/me"), headers={"Authorization": f"Bearer {token}"}
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert expired.status_code == 401
    assert resp.status_code == 200
    assert lambdas.event()["requestContext"]["authorizer"] == {
        "jwt": {
            "claims": {
                "iss": "https://issuer",
                "aud": "app",
                "sub": "u1",
                "scope": "read",
            },
            "scopes": ["read"],
        }
    }


def test_jwt_authorizer_scopes(lambdas, client):
    api_id = create_jwt_api(client, scopes=["write"])
    token = jwt({"iss": "https://issuer", "client_id": "app", "scope": "read"})

    resp = requests.get(url(api_id, "/me"), headers={"Authorization": token})

    assert resp.status_code == 403


def test_cors(lambdas, client):
    api_id = create_api(client, ["GET /items"])
    client.update_api(
        ApiId=api_id,
        CorsConfiguration={
            "AllowOrigins": ["https://app.example.com"],
            "AllowMethods": ["GET", "POST"],
            "AllowHeaders": ["authorization"],
            "ExposeHeaders": ["x-total"],
            "MaxAge": 600,
            "AllowCredentials": True,
        },
    )
    lambdas.handlers["handler"] = lambda e: {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*"},
        "body": "",
    }

    preflight = requests.options(
        url(api_id, "/anything"),
        headers={
            "Origin": "https://app.example.com",
            "Access-Control-Request-Method": "POST",
        },
    )
    resp = requests.get(
        url(api_id, "/items"), headers={"Origin": "https://app.example.com"}
    )

    assert preflight.status_code == 204
    assert preflight.headers["Access-Control-Allow-Origin"] == "https://app.example.com"
    assert preflight.headers["Access-Control-Allow-Methods"] == "GET,POST"
    assert preflight.headers["Access-Control-Allow-Headers"] == "authorization"
    assert preflight.headers["Access-Control-Max-Age"] == "600"
    assert resp.headers["Access-Control-Allow-Origin"] == "https://app.example.com"
    assert resp.headers["Access-Control-Allow-Credentials"] == "true"
    assert resp.headers["Access-Control-Expose-Headers"] == "x-total"
    assert lambdas.events and all(n == "handler" for n, _ in lambdas.events)


def test_http_proxy_integration(lambdas, client):
    # The backend is another HTTP API, so the proxied request stays inside Moto
    backend_id = create_api(client, ["ANY /{proxy+}"])
    api_id = client.create_api(Name="proxy", ProtocolType="HTTP")["ApiId"]
    integration_id = client.create_integration(
        ApiId=api_id,
        IntegrationType="HTTP_PROXY",
        IntegrationMethod="ANY",
        IntegrationUri=url(backend_id, "/backend/{proxy}"),
        PayloadFormatVersion="1.0",
        RequestParameters={"append:header.x-mapped": "$request.path.proxy"},
    )["IntegrationId"]
    client.create_route(
        ApiId=api_id,
        RouteKey="ANY /api/{proxy+}",
        Target=f"integrations/{integration_id}",
    )
    client.create_stage(ApiId=api_id, StageName="$default")
    lambdas.handlers["handler"] = lambda e: {
        "statusCode": 418,
        "headers": {"X-Backend": "yes"},
        "body": "teapot",
    }

    resp = requests.post(url(api_id, "/api/a/b?q=1"), data="hi")

    assert (resp.status_code, resp.text, resp.headers["X-Backend"]) == (
        418,
        "teapot",
        "yes",
    )
    event = lambdas.event()
    assert event["rawPath"] == "/backend/a/b"
    assert event["rawQueryString"] == "q=1"
    assert event["requestContext"]["http"]["method"] == "POST"
    assert event["headers"]["x-mapped"] == "a/b"
    assert base64.b64decode(event["body"]) == b"hi"


def create_domain(
    client: Any, mappings: dict[str, str], stage: str = "$default"
) -> str:
    client.create_domain_name(
        DomainName="dp.example.com",
        DomainNameConfigurations=[{"EndpointType": "REGIONAL"}],
    )
    for key, api_id in mappings.items():
        client.create_api_mapping(
            DomainName="dp.example.com", ApiId=api_id, ApiMappingKey=key, Stage=stage
        )
    configuration = client.get_domain_name(DomainName="dp.example.com")[
        "DomainNameConfigurations"
    ][0]
    return configuration["ApiGatewayDomainName"]


def test_custom_domain_api_mappings(lambdas, client):
    root = create_api(client, ROUTES)
    flows = create_api(client, ["GET /items/{id}"])
    regional = create_domain(client, {"": root, "flows": flows})
    assert regional.endswith(f".execute-api.{REGION}.amazonaws.com")

    requests.get(f"https://{regional}/flows/items/1")
    event = lambdas.event()
    assert (event["requestContext"]["apiId"], event["routeKey"], event["rawPath"]) == (
        flows,
        "GET /items/{id}",
        "/items/1",
    )

    requests.get(f"https://{regional}/items/latest")
    assert lambdas.event()["requestContext"]["apiId"] == root


def test_custom_domain_host_in_server_mode(lambdas, client):
    root = create_api(client, ["GET /"])
    flows = create_api(client, ["GET /items/{id}"], version="1.0")
    create_domain(client, {"": root, "flows": flows})
    server = Client(DomainDispatcherApplication(create_backend_app))

    resp = server.get("/flows/items/1", headers={"Host": "dp.example.com"})

    assert resp.status_code == 200
    event = lambdas.event()
    assert event["requestContext"]["apiId"] == flows
    # 1.0 keeps the mapping key in the path
    assert event["path"] == "/flows/items/1"
    assert event["requestContext"]["domainName"] == "dp.example.com"

    resp = server.get("/", headers={"Host": "dp.example.com"})
    assert resp.status_code == 200
    assert lambdas.event()["requestContext"]["apiId"] == root


def test_execute_api_host_in_server_mode(lambdas, client):
    api_id = create_api(client, ["GET /items/{id}"])
    server = Client(DomainDispatcherApplication(create_backend_app))

    resp = server.get(
        "/items/3", headers={"Host": f"{api_id}.execute-api.{REGION}.amazonaws.com"}
    )
    missing = server.get(
        "/nothing", headers={"Host": f"{api_id}.execute-api.{REGION}.amazonaws.com"}
    )

    assert resp.status_code == 200
    assert resp.headers["Apigw-Requestid"]
    assert lambdas.event()["pathParameters"] == {"id": "3"}
    assert missing.status_code == 404
    assert json.loads(missing.data) == {"message": "Not Found"}
