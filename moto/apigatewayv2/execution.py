"""The data plane of HTTP APIs.

Requests to `{api-id}.execute-api.{region}.amazonaws.com`, to a custom domain name's
`d-....execute-api.{region}.amazonaws.com` endpoint, or (in server mode) to the custom domain
name itself are routed to an API's routes, authorized and handed to their integrations, the way
API Gateway does it:
https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-routes.html
"""

import base64
import fnmatch
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

from werkzeug.datastructures import Headers

from moto.core.request import Request, normalize_request
from moto.core.responses import TYPE_RESPONSE
from moto.moto_api._internal import mock_random

if TYPE_CHECKING:
    from .models import (
        Api,
        ApiGatewayV2Backend,
        Authorizer,
        DomainName,
        Integration,
        Route,
    )

log = logging.getLogger(__name__)

EXECUTE_API_HOST = re.compile(
    r"^(?P<prefix>[^.]+)\.execute-api\.(?P<region>[^.]+)\.amazonaws\.com(?:\.cn)?$"
)
STAGE_VARIABLE = re.compile(r"\$\{stageVariables\.([A-Za-z0-9_]+)\}")
TEXT_MEDIA_TYPES = ("json", "xml", "javascript", "text/")
HOP_BY_HOP = {
    "connection",
    "content-encoding",
    "content-length",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def _joined(headers: Headers) -> dict[str, str]:
    return {
        name: ", ".join(headers.getlist(name))
        for name in dict.fromkeys(k for k, _ in headers.items())
    }


class HttpApiError(Exception):
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message


NOT_FOUND = HttpApiError(404, "Not Found")
UNAUTHORIZED = HttpApiError(401, "Unauthorized")
FORBIDDEN = HttpApiError(403, "Forbidden")
INTERNAL_SERVER_ERROR = HttpApiError(500, "Internal Server Error")


def _backends() -> "list[ApiGatewayV2Backend]":
    from .models import apigatewayv2_backends

    # Only the backends that exist already: indexing a BackendDict would create one per region
    return [
        backend
        for account in dict.values(apigatewayv2_backends)
        for backend in dict.values(account)
    ]


def find_domain_name(host: str) -> "tuple[ApiGatewayV2Backend, DomainName] | None":
    """The custom domain name served at `host`: its own name, or its API Gateway endpoint."""
    host = host.split(":")[0].lower()
    for backend in _backends():
        if host in backend.domain_names:
            return backend, backend.domain_names[host]
        for domain in backend.domain_names.values():
            if domain.regional_domain_name == host:
                return backend, domain
    return None


def is_http_api_host(host: str | None) -> bool:
    if not host:
        return False
    host = host.split(":")[0].lower()
    return bool(EXECUTE_API_HOST.match(host)) or find_domain_name(host) is not None


def _truthy(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def _first_lower(value: Any) -> Any:
    if isinstance(value, dict):
        return {k[:1].lower() + k[1:]: v for k, v in value.items()}
    return value


@dataclass
class RouteMatch:
    route: "Route"
    path_parameters: dict[str, str]


@dataclass
class AuthorizerResult:
    """What an authorizer hands the integration: a Lambda's context, or a JWT's claims."""

    lambda_context: dict[str, Any] | None = None
    principal_id: str | None = None
    jwt_claims: dict[str, str] | None = None
    jwt_scopes: list[str] | None = None
    iam: dict[str, Any] | None = None

    def for_payload(self, version: str) -> dict[str, Any]:
        if self.jwt_claims is not None:
            jwt = {"claims": self.jwt_claims, "scopes": self.jwt_scopes}
            return {"jwt": jwt} if version == "2.0" else jwt
        if self.iam is not None:
            return {"iam": self.iam}
        context = self.lambda_context or {}
        if version == "2.0":
            result: dict[str, Any] = {"lambda": context}
            if self.principal_id is not None:
                result["principalId"] = self.principal_id
            return result
        return {**context, "principalId": self.principal_id, "integrationLatency": 0}


@dataclass
class CachedAuthorization:
    expires: float
    allowed: bool
    result: AuthorizerResult
    policy: list[dict[str, Any]] = field(default_factory=list)


def _route_rank(path: str) -> tuple[int, ...]:
    """Literal segments beat path variables, which beat greedy ones, from left to right."""
    return tuple(
        0 if seg.endswith("+}") else 1 if seg.startswith("{") else 2
        for seg in path.strip("/").split("/")
        if seg
    )


def _route_regex(path: str) -> re.Pattern[str]:
    parts = []
    for seg in path.strip("/").split("/") if path.strip("/") else []:
        if seg.startswith("{") and seg.endswith("+}"):
            # Matches nothing after a slash too: API Gateway routes `/a/` to `/a/{proxy+}`
            parts.append(f"(?P<{seg[1:-2]}>.*)")
        elif seg.startswith("{") and seg.endswith("}"):
            parts.append(f"(?P<{seg[1:-1]}>[^/]+)")
        else:
            parts.append(re.escape(seg))
    return re.compile("^/" + "/".join(parts) + "/?$")


def match_route(api: "Api", method: str, path: str) -> RouteMatch | None:
    candidates = []
    default = None
    for route in api.routes.values():
        if route.route_key == "$default":
            default = route
            continue
        route_method, _, route_path = route.route_key.partition(" ")
        if route_method not in ("ANY", method):
            continue
        match = _route_regex(route_path).match(path)
        if match:
            candidates.append(
                ((_route_rank(route_path), route_method != "ANY"), route, match)
            )
    if candidates:
        _, route, match = max(candidates, key=lambda c: c[0])
        return RouteMatch(route, dict(match.groupdict()))
    if default is not None:
        return RouteMatch(default, {})
    return None


def lambda_function_arn(uri: str, stage_variables: dict[str, str]) -> str:
    uri = STAGE_VARIABLE.sub(lambda m: stage_variables.get(m[1], ""), uri)
    match = re.search(r"functions/(.+?)/invocations", uri)
    return match[1] if match else uri


def _claim_text(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list):
        return "[" + " ".join(_claim_text(v) for v in value) + "]"
    if isinstance(value, dict):
        return json.dumps(value)
    return str(value)


def _decode_jwt(token: str) -> dict[str, Any] | None:
    try:
        payload = token.split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
    except (IndexError, ValueError):
        return None
    return claims if isinstance(claims, dict) else None


class HttpApiInvocation:
    """One request to an HTTP API."""

    def __init__(self, request: Request):
        self.request = request
        self.host = (
            request.headers.get("Host") or urlsplit(request.url).netloc
        ).lower()
        self.host = self.host.split(":")[0]
        self.method = request.method.upper()
        self.raw_request_path = request.raw_path or "/"
        self.query_string = request.query_string.decode("utf-8")
        self.query = parse_qsl(self.query_string, keep_blank_values=True)
        self.body = request.get_data()
        self.request_id = str(mock_random.uuid4())
        self.now = datetime.now(timezone.utc)
        self.backend: ApiGatewayV2Backend
        self.api: Api
        self.stage_name = "$default"
        self.stage_variables: dict[str, str] = {}
        # The path the routes see, decoded, and the raw one the 2.0 payload carries
        self.path = "/"
        self.raw_path = "/"
        self._match: RouteMatch | None = None

    def handle(self) -> TYPE_RESPONSE:
        try:
            status, headers, body = self._handle()
        except HttpApiError as e:
            status, headers, body = (
                e.status,
                Headers({"Content-Type": "application/json"}),
                json.dumps({"message": e.message}).encode(),
            )
        headers["Apigw-Requestid"] = self.request_id
        if not self.request.from_wsgi_server:
            # The decorator's responses keep a single value per header name
            return status, _joined(headers), body
        return status, headers, body  # type: ignore[return-value]

    def _handle(self) -> tuple[int, Headers, bytes]:
        self._select_api()
        cors = _first_lower(self.api.cors_configuration) or None
        if (
            cors
            and self.method == "OPTIONS"
            and "Origin" in self.request.headers
            and "Access-Control-Request-Method" in self.request.headers
        ):
            return 204, self._preflight_headers(cors), b""
        match = match_route(self.api, self.method, self.path)
        if match is None:
            raise NOT_FOUND
        auth = self._authorize(match)
        integration = self._integration(match.route)
        if integration.integration_type == "AWS_PROXY":
            status, headers, body = self._invoke_lambda_integration(
                integration, match, auth
            )
        elif integration.integration_type == "HTTP_PROXY":
            status, headers, body = self._invoke_http_integration(
                integration, match, auth
            )
        else:
            # HTTP APIs offer only proxy integrations, and AWS service integrations (subtypes)
            raise INTERNAL_SERVER_ERROR
        if cors:
            for name in list(headers.keys()):
                if name.lower().startswith("access-control-"):
                    del headers[name]
            headers.extend(self._cors_headers(cors))
        return status, headers, body

    def _select_api(self) -> None:
        from .models import Api

        execute_api = EXECUTE_API_HOST.match(self.host)
        domain = find_domain_name(self.host)
        if domain is not None:
            backend, domain_name = domain
            # The longest mapping key that prefixes the path wins; "" is the domain's root
            mappings = sorted(
                backend.get_api_mappings(domain_name.domain_name),
                key=lambda m: -len(m.api_mapping_key or ""),
            )
            decoded = unquote(self.raw_request_path)
            for mapping in mappings:
                key = mapping.api_mapping_key or ""
                if key and decoded != f"/{key}" and not decoded.startswith(f"/{key}/"):
                    continue
                api = backend.apis.get(mapping.api_id)
                if api is None or mapping.stage not in api.stages:
                    raise NOT_FOUND
                self.backend, self.api, self.stage_name = backend, api, mapping.stage
                # API Gateway strips the mapping key from 2.0's rawPath and keeps it in 1.0's path
                self.raw_path = self._strip_prefix(self.raw_request_path, key)
                self.path = unquote(self.raw_path)
                break
            else:
                raise NOT_FOUND
        elif execute_api:
            api_id, region = execute_api["prefix"], execute_api["region"]
            found: tuple[ApiGatewayV2Backend, Api] | None = None
            for backend in _backends():
                if backend.region_name == region and api_id in backend.apis:
                    found = backend, backend.apis[api_id]
                    break
            if found is None:
                raise NOT_FOUND
            self.backend, self.api = found
            if _truthy(self.api.disable_execute_api_endpoint):
                raise FORBIDDEN
            first = self.raw_request_path.strip("/").split("/")[0]
            # A named stage is the first path segment; anything else goes to $default
            if first and first != "$default" and first in self.api.stages:
                self.stage_name = first
                self.path = unquote(self._strip_prefix(self.raw_request_path, first))
            elif "$default" in self.api.stages:
                self.stage_name = "$default"
                self.path = unquote(self.raw_request_path)
            else:
                raise NOT_FOUND
            # rawPath keeps a named stage's prefix, as API Gateway sends it
            self.raw_path = self.raw_request_path
        else:
            raise NOT_FOUND
        assert isinstance(self.api, Api)
        self.stage_variables = dict(
            self.api.stages[self.stage_name].stage_variables or {}
        )

    @staticmethod
    def _strip_prefix(path: str, segment: str) -> str:
        if not segment:
            return path
        return path[len(segment) + 1 :] or "/"

    def _integration(self, route: "Route") -> "Integration":
        target = (route.target or "").removeprefix("integrations/")
        integration = self.api.integrations.get(target)
        if integration is None:
            raise NOT_FOUND
        return integration

    # Requests and payloads

    @property
    def source_ip(self) -> str:
        return self.request.remote_addr or "127.0.0.1"

    @property
    def user_agent(self) -> str:
        return self.request.headers.get("User-Agent", "")

    def _request_headers(self) -> Headers:
        headers = Headers(list(self.request.headers.items()))
        forwarded = headers.get("X-Forwarded-For")
        if forwarded is None:
            headers["X-Forwarded-For"] = self.source_ip
        headers.setdefault("X-Forwarded-Proto", "https")
        headers.setdefault(
            "X-Forwarded-Port",
            "443" if headers["X-Forwarded-Proto"] == "https" else "80",
        )
        return headers

    def _encoded_body(self, headers: Headers) -> tuple[str | None, bool]:
        if not self.body:
            return None, False
        content_type = (headers.get("Content-Type") or "").lower()
        if any(t in content_type for t in TEXT_MEDIA_TYPES):
            try:
                return self.body.decode("utf-8"), False
            except UnicodeDecodeError:
                pass
        return base64.b64encode(self.body).decode(), True

    def _request_context(self, route: "Route") -> dict[str, Any]:
        epoch_ms = int(self.now.timestamp() * 1000)
        return {
            "accountId": self.backend.account_id,
            "apiId": self.api.api_id,
            "domainName": self.host,
            "domainPrefix": self.host.split(".")[0],
            "requestId": self.request_id,
            "routeKey": route.route_key,
            "stage": self.stage_name,
            "time": self.now.strftime("%d/%b/%Y:%H:%M:%S +0000"),
            "timeEpoch": epoch_ms,
        }

    def event(
        self,
        version: str,
        match: RouteMatch,
        headers: Headers,
        query: list[tuple[str, str]],
        auth: AuthorizerResult | None,
        with_body: bool = True,
    ) -> dict[str, Any]:
        route = match.route
        body, is_base64 = self._encoded_body(headers) if with_body else (None, False)
        context = self._request_context(route)
        if version == "1.0":
            route_path = route.route_key.partition(" ")[2] or route.route_key
            multi_query: dict[str, list[str]] = {}
            for k, v in query:
                multi_query.setdefault(k, []).append(v)
            context.update(
                {
                    "extendedRequestId": self.request_id,
                    "httpMethod": self.method,
                    "identity": {
                        "accessKey": None,
                        "accountId": None,
                        "caller": None,
                        "cognitoAuthenticationProvider": None,
                        "cognitoAuthenticationType": None,
                        "cognitoIdentityId": None,
                        "cognitoIdentityPoolId": None,
                        "principalOrgId": None,
                        "sourceIp": self.source_ip,
                        "user": None,
                        "userAgent": self.user_agent,
                        "userArn": None,
                    },
                    "path": self.raw_request_path,
                    "protocol": "HTTP/1.1",
                    "requestTime": context["time"],
                    "requestTimeEpoch": context["timeEpoch"],
                    "resourceId": route.route_key,
                    "resourcePath": route_path,
                }
            )
            del context["time"], context["timeEpoch"]
            if auth is not None:
                context["authorizer"] = auth.for_payload("1.0")
            names = list(dict.fromkeys(k for k, _ in headers.items()))
            return {
                "version": "1.0",
                "resource": route_path,
                "path": self.raw_request_path,
                "httpMethod": self.method,
                "headers": {k: headers.getlist(k)[-1] for k in names},
                "multiValueHeaders": {k: headers.getlist(k) for k in names},
                "queryStringParameters": {k: v[-1] for k, v in multi_query.items()}
                or None,
                "multiValueQueryStringParameters": multi_query or None,
                "requestContext": context,
                "pathParameters": match.path_parameters or None,
                "stageVariables": self.stage_variables or None,
                "body": body,
                "isBase64Encoded": is_base64,
            }
        context["http"] = {
            "method": self.method,
            "path": self.raw_path,
            "protocol": "HTTP/1.1",
            "sourceIp": self.source_ip,
            "userAgent": self.user_agent,
        }
        if auth is not None:
            context["authorizer"] = auth.for_payload("2.0")
        lowered: dict[str, list[str]] = {}
        for k, v in headers.items():
            lowered.setdefault(k.lower(), []).append(v)
        # 2.0 moves the cookies out of the headers
        cookies = [
            c.strip()
            for v in lowered.pop("cookie", [])
            for c in v.split(";")
            if c.strip()
        ]
        joined_query: dict[str, list[str]] = {}
        for k, v in query:
            joined_query.setdefault(k, []).append(v)
        event: dict[str, Any] = {
            "version": "2.0",
            "routeKey": route.route_key,
            "rawPath": self.raw_path,
            "rawQueryString": self.query_string,
        }
        if cookies:
            event["cookies"] = cookies
        event["headers"] = {k: ",".join(v) for k, v in lowered.items()}
        if joined_query:
            event["queryStringParameters"] = {
                k: ",".join(v) for k, v in joined_query.items()
            }
        if match.path_parameters:
            event["pathParameters"] = match.path_parameters
        if self.stage_variables:
            event["stageVariables"] = self.stage_variables
        event["requestContext"] = context
        if body is not None:
            event["body"] = body
        event["isBase64Encoded"] = is_base64
        return event

    # Lambda

    def invoke_lambda(self, uri: str, event: dict[str, Any]) -> tuple[Any, bool]:
        """Invokes the function through Moto's Lambda, so Docker, MOTO_LAMBDA_INVOKE_ENDPOINT and
        the simple backend all apply. Returns the payload and whether the function failed."""
        from moto.awslambda.utils import get_backend

        arn = lambda_function_arn(uri, self.stage_variables)
        parts = arn.split(":")
        if arn.startswith("arn:") and len(parts) >= 7:
            account_id, region = parts[4], parts[3]
        else:
            account_id, region = self.backend.account_id, self.backend.region_name
        response_headers: dict[str, str] = {}
        try:
            payload = get_backend(account_id, region).invoke(
                arn, None, json.dumps(event), {}, response_headers
            )
        except Exception as e:
            log.warning("HTTP API %s could not invoke %s: %s", self.api.api_id, arn, e)
            return None, True
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        errored = bool(response_headers.get("x-amz-function-error"))
        return payload, errored

    def _invoke_lambda_integration(
        self,
        integration: "Integration",
        match: RouteMatch,
        auth: AuthorizerResult | None,
    ) -> tuple[int, Headers, bytes]:
        if integration.integration_subtype:
            raise INTERNAL_SERVER_ERROR
        version = integration.payload_format_version or "2.0"
        headers = self._request_headers()
        query = list(self.query)
        for key, source in (integration.request_parameters or {}).items():
            action, _, target = key.partition(":")
            value = self._mapped_value(source, match, auth)
            self._apply_mapping(action, target, value, headers, query)
        payload, errored = self.invoke_lambda(
            integration.integration_uri,
            self.event(version, match, headers, query, auth),
        )
        if errored or payload is None:
            raise INTERNAL_SERVER_ERROR
        return self._lambda_response(payload, version)

    @staticmethod
    def _lambda_response(payload: str, version: str) -> tuple[int, Headers, bytes]:
        try:
            output = json.loads(payload)
        except ValueError:
            output = None
        if not (isinstance(output, dict) and "statusCode" in output):
            if version == "1.0":
                raise INTERNAL_SERVER_ERROR
            # 2.0 takes any other output as the body of a 200 JSON response
            return 200, Headers({"Content-Type": "application/json"}), payload.encode()
        try:
            status = int(output["statusCode"])
        except (TypeError, ValueError):
            raise INTERNAL_SERVER_ERROR
        body = output.get("body")
        if body is None:
            data = b""
        elif not isinstance(body, str):
            data = json.dumps(body).encode()
        elif _truthy(output.get("isBase64Encoded")):
            data = base64.b64decode(body)
        else:
            data = body.encode()
        headers = Headers()
        for name, value in (output.get("headers") or {}).items():
            headers[name] = (
                ",".join(map(str, value)) if isinstance(value, list) else str(value)
            )
        for name, values in (output.get("multiValueHeaders") or {}).items():
            if name in headers:
                del headers[name]
            for value in values if isinstance(values, list) else [values]:
                headers.add(name, str(value))
        if version == "2.0":
            for cookie in output.get("cookies") or []:
                headers.add("Set-Cookie", str(cookie))
        return status, headers, data

    # HTTP_PROXY

    def _invoke_http_integration(
        self,
        integration: "Integration",
        match: RouteMatch,
        auth: AuthorizerResult | None,
    ) -> tuple[int, Headers, bytes]:
        import requests

        uri = STAGE_VARIABLE.sub(
            lambda m: self.stage_variables.get(m[1], ""), integration.integration_uri
        )
        uri = re.sub(
            r"\{([A-Za-z0-9_]+)\+?\}",
            lambda m: match.path_parameters.get(m[1], ""),
            uri,
        )
        headers = Headers(
            [
                (k, v)
                for k, v in self.request.headers.items()
                if k.lower() not in HOP_BY_HOP and k.lower() != "host"
            ]
        )
        query = list(self.query)
        for key, source in (integration.request_parameters or {}).items():
            action, _, target = key.partition(":")
            value = self._mapped_value(source, match, auth)
            if target == "path" and action == "overwrite":
                uri = urlunsplit(urlsplit(uri)._replace(path=value))
            else:
                self._apply_mapping(action, target, value, headers, query)
        parts = urlsplit(uri)
        merged_query = parse_qsl(parts.query, keep_blank_values=True) + query
        uri = urlunsplit(parts._replace(query=urlencode(merged_query)))
        method = integration.integration_method or self.method
        timeout = (integration.timeout_in_millis or 30000) / 1000
        try:
            resp = requests.request(
                self.method if method == "ANY" else method,
                uri,
                headers=_joined(headers),
                data=self.body or None,
                allow_redirects=False,
                timeout=timeout,
            )
        except requests.Timeout:
            raise HttpApiError(503, "Service Unavailable")
        except requests.RequestException:
            raise INTERNAL_SERVER_ERROR
        out = Headers()
        for name, value in (getattr(resp.raw, "headers", None) or resp.headers).items():
            if name.lower() not in HOP_BY_HOP:
                out.add(name, value)
        return resp.status_code, out, resp.content

    # Parameter mapping

    @staticmethod
    def _apply_mapping(
        action: str,
        target: str,
        value: str,
        headers: Headers,
        query: list[tuple[str, str]],
    ) -> None:
        if target.startswith("header."):
            name = target.split(".", 1)[1]
            if action in ("overwrite", "remove"):
                headers.remove(name)
            if action in ("overwrite", "append"):
                headers.add(name, value)
        elif target.startswith("querystring."):
            name = target.split(".", 1)[1]
            if action in ("overwrite", "remove"):
                query[:] = [(k, v) for k, v in query if k != name]
            if action in ("overwrite", "append"):
                query.append((name, value))

    def _mapped_value(
        self, source: str, match: RouteMatch, auth: AuthorizerResult | None
    ) -> str:
        """A parameter mapping's value: static text with $request, $context and $stageVariables in it."""

        def value(ref: str) -> str:
            if ref == "request.path":
                return self.path
            if ref.startswith("request.path."):
                return match.path_parameters.get(ref.split(".", 2)[2], "")
            if ref.startswith("request.header."):
                return self.request.headers.get(ref.split(".", 2)[2], "")
            if ref.startswith("request.querystring."):
                name = ref.split(".", 2)[2]
                return ",".join(v for k, v in self.query if k == name)
            if ref.startswith("stageVariables."):
                return self.stage_variables.get(ref.split(".", 1)[1], "")
            if ref.startswith("context."):
                return self._context_variable(ref.split(".", 1)[1], match, auth)
            return ""

        return re.sub(
            r"\$\{([^}]+)\}|\$([A-Za-z][\w.-]*)",
            lambda m: value(m[1] or m[2]),
            source,
        )

    def _context_variable(
        self, name: str, match: RouteMatch | None, auth: AuthorizerResult | None
    ) -> str:
        if name.startswith("authorizer.") and auth is not None:
            key = name.split(".", 1)[1]
            if auth.jwt_claims is not None and key.startswith("claims."):
                return auth.jwt_claims.get(key.split(".", 1)[1], "")
            if key == "principalId":
                return auth.principal_id or ""
            return _claim_text((auth.lambda_context or {}).get(key, ""))
        variables = {
            "accountId": self.backend.account_id,
            "apiId": self.api.api_id,
            "domainName": self.host,
            "domainPrefix": self.host.split(".")[0],
            "httpMethod": self.method,
            "identity.sourceIp": self.source_ip,
            "identity.userAgent": self.user_agent,
            "path": self.raw_request_path,
            "protocol": "HTTP/1.1",
            "requestId": self.request_id,
            "requestTime": self.now.strftime("%d/%b/%Y:%H:%M:%S +0000"),
            # Whole seconds in parameter mappings, unlike the event's timeEpoch: functions behind HTTP APIs read
            # this header with datetime.utcfromtimestamp
            "requestTimeEpoch": str(int(self.now.timestamp())),
            "routeKey": match.route.route_key if match else "",
            "stage": self.stage_name,
        }
        return variables.get(name, "")

    # Authorization

    def _identity(self, source: str) -> str:
        source = source.strip()
        if source.startswith("$request.header."):
            return self.request.headers.get(source.split(".", 2)[2], "")
        if source.startswith("$request.querystring."):
            name = source.split(".", 2)[2]
            return ",".join(v for k, v in self.query if k == name)
        if source.startswith("$stageVariables."):
            return self.stage_variables.get(source.split(".", 1)[1], "")
        if source.startswith("$context."):
            return self._context_variable(source.split(".", 1)[1], self._match, None)
        return ""

    def _route_arn(self) -> str:
        partition = "aws-cn" if self.backend.region_name.startswith("cn-") else "aws"
        return (
            f"arn:{partition}:execute-api:{self.backend.region_name}:{self.backend.account_id}:"
            f"{self.api.api_id}/{self.stage_name}/{self.method}{self.path}"
        )

    def _authorize(self, match: RouteMatch) -> AuthorizerResult | None:
        self._match = match
        route = match.route
        auth_type = (route.authorization_type or "NONE").upper()
        if auth_type == "NONE":
            return None
        if auth_type == "AWS_IAM":
            authorization = self.request.headers.get("Authorization", "")
            if not authorization.startswith("AWS4-HMAC-SHA256"):
                raise FORBIDDEN
            access_key = authorization.split("Credential=")[-1].split("/")[0]
            return AuthorizerResult(
                iam={
                    "accessKey": access_key,
                    "accountId": self.backend.account_id,
                    "callerId": access_key,
                    "principalOrgId": None,
                    "userArn": f"arn:aws:iam::{self.backend.account_id}:user/moto",
                    "userId": access_key,
                }
            )
        authorizer = self.api.authorizers.get(route.authorizer_id or "")
        if authorizer is None:
            raise INTERNAL_SERVER_ERROR
        if (authorizer.authorizer_type or "").upper() == "JWT":
            return self._authorize_jwt(authorizer, route)
        return self._authorize_lambda(authorizer, match)

    def _identity_sources(
        self, authorizer: "Authorizer", default: list[str]
    ) -> list[str]:
        sources = authorizer.identity_source or default
        if isinstance(sources, str):
            sources = sources.split(",")
        return [s.strip() for s in sources if s.strip()]

    def _authorize_jwt(
        self, authorizer: "Authorizer", route: "Route"
    ) -> AuthorizerResult:
        """Checks the token's issuer, audience and expiry. Moto does not verify the signature."""
        sources = self._identity_sources(authorizer, ["$request.header.Authorization"])
        token = self._identity(sources[0]) if sources else ""
        token = re.sub(r"^Bearer\s+", "", token, flags=re.IGNORECASE).strip()
        claims = _decode_jwt(token) if token else None
        if claims is None:
            raise UNAUTHORIZED
        config = _first_lower(authorizer.jwt_config) or {}
        issuer = config.get("issuer")
        if issuer and claims.get("iss") != issuer:
            raise UNAUTHORIZED
        audiences = config.get("audience") or []
        token_audiences = claims.get("aud") or claims.get("client_id") or []
        if isinstance(token_audiences, str):
            token_audiences = [token_audiences]
        if audiences and not set(audiences) & set(token_audiences):
            raise UNAUTHORIZED
        exp = claims.get("exp")
        if isinstance(exp, (int, float)) and exp < time.time():
            raise UNAUTHORIZED
        scopes = str(claims.get("scope") or claims.get("scp") or "").split()
        required = route.authorization_scopes or []
        if required and not set(required) & set(scopes):
            raise FORBIDDEN
        return AuthorizerResult(
            jwt_claims={k: _claim_text(v) for k, v in claims.items()},
            jwt_scopes=scopes or None,
        )

    def _authorize_lambda(
        self, authorizer: "Authorizer", match: RouteMatch
    ) -> AuthorizerResult:
        sources = self._identity_sources(authorizer, [])
        identity = [self._identity(s) for s in sources]
        if sources and not all(identity):
            raise UNAUTHORIZED
        ttl = int(authorizer.auth_result_ttl or 0)
        simple = _truthy(authorizer.enable_simple_response)
        cache_key = json.dumps([self.stage_name, identity])
        cached = authorizer.cached_results.get(cache_key)
        if ttl and sources and cached and cached.expires > time.time():
            allowed = cached.allowed if simple else self._policy_allows(cached.policy)
            if not allowed:
                raise FORBIDDEN
            return cached.result

        version = authorizer.auth_payload_format_version or "2.0"
        route_arn = self._route_arn()
        headers = self._request_headers()
        event = self.event(version, match, headers, list(self.query), None, False)
        event.pop("body", None)
        if version == "2.0":
            event = {
                **event,
                "type": "REQUEST",
                "routeArn": route_arn,
                "identitySource": identity,
            }
        else:
            event = {
                **event,
                "type": "REQUEST",
                "methodArn": route_arn,
                "identitySource": ",".join(identity),
                "authorizationToken": identity[0] if identity else None,
            }
        payload, errored = self.invoke_lambda(authorizer.authorizer_uri or "", event)
        try:
            output = json.loads(payload) if payload is not None else None
        except ValueError:
            output = None
        if errored or not isinstance(output, dict):
            # API Gateway answers 500 when its authorizer fails or returns an invalid response
            raise INTERNAL_SERVER_ERROR

        context = output.get("context") or {}
        policy: list[dict[str, Any]] = []
        if simple and version == "2.0":
            if not isinstance(output.get("isAuthorized"), bool):
                raise INTERNAL_SERVER_ERROR
            allowed = output["isAuthorized"]
            result = AuthorizerResult(lambda_context=context)
        else:
            document = output.get("policyDocument")
            if not isinstance(document, dict):
                raise INTERNAL_SERVER_ERROR
            statements = document.get("Statement") or []
            policy = statements if isinstance(statements, list) else [statements]
            allowed = self._policy_allows(policy)
            result = AuthorizerResult(
                lambda_context=context,
                principal_id=output.get("principalId"),
            )
        if ttl and sources:
            authorizer.cached_results[cache_key] = CachedAuthorization(
                time.time() + ttl, allowed, result, policy
            )
        if not allowed:
            raise FORBIDDEN
        return result

    def _policy_allows(self, statements: list[dict[str, Any]]) -> bool:
        route_arn = self._route_arn()

        def applies(statement: dict[str, Any]) -> bool:
            actions = statement.get("Action") or []
            actions = [actions] if isinstance(actions, str) else actions
            resources = statement.get("Resource") or []
            resources = [resources] if isinstance(resources, str) else resources
            return any(
                fnmatch.fnmatchcase("execute-api:invoke", a.lower()) for a in actions
            ) and any(fnmatch.fnmatchcase(route_arn, r) for r in resources)

        effects = [s.get("Effect") for s in statements if applies(s)]
        return "Allow" in effects and "Deny" not in effects

    # CORS

    def _cors_headers(self, cors: dict[str, Any]) -> Headers:
        origin = self.request.headers.get("Origin")
        allowed = cors.get("allowOrigins") or []
        headers = Headers()
        if origin is None or not ("*" in allowed or origin in allowed):
            return headers
        headers["Access-Control-Allow-Origin"] = "*" if "*" in allowed else origin
        if _truthy(cors.get("allowCredentials")):
            headers["Access-Control-Allow-Credentials"] = "true"
        if cors.get("exposeHeaders"):
            headers["Access-Control-Expose-Headers"] = ",".join(cors["exposeHeaders"])
        return headers

    def _preflight_headers(self, cors: dict[str, Any]) -> Headers:
        headers = self._cors_headers(cors)
        if not headers:
            return headers
        methods = cors.get("allowMethods") or []
        requested_method = self.request.headers.get("Access-Control-Request-Method", "")
        if methods:
            headers["Access-Control-Allow-Methods"] = (
                requested_method if "*" in methods else ",".join(methods)
            )
        allow_headers = cors.get("allowHeaders") or []
        requested_headers = self.request.headers.get(
            "Access-Control-Request-Headers", ""
        )
        if allow_headers:
            headers["Access-Control-Allow-Headers"] = (
                requested_headers if "*" in allow_headers else ",".join(allow_headers)
            )
        if cors.get("maxAge") is not None:
            headers["Access-Control-Max-Age"] = str(cors["maxAge"])
        headers.remove("Access-Control-Expose-Headers")
        return headers


def invoke_http_api(request: Any, full_url: str, headers: Any) -> TYPE_RESPONSE:
    return HttpApiInvocation(normalize_request(request)).handle()
