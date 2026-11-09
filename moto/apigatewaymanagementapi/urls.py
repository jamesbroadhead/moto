"""apigatewaymanagementapi base URL and path."""

from typing import Any

from moto.core.responses import TYPE_RESPONSE

from ..apigatewayv2.execution import invoke_http_api
from .responses import ApiGatewayManagementApiResponse

# execute-api.us-east-1.amazonaws.com
# api_id.execute-api.us-east-1.amazonaws.com
url_bases = [r"https?://([^.]+\.)*execute-api\.[^.]+\.amazonaws\.com"]


response = ApiGatewayManagementApiResponse()


def execute_api(request: Any, full_url: str, headers: Any) -> TYPE_RESPONSE:
    """The host also serves the HTTP APIs themselves, at every path other than a connection's."""
    if "/@connections/" in full_url.split("?")[0]:
        return response.connect_to_apigateway(request, full_url, headers)
    return invoke_http_api(request, full_url, headers)


url_paths = {
    "{0}/@connections/(?P<connection_id>[^/]+)$": response.dispatch,
    "{0}/(?P<stage_name>.+/)+@connections/(?P<connection_id>[^/]+)$": response.connect_to_apigateway,
    "{0}/$": execute_api,
    "{0}/.*": execute_api,
}
