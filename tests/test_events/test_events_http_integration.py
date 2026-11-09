import json
import os
import time
from unittest import SkipTest, mock

import boto3
import responses

from moto import mock_aws, settings


@mock_aws
@mock.patch.dict(os.environ, {"MOTO_EVENTS_INVOKE_HTTP": "true"})
def test_invoke_http_request_on_event():
    if settings.TEST_SERVER_MODE:
        raise SkipTest("Can't intercept HTTP requests in ServerMode")
    events = boto3.client("events", region_name="eu-west-1")

    #
    # Create API endpoint to invoke
    response = events.create_connection(
        Name="test",
        Description="test description",
        AuthorizationType="API_KEY",
        AuthParameters={
            "ApiKeyAuthParameters": {"ApiKeyName": "test", "ApiKeyValue": "test"}
        },
    )

    destination_response = events.create_api_destination(
        Name="test",
        Description="test-description",
        ConnectionArn=response.get("ConnectionArn"),
        InvocationEndpoint="https://www.google.com",
        HttpMethod="GET",
    )
    destination_arn = destination_response["ApiDestinationArn"]

    #
    # Create Rules when to invoke the connection
    pattern = {"source": ["test-source"], "detail-type": ["test-detail-type"]}
    rule_name = "test-event-rule"
    events.put_rule(
        Name=rule_name,
        State="ENABLED",
        EventPattern=json.dumps(pattern),
    )

    events.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "123",
                "Arn": destination_arn,
                "HttpParameters": {
                    "HeaderParameters": {"header1": "value1"},
                    "QueryStringParameters": {"qs1": "qv2"},
                },
            }
        ],
    )

    #
    # Ensure we intercept HTTP requests
    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        # test that both json and urlencoded body are empty in matcher and in request
        rsps.add(
            method=responses.GET,
            url="https://www.google.com/",
            match=[
                responses.matchers.header_matcher({"header1": "value1"}),
                responses.matchers.query_param_matcher({"qs1": "qv2"}),
            ],
        )

        #
        # Invoke HTTP requests
        events.put_events(
            Entries=[
                {
                    "Source": "test-source",
                    "DetailType": "test-detail-type",
                    "Detail": "{}",
                }
            ]
        )
        _wait_for_calls(rsps, 1)


def _wait_for_calls(rsps: responses.RequestsMock, count: int) -> None:
    # EventBridge delivers to API destinations asynchronously
    deadline = time.time() + 10
    while len(rsps.calls) < count and time.time() < deadline:
        time.sleep(0.05)


@mock_aws
@mock.patch.dict(os.environ, {"MOTO_EVENTS_INVOKE_HTTP": "true"})
def test_api_destination_sends_the_input_with_the_connection_key_and_retries():
    if settings.TEST_SERVER_MODE:
        raise SkipTest("Can't intercept HTTP requests in ServerMode")
    events = boto3.client("events", region_name="eu-west-1")
    connection_arn = events.create_connection(
        Name="decide",
        AuthorizationType="API_KEY",
        AuthParameters={
            "ApiKeyAuthParameters": {"ApiKeyName": "X-Api-Key", "ApiKeyValue": "secret"}
        },
    )["ConnectionArn"]
    destination_arn = events.create_api_destination(
        Name="decide",
        ConnectionArn=connection_arn,
        InvocationEndpoint="https://decide.example.com/flows/*/decide",
        HttpMethod="POST",
    )["ApiDestinationArn"]
    events.put_rule(
        Name="rule", State="ENABLED", EventPattern=json.dumps({"source": ["s"]})
    )
    events.put_targets(
        Rule="rule",
        Targets=[
            {
                "Id": "1",
                "Arn": destination_arn,
                "HttpParameters": {"PathParameterValues": ["webhooks"]},
                "RetryPolicy": {"MaximumRetryAttempts": 2},
                "InputTransformer": {
                    "InputPathsMap": {"id": "$.detail.id"},
                    "InputTemplate": '{"data": {"id": "<id>"}}',
                },
            }
        ],
    )

    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        url = "https://decide.example.com/flows/webhooks/decide"
        rsps.add(responses.POST, url, status=503)
        rsps.add(
            responses.POST,
            url,
            status=200,
            match=[
                responses.matchers.header_matcher({"X-Api-Key": "secret"}),
                responses.matchers.json_params_matcher({"data": {"id": "42"}}),
            ],
        )
        events.put_events(
            Entries=[{"Source": "s", "DetailType": "t", "Detail": '{"id": "42"}'}]
        )
        _wait_for_calls(rsps, 2)
        assert [c.response.status_code for c in rsps.calls] == [503, 200]
