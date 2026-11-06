import uuid

import boto3
import pytest

from moto import mock_aws


@mock_aws
def test_query_definition_lifecycle():
    client = boto3.client("logs", region_name="eu-west-2")
    assert (
        client.describe_query_definitions(queryDefinitionNamePrefix="ErrorMonitoring-")[
            "queryDefinitions"
        ]
        == []
    )
    token = str(uuid.uuid4())
    request = {
        "name": "ErrorMonitoring-app",
        "queryString": "fields @message",
        "logGroupNames": ["/app"],
        "clientToken": token,
    }
    query_id = client.put_query_definition(**request)["queryDefinitionId"]
    assert client.put_query_definition(**request)["queryDefinitionId"] == query_id
    definitions = client.describe_query_definitions()["queryDefinitions"]
    assert len(definitions) == 1
    definition = definitions[0]
    assert definition["queryDefinitionId"] == query_id
    assert definition["queryLanguage"] == "CWLI"
    assert definition["logGroupNames"] == ["/app"]
    assert isinstance(definition["lastModified"], int)
    client.put_query_definition(
        name="renamed", queryString="stats count(*)", queryDefinitionId=query_id
    )
    updated = client.describe_query_definitions()["queryDefinitions"][0]
    assert updated["name"] == "renamed"
    assert updated["queryString"] == "stats count(*)"
    assert not updated.get("logGroupNames")
    assert updated["lastModified"] >= definition["lastModified"]
    assert (
        boto3.client("logs", region_name="us-east-1").describe_query_definitions()[
            "queryDefinitions"
        ]
        == []
    )
    assert client.delete_query_definition(queryDefinitionId=query_id)["success"] is True
    assert client.describe_query_definitions()["queryDefinitions"] == []
    with pytest.raises(client.exceptions.ResourceNotFoundException):
        client.put_query_definition(
            name="missing", queryString="fields @message", queryDefinitionId=query_id
        )


@mock_aws
def test_query_definition_filters_and_pagination():
    client = boto3.client("logs", region_name="eu-west-2")
    for name, language in [
        ("app/errors", "CWLI"),
        ("app/count", "CWLI"),
        ("other", "CWLI"),
        ("app/sql", "SQL"),
    ]:
        client.put_query_definition(
            name=name, queryString="test query", queryLanguage=language
        )
    filters = {
        "queryDefinitionNamePrefix": "app/",
        "queryLanguage": "CWLI",
        "maxResults": 1,
    }
    first = client.describe_query_definitions(**filters)
    second = client.describe_query_definitions(**filters, nextToken=first["nextToken"])
    pages = [first, second]
    assert len(pages) == 2
    assert {q["name"] for page in pages for q in page["queryDefinitions"]} == {
        "app/errors",
        "app/count",
    }
    assert "nextToken" not in pages[-1]
    with pytest.raises(client.exceptions.InvalidParameterException):
        client.describe_query_definitions(nextToken="bad-token")
    with pytest.raises(client.exceptions.InvalidParameterException):
        client.describe_query_definitions(maxResults=1001)
    with pytest.raises(client.exceptions.InvalidParameterException):
        client.describe_query_definitions(
            queryDefinitionNamePrefix="other", nextToken=first["nextToken"]
        )


@mock_aws
def test_query_definition_rejects_invalid_language_and_token_reuse():
    client = boto3.client("logs", region_name="eu-west-2")
    with pytest.raises(client.exceptions.InvalidParameterException):
        client.put_query_definition(
            name="test", queryString="test", queryLanguage="invalid"
        )
    token = str(uuid.uuid4())
    client.put_query_definition(
        name="test", queryString="fields @message", clientToken=token
    )
    with pytest.raises(client.exceptions.InvalidParameterException):
        client.put_query_definition(
            name="changed", queryString="fields @message", clientToken=token
        )
    assert client.describe_query_definitions()["queryDefinitions"][0]["name"] == "test"
