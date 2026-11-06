# mypy: ignore-errors
import boto3
import pytest

from moto import mock_aws
from moto.core.serialize import XFormedAttributePicker


@mock_aws
@pytest.mark.parametrize(
    "operation", ["get_item", "batch_get_item", "query", "scan", "update_item"]
)
@pytest.mark.parametrize("record_count", [1, 20])
def test_sdk_item_responses_preserve_values_without_attribute_alias_search(
    monkeypatch, operation, record_count
):
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName="serialization",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    item = {
        "id": {"S": "one"},
        "empty_string": {"S": ""},
        "number": {"N": "1.25"},
        "binary": {"B": b"\x00\x01"},
        "boolean": {"BOOL": False},
        "null": {"NULL": True},
        "strings": {"SS": ["a"]},
        "numbers": {"NS": ["1"]},
        "binaries": {"BS": [b"a"]},
        "empty_map": {"M": {}},
        "empty_list": {"L": []},
        "records": {
            "L": [
                {"M": {"S": {"L": [{"N": str(i)}, {"BOOL": False}]}}}
                for i in range(record_count)
            ]
        },
    }
    client.put_item(TableName="serialization", Item=item)
    original = XFormedAttributePicker.__call__
    attribute_lookups = []

    def track(self, context):
        parent = getattr(context.shape, "parent", None)
        if parent is not None and parent.name == "AttributeValue":
            attribute_lookups.append(context.key)
        return original(self, context)

    monkeypatch.setattr(XFormedAttributePicker, "__call__", track)
    key = {"id": {"S": "one"}}
    if operation == "get_item":
        actual = client.get_item(TableName="serialization", Key=key)["Item"]
    elif operation == "batch_get_item":
        actual = client.batch_get_item(RequestItems={"serialization": {"Keys": [key]}})[
            "Responses"
        ]["serialization"][0]
    elif operation == "query":
        actual = client.query(
            TableName="serialization",
            KeyConditionExpression="id = :id",
            ExpressionAttributeValues={":id": {"S": "one"}},
        )["Items"][0]
    elif operation == "scan":
        actual = client.scan(TableName="serialization")["Items"][0]
    else:
        actual = client.update_item(
            TableName="serialization",
            Key=key,
            UpdateExpression="SET empty_string = :value",
            ExpressionAttributeValues={":value": {"S": ""}},
            ReturnValues="ALL_NEW",
        )["Attributes"]

    assert actual == item
    assert attribute_lookups == []
