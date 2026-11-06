from concurrent.futures import ThreadPoolExecutor
from threading import Event

import boto3
import pytest

from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID
from moto.dynamodb.models import dynamodb_backends


@pytest.mark.parametrize("partition", ["existing", "new"])
@mock_aws
def test_index_scan_while_another_thread_adds_an_item(partition):
    client = boto3.client("dynamodb", region_name="us-east-1")
    client.create_table(
        TableName="checkpoints",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": name, "AttributeType": "S"}
            for name in ["pk", "sk", "gsi"]
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-session",
                "KeySchema": [{"AttributeName": "gsi", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    client.put_item(
        TableName="checkpoints",
        Item={"pk": {"S": "existing"}, "sk": {"S": "first"}, "gsi": {"S": "session"}},
    )
    reading = Event()
    written = Event()

    class PausingAttributes(dict):
        def __iter__(self):
            reading.set()
            assert written.wait(5), "Writer did not complete"
            return super().__iter__()

    table = dynamodb_backends[DEFAULT_ACCOUNT_ID]["us-east-1"].get_table("checkpoints")
    item = table.all_items()[0]
    item.attrs = PausingAttributes(item.attrs)
    with ThreadPoolExecutor(max_workers=1) as executor:
        scan = executor.submit(
            client.scan, TableName="checkpoints", IndexName="by-session"
        )
        try:
            assert reading.wait(5), "Scan did not begin reading the item"
            client.put_item(
                TableName="checkpoints",
                Item={
                    "pk": {"S": partition},
                    "sk": {"S": "second"},
                    "gsi": {"S": "session"},
                },
            )
        finally:
            written.set()
        assert scan.result()["Count"] >= 1
    assert client.scan(TableName="checkpoints", IndexName="by-session")["Count"] == 2
