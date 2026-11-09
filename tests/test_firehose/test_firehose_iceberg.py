import json
from typing import Any
from unittest import SkipTest

import boto3
import pytest

from moto import mock_aws, settings

for _module in ("sqlglot", "pyiceberg", "pyarrow", "duckdb", "jq"):
    pytest.importorskip(_module)

REGION = "us-east-1"


def _query(athena: Any, sql: str) -> list[list[str | None]]:
    execution_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "history"},
        ResultConfiguration={"OutputLocation": "s3://lake/results/"},
    )["QueryExecutionId"]
    state = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"][
        "Status"
    ]
    assert state["State"] == "SUCCEEDED", state.get("StateChangeReason")
    rows = athena.get_query_results(QueryExecutionId=execution_id)["ResultSet"]["Rows"]
    return [[c.get("VarCharValue") for c in r["Data"]] for r in rows[1:]]


def test_iceberg_destination_routes_records_to_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The engine setting is read by the server process")
    monkeypatch.setenv("MOTO_ATHENA_SQL_ENGINE", "iceberg")
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket="lake")
        boto3.client("glue", region_name=REGION).create_database(
            DatabaseInput={"Name": "history"}
        )
        athena = boto3.client("athena", region_name=REGION)
        for table in ("outcome", "outcome_sandbox"):
            _query(
                athena,
                f"CREATE TABLE {table} (decision_id string, reported_at timestamp, payload string) "
                f"LOCATION 's3://lake/history/{table}' TBLPROPERTIES ('table_type' = 'ICEBERG')",
            )

        firehose = boto3.client("firehose", region_name=REGION)
        query = '{destinationDatabaseName:"history",destinationTableName:(.destination_table // "outcome")}'
        firehose.create_delivery_stream(
            DeliveryStreamName="outcomes",
            DeliveryStreamType="DirectPut",
            IcebergDestinationConfiguration={
                "RoleARN": "arn:aws:iam::123456789012:role/firehose",
                "CatalogConfiguration": {
                    "CatalogARN": "arn:aws:glue:us-east-1:123456789012:catalog"
                },
                "ProcessingConfiguration": {
                    "Enabled": True,
                    "Processors": [
                        {
                            "Type": "MetadataExtraction",
                            "Parameters": [
                                {
                                    "ParameterName": "MetadataExtractionQuery",
                                    "ParameterValue": query,
                                },
                                {
                                    "ParameterName": "JsonParsingEngine",
                                    "ParameterValue": "JQ-1.6",
                                },
                            ],
                        }
                    ],
                },
                "DestinationTableConfigurationList": [
                    {
                        "DestinationDatabaseName": "history",
                        "DestinationTableName": "outcome",
                    },
                    {
                        "DestinationDatabaseName": "history",
                        "DestinationTableName": "outcome_sandbox",
                    },
                ],
                "S3Configuration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/firehose",
                    "BucketARN": "arn:aws:s3:::lake",
                },
            },
        )
        records = [
            {
                "decision_id": "d1",
                "reported_at": "2024-03-01T10:00:00Z",
                "payload": {"a": 1},
            },
            {
                "decision_id": "d2",
                "reported_at": "2024-03-01T11:00:00.5+00:00",
                "payload": "x",
                "destination_table": "outcome_sandbox",
            },
        ]
        response = firehose.put_record_batch(
            DeliveryStreamName="outcomes",
            Records=[{"Data": json.dumps(r).encode()} for r in records],
        )
        assert len(response["RequestResponses"]) == 2

        assert _query(athena, "SELECT decision_id, payload FROM outcome") == [
            ["d1", '{"a": 1}']
        ]
        assert _query(
            athena,
            "SELECT decision_id, CAST(reported_at AS VARCHAR) FROM outcome_sandbox",
        ) == [["d2", "2024-03-01 11:00:00.5"]]
