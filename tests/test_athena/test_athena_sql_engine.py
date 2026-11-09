"""MOTO_ATHENA_SQL_ENGINE=iceberg: Athena queries run over Glue and Iceberg tables in moto's S3."""

import gzip
import io
import json
import os
import textwrap
from typing import Any
from unittest import SkipTest

import boto3
import pytest

from moto import mock_aws, settings

for _module in ("sqlglot", "pyiceberg", "pyarrow", "duckdb"):
    pytest.importorskip(_module)

import pyarrow.parquet as pq  # noqa: E402

REGION = "us-east-1"
BUCKET = "history-bucket"
DATABASE = "history"
OUTPUT = f"s3://{BUCKET}/athena-results/"


@pytest.fixture(name="athena")
def fixture_athena(monkeypatch: pytest.MonkeyPatch) -> Any:
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The engine setting is read by the server process")
    monkeypatch.setenv("MOTO_ATHENA_SQL_ENGINE", "iceberg")
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        glue = boto3.client("glue", region_name=REGION)
        glue.create_database(DatabaseInput={"Name": DATABASE})
        yield boto3.client("athena", region_name=REGION)


def run(athena: Any, sql: str, expect: str = "SUCCEEDED") -> dict[str, Any]:
    execution_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]
    execution = athena.get_query_execution(QueryExecutionId=execution_id)
    status = execution["QueryExecution"]["Status"]
    assert status["State"] == expect, status.get("StateChangeReason")
    return execution["QueryExecution"]


def rows(athena: Any, sql: str) -> list[list[str | None]]:
    """The query's data rows; Athena puts a header row before a SELECT's rows only."""
    execution = run(athena, sql)
    result = athena.get_query_results(QueryExecutionId=execution["QueryExecutionId"])
    data = result["ResultSet"]["Rows"]
    if execution["StatementType"] == "DML":
        data = data[1:]
    return [[cell.get("VarCharValue") for cell in row["Data"]] for row in data]


ORDERS = """
    CREATE TABLE IF NOT EXISTS orders (
        id string,
        amount double,
        quantity int,
        created_at timestamp,
        tags array<string>,
        attributes map<string, string>,
        price decimal(10, 2)
    )
    PARTITIONED BY (day(created_at))
    LOCATION 's3://history-bucket/history/orders/'
    TBLPROPERTIES ('table_type' = 'iceberg', 'format' = 'parquet', 'write_compression' = 'ZSTD')
"""


def test_create_iceberg_table_registers_it_in_glue(athena: Any) -> None:
    execution = run(athena, ORDERS)
    assert execution["StatementType"] == "DDL"

    glue = boto3.client("glue", region_name=REGION)
    table = glue.get_table(DatabaseName=DATABASE, Name="orders")["Table"]
    assert table["Parameters"]["table_type"] == "ICEBERG"
    metadata_location = table["Parameters"]["metadata_location"]
    assert metadata_location.startswith("s3://history-bucket/history/orders/metadata/")

    bucket, key = metadata_location.removeprefix("s3://").split("/", 1)
    s3 = boto3.client("s3", region_name=REGION)
    metadata = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    assert metadata["location"] == "s3://history-bucket/history/orders"
    assert metadata["properties"]["write.parquet.compression-codec"] == "zstd"
    fields = {f["name"]: f["type"] for f in metadata["schemas"][0]["fields"]}
    assert fields["created_at"] == "timestamp"
    assert fields["price"] == "decimal(10, 2)"
    assert fields["tags"]["type"] == "list"
    [spec] = metadata["partition-specs"]
    assert spec["fields"][0]["transform"] == "day"

    # IF NOT EXISTS leaves the table alone; without it, creating it again fails
    run(athena, ORDERS)
    failed = run(athena, ORDERS.replace("IF NOT EXISTS ", ""), expect="FAILED")
    assert "already exists" in failed["Status"]["StateChangeReason"]


def test_insert_and_select_iceberg_rows(athena: Any) -> None:
    run(athena, ORDERS)
    execution = run(
        athena,
        """
        INSERT INTO orders VALUES
            ('a', 1.5, 2, TIMESTAMP '2024-01-01 10:00:00', ARRAY['x', 'y'], MAP(ARRAY['k'], ARRAY['v']), 9.99),
            ('b', 3.0, 1, TIMESTAMP '2024-01-02 11:30:00', NULL, NULL, NULL)
        """,
    )
    assert execution["SubstatementType"] == "INSERT"
    result = athena.get_query_results(QueryExecutionId=execution["QueryExecutionId"])
    assert result["UpdateCount"] == 2

    query = run(athena, "SELECT * FROM orders ORDER BY id")
    result = athena.get_query_results(QueryExecutionId=query["QueryExecutionId"])
    column_info = result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    assert [(c["Name"], c["Type"]) for c in column_info] == [
        ("id", "varchar"),
        ("amount", "double"),
        ("quantity", "integer"),
        ("created_at", "timestamp"),
        ("tags", "array"),
        ("attributes", "map"),
        ("price", "decimal"),
    ]
    data = [
        [c.get("VarCharValue") for c in r["Data"]] for r in result["ResultSet"]["Rows"]
    ]
    assert data == [
        ["id", "amount", "quantity", "created_at", "tags", "attributes", "price"],
        ["a", "1.5", "2", "2024-01-01 10:00:00.000000", "[x, y]", "{k=v}", "9.99"],
        ["b", "3.0", "1", "2024-01-02 11:30:00.000000", None, None, None],
    ]

    # Trino functions are translated for DuckDB; the database may be named
    assert rows(
        athena,
        "SELECT id, date_format(created_at, '%Y-%m-%d') FROM history.orders "
        "WHERE created_at > from_iso8601_timestamp('2024-01-02T00:00:00') ",
    ) == [["b", "2024-01-02"]]

    # Data files are written per day partition
    s3 = boto3.client("s3", region_name=REGION)
    keys = [
        o["Key"]
        for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="history/orders/data/")[
            "Contents"
        ]
    ]
    assert {k.split("/")[3] for k in keys} == {
        "created_at_day=2024-01-01",
        "created_at_day=2024-01-02",
    }


def test_unload_writes_the_result_to_s3(athena: Any) -> None:
    run(athena, ORDERS)
    run(
        athena,
        """
        INSERT INTO orders VALUES
            ('a', 1.5, 2, TIMESTAMP '2024-01-01 10:00:00.123456', ARRAY['x'], MAP(ARRAY['k'], ARRAY['v']), 9.99),
            ('b', 3.0, 1, TIMESTAMP '2024-01-02 11:30:00', NULL, NULL, NULL)
        """,
    )
    s3 = boto3.client("s3", region_name=REGION)

    execution = run(
        athena,
        """
        UNLOAD (WITH q AS (SELECT id, created_at, tags, attributes, price FROM orders)
                SELECT * FROM q ORDER BY id)
        TO 's3://history-bucket/unloads/json/' WITH (format = 'JSON')
        """,
    )
    assert (execution["StatementType"], execution["SubstatementType"]) == (
        "DML",
        "UNLOAD",
    )
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix="unloads/json/")["Contents"]
    assert len(objects) == 1 and objects[0]["Key"].endswith(".gz")
    body = s3.get_object(Bucket=BUCKET, Key=objects[0]["Key"])["Body"].read()
    lines = [json.loads(line) for line in gzip.decompress(body).decode().splitlines()]
    assert lines == [
        {
            "id": "a",
            "created_at": "2024-01-01 10:00:00.123",
            "tags": ["x"],
            "attributes": {"k": "v"},
            "price": 9.99,
        },
        {
            "id": "b",
            "created_at": "2024-01-02 11:30:00.000",
            "tags": None,
            "attributes": None,
            "price": None,
        },
    ]

    run(
        athena,
        "UNLOAD (SELECT id, quantity FROM orders) TO 's3://history-bucket/unloads/parquet/'",
    )
    key = s3.list_objects_v2(Bucket=BUCKET, Prefix="unloads/parquet/")["Contents"][0][
        "Key"
    ]
    table = pq.read_table(
        io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    )
    assert sorted(table.to_pylist(), key=lambda r: r["id"]) == [
        {"id": "a", "quantity": 2},
        {"id": "b", "quantity": 1},
    ]

    # Athena writes no files for an empty result
    run(
        athena,
        "UNLOAD (SELECT id FROM orders WHERE id = 'z') TO 's3://history-bucket/unloads/empty/' WITH (format = 'JSON')",
    )
    assert "Contents" not in s3.list_objects_v2(Bucket=BUCKET, Prefix="unloads/empty/")


def test_insert_select_update_and_delete(athena: Any) -> None:
    run(athena, ORDERS)
    run(athena, "INSERT INTO orders (id, quantity) VALUES ('a', 1), ('b', 2), ('c', 3)")
    run(
        athena,
        "INSERT INTO orders (id, quantity) SELECT id || '2', quantity * 10 FROM orders WHERE id <> 'a'",
    )

    update = run(
        athena,
        "UPDATE orders SET quantity = quantity + 1, amount = 0.5 WHERE orders.id IN ('a', 'b2')",
    )
    result = athena.get_query_results(QueryExecutionId=update["QueryExecutionId"])
    assert result["UpdateCount"] == 2

    delete = run(athena, "DELETE FROM orders WHERE quantity >= 30")
    result = athena.get_query_results(QueryExecutionId=delete["QueryExecutionId"])
    assert result["UpdateCount"] == 1

    assert rows(athena, "SELECT id, quantity, amount FROM orders ORDER BY id") == [
        ["a", "2", "0.5"],
        ["b", "2", None],
        ["b2", "21", "0.5"],
        ["c", "3", None],
    ]


def test_partition_transforms(athena: Any) -> None:
    pytest.importorskip("pyiceberg_core")
    run(
        athena,
        """
        CREATE TABLE events (id string, kind string, at timestamp)
        PARTITIONED BY (bucket(4, id), kind, month(at))
        LOCATION 's3://history-bucket/history/events'
        TBLPROPERTIES ('table_type' = 'ICEBERG')
        """,
    )
    run(
        athena,
        "INSERT INTO events VALUES ('1', 'click', TIMESTAMP '2024-03-01 00:00:00')",
    )
    assert rows(athena, "SELECT id, kind FROM events") == [["1", "click"]]

    table = boto3.client("glue", region_name=REGION).get_table(
        DatabaseName=DATABASE, Name="events"
    )["Table"]
    bucket, key = (
        table["Parameters"]["metadata_location"].removeprefix("s3://").split("/", 1)
    )
    s3 = boto3.client("s3", region_name=REGION)
    metadata = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    spec = metadata["partition-specs"][0]["fields"]
    assert [f["transform"] for f in spec] == ["bucket[4]", "identity", "month"]


def test_create_table_as_select(athena: Any) -> None:
    pytest.importorskip("pyiceberg_core")
    run(athena, ORDERS)
    run(
        athena,
        """
        INSERT INTO orders VALUES
            ('a', 1.5, 2, TIMESTAMP '2024-01-01 10:00:00', NULL, NULL, NULL),
            ('b', 3.0, 1, TIMESTAMP '2024-01-02 11:30:00', NULL, NULL, NULL),
            ('c', 4.5, 1, TIMESTAMP '2024-01-02 12:00:00', NULL, NULL, NULL)
        """,
    )
    ctas = """
        CREATE TABLE IF NOT EXISTS "history"."totals"
        WITH (
            table_type = 'ICEBERG',
            location = 's3://history-bucket/history/totals',
            partitioning = ARRAY['day(__timestamp__)', 'bucket(quantity, 16)'],
            is_external = false,
            write_compression = 'SNAPPY',
            format = 'PARQUET'
        ) AS
        SELECT *, TIMESTAMP '1970-01-01 00:00:00.000000' AS __timestamp__ FROM (
            SELECT quantity, SUM(amount) AS total FROM orders GROUP BY quantity
        )
    """
    execution = run(athena, ctas)
    assert execution["SubstatementType"] == "CREATE_TABLE_AS_SELECT"
    result = athena.get_query_results(QueryExecutionId=execution["QueryExecutionId"])
    assert result["UpdateCount"] == 2
    assert rows(athena, "SELECT quantity, total FROM totals ORDER BY quantity") == [
        ["1", "7.5"],
        ["2", "1.5"],
    ]

    table = boto3.client("glue", region_name=REGION).get_table(
        DatabaseName=DATABASE, Name="totals"
    )["Table"]
    bucket, key = (
        table["Parameters"]["metadata_location"].removeprefix("s3://").split("/", 1)
    )
    s3 = boto3.client("s3", region_name=REGION)
    metadata = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    spec = metadata["partition-specs"][0]["fields"]
    assert [f["transform"] for f in spec] == ["day", "bucket[16]"]
    assert metadata["properties"]["write.parquet.compression-codec"] == "snappy"

    # IF NOT EXISTS leaves the table and its rows alone
    run(athena, ctas)
    assert len(rows(athena, "SELECT * FROM totals")) == 2

    hive = "CREATE TABLE copy WITH (format = 'PARQUET') AS SELECT * FROM orders"
    failed = run(athena, hive, expect="FAILED")
    assert "Iceberg" in failed["Status"]["StateChangeReason"]


def test_execution_parameters(athena: Any) -> None:
    run(athena, ORDERS)
    run(
        athena,
        """
        INSERT INTO orders VALUES
            ('a', 1.5, 2, TIMESTAMP '2024-01-01 10:00:00', NULL, NULL, NULL),
            ('b?', 3.0, 1, TIMESTAMP '2024-01-02 11:30:00', NULL, NULL, NULL)
        """,
    )

    def query(sql: str, parameters: list[str], expect: str = "SUCCEEDED") -> Any:
        execution_id = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": OUTPUT},
            ExecutionParameters=parameters,
        )["QueryExecutionId"]
        execution = athena.get_query_execution(QueryExecutionId=execution_id)
        assert execution["QueryExecution"]["Status"]["State"] == expect
        return execution_id

    execution_id = query(
        "SELECT id FROM orders WHERE amount > ? AND id <> '?' -- why?\n AND quantity = ?",
        ["1.0", "1"],
    )
    result = athena.get_query_results(QueryExecutionId=execution_id)
    assert [r["Data"][0]["VarCharValue"] for r in result["ResultSet"]["Rows"]] == [
        "id",
        "b?",
    ]

    # A parameter that is not a SQL literal is read as a string
    execution_id = query(
        "SELECT id FROM orders WHERE created_at < CAST(? AS TIMESTAMP) AND id = ?",
        ["2024-01-01 12:00:00.000000", "'a'"],
    )
    result = athena.get_query_results(QueryExecutionId=execution_id)
    assert result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"] == "a"

    query("SELECT id FROM orders WHERE id = ?", ["'a'", "'b'"], expect="FAILED")


def test_struct_fields_and_qualified_columns(athena: Any) -> None:
    lines = [
        {
            "id": "p1",
            "card": {"last4": "4242", "customer": {"id": "c1", "name": "Ada"}},
        },
        {
            "id": "p2",
            "card": {"last4": "1111", "customer": {"id": "c2", "name": "Bob"}},
        },
    ]
    boto3.client("s3", region_name=REGION).put_object(
        Bucket=BUCKET,
        Key="payments/part-0.json",
        Body="\n".join(json.dumps(line) for line in lines),
    )
    run(
        athena,
        """
        CREATE EXTERNAL TABLE payments (
            id string,
            card struct<last4:string,customer:struct<id:string,name:string>>
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION 's3://history-bucket/payments/'
        """,
    )
    assert rows(
        athena,
        'SELECT "card"."customer"."name", card.last4 FROM payments '
        'WHERE "card"."customer"."id" = \'c1\'',
    ) == [["Ada", "4242"]]
    assert rows(
        athena,
        'SELECT "history"."payments"."id" FROM "history"."payments" ORDER BY 1',
    ) == [["p1"], ["p2"]]
    assert rows(athena, "SELECT p.card.customer.id FROM payments p ORDER BY p.id") == [
        ["c1"],
        ["c2"],
    ]


def test_json_extract_cast_to_varchar_is_the_value(athena: Any) -> None:
    run(
        athena,
        "CREATE TABLE docs (id string, body string) "
        "LOCATION 's3://history-bucket/history/docs' TBLPROPERTIES ('table_type' = 'ICEBERG')",
    )
    run(athena, """INSERT INTO docs VALUES ('1', '{"name": "Ada", "n": 2}')""")
    assert rows(
        athena,
        "SELECT CAST(JSON_EXTRACT(body, '$.name') AS VARCHAR), "
        "CAST(JSON_EXTRACT(body, '$.n') AS INTEGER) FROM docs",
    ) == [["Ada", "2"]]


def test_map_agg(athena: Any) -> None:
    run(athena, ORDERS)
    run(
        athena,
        """
        INSERT INTO orders VALUES
            ('a', 1.5, 2, TIMESTAMP '2024-01-01 10:00:00', NULL, NULL, NULL),
            ('b', 3.0, 1, TIMESTAMP '2024-01-02 11:30:00', NULL, NULL, NULL)
        """,
    )
    assert rows(
        athena,
        "SELECT element_at(m, 'b') FROM (SELECT map_agg(id, quantity) AS m FROM orders)",
    ) == [["1"]]


def test_federated_postgres_catalog(
    athena: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn = os.environ.get("MOTO_TEST_POSTGRES_DSN")
    if not dsn:
        raise SkipTest(
            "Set MOTO_TEST_POSTGRES_DSN to a PostgreSQL database to run this"
        )
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS moto_fed")
        conn.execute("DROP TABLE IF EXISTS moto_fed.items")
        conn.execute("CREATE TABLE moto_fed.items (id text, data jsonb)")
        conn.execute(
            """INSERT INTO moto_fed.items VALUES ('a', '{"tag": "x"}'), ('b', '{"tag": "y"}')"""
        )
    monkeypatch.setenv("MOTO_ATHENA_FEDERATED_CATALOGS", json.dumps({"pg-*": dsn}))
    execution_id = athena.start_query_execution(
        QueryString="""SELECT id FROM "pg-main"."moto_fed"."items" """
        "WHERE CAST(JSON_EXTRACT(data, '$.tag') AS VARCHAR) = 'y'",
        QueryExecutionContext={"Catalog": "pg-main", "Database": "moto_fed"},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]
    status = athena.get_query_execution(QueryExecutionId=execution_id)[
        "QueryExecution"
    ]["Status"]
    assert status["State"] == "SUCCEEDED", status.get("StateChangeReason")
    result = athena.get_query_results(QueryExecutionId=execution_id)
    assert result["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"] == "b"


def test_alter_table_add_columns(athena: Any) -> None:
    run(athena, ORDERS)
    run(athena, "INSERT INTO orders (id) VALUES ('a')")
    run(
        athena,
        "ALTER TABLE orders ADD COLUMNS (note string, sets map<string, array<double>>)",
    )
    run(athena, "INSERT INTO orders (id, note) VALUES ('b', 'hello')")

    assert rows(athena, "SELECT id, note, sets FROM orders ORDER BY id") == [
        ["a", None, None],
        ["b", "hello", None],
    ]
    columns = athena.get_table_metadata(
        CatalogName="AwsDataCatalog", DatabaseName=DATABASE, TableName="orders"
    )["TableMetadata"]["Columns"]
    assert columns[-2:] == [
        {"Name": "note", "Type": "string"},
        {"Name": "sets", "Type": "map<string,array<double>>"},
    ]

    failed = run(
        athena, "ALTER TABLE orders ADD COLUMNS (note string)", expect="FAILED"
    )
    assert "note" in failed["Status"]["StateChangeReason"]


def test_hive_json_table(athena: Any) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    lines = [
        {"data_retention": {"value": 30, "unit": "days"}, "created_by": "alice"},
        {"created_by": "bob", "unknown": 1},
    ]
    body = "\n".join(json.dumps(line) for line in lines)
    s3.put_object(
        Bucket=BUCKET, Key="settings/workspace_settings/part-0.json", Body=body
    )
    run(
        athena,
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS workspace_settings (
            data_retention struct<value:int,unit:string>,
            created_by string
        )
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION 's3://history-bucket/settings/workspace_settings/'
        TBLPROPERTIES ('ignore.malformed.json' = 'true');
        """,
    )

    table = boto3.client("glue", region_name=REGION).get_table(
        DatabaseName=DATABASE, Name="workspace_settings"
    )["Table"]
    assert "metadata_location" not in table["Parameters"]
    storage = table["StorageDescriptor"]
    assert storage["Location"] == "s3://history-bucket/settings/workspace_settings/"
    assert (
        storage["SerdeInfo"]["SerializationLibrary"]
        == "org.apache.hive.hcatalog.data.JsonSerDe"
    )
    assert storage["Columns"] == [
        {"Name": "data_retention", "Type": "struct<value:int,unit:string>"},
        {"Name": "created_by", "Type": "string"},
    ]

    assert rows(
        athena,
        "SELECT created_by, data_retention.value, data_retention FROM workspace_settings ORDER BY created_by",
    ) == [["alice", "30", "{value=30, unit=days}"], ["bob", None, None]]

    failed = run(
        athena,
        "INSERT INTO workspace_settings (created_by) VALUES ('x')",
        expect="FAILED",
    )
    assert "non-Iceberg" in failed["Status"]["StateChangeReason"]


def test_metadata_statements(athena: Any) -> None:
    run(athena, ORDERS)
    assert rows(athena, "SHOW TABLES") == [["orders"]]
    assert [r[0] for r in rows(athena, "SHOW COLUMNS IN orders")][:2] == [
        "id",
        "amount",
    ]
    assert rows(athena, "DESCRIBE orders")[0][:2] == ["id", "string"]

    run(athena, "DROP TABLE orders")
    assert rows(athena, "SHOW TABLES") == []
    s3 = boto3.client("s3", region_name=REGION)
    assert (
        s3.list_objects_v2(Bucket=BUCKET, Prefix="history/orders/").get("KeyCount") == 0
    )
    run(athena, "DROP TABLE IF EXISTS orders")


def test_failures_are_reported(athena: Any) -> None:
    failed = run(athena, "SELECT * FROM missing", expect="FAILED")
    assert "missing" in failed["Status"]["StateChangeReason"]
    assert (
        failed["Status"]["AthenaError"]["ErrorMessage"]
        == failed["Status"]["StateChangeReason"]
    )
    with pytest.raises(athena.exceptions.InvalidRequestException):
        athena.get_query_results(QueryExecutionId=failed["QueryExecutionId"])

    run(athena, "SELEC nonsense", expect="FAILED")


def test_get_table_metadata(athena: Any) -> None:
    run(athena, ORDERS)
    metadata = athena.get_table_metadata(
        CatalogName="AwsDataCatalog", DatabaseName=DATABASE, TableName="orders"
    )["TableMetadata"]
    assert metadata["Name"] == "orders"
    assert metadata["Parameters"]["table_type"] == "ICEBERG"
    assert metadata["Columns"][0] == {"Name": "id", "Type": "string"}

    listed = athena.list_table_metadata(
        CatalogName="AwsDataCatalog", DatabaseName=DATABASE
    )
    assert [t["Name"] for t in listed["TableMetadataList"]] == ["orders"]

    with pytest.raises(athena.exceptions.MetadataException):
        athena.get_table_metadata(
            CatalogName="AwsDataCatalog", DatabaseName=DATABASE, TableName="missing"
        )


@mock_aws
def test_queries_are_not_run_without_the_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if settings.TEST_SERVER_MODE:
        raise SkipTest("The engine setting is read by the server process")
    monkeypatch.delenv("MOTO_ATHENA_SQL_ENGINE", raising=False)
    boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
    boto3.client("glue", region_name=REGION).create_database(
        DatabaseInput={"Name": DATABASE}
    )
    athena = boto3.client("athena", region_name=REGION)

    execution = run(athena, ORDERS)
    assert "SubstatementType" not in execution
    result = athena.get_query_results(QueryExecutionId=execution["QueryExecutionId"])
    assert result["ResultSet"]["Rows"] == []
    tables = boto3.client("glue", region_name=REGION).get_tables(DatabaseName=DATABASE)[
        "TableList"
    ]
    assert tables == []
    run(athena, "SELECT * FROM missing")


ALEMBIC_ENV = """
from alembic import context
from alembic.ddl.impl import DefaultImpl


class AthenaImpl(DefaultImpl):
    __dialect__ = "awsathena"


with context.config.attributes["connection"] as connection:
    context.configure(connection=connection, transaction_per_migration=True)
    with context.begin_transaction():
        context.run_migrations()
"""

ALEMBIC_BASELINE = """
import sqlalchemy as sa
from alembic import op
from sqlalchemy.schema import CreateTable

revision = "0001"
down_revision = None


def upgrade():
    table = sa.Table(
        "decisions",
        sa.MetaData(),
        sa.Column("decision_id", sa.String(), nullable=False),
        sa.Column("status_code", sa.Integer()),
        sa.Column(
            "start_datetime",
            sa.DateTime(timezone=True),
            awsathena_partition=True,
            awsathena_partition_transform="day",
        ),
        awsathena_location="s3://history-bucket/history/decisions/",
        awsathena_tblproperties={"table_type": "iceberg", "format": "parquet"},
    )
    op.get_bind().execute(CreateTable(table, if_not_exists=True))
"""

ALEMBIC_ADD_COLUMN = """
import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"


def upgrade():
    existing = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("decisions")}
    if "flow_slug" not in existing:
        op.execute("ALTER TABLE decisions ADD COLUMNS (flow_slug string)")
"""


def test_alembic_migrations_through_pyathena(athena: Any, tmp_path: Any) -> None:
    pytest.importorskip("pyathena")
    pytest.importorskip("alembic")
    import sqlalchemy as sa
    from alembic import command
    from alembic.config import Config

    (tmp_path / "versions").mkdir()
    (tmp_path / "env.py").write_text(textwrap.dedent(ALEMBIC_ENV))
    (tmp_path / "script.py.mako").write_text("")
    (tmp_path / "versions" / "0001_baseline.py").write_text(ALEMBIC_BASELINE)
    (tmp_path / "versions" / "0002_add_column.py").write_text(ALEMBIC_ADD_COLUMN)

    engine = sa.create_engine(
        f"awsathena+rest://@athena.{REGION}.amazonaws.com:443/{DATABASE}"
        f"?s3_staging_dir={OUTPUT}&region_name={REGION}"
    )
    # Alembic cannot create its version table as an Iceberg table, so it is created up front, as in production
    with engine.connect() as connection:
        connection.execute(
            sa.text(
                "CREATE TABLE alembic_version (version_num string) "
                "LOCATION 's3://history-bucket/history/alembic_version' "
                "TBLPROPERTIES ('table_type' = 'iceberg')"
            )
        )

    config = Config()
    config.set_main_option("script_location", str(tmp_path))
    for target in ("0001", "head"):
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, target)

    with engine.connect() as connection:
        versions = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).fetchall()
        assert versions == [("0002",)]
        inspector = sa.inspect(connection)
        columns = {c["name"]: c["type"] for c in inspector.get_columns("decisions")}
        assert list(columns) == [
            "decision_id",
            "status_code",
            "start_datetime",
            "flow_slug",
        ]
        assert isinstance(columns["start_datetime"], sa.TIMESTAMP)
        assert inspector.has_table("alembic_version")
        assert not inspector.has_table("missing")

        connection.execute(
            sa.text(
                "INSERT INTO decisions VALUES ('d1', 200, TIMESTAMP '2024-05-01 12:00:00', 'my-flow')"
            )
        )
        result = connection.execute(
            sa.text("SELECT decision_id, start_datetime, flow_slug FROM decisions")
        )
        [(decision_id, start_datetime, flow_slug)] = result.fetchall()
        assert (decision_id, start_datetime.isoformat(), flow_slug) == (
            "d1",
            "2024-05-01T12:00:00",
            "my-flow",
        )

    # Upgrading again is a no-op
    with engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")
    with engine.connect() as connection:
        assert (
            connection.execute(sa.text("SELECT count(*) FROM alembic_version")).scalar()
            == 1
        )


def test_query_state_change_events(athena: Any) -> None:
    sqs = boto3.client("sqs", region_name=REGION)
    queue_url = sqs.create_queue(QueueName="athena-events")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    events = boto3.client("events", region_name=REGION)
    events.put_rule(
        Name="athena-done",
        EventPattern=json.dumps(
            {
                "source": ["aws.athena"],
                "detail-type": ["Athena Query State Change"],
                "detail": {"currentState": ["SUCCEEDED", "FAILED"]},
            }
        ),
    )
    events.put_targets(Rule="athena-done", Targets=[{"Id": "q", "Arn": queue_arn}])

    succeeded = run(athena, "SELECT 1")["QueryExecutionId"]
    failed = run(athena, "SELECT * FROM missing", expect="FAILED")["QueryExecutionId"]

    received: dict[str, Any] = {}
    while len(received) < 2:
        messages = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5
        ).get("Messages")
        assert messages, f"only got events for {list(received)}"
        for message in messages:
            event = json.loads(message["Body"])
            received[event["detail"]["queryExecutionId"]] = event
    assert received[succeeded]["detail"]["currentState"] == "SUCCEEDED"
    assert received[succeeded]["detail"]["workgroupName"] == "primary"
    assert received[failed]["detail"]["currentState"] == "FAILED"
