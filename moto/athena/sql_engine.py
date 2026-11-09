"""Runs Athena SQL against moto's Glue Data Catalog and S3; enabled with MOTO_ATHENA_SQL_ENGINE=iceberg.

sqlglot parses the query in Athena's dialects (Hive for DDL, Trino for DML). Iceberg tables are read and written
with pyiceberg through a FileIO over moto's in-process S3, so decorator and server mode behave alike. Rows are
computed in DuckDB: each table a query names is loaded into it first, so this suits test-sized data only.

sqlglot, pyiceberg, pyarrow and duckdb are optional dependencies, imported only when this module is.
"""

import gzip
import io
import json
import logging
import os
import re
import tempfile
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import sqlglot
from pyiceberg.catalog import Catalog, MetastoreCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
)
from sqlglot import exp

from moto import settings
from moto.athena.models import glue_table_columns
from moto.glue.exceptions import EntityNotFoundException
from moto.glue.models import glue_backends
from moto.s3.models import s3_backends
from moto.utilities import iceberg_rest
from moto.utilities.utils import get_partition

T = exp.DataType.Type
_logger = logging.getLogger(__name__)
# Commits read and replace a table's current metadata; serialise them across server threads
_LOCK = threading.RLock()


class QueryFailed(Exception):
    pass


_UNLOAD = re.compile(
    r"^UNLOAD\s*\((?P<query>.*)\)\s*TO\s*'(?P<location>[^']*)'\s*(?:WITH\s*\((?P<options>.*)\))?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _unload_value(value: Any) -> Any:
    """A value as Athena writes it in UNLOAD's JSON and text output."""
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat(sep=" ", timespec="milliseconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value) if value != value.to_integral_value() else int(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: _unload_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unload_value(v) for v in value]
    return value


@dataclass
class Outcome:
    statement_type: str
    substatement_type: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    column_info: list[dict[str, Any]] = field(default_factory=list)
    update_count: int | None = None
    error: str | None = None


class _MotoS3Handler(pafs.FileSystemHandler):  # type: ignore[misc]
    """A pyarrow filesystem over moto's S3 backend; paths are `bucket/key`."""

    def __init__(self, account_id: str, partition: str):
        self.account_id = account_id
        self.partition = partition

    @property
    def backend(self) -> Any:
        return s3_backends[self.account_id][self.partition]

    @staticmethod
    def _split(path: str) -> tuple[str, str]:
        bucket, _, key = path.partition("/")
        return bucket, key

    def _get(self, path: str) -> Any:
        try:
            return self.backend.get_object(*self._split(path))
        except Exception:
            return None

    def _keys(self, prefix: str) -> Iterator[tuple[str, Any]]:
        bucket, key_prefix = self._split(prefix.rstrip("/") + "/")
        for name, key in list(self.backend.get_bucket(bucket).keys.items()):
            if name.startswith(key_prefix):
                yield f"{bucket}/{name}", key

    def get_type_name(self) -> str:
        return "moto-s3"

    def normalize_path(self, path: str) -> str:
        return path

    def equals(self, other: Any) -> bool:
        return isinstance(other, _MotoS3Handler) and (
            other.account_id,
            other.partition,
        ) == (
            self.account_id,
            self.partition,
        )

    def get_file_info(self, paths: list[str]) -> list[pafs.FileInfo]:
        infos = []
        for path in paths:
            key = self._get(path)
            if key is not None:
                infos.append(pafs.FileInfo(path, pafs.FileType.File, size=key.size))
            elif any(True for _ in self._keys(path)):
                infos.append(pafs.FileInfo(path, pafs.FileType.Directory))
            else:
                infos.append(pafs.FileInfo(path, pafs.FileType.NotFound))
        return infos

    def get_file_info_selector(
        self, selector: pafs.FileSelector
    ) -> list[pafs.FileInfo]:
        return [
            pafs.FileInfo(path, pafs.FileType.File, size=key.size)
            for path, key in self._keys(selector.base_dir)
        ]

    def create_dir(self, path: str, recursive: bool) -> None:
        pass

    def delete_dir(self, path: str) -> None:
        self.delete_dir_contents(path)

    def delete_dir_contents(self, path: str, missing_dir_ok: bool = False) -> None:
        for name, _ in list(self._keys(path)):
            self.delete_file(name)

    def delete_root_dir_contents(self) -> None:
        raise NotImplementedError

    def delete_file(self, path: str) -> None:
        self.backend.delete_object(*self._split(path))

    def move(self, src: str, dest: str) -> None:
        self.copy_file(src, dest)
        self.delete_file(src)

    def copy_file(self, src: str, dest: str) -> None:
        self.backend.put_object(*self._split(dest), self._read(src))

    def _read(self, path: str) -> bytes:
        key = self._get(path)
        if key is None:
            raise FileNotFoundError(path)
        return key.value

    def open_input_stream(self, path: str) -> pa.NativeFile:
        return pa.BufferReader(self._read(path))

    def open_input_file(self, path: str) -> pa.NativeFile:
        return pa.BufferReader(self._read(path))

    def open_output_stream(self, path: str, metadata: Any) -> pa.NativeFile:
        return pa.PythonFile(_Upload(self, path), mode="w")

    def open_append_stream(self, path: str, metadata: Any) -> pa.NativeFile:
        raise NotImplementedError


class _Upload:
    """Buffers a write and stores it as one S3 object on close."""

    def __init__(self, handler: _MotoS3Handler, path: str):
        self.handler = handler
        self.path = path
        self.chunks: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> int:
        self.chunks.append(bytes(data))
        return len(data)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            bucket, key = self.handler._split(self.path)
            self.handler.backend.put_object(bucket, key, b"".join(self.chunks))


class MotoS3FileIO(PyArrowFileIO):
    """pyiceberg FileIO that reads and writes moto's S3 in-process instead of over the network."""

    def _initialize_fs(self, scheme: str, netloc: str | None = None) -> pafs.FileSystem:
        if scheme in {"s3", "s3a", "s3n"}:
            handler = _MotoS3Handler(
                self.properties["moto.account-id"], self.properties["moto.partition"]
            )
            return pafs.PyFileSystem(handler)
        return super()._initialize_fs(scheme, netloc)


def _not_supported(*_: Any, **__: Any) -> Any:
    raise NotImplementedError


class _GlueCatalog(MetastoreCatalog):
    """pyiceberg catalog over moto's Glue backend, recording `metadata_location` on the table as Athena does."""

    def __init__(self, glue: Any, account_id: str, partition: str):
        properties = {
            PY_IO_IMPL: f"{__name__}.MotoS3FileIO",
            "moto.account-id": account_id,
            "moto.partition": partition,
        }
        super().__init__("AwsDataCatalog", **properties)
        self.glue = glue

    def load_table(self, identifier: str | tuple[str, ...]) -> Table:
        database, name = self.identifier_to_database_and_table(
            identifier, NoSuchTableError
        )
        try:
            glue_table = self.glue.get_table(database, name).as_dict()
        except EntityNotFoundException:
            raise NoSuchTableError(f"{database}.{name}")
        location = glue_table.get("Parameters", {}).get("metadata_location")
        if not location:
            raise NoSuchTableError(f"{database}.{name} is not an Iceberg table")
        io = self._load_file_io(location=location)
        metadata = FromInputFile.table_metadata(io.new_input(location))
        return Table((database, name), metadata, location, io, self)

    def create_table(  # type: ignore[override]
        self,
        identifier: tuple[str, str],
        schema: Schema,
        location: str | None,
        partition_spec: PartitionSpec,
        properties: dict[str, str],
    ) -> Table:
        staged = self._create_staged_table(
            identifier, schema, location, partition_spec, properties=properties
        )
        self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
        self._register(identifier, staged.metadata, staged.metadata_location, None)
        return self.load_table(identifier)

    def commit_table(
        self, table: Table, requirements: tuple[Any, ...], updates: tuple[Any, ...]
    ) -> CommitTableResponse:
        identifier = self.identifier_to_database_and_table(
            table.name(), NoSuchTableError
        )
        current = self.load_table(identifier)
        staged = self._update_and_stage_table(
            current, identifier, requirements, updates
        )
        self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
        self._register(
            identifier,
            staged.metadata,
            staged.metadata_location,
            current.metadata_location,
        )
        return CommitTableResponse(
            metadata=staged.metadata,
            metadata_location=staged.metadata_location,  # type: ignore[call-arg]
        )

    def _register(
        self,
        identifier: tuple[str, str],
        metadata: Any,
        location: str,
        previous: str | None,
    ) -> None:
        database, name = identifier
        columns = [
            {"Name": f.name, "Type": _hive_type(f.field_type)}
            for f in metadata.schema().fields
        ]
        parameters = {"table_type": "ICEBERG", "metadata_location": location}
        if previous is None:
            table_input = {
                "Name": name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": parameters,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": metadata.location,
                },
            }
            self.glue.create_table(database, name, table_input)
            return
        current = self.glue.get_table(database, name).as_dict()
        table_input = {
            k: v
            for k, v in current.items()
            if k in ("Name", "TableType", "Description", "PartitionKeys")
        }
        table_input["StorageDescriptor"] = {
            **current.get("StorageDescriptor", {}),
            "Columns": columns,
        }
        table_input["Parameters"] = {
            **current.get("Parameters", {}),
            **parameters,
            "previous_metadata_location": previous,
        }
        self.glue.update_table(database, name, table_input)

    load_namespace_properties = _not_supported
    register_table = _not_supported
    drop_table = _not_supported
    rename_table = _not_supported
    create_namespace = _not_supported
    drop_namespace = _not_supported
    list_tables = _not_supported
    list_namespaces = _not_supported
    list_views = _not_supported
    load_view = _not_supported
    update_namespace_properties = _not_supported
    register_view = _not_supported
    drop_view = _not_supported
    view_exists = _not_supported


# Athena DDL type → Iceberg type. Athena writes `timestamp` as Iceberg `timestamp` (no time zone): its DDL cannot
# create `timestamptz` columns, so only the explicit `timestamp with time zone` spelling maps to that.
_PRIMITIVES: dict[Any, IcebergType] = {
    **dict.fromkeys([T.TEXT, T.VARCHAR, T.CHAR, T.NVARCHAR, T.NCHAR], StringType()),
    **dict.fromkeys([T.INT, T.SMALLINT, T.TINYINT], IntegerType()),
    T.BIGINT: LongType(),
    T.DOUBLE: DoubleType(),
    T.FLOAT: FloatType(),
    T.BOOLEAN: BooleanType(),
    T.DATE: DateType(),
    **dict.fromkeys([T.TIMESTAMP, T.DATETIME], TimestampType()),
    **dict.fromkeys([T.TIMESTAMPTZ, T.TIMESTAMPLTZ], TimestamptzType()),
    **dict.fromkeys([T.BINARY, T.VARBINARY], BinaryType()),
}


class _Ids:
    """Placeholder field ids; pyiceberg assigns the real ones when it creates or alters a table."""

    def __init__(self) -> None:
        self.last = 0

    def __call__(self) -> int:
        self.last += 1
        return self.last


def _iceberg_type(data_type: exp.DataType, ids: _Ids) -> IcebergType:
    kind = data_type.this
    if kind in _PRIMITIVES:
        return _PRIMITIVES[kind]
    params = data_type.expressions
    if kind == T.DECIMAL:
        values = [int(p.name) for p in params]
        return DecimalType(
            values[0] if values else 10, values[1] if len(values) > 1 else 0
        )
    if kind == T.ARRAY:
        return ListType(ids(), _iceberg_type(params[0], ids), element_required=False)
    if kind == T.MAP:
        key, value = params
        return MapType(
            ids(),
            _iceberg_type(key, ids),
            ids(),
            _iceberg_type(value, ids),
            value_required=False,
        )
    if kind == T.STRUCT:
        fields = [
            NestedField(
                ids(), p.name, _iceberg_type(p.args["kind"], ids), required=False
            )
            for p in params
        ]
        return StructType(*fields)
    raise QueryFailed(
        f"NOT_SUPPORTED: Unsupported column type: {data_type.sql('hive')}"
    )


def _hive_type(iceberg_type: IcebergType) -> str:
    """The type as Athena records it in the Glue Data Catalog."""
    if isinstance(iceberg_type, ListType):
        return f"array<{_hive_type(iceberg_type.element_type)}>"
    if isinstance(iceberg_type, MapType):
        return f"map<{_hive_type(iceberg_type.key_type)},{_hive_type(iceberg_type.value_type)}>"
    if isinstance(iceberg_type, StructType):
        return (
            "struct<"
            + ",".join(
                f"{f.name}:{_hive_type(f.field_type)}" for f in iceberg_type.fields
            )
            + ">"
        )
    if isinstance(iceberg_type, DecimalType):
        return f"decimal({iceberg_type.precision},{iceberg_type.scale})"
    names: dict[type[IcebergType], str] = {
        StringType: "string",
        IntegerType: "int",
        LongType: "bigint",
        DoubleType: "double",
        FloatType: "float",
        BooleanType: "boolean",
        DateType: "date",
        TimestampType: "timestamp",
        TimestamptzType: "timestamp with time zone",
        BinaryType: "binary",
    }
    return names.get(type(iceberg_type), str(iceberg_type))


_TRANSFORMS: dict[str, tuple[Any, str]] = {
    "year": (YearTransform, "year"),
    "month": (MonthTransform, "month"),
    "day": (DayTransform, "day"),
    "hour": (HourTransform, "hour"),
}


_ATHENA_TRANSFORM = re.compile(
    r"^\s*(?:(?P<function>\w+)\s*\(\s*(?P<column>\w+)\s*(?:,\s*(?P<width>\d+)\s*)?\)|(?P<identity>\w+))\s*$"
)


def _athena_partitioning(transforms: list[str], schema: Schema) -> PartitionSpec:
    """The partition spec of a CTAS's partitioning property: ARRAY['day(ts)', 'bucket(id, 16)', 'region']."""
    fields = []
    for i, text in enumerate(transforms):
        match = _ATHENA_TRANSFORM.match(text)
        if match is None:
            raise QueryFailed(f"NOT_SUPPORTED: Unsupported partition transform: {text}")
        column = (match["column"] or match["identity"]).lower()
        source = schema.find_field(column)
        function = (match["function"] or "").lower()
        transform: Transform[Any, Any]
        if not function:
            transform, name = IdentityTransform(), source.name
        elif function == "bucket" and match["width"]:
            transform, name = (
                BucketTransform(int(match["width"])),
                f"{source.name}_bucket",
            )
        elif function == "truncate" and match["width"]:
            transform, name = (
                TruncateTransform(int(match["width"])),
                f"{source.name}_trunc",
            )
        elif function in _TRANSFORMS:
            transform_class, suffix = _TRANSFORMS[function]
            transform, name = transform_class(), f"{source.name}_{suffix}"
        else:
            raise QueryFailed(f"NOT_SUPPORTED: Unsupported partition transform: {text}")
        fields.append(PartitionField(source.field_id, 1000 + i, transform, name))
    return PartitionSpec(*fields)


def _partition_spec(expressions: list[exp.Expression], schema: Schema) -> PartitionSpec:
    fields = []
    for i, expression in enumerate(expressions):
        column = (
            expression.find(exp.Column)
            if not isinstance(expression, exp.Identifier)
            else expression
        )
        if column is None:
            raise QueryFailed(
                f"NOT_SUPPORTED: Unsupported partition transform: {expression.sql('hive')}"
            )
        source = schema.find_field(column.name.lower())
        transform: Transform[Any, Any]
        if isinstance(expression, (exp.Identifier, exp.Column)):
            transform, name = IdentityTransform(), source.name
        elif isinstance(expression, exp.PartitionedByBucket):
            transform, name = (
                BucketTransform(int(expression.expression.name)),
                f"{source.name}_bucket",
            )
        elif isinstance(expression, exp.PartitionByTruncate):
            transform, name = (
                TruncateTransform(int(expression.expression.name)),
                f"{source.name}_trunc",
            )
        else:
            function = (
                expression.name
                if isinstance(expression, exp.Anonymous)
                else expression.key
            )
            function = function.lower().removesuffix("s")
            if function not in _TRANSFORMS:
                raise QueryFailed(
                    f"NOT_SUPPORTED: Unsupported partition transform: {expression.sql('hive')}"
                )
            transform_class, suffix = _TRANSFORMS[function]
            transform, name = transform_class(), f"{source.name}_{suffix}"
        fields.append(PartitionField(source.field_id, 1000 + i, transform, name))
    return PartitionSpec(*fields)


# Athena table properties with an Iceberg name of their own
_ICEBERG_PROPERTIES = {
    "format": "write.format.default",
    "write_compression": "write.parquet.compression-codec",
    "write_target_data_file_size_bytes": "write.target-file-size-bytes",
}


def _arrow(relation: Any) -> pa.Table:
    if hasattr(relation, "to_arrow_table"):
        return relation.to_arrow_table()
    return relation.arrow()


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duckdb_type(hive_type: str) -> str:
    return exp.DataType.build(hive_type, dialect="hive").sql("duckdb")


class SqlEngine:
    def __init__(self, account_id: str, region_name: str):
        self.account_id = account_id
        self.partition = get_partition(region_name)
        self.glue = glue_backends[account_id][region_name]
        self.s3 = s3_backends[account_id][self.partition]
        self.catalog = _GlueCatalog(self.glue, account_id, self.partition)

    def execute(
        self,
        query: str,
        database: str,
        parameters: list[str] | None = None,
        catalog: str = "AwsDataCatalog",
    ) -> Outcome:
        statement = query.strip().rstrip(";").strip()
        outcome = Outcome(*_statement_types(statement))
        try:
            statement = _bind_parameters(statement, parameters or [])
            with _LOCK, tempfile.TemporaryDirectory(prefix="moto-athena-") as scratch:
                running = _Query(self, statement, database.lower(), scratch, outcome)
                running.catalog = catalog
                try:
                    running.run()
                finally:
                    running.duckdb.close()
        except QueryFailed as e:
            outcome.error = str(e)
        except Exception as e:
            outcome.error = f"GENERIC_INTERNAL_ERROR: {type(e).__name__}: {e}"
            _logger.exception("Athena query failed: %s", statement)
        return outcome


_SQL_LITERAL = re.compile(
    r"^\s*(?:[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?|'(?:[^']|'')*'|true|false|null"
    r"|(?:date|time|timestamp|interval|decimal|x)\s*'(?:[^']|'')*'(?:\s+\w+)?"
    r"|(?:array\s*\[|row\s*\(|cast\s*\(|map\s*\().*)\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _parameter_literal(parameter: str) -> str:
    """A parameter as SQL: a literal stays as is; Athena reads any other text as a string."""
    if _SQL_LITERAL.match(parameter):
        return parameter
    return "'" + parameter.replace("'", "''") + "'"


def federated_dsn(catalog: str) -> str | None:
    """The PostgreSQL DSN MOTO_ATHENA_FEDERATED_CATALOGS gives a catalog, if any."""
    import fnmatch

    for pattern, dsn in settings.athena_federated_catalogs().items():
        if fnmatch.fnmatchcase(catalog, pattern):
            return dsn
    return None


def _bind_parameters(statement: str, parameters: list[str]) -> str:
    """Replaces the query's ? placeholders, outside quotes and comments, with ExecutionParameters in order.

    Each parameter is SQL text, as Athena takes them: a string parameter carries its own quotes.
    """
    out: list[str] = []
    remaining = list(parameters)
    placeholders = 0
    i = 0
    while i < len(statement):
        char = statement[i]
        if char in "'\"":
            end = i + 1
            while end < len(statement):
                if statement[end] == char:
                    if (
                        statement[end + 1 : end + 2] == char
                    ):  # a doubled quote inside the literal
                        end += 2
                        continue
                    break
                end += 1
            out.append(statement[i : end + 1])
            i = end + 1
        elif statement.startswith("--", i):
            end = statement.find("\n", i)
            end = len(statement) if end == -1 else end
            out.append(statement[i:end])
            i = end
        elif statement.startswith("/*", i):
            end = statement.find("*/", i + 2)
            end = len(statement) if end == -1 else end + 2
            out.append(statement[i:end])
            i = end
        elif char == "?":
            placeholders += 1
            if remaining:
                out.append(_parameter_literal(remaining.pop(0)))
            i += 1
        else:
            out.append(char)
            i += 1
    if placeholders != len(parameters):
        raise QueryFailed(
            f"INVALID_QUERY: The query has {placeholders} parameters, "
            f"but {len(parameters)} execution parameters were provided"
        )
    return "".join(out)


def _statement_types(statement: str) -> tuple[str, str]:
    words = statement.upper().split()
    first = words[0] if words else ""
    if first in ("SELECT", "WITH", "VALUES", "TABLE"):
        return "DML", "SELECT"
    if first == "UNLOAD":
        return "DML", "UNLOAD"
    if first in ("INSERT", "UPDATE", "DELETE", "MERGE"):
        return "DML", first
    if first == "CREATE" and "AS" in words and "SELECT" in words:
        return "DML", "CREATE_TABLE_AS_SELECT"
    noun = next(
        (
            w
            for w in words[1:]
            if w in ("TABLE", "DATABASE", "SCHEMA", "VIEW", "TABLES", "COLUMNS")
        ),
        "",
    )
    return "DDL", f"{first}_{noun}".strip("_")


class _Query:
    def __init__(
        self,
        engine: SqlEngine,
        statement: str,
        database: str,
        scratch: str,
        outcome: Outcome,
    ):
        self.engine = engine
        self.glue = engine.glue
        self.statement = statement
        self.database = database
        self.scratch = scratch
        self.outcome = outcome
        self.duckdb = duckdb.connect()
        self.registered = 0
        self.catalog = "AwsDataCatalog"
        self.attached: dict[str, str] = {}

    def run(self) -> None:
        words = self.statement.upper().split()
        if words[:1] in (["OPTIMIZE"], ["VACUUM"]) or words[:2] == ["MSCK", "REPAIR"]:
            return
        if words[:2] == ["SHOW", "TABLES"]:
            return self._show_tables()
        if words[:2] == ["SHOW", "COLUMNS"]:
            return self._show_columns()
        if words[:1] == ["UNLOAD"]:
            return self._unload()
        try:
            statements = [
                s for s in sqlglot.parse(self.statement, read="athena") if s is not None
            ]
        except sqlglot.errors.ParseError as e:
            raise QueryFailed(f"INVALID_QUERY: {e}")
        if len(statements) != 1:
            raise QueryFailed("INVALID_QUERY: Only one SQL statement is allowed")
        tree = statements[0]
        if isinstance(tree, exp.Create):
            return self._create(tree)
        if isinstance(tree, exp.Drop):
            return self._drop(tree)
        if isinstance(tree, exp.Alter):
            return self._alter(tree)
        if isinstance(tree, exp.Describe):
            return self._describe(tree)
        if isinstance(tree, exp.Insert):
            return self._insert(tree)
        if isinstance(tree, exp.Update):
            return self._update(tree)
        if isinstance(tree, exp.Delete):
            return self._delete(tree)
        if isinstance(tree, exp.Query):
            return self._select(tree)
        raise QueryFailed(
            f"NOT_SUPPORTED: moto does not run this statement: {self.statement[:100]}"
        )

    # Catalog

    def _name(self, table: exp.Table) -> tuple[str, str]:
        return (table.db or self.database).lower(), table.name.lower()

    def _glue_table(self, database: str, name: str) -> dict[str, Any] | None:
        self._require_database(database)
        try:
            return self.glue.get_table(database, name).as_dict()
        except EntityNotFoundException:
            return None

    def _require_database(self, database: str) -> None:
        try:
            self.glue.get_database(database)
        except EntityNotFoundException:
            raise QueryFailed(f"SCHEMA_NOT_FOUND: Schema '{database}' does not exist")

    def _require_table(self, database: str, name: str) -> dict[str, Any]:
        table = self._glue_table(database, name)
        if table is None:
            raise QueryFailed(
                f"TABLE_NOT_FOUND: Table 'awsdatacatalog.{database}.{name}' does not exist"
            )
        return table

    def _iceberg(self, database: str, name: str) -> Table:
        table = self._require_table(database, name)
        if not _is_iceberg(table):
            raise QueryFailed(
                f"NOT_SUPPORTED: Modifying non-Iceberg table '{database}.{name}' is not supported"
            )
        return self.engine.catalog.load_table((database, name))

    # DDL

    def _create(self, tree: exp.Create) -> None:
        kind = (tree.args.get("kind") or "").upper()
        if kind in ("DATABASE", "SCHEMA"):
            return self._create_database(tree)
        if kind != "TABLE":
            raise QueryFailed(f"NOT_SUPPORTED: CREATE {kind} is not supported by moto")
        if tree.expression is not None:
            return self._create_as_select(tree)
        schema = tree.this
        table = schema.this if isinstance(schema, exp.Schema) else schema
        database, name = self._name(table)
        if self._glue_table(database, name) is not None:
            if tree.args.get("exists"):
                return
            raise QueryFailed(
                f"ALREADY_EXISTS: Table '{database}.{name}' already exists"
            )
        properties = tree.args.get("properties")
        props = properties.expressions if properties else []
        tblproperties = {
            p.name.lower(): p.args["value"].name
            for p in props
            if type(p) is exp.Property and p.args.get("value")
        }
        location = next(
            (p.this.name for p in props if isinstance(p, exp.LocationProperty)), None
        )
        using_iceberg = any(
            isinstance(p, exp.FileFormatProperty) and p.name.lower() == "iceberg"
            for p in props
        )
        columns = (
            [c for c in schema.expressions if isinstance(c, exp.ColumnDef)]
            if isinstance(schema, exp.Schema)
            else []
        )
        if tblproperties.get("table_type", "").lower() == "iceberg" or using_iceberg:
            self._create_iceberg(
                database, name, columns, props, tblproperties, location
            )
        else:
            self._create_hive(database, name, columns, props, tblproperties, location)

    def _create_as_select(self, tree: exp.Create) -> None:
        table_node = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
        database, name = self._name(table_node)
        if self._glue_table(database, name) is not None:
            if tree.args.get("exists"):
                return
            raise QueryFailed(
                f"ALREADY_EXISTS: Table '{database}.{name}' already exists"
            )
        properties = tree.args.get("properties")
        props = properties.expressions if properties else []
        values = {
            p.name.lower(): p.args["value"]
            for p in props
            if type(p) is exp.Property and p.args.get("value")
        }
        if values.get("table_type", exp.Literal.string("")).name.lower() != "iceberg":
            raise QueryFailed(
                "NOT_SUPPORTED: CREATE TABLE AS SELECT is only supported by moto for Iceberg tables"
            )
        location = next(
            (p.this.name for p in props if isinstance(p, exp.LocationProperty)), None
        )
        if not location:
            raise QueryFailed("INVALID_TABLE_PROPERTY: Iceberg tables need a LOCATION")
        source = tree.expression
        self._bind(source)
        rows = self._query(source)
        rows = rows.rename_columns([c.lower() for c in rows.column_names])
        schema = assign_fresh_schema_ids(Catalog._convert_schema_if_needed(rows.schema))
        partitioning = values.get("partitioning")
        spec = _athena_partitioning(
            [e.name for e in partitioning.expressions] if partitioning else [], schema
        )
        table_properties = {}
        for key, value in values.items():
            if key in ("table_type", "partitioning", "is_external"):
                continue
            table_properties[_ICEBERG_PROPERTIES.get(key, key)] = value.name.lower()
        fmt = next(
            (p.name for p in props if isinstance(p, exp.FileFormatProperty)), None
        )
        if fmt:
            table_properties[_ICEBERG_PROPERTIES["format"]] = fmt.lower()
        table = self.engine.catalog.create_table(
            (database, name), schema, location, spec, table_properties
        )
        rows = _conform(rows, table.schema().as_arrow())
        if rows.num_rows:
            table.append(rows)
        self.outcome.update_count = rows.num_rows

    def _create_iceberg(
        self,
        database: str,
        name: str,
        columns: list[exp.ColumnDef],
        props: list[exp.Expression],
        tblproperties: dict[str, str],
        location: str | None,
    ) -> None:
        if not location:
            database_location = self.glue.get_database(database).input.get(
                "LocationUri"
            )
            if not database_location:
                raise QueryFailed(
                    "INVALID_TABLE_PROPERTY: Iceberg tables need a LOCATION"
                )
            location = f"{database_location.rstrip('/')}/{name}"
        ids = _Ids()
        fields = [
            NestedField(
                ids(),
                c.name.lower(),
                _iceberg_type(c.args["kind"], ids),
                required=False,
            )
            for c in columns
        ]
        schema = Schema(*fields)
        partitioned_by = next(
            (p.this for p in props if isinstance(p, exp.PartitionedByProperty)), None
        )
        spec = _partition_spec(
            partitioned_by.expressions if partitioned_by else [], schema
        )
        properties = {}
        for key, value in tblproperties.items():
            if key == "table_type":
                continue
            if key in _ICEBERG_PROPERTIES:
                properties[_ICEBERG_PROPERTIES[key]] = value.lower()
            else:
                properties[key] = value
        self.engine.catalog.create_table(
            (database, name), schema, location, spec, properties
        )

    def _create_hive(
        self,
        database: str,
        name: str,
        columns: list[exp.ColumnDef],
        props: list[exp.Expression],
        tblproperties: dict[str, str],
        location: str | None,
    ) -> None:
        if not location:
            raise QueryFailed("INVALID_TABLE_PROPERTY: External tables need a LOCATION")
        partitioned_by = next(
            (p.this for p in props if isinstance(p, exp.PartitionedByProperty)), None
        )
        partition_keys = [
            _glue_column(c)
            for c in (partitioned_by.expressions if partitioned_by else [])
        ]
        serde, serde_parameters = _serde(props)
        storage = {
            "Columns": [_glue_column(c) for c in columns],
            "Location": location,
            "SerdeInfo": {
                "SerializationLibrary": serde,
                "Parameters": serde_parameters,
            },
            **_FORMATS.get(serde, _FORMATS[_TEXT_SERDE])[1],
        }
        table_input = {
            "Name": name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE", **tblproperties},
            "PartitionKeys": partition_keys,
            "StorageDescriptor": storage,
        }
        self.glue.create_table(database, name, table_input)

    def _create_database(self, tree: exp.Create) -> None:
        name = tree.this.name.lower()
        if name in [db.name for db in self.glue.get_databases()]:
            if tree.args.get("exists"):
                return
            raise QueryFailed(f"ALREADY_EXISTS: Schema '{name}' already exists")
        database_input: dict[str, Any] = {"Name": name}
        properties = tree.args.get("properties")
        for p in properties.expressions if properties else []:
            if isinstance(p, exp.LocationProperty):
                database_input["LocationUri"] = p.this.name
            elif isinstance(p, exp.SchemaCommentProperty):
                database_input["Description"] = p.this.name
        self.glue.create_database(name, database_input)

    def _drop(self, tree: exp.Drop) -> None:
        kind = (tree.args.get("kind") or "").upper()
        target = (tree.args.get("tables") or [tree.this])[0]
        if kind in ("DATABASE", "SCHEMA"):
            name = target.name.lower()
            try:
                self.glue.delete_database(name)
            except EntityNotFoundException:
                if not tree.args.get("exists"):
                    raise QueryFailed(
                        f"SCHEMA_NOT_FOUND: Schema '{name}' does not exist"
                    )
            return
        if kind != "TABLE":
            raise QueryFailed(f"NOT_SUPPORTED: DROP {kind} is not supported by moto")
        database, name = self._name(target)
        table = self._glue_table(database, name)
        if table is None:
            if tree.args.get("exists"):
                return
            raise QueryFailed(
                f"TABLE_NOT_FOUND: Table '{database}.{name}' does not exist"
            )
        self.glue.delete_table(database, name)
        # Athena removes an Iceberg table's data with the table; a Hive table's data stays
        location = table.get("StorageDescriptor", {}).get("Location")
        if _is_iceberg(table) and location:
            handler = _MotoS3Handler(self.engine.account_id, self.engine.partition)
            handler.delete_dir_contents(location.removeprefix("s3://"))

    def _alter(self, tree: exp.Alter) -> None:
        database, name = self._name(tree.this)
        glue_table = self._require_table(database, name)
        actions = tree.args.get("actions") or []
        added = [
            c
            for a in actions
            if isinstance(a, exp.Schema)
            for c in a.expressions
            if isinstance(c, exp.ColumnDef)
        ]
        added += [a for a in actions if isinstance(a, exp.ColumnDef)]
        dropped = [
            c.name.lower()
            for a in actions
            if isinstance(a, exp.Drop)
            for c in a.args.get("tables") or []
        ]
        if len(added) + len(dropped) == 0 or (added and dropped):
            raise QueryFailed(
                f"NOT_SUPPORTED: moto supports ALTER TABLE ADD COLUMNS and DROP COLUMN only: {self.statement[:100]}"
            )
        if not _is_iceberg(glue_table):
            return self._alter_hive(database, name, glue_table, added, dropped)
        table = self.engine.catalog.load_table((database, name))
        try:
            with table.update_schema() as update:
                for column in added:
                    update.add_column(
                        column.name.lower(), _iceberg_type(column.args["kind"], _Ids())
                    )
                for column_name in dropped:
                    update.delete_column(column_name)
        except ValueError as e:
            raise QueryFailed(f"INVALID_COLUMN: {e}")

    def _alter_hive(
        self,
        database: str,
        name: str,
        glue_table: dict[str, Any],
        added: list[exp.ColumnDef],
        dropped: list[str],
    ) -> None:
        storage = dict(glue_table.get("StorageDescriptor", {}))
        columns = [c for c in storage.get("Columns", []) if c["Name"] not in dropped]
        existing = {c["Name"] for c in columns}
        for column in added:
            if column.name.lower() in existing:
                raise QueryFailed(
                    f"INVALID_COLUMN: Column '{column.name}' already exists"
                )
            columns.append(_glue_column(column))
        storage["Columns"] = columns
        keep = ("Name", "TableType", "Description", "PartitionKeys", "Parameters")
        table_input = {k: v for k, v in glue_table.items() if k in keep}
        self.glue.update_table(
            database, name, {**table_input, "StorageDescriptor": storage}
        )

    # Metadata queries

    def _columns(self, database: str, name: str) -> list[dict[str, Any]]:
        table = self._require_table(database, name)
        reader = iceberg_rest.s3_reader(self.engine.account_id, self.engine.partition)
        columns, partition_keys = glue_table_columns(table, reader)
        return columns + partition_keys

    def _text_result(self, column: str, values: list[str]) -> None:
        self.outcome.column_info = [_column_info(column, "varchar")]
        self.outcome.rows = [{"Data": [{"VarCharValue": v}]} for v in values]

    def _show_tables(self) -> None:
        match = re.match(
            r"SHOW\s+TABLES(?:\s+(?:IN|FROM)\s+([\w`\"]+))?(?:\s+(?:LIKE\s+)?'([^']*)')?\s*$",
            self.statement,
            re.I,
        )
        if not match:
            raise QueryFailed(f"INVALID_QUERY: {self.statement}")
        database = (match[1] or self.database).strip('`"').lower()
        self._require_database(database)
        names = sorted(t.name for t in self.glue.get_tables(database, expression=None))
        if match[2]:
            pattern = "|".join(
                f"(?:{p.strip().replace('*', '.*')})" for p in match[2].split("|")
            )
            names = [n for n in names if re.fullmatch(pattern, n, re.I)]
        self._text_result("tab_name", names)

    def _show_columns(self) -> None:
        match = re.match(
            r"SHOW\s+COLUMNS\s+(?:IN|FROM)\s+([\w.`\"]+)(?:\s+(?:IN|FROM)\s+([\w`\"]+))?\s*$",
            self.statement,
            re.I,
        )
        if not match:
            raise QueryFailed(f"INVALID_QUERY: {self.statement}")
        parts = match[1].replace("`", "").replace('"', "").lower().split(".")
        database = (
            (match[2] or (parts[-2] if len(parts) > 1 else self.database))
            .strip('`"')
            .lower()
        )
        self._text_result(
            "field", [c["Name"] for c in self._columns(database, parts[-1])]
        )

    def _describe(self, tree: exp.Describe) -> None:
        database, name = self._name(tree.this)
        columns = self._columns(database, name)
        self.outcome.column_info = [
            _column_info(n, "varchar") for n in ("col_name", "data_type", "comment")
        ]
        self.outcome.rows = [
            {
                "Data": [
                    {"VarCharValue": c["Name"]},
                    {"VarCharValue": c["Type"]},
                    {"VarCharValue": c.get("Comment", "")},
                ]
            }
            for c in columns
        ]

    # Queries

    def _bind(self, tree: exp.Expression) -> None:
        """Loads every table the query reads into DuckDB, and points the query at the copies."""
        ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
        for table in list(tree.find_all(exp.Table)):
            if not isinstance(table.this, exp.Identifier) or (
                not table.db and table.name.lower() in ctes
            ):
                continue
            catalog = table.catalog or (
                self.catalog if self.catalog.lower() != "awsdatacatalog" else ""
            )
            if catalog and catalog.lower() != "awsdatacatalog":
                table.set("catalog", exp.to_identifier(self._attach(catalog)))
                table.set("db", exp.to_identifier(table.db or self.database))
                continue
            database, name = self._name(table)
            if database == "information_schema":
                raise QueryFailed(
                    "NOT_SUPPORTED: moto does not serve information_schema"
                )
            registered = self._load(database, name)
            alias = table.alias or name
            table.replace(
                exp.Table(
                    this=exp.to_identifier(registered),
                    alias=exp.TableAlias(this=exp.to_identifier(alias)),
                )
            )
        # DuckDB knows the bound tables only by their aliases. A name's other parts are struct fields:
        # Athena reads "card"."customer"."id" as field id of field customer of column card.
        relations = ctes | {
            node.alias_or_name.lower()
            for node in tree.find_all(exp.Table, exp.Subquery)
            if node.alias_or_name
        }
        for column in list(tree.find_all(exp.Column)):
            parts = [
                p.name
                for p in (
                    column.args.get("catalog"),
                    column.args.get("db"),
                    column.args.get("table"),
                    column.this,
                )
                if p is not None and p.name
            ]
            if len(parts) < 2 or isinstance(column.this, exp.Star):
                continue
            # Drop catalog and database qualifiers in front of a relation's name
            start = next(
                (i for i, part in enumerate(parts[:-1]) if part.lower() in relations),
                None,
            )
            if start is None:
                node: exp.Expression = exp.column(parts[0], quoted=True)
                fields = parts[1:]
            else:
                node = exp.column(parts[start + 1], table=parts[start], quoted=True)
                fields = parts[start + 2 :]
            for name in fields:
                node = exp.Bracket(this=node, expressions=[exp.Literal.string(name)])
            column.replace(node)
        # Trino's map_agg(key, value) aggregate; DuckDB builds the map from two lists
        for call in list(tree.find_all(exp.Anonymous)):
            if call.name.lower() == "map_agg" and len(call.expressions) == 2:
                key, value = (e.sql(dialect="duckdb") for e in call.expressions)
                call.replace(
                    sqlglot.parse_one(
                        f"MAP(LIST({key}) FILTER (WHERE {key} IS NOT NULL), "
                        f"LIST({value}) FILTER (WHERE {key} IS NOT NULL))",
                        read="duckdb",
                    )
                )
        # Trino casts a JSON string to VARCHAR as its value; DuckDB keeps the quotes
        for cast in list(tree.find_all(exp.Cast)):
            if isinstance(cast.this, exp.JSONExtract) and cast.to.is_type(
                *exp.DataType.TEXT_TYPES
            ):
                cast.replace(
                    exp.JSONExtractScalar(
                        this=cast.this.this, expression=cast.this.expression
                    )
                )

    def _attach(self, catalog: str) -> str:
        """Attaches a federated catalog's PostgreSQL database to DuckDB, read-only."""
        if catalog not in self.attached:
            dsn = federated_dsn(catalog)
            if dsn is None:
                raise QueryFailed(
                    f"CATALOG_NOT_FOUND: Catalog '{catalog}' does not exist"
                )
            alias = f"__moto_c{len(self.attached)}"
            try:
                self.duckdb.execute("LOAD postgres")
            except duckdb.Error:
                self.duckdb.execute("INSTALL postgres")
                self.duckdb.execute("LOAD postgres")
            self.duckdb.execute(
                f"ATTACH {_quote(dsn)} AS {alias} (TYPE postgres, READ_ONLY)"
            )
            self.attached[catalog] = alias
        return self.attached[catalog]

    def _load(self, database: str, name: str) -> str:
        glue_table = self._require_table(database, name)
        registered = f"__moto_t{self.registered}"
        self.registered += 1
        if _is_iceberg(glue_table):
            data = self.engine.catalog.load_table((database, name)).scan().to_arrow()
            self.duckdb.register(registered, data)
        else:
            self.duckdb.execute(
                f"CREATE TEMP VIEW {registered} AS {self._hive_source(glue_table, registered)}"
            )
        return registered

    def _hive_source(self, glue_table: dict[str, Any], registered: str) -> str:
        storage = glue_table.get("StorageDescriptor", {})
        columns = storage.get("Columns", [])
        partition_keys = glue_table.get("PartitionKeys") or []
        all_columns = columns + partition_keys
        select = ", ".join(
            f'CAST("{c["Name"]}" AS {_duckdb_type(c["Type"])}) AS "{c["Name"]}"'
            for c in all_columns
        )
        files = self._download(storage.get("Location", ""), registered)
        if not files:
            nulls = ", ".join(
                f'CAST(NULL AS {_duckdb_type(c["Type"])}) AS "{c["Name"]}"'
                for c in all_columns
            )
            return f"SELECT {nulls} LIMIT 0"
        file_list = "[" + ", ".join(_quote(f) for f in files) + "]"
        hive = ", hive_partitioning = true" if partition_keys else ""
        serde = storage.get("SerdeInfo", {}).get("SerializationLibrary", "")
        input_format = storage.get("InputFormat", "")
        column_types = (
            "{"
            + ", ".join(
                f"{_quote(c['Name'])}: {_quote(_duckdb_type(c['Type']))}"
                for c in columns
            )
            + "}"
        )
        if "json" in serde.lower():
            reader = f"read_json({file_list}, format = 'newline_delimited', columns = {column_types}, ignore_errors = true{hive})"
        elif "parquet" in serde.lower() or "parquet" in input_format.lower():
            reader = f"read_parquet({file_list}, union_by_name = true{hive})"
        else:
            parameters = storage.get("SerdeInfo", {}).get("Parameters") or {}
            delimiter = parameters.get(
                "field.delim", parameters.get("separatorChar", "\x01")
            )
            reader = f"read_csv({file_list}, delim = {_quote(delimiter)}, header = false, columns = {column_types}{hive})"
        return f"SELECT {select} FROM {reader}"

    def _download(self, location: str, registered: str) -> list[str]:
        """Copies a Hive table's objects to local files, keeping `key=value` partition directories."""
        bucket_name, _, prefix = location.removeprefix("s3://").partition("/")
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        files = []
        for key_name, key in list(self.engine.s3.get_bucket(bucket_name).keys.items()):
            relative = key_name[len(prefix) :]
            if (
                not key_name.startswith(prefix)
                or not relative
                or relative.endswith("/")
            ):
                continue
            if any(part.startswith(("_", ".")) for part in relative.split("/")):
                continue
            path = os.path.join(self.scratch, registered, *relative.split("/"))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(key.value)
            files.append(path)
        return files

    def _query(self, tree: exp.Expression) -> pa.Table:
        try:
            return _arrow(self.duckdb.sql(tree.sql(dialect="duckdb")))
        except duckdb.Error as e:
            raise QueryFailed(f"INVALID_QUERY: {e}")

    def _select(self, tree: Any) -> None:
        self._bind(tree)
        result = self._query(tree)
        self.outcome.column_info = [
            _column_info(f.name, *_athena_type(f.type)) for f in result.schema
        ]
        header = {"Data": [{"VarCharValue": f.name} for f in result.schema]}
        rows = zip(
            *[c.to_pylist(maps_as_pydicts="strict") for c in result.columns],
            strict=True,
        )
        self.outcome.rows = [header] + [
            {"Data": [_cell(v) for v in row]} for row in rows
        ]

    def _unload(self) -> None:
        match = _UNLOAD.match(self.statement)
        if not match:
            raise QueryFailed(
                f"INVALID_QUERY: Cannot parse UNLOAD: {self.statement[:100]}"
            )
        options = {
            k.lower(): v
            for k, v in re.findall(
                r"(\w+)\s*=\s*'([^']*)'", match.group("options") or ""
            )
        }
        fmt = options.get("format", "PARQUET").upper()
        compression = options.get("compression", "").upper()
        try:
            tree = sqlglot.parse_one(match.group("query"), read="athena")
        except sqlglot.errors.ParseError as e:
            raise QueryFailed(f"INVALID_QUERY: {e}")
        self._bind(tree)
        result = self._query(tree)
        bucket, _, prefix = match.group("location").removeprefix("s3://").partition("/")
        if not result.num_rows:
            return  # Athena writes no files for an empty result
        name = f"{prefix.rstrip('/')}/{os.urandom(8).hex()}_00000"
        if fmt == "PARQUET":
            buffer = io.BytesIO()
            pq.write_table(
                result, buffer, compression=(compression or "SNAPPY").lower()
            )
            body, key = buffer.getvalue(), name
        elif fmt in ("JSON", "TEXTFILE"):
            if fmt == "JSON":
                lines = [
                    json.dumps(
                        {k: _unload_value(v) for k, v in row.items()},
                        separators=(",", ":"),
                    )
                    for row in result.to_pylist(maps_as_pydicts="strict")
                ]
            else:
                delimiter = options.get("field_delimiter", "\x01")
                lines = [
                    delimiter.join(
                        "" if v is None else str(_unload_value(v)) for v in row.values()
                    )
                    for row in result.to_pylist(maps_as_pydicts="strict")
                ]
            body = ("\n".join(lines) + "\n").encode()
            key = name
            if compression in ("", "GZIP"):
                body, key = gzip.compress(body), f"{name}.gz"
            elif compression != "NONE":
                raise QueryFailed(
                    f"NOT_SUPPORTED: UNLOAD compression {compression} is not supported by moto"
                )
        else:
            raise QueryFailed(
                f"NOT_SUPPORTED: UNLOAD format {fmt} is not supported by moto"
            )
        self.engine.s3.put_object(bucket, key.lstrip("/"), body)

    def _insert(self, tree: exp.Insert) -> None:
        target = tree.this
        table_node = target.this if isinstance(target, exp.Schema) else target
        database, name = self._name(table_node)
        table = self._iceberg(database, name)
        schema = table.schema().as_arrow()
        self.duckdb.register("__moto_empty", schema.empty_table())
        self.duckdb.execute(
            "CREATE TEMP TABLE __moto_target AS SELECT * FROM __moto_empty"
        )
        source = tree.expression
        self._bind(source)
        columns = ""
        if isinstance(target, exp.Schema) and target.expressions:
            columns = (
                "(" + ", ".join(f'"{c.name.lower()}"' for c in target.expressions) + ")"
            )
        try:
            self.duckdb.execute(
                f"INSERT INTO __moto_target {columns} {source.sql(dialect='duckdb')}"
            )
        except duckdb.Error as e:
            raise QueryFailed(f"INVALID_QUERY: {e}")
        rows = _conform(_arrow(self.duckdb.sql("SELECT * FROM __moto_target")), schema)
        if rows.num_rows:
            table.append(rows)
        self.outcome.update_count = rows.num_rows

    def _update(self, tree: exp.Update) -> None:
        database, name = self._name(tree.this)
        table = self._iceberg(database, name)
        alias = tree.this.alias or name
        self._bind(tree)
        source = tree.this
        where = tree.args.get("where")
        condition = where.this if where else exp.true()
        assignments = {e.this.name.lower(): e.expression for e in tree.expressions}
        unknown = set(assignments) - {f.name for f in table.schema().fields}
        if unknown:
            raise QueryFailed(
                f"COLUMN_NOT_FOUND: Column '{sorted(unknown)[0]}' cannot be resolved"
            )
        projections = []
        for column in [f.name for f in table.schema().fields]:
            value: exp.Expression = exp.column(column, table=alias, quoted=True)
            if column in assignments:
                value = (
                    exp.Case()
                    .when(condition.copy(), assignments[column].copy())
                    .else_(value)
                )
            projections.append(exp.alias_(value, column, quoted=True))
        count = (
            self._query(
                exp.select("COUNT(*)").from_(source.copy()).where(condition.copy())
            )
            .column(0)[0]
            .as_py()
        )
        if count:
            rows = self._query(exp.select(*projections).from_(source.copy()))
            table.overwrite(_conform(rows, table.schema().as_arrow()))
        self.outcome.update_count = count

    def _delete(self, tree: exp.Delete) -> None:
        database, name = self._name(tree.this)
        table = self._iceberg(database, name)
        self._bind(tree)
        source = tree.this
        where = tree.args.get("where")
        condition = where.this if where else exp.true()
        total = (
            self._query(exp.select("COUNT(*)").from_(source.copy()))
            .column(0)[0]
            .as_py()
        )
        keep = exp.not_(exp.func("COALESCE", exp.paren(condition.copy()), exp.false()))
        rows = self._query(exp.select("*").from_(source.copy()).where(keep))
        if total - rows.num_rows:
            table.overwrite(_conform(rows, table.schema().as_arrow()))
        self.outcome.update_count = total - rows.num_rows


def _is_iceberg(glue_table: dict[str, Any]) -> bool:
    return (glue_table.get("Parameters") or {}).get(
        "table_type", ""
    ).upper() == "ICEBERG"


def _glue_column(column: exp.ColumnDef) -> dict[str, str]:
    data_type = column.args["kind"]
    return {
        "Name": column.name.lower(),
        "Type": data_type.sql("hive").lower().replace(" ", ""),
    }


_TEXT_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
_TEXT_FORMATS = {
    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
}
_FORMATS: dict[str, tuple[str, dict[str, str]]] = {
    "org.apache.hive.hcatalog.data.JsonSerDe": ("JSON", _TEXT_FORMATS),
    "org.openx.data.jsonserde.JsonSerDe": ("JSON", _TEXT_FORMATS),
    _TEXT_SERDE: ("TEXTFILE", _TEXT_FORMATS),
    "org.apache.hadoop.hive.serde2.OpenCSVSerde": ("CSV", _TEXT_FORMATS),
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe": (
        "PARQUET",
        {
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        },
    ),
}
_SERDE_BY_FORMAT = {fmt: serde for serde, (fmt, _) in _FORMATS.items()}


def _serde(props: list[exp.Expression]) -> tuple[str, dict[str, str]]:
    serde, parameters = _TEXT_SERDE, {}
    for p in props:
        if isinstance(p, exp.RowFormatSerdeProperty):
            serde = p.this.name
            serde_properties = p.args.get("serde_properties")
            if serde_properties:
                parameters.update(
                    {e.name: e.args["value"].name for e in serde_properties.expressions}
                )
        elif isinstance(p, exp.RowFormatDelimitedProperty):
            if p.args.get("fields"):
                parameters["field.delim"] = p.args["fields"].name
        elif isinstance(p, exp.SerdeProperties):
            parameters.update({e.name: e.args["value"].name for e in p.expressions})
        elif (
            isinstance(p, exp.FileFormatProperty) and p.name.upper() in _SERDE_BY_FORMAT
        ):
            if p.name.upper() != "TEXTFILE":
                serde = _SERDE_BY_FORMAT[p.name.upper()]
    return serde, parameters


def _conform(rows: pa.Table, schema: pa.Schema) -> pa.Table:
    """Casts DuckDB's output to the Iceberg table's Arrow schema, matching columns by position."""
    columns = [rows.column(i).cast(f.type) for i, f in enumerate(schema)]
    return pa.Table.from_arrays(columns, schema=schema)


def _column_info(
    name: str, athena_type: str, precision: int = 0, scale: int = 0
) -> dict[str, Any]:
    return {
        "CatalogName": "hive",
        "SchemaName": "",
        "TableName": "",
        "Name": name,
        "Label": name,
        "Type": athena_type,
        "Precision": precision,
        "Scale": scale,
        "Nullable": "UNKNOWN",
        "CaseSensitive": athena_type == "varchar",
    }


def _athena_type(arrow_type: pa.DataType) -> tuple[str, int, int]:
    """The Trino type name Athena reports for a result column, with its precision and scale."""
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_null(arrow_type)
    ):
        return "varchar", 2147483647, 0
    if pa.types.is_decimal(arrow_type):
        return "decimal", arrow_type.precision, arrow_type.scale
    if pa.types.is_timestamp(arrow_type):
        return ("timestamp with time zone" if arrow_type.tz else "timestamp"), 6, 0
    checks = [
        (pa.types.is_boolean, "boolean", 0),
        (pa.types.is_int8, "tinyint", 3),
        (pa.types.is_int16, "smallint", 5),
        (pa.types.is_int32, "integer", 10),
        (pa.types.is_int64, "bigint", 19),
        (pa.types.is_unsigned_integer, "bigint", 19),
        (pa.types.is_float32, "real", 24),
        (pa.types.is_float64, "double", 53),
        (pa.types.is_date, "date", 0),
        (pa.types.is_time, "time", 0),
        (
            lambda t: pa.types.is_binary(t) or pa.types.is_large_binary(t),
            "varbinary",
            2147483647,
        ),
        (lambda t: pa.types.is_list(t) or pa.types.is_large_list(t), "array", 0),
        (pa.types.is_map, "map", 0),
        (pa.types.is_struct, "row", 0),
    ]
    for check, name, precision in checks:
        if check(arrow_type):
            return name, precision, 0
    return "varchar", 2147483647, 0


def _cell(value: Any) -> dict[str, str]:
    return {} if value is None else {"VarCharValue": _text(value)}


def _text(value: Any) -> str:
    """Renders a value the way Athena writes it in query results."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return (
                value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC"
            )
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, (date, time, Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return " ".join(f"{b:02x}" for b in value)
    if isinstance(value, list):
        return "[" + ", ".join(_text(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_text(k)}={_text(v)}" for k, v in value.items()) + "}"
    return str(value)
