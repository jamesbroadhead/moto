"""A read-only Apache Iceberg REST catalog over moto's own catalogs.

AWS serves the Iceberg REST API for the Glue Data Catalog (`glue.<region>.amazonaws.com/iceberg`) and for
S3 Tables (`s3tables.<region>.amazonaws.com/iceberg`). Query engines such as DuckDB and Spark attach to these
endpoints. Table metadata is read from moto's S3, at the metadata location the catalog records.
"""

import json
import re
from collections.abc import Callable
from typing import Any
from urllib.parse import unquote

from moto.core.common_types import TYPE_RESPONSE

# The route after `/iceberg/v1/`, with an optional catalog prefix before `namespaces`
_ROUTE = re.compile(
    r"^(?:(?P<prefix>.+?)/)?namespaces(?:/(?P<namespace>[^/]+)(?:/tables(?:/(?P<table>[^/]+))?)?)?$"
)
# Iceberg encodes multi-level namespaces with the unit separator
_NAMESPACE_SEPARATOR = "\x1f"


class IcebergCatalog:
    """What a moto service must provide to be served as an Iceberg REST catalog."""

    def namespaces(self) -> list[str]:
        raise NotImplementedError

    def tables(self, namespace: str) -> list[str]:
        raise NotImplementedError

    def metadata_location(self, namespace: str, table: str) -> str | None:
        """Returns None for a missing table."""
        raise NotImplementedError

    def create_namespace(self, namespace: str, properties: dict[str, str]) -> None:
        raise NotImplementedError

    def default_location(self, namespace: str, table: str) -> str | None:
        """Where a new table goes when the request names no location."""
        return None

    def register(self, namespace: str, table: str, metadata_location: str, previous: str | None) -> None:
        """Points the table at new metadata; creates it when `previous` is None."""
        raise NotImplementedError


def _json(status: int, body: Any) -> TYPE_RESPONSE:
    return status, {"Content-Type": "application/json"}, json.dumps(body)


def _not_found(kind: str, name: str) -> TYPE_RESPONSE:
    error = {"message": f"{kind} does not exist: {name}", "type": f"NoSuch{kind}Exception", "code": 404}
    return _json(404, {"error": error})


def _bad_request(message: str, kind: str = "BadRequestException") -> TYPE_RESPONSE:
    return _json(400, {"error": {"message": message, "type": kind, "code": 400}})


def _metadata_path(location: str) -> str:
    import uuid

    return f"{location.rstrip('/')}/metadata/00000-{uuid.uuid4()}.metadata.json"


def _next_metadata_path(location: str, previous: str) -> str:
    import uuid

    name = previous.rsplit("/", 1)[-1]
    try:
        version = int(name.split("-", 1)[0]) + 1
    except ValueError:
        version = 1
    return f"{location.rstrip('/')}/metadata/{version:05d}-{uuid.uuid4()}.metadata.json"


def _create_table(
    namespace: str, body: dict[str, Any], catalog: IcebergCatalog, write_object: Callable[[str, bytes], None]
) -> TYPE_RESPONSE:
    # pyiceberg applies Iceberg's metadata rules; it is optional for moto, needed only for writes
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder

    name = body["name"]
    if catalog.metadata_location(namespace, name):
        return _json(409, {"error": {"message": f"Table already exists: {namespace}.{name}", "type": "AlreadyExistsException", "code": 409}})
    location = body.get("location") or catalog.default_location(namespace, name)
    if not location:
        return _bad_request("A location is required: the catalog has no default for this namespace")
    metadata = new_table_metadata(
        schema=Schema.model_validate(body["schema"]),
        partition_spec=PartitionSpec.model_validate(body["partition-spec"]) if body.get("partition-spec") else UNPARTITIONED_PARTITION_SPEC,
        sort_order=SortOrder.model_validate(body["write-order"]) if body.get("write-order") else UNSORTED_SORT_ORDER,
        location=location,
        properties=body.get("properties") or {},
    )
    metadata_location = _metadata_path(location)
    if not body.get("stage-create"):
        write_object(metadata_location, metadata.model_dump_json().encode())
        catalog.register(namespace, name, metadata_location, None)
    return _json(200, {"metadata-location": metadata_location, "metadata": json.loads(metadata.model_dump_json()), "config": {}})


def _commit(
    namespace: str,
    table: str,
    body: dict[str, Any],
    catalog: IcebergCatalog,
    read_object: Callable[[str], bytes],
    write_object: Callable[[str, bytes], None],
) -> TYPE_RESPONSE:
    from pydantic import TypeAdapter
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table.metadata import TableMetadataUtil
    from pyiceberg.table.update import TableRequirement, TableUpdate, update_table_metadata

    current_location = catalog.metadata_location(namespace, table)
    requirements = TypeAdapter(list[TableRequirement]).validate_python(body.get("requirements") or [])
    updates = TypeAdapter(list[TableUpdate]).validate_python(body.get("updates") or [])
    if current_location is None:
        # A staged create: the table exists once its first commit lands
        base = None
    else:
        base = TableMetadataUtil.parse_raw(read_object(current_location))
    try:
        for requirement in requirements:
            requirement.validate(base)
    except CommitFailedException as e:
        return _json(409, {"error": {"message": str(e), "type": "CommitFailedException", "code": 409}})
    if base is None:
        return _not_found("Table", f"{namespace}.{table}")
    new = update_table_metadata(base, tuple(updates), metadata_location=current_location)
    new_location = _next_metadata_path(new.location, current_location or "")
    write_object(new_location, new.model_dump_json().encode())
    catalog.register(namespace, table, new_location, current_location)
    return _json(200, {"metadata-location": new_location, "metadata": json.loads(new.model_dump_json())})


def handle(
    method: str,
    path: str,
    catalog: IcebergCatalog,
    read_object: Callable[[str], bytes],
    write_object: Callable[[str, bytes], None] | None = None,
    body: bytes | None = None,
) -> TYPE_RESPONSE:
    """Serves `path`, the part of the URL after `/iceberg/v1/`."""
    path = path.strip("/")
    payload = json.loads(body) if body else {}
    if path == "config":
        return _json(200, {"defaults": {}, "overrides": {}})
    match = _ROUTE.match(path)
    if not match:
        return _json(404, {"error": {"message": f"Unsupported route: {path}", "type": "NotFound", "code": 404}})
    namespace = match["namespace"] and unquote(match["namespace"]).replace(_NAMESPACE_SEPARATOR, ".")
    table = match["table"] and unquote(match["table"])
    if namespace is None:
        if method == "POST":
            name = ".".join(payload["namespace"])
            if name in catalog.namespaces():
                return _json(409, {"error": {"message": f"Namespace already exists: {name}", "type": "AlreadyExistsException", "code": 409}})
            catalog.create_namespace(name, payload.get("properties") or {})
            return _json(200, {"namespace": payload["namespace"], "properties": payload.get("properties") or {}})
        return _json(200, {"namespaces": [ns.split(".") for ns in catalog.namespaces()]})
    if namespace not in catalog.namespaces():
        return _not_found("Namespace", namespace)
    if path.endswith("/tables"):
        if method == "POST":
            if write_object is None:
                return _bad_request("This catalog is read-only")
            return _create_table(namespace, payload, catalog, write_object)
        identifiers = [{"namespace": namespace.split("."), "name": t} for t in catalog.tables(namespace)]
        return _json(200, {"identifiers": identifiers})
    if table is not None and method == "POST":
        if write_object is None:
            return _bad_request("This catalog is read-only")
        return _commit(namespace, table, payload, catalog, read_object, write_object)
    if table is None:
        return (204, {}, "") if method == "HEAD" else _json(200, {"namespace": namespace.split("."), "properties": {}})
    location = catalog.metadata_location(namespace, table)
    if not location:
        return _not_found("Table", f"{namespace}.{table}")
    if method == "HEAD":
        return 204, {}, ""
    metadata = json.loads(read_object(location))
    return _json(200, {"metadata-location": location, "metadata": metadata, "config": {}})


def s3_writer(account_id: str, partition: str) -> Callable[[str, bytes], None]:
    from moto.s3.models import s3_backends

    def write(location: str, data: bytes) -> None:
        bucket, key = location.removeprefix("s3://").split("/", 1)
        s3_backends[account_id][partition].put_object(bucket, key, data)

    return write


def s3_reader(account_id: str, partition: str) -> Callable[[str], bytes]:
    from moto.s3.models import s3_backends

    def read(location: str) -> bytes:
        bucket, key = location.removeprefix("s3://").split("/", 1)
        return s3_backends[account_id][partition].get_object(bucket, key).value  # type: ignore[union-attr]

    return read
