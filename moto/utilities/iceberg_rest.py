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


def _json(status: int, body: Any) -> TYPE_RESPONSE:
    return status, {"Content-Type": "application/json"}, json.dumps(body)


def _not_found(kind: str, name: str) -> TYPE_RESPONSE:
    error = {"message": f"{kind} does not exist: {name}", "type": f"NoSuch{kind}Exception", "code": 404}
    return _json(404, {"error": error})


def handle(
    method: str,
    path: str,
    catalog: IcebergCatalog,
    read_object: Callable[[str], bytes],
) -> TYPE_RESPONSE:
    """Serves `path`, the part of the URL after `/iceberg/v1/`."""
    path = path.strip("/")
    if path == "config":
        return _json(200, {"defaults": {}, "overrides": {}})
    match = _ROUTE.match(path)
    if not match:
        return _json(404, {"error": {"message": f"Unsupported route: {path}", "type": "NotFound", "code": 404}})
    namespace = match["namespace"] and unquote(match["namespace"]).replace(_NAMESPACE_SEPARATOR, ".")
    table = match["table"] and unquote(match["table"])
    if namespace is None:
        return _json(200, {"namespaces": [ns.split(".") for ns in catalog.namespaces()]})
    if namespace not in catalog.namespaces():
        return _not_found("Namespace", namespace)
    if path.endswith("/tables"):
        identifiers = [{"namespace": namespace.split("."), "name": t} for t in catalog.tables(namespace)]
        return _json(200, {"identifiers": identifiers})
    if table is None:
        return (204, {}, "") if method == "HEAD" else _json(200, {"namespace": namespace.split("."), "properties": {}})
    location = catalog.metadata_location(namespace, table)
    if not location:
        return _not_found("Table", f"{namespace}.{table}")
    if method == "HEAD":
        return 204, {}, ""
    metadata = json.loads(read_object(location))
    return _json(200, {"metadata-location": location, "metadata": metadata, "config": {}})


def s3_reader(account_id: str, partition: str) -> Callable[[str], bytes]:
    from moto.s3.models import s3_backends

    def read(location: str) -> bytes:
        bucket, key = location.removeprefix("s3://").split("/", 1)
        return s3_backends[account_id][partition].get_object(bucket, key).value  # type: ignore[union-attr]

    return read
