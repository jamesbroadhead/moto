"""Handles incoming s3tables requests, invokes methods, returns responses."""

import json
from typing import Any

from moto.core.common_types import TYPE_RESPONSE
from moto.core.responses import BaseResponse

from .models import S3TablesBackend, s3tables_backends


class S3TablesResponse(BaseResponse):
    """Handler for S3Tables requests and responses."""

    def __init__(self) -> None:
        super().__init__(service_name="s3tables")
        self.automated_parameter_parsing = True
        self.default_response_headers = {"Content-Type": "application/json"}

    @property
    def s3tables_backend(self) -> S3TablesBackend:
        """Return backend instance specific for this region."""
        return s3tables_backends[self.current_account][self.region]

    def create_table_bucket(self) -> TYPE_RESPONSE:
        name = self._get_param("name")
        bucket = self.s3tables_backend.create_table_bucket(
            name=name,
        )
        return 200, self.default_response_headers, json.dumps({"arn": bucket.arn})

    def list_table_buckets(self) -> TYPE_RESPONSE:
        prefix = self._get_param("prefix")
        continuation_token = self._get_param("continuationToken")
        max_buckets = self._get_int_param("maxBuckets")
        table_buckets, continuation_token = self.s3tables_backend.list_table_buckets(
            prefix=prefix,
            continuation_token=continuation_token,
            max_buckets=max_buckets,
        )

        body: dict[str, Any] = {
            "tableBuckets": [
                {
                    "arn": b.arn,
                    "name": b.name,
                    "ownerAccountId": b.account_id,
                    "createdAt": b.creation_date.isoformat(),
                }
                for b in table_buckets
            ]
        }
        if continuation_token:
            body.update(continuationToken=continuation_token)

        return 200, self.default_response_headers, json.dumps(body)

    def get_table_bucket(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        bucket = self.s3tables_backend.get_table_bucket(
            table_bucket_arn=table_bucket_arn,
        )

        return (
            200,
            self.default_response_headers,
            json.dumps(
                {
                    "arn": bucket.arn,
                    "name": bucket.name,
                    "ownerAccountId": bucket.account_id,
                    "createdAt": bucket.creation_date.isoformat(),
                }
            ),
        )

    def delete_table_bucket(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        self.s3tables_backend.delete_table_bucket(
            table_bucket_arn=table_bucket_arn,
        )

        return 204, {}, ""

    def create_namespace(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        name = self._get_param("namespace")[0]
        namespace = self.s3tables_backend.create_namespace(
            table_bucket_arn=table_bucket_arn,
            namespace=name,
        )
        return (
            200,
            self.default_response_headers,
            json.dumps(
                {"tableBucketArn": table_bucket_arn, "namespace": [namespace.name]}
            ),
        )

    def list_namespaces(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")

        continuation_token = self._get_param("continuationToken")
        max_namespaces = self._get_int_param("maxNamespaces")
        prefix = self._get_param("prefix")

        namespaces, continuation_token = self.s3tables_backend.list_namespaces(
            table_bucket_arn=table_bucket_arn,
            prefix=prefix,
            continuation_token=continuation_token,
            max_namespaces=max_namespaces,
        )

        body: dict[str, Any] = {
            "namespaces": [
                {
                    "namespace": [ns.name],
                    "createdAt": ns.creation_date.isoformat(),
                    "createdBy": ns.created_by,
                    "ownerAccountId": ns.account_id,
                }
                for ns in namespaces
            ]
        }
        if continuation_token:
            body.update(continuationToken=continuation_token)

        return 200, self.default_response_headers, json.dumps(body)

    def get_namespace(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        name = self._get_param("namespace")
        namespace = self.s3tables_backend.get_namespace(
            table_bucket_arn=table_bucket_arn,
            namespace=name,
        )
        return (
            200,
            self.default_response_headers,
            json.dumps(
                {
                    "namespace": [namespace.name],
                    "createdAt": namespace.creation_date.isoformat(),
                    "createdBy": namespace.created_by,
                    "ownerAccountId": namespace.account_id,
                }
            ),
        )

    def delete_namespace(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        self.s3tables_backend.delete_namespace(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
        )
        return 204, self.default_response_headers, ""

    def create_table(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        name = self._get_param("name")
        format = self._get_param("format")
        table = self.s3tables_backend.create_table(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            name=name,
            format=format,
        )
        return (
            200,
            self.default_response_headers,
            json.dumps({"tableARN": table.arn, "versionToken": table.version_token}),
        )

    def get_table(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        name = self._get_param("name")
        table = self.s3tables_backend.get_table(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            name=name,
        )
        return (
            200,
            self.default_response_headers,
            json.dumps(
                {
                    "name": table.name,
                    "type": table.type,
                    "tableARN": table.arn,
                    "namespace": [namespace],
                    "versionToken": table.version_token,
                    "metadataLocation": table.metadata_location,
                    "warehouseLocation": table.warehouse_location,
                    "createdAt": table.creation_date.isoformat(),
                    "createdBy": table.account_id,
                    "managedByService": table.managed_by_service,
                    "modifiedAt": table.last_modified.isoformat(),
                    "modifiedBy": table.modified_by,
                    "ownerAccountId": table.account_id,
                    "format": table.format,
                }
            ),
        )

    def list_tables(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        prefix = self._get_param("prefix")
        continuation_token = self._get_param("continuationToken")
        max_tables = self._get_int_param("maxTables")
        tables, continuation_token = self.s3tables_backend.list_tables(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            prefix=prefix,
            continuation_token=continuation_token,
            max_tables=max_tables,
        )
        body: dict[str, Any] = {
            "tables": [
                {
                    "namespace": [table.namespace],
                    "name": table.name,
                    "createdAt": table.creation_date.isoformat(),
                    "modifiedAt": table.last_modified.isoformat(),
                }
                for table in tables
            ]
        }

        if continuation_token:
            body.update(continuationToken=continuation_token)

        return 200, self.default_response_headers, json.dumps(body)

    def delete_table(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        name = self._get_param("name")
        version_token = self._get_param("versionToken")
        self.s3tables_backend.delete_table(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            name=name,
            version_token=version_token,
        )
        return 204, {}, ""

    def get_table_metadata_location(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        name = self._get_param("name")
        table = self.s3tables_backend.get_table(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            name=name,
        )
        return (
            200,
            self.default_response_headers,
            json.dumps(
                {
                    "versionToken": table.version_token,
                    "metadataLocation": table.metadata_location,
                    "warehouseLocation": table.warehouse_location,
                }
            ),
        )

    def update_table_metadata_location(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        name = self._get_param("name")
        metadata_location = self._get_param("metadataLocation")
        version_token = self._get_param("versionToken")
        table = self.s3tables_backend.update_table_metadata_location(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            name=name,
            version_token=version_token,
            metadata_location=metadata_location,
        )
        return (
            200,
            self.default_response_headers,
            json.dumps(
                {
                    "name": table.name,
                    "tableArn": table.arn,
                    "namespace": namespace,
                    "versionToken": table.version_token,
                    "metadataLocation": table.metadata_location,
                }
            ),
        )

    def rename_table(self) -> TYPE_RESPONSE:
        table_bucket_arn = self._get_param("tableBucketARN")
        namespace = self._get_param("namespace")
        name = self._get_param("name")
        version_token = self._get_param("versionToken")
        new_namespace_name = self._get_param("newNamespaceName")
        new_name = self._get_param("newName")
        self.s3tables_backend.rename_table(
            table_bucket_arn=table_bucket_arn,
            namespace=namespace,
            name=name,
            new_namespace_name=new_namespace_name,
            new_name=new_name,
            version_token=version_token,
        )
        return 200, {}, ""

    @classmethod
    def iceberg_dispatch(cls, request: Any, full_url: str, headers: Any) -> TYPE_RESPONSE:  # type: ignore[misc]
        from urllib.parse import parse_qs, unquote, urlparse

        from moto.utilities import iceberg_rest
        from moto.utilities.utils import get_partition

        response = cls()
        response.setup_class(request, full_url, headers)
        url = urlparse(full_url)
        path = url.path.split("/iceberg/v1/", 1)[1]
        # The warehouse is the table bucket ARN: a query parameter of /config, then the path prefix
        warehouse = parse_qs(url.query).get("warehouse", [None])[0]
        if warehouse is None and "/namespaces" in path:
            warehouse = unquote(path.split("/namespaces", 1)[0])
            path = "namespaces" + path.split("/namespaces", 1)[1]
        if path.strip("/") == "config":
            body = {"defaults": {}, "overrides": {"prefix": warehouse or ""}}
            return 200, {"Content-Type": "application/json"}, json.dumps(body)
        bucket = response.s3tables_backend.table_buckets.get(warehouse or "")
        if bucket is None:
            return 404, {"Content-Type": "application/json"}, json.dumps(
                {"error": {"message": f"Table bucket does not exist: {warehouse}", "type": "NoSuchWarehouseException", "code": 404}}
            )
        reader = iceberg_rest.s3_reader(response.current_account, get_partition(response.region))
        return iceberg_rest.handle(request.method, path, _S3TablesIcebergCatalog(bucket), reader)


class _S3TablesIcebergCatalog:
    def __init__(self, bucket: Any):
        self.bucket = bucket

    def namespaces(self) -> list[str]:
        return list(self.bucket.namespaces)

    def tables(self, namespace: str) -> list[str]:
        return list(self.bucket.namespaces[namespace].tables)

    def metadata_location(self, namespace: str, table: str) -> str | None:
        ns = self.bucket.namespaces.get(namespace)
        t = ns.tables.get(table) if ns else None
        return t.metadata_location if t else None
