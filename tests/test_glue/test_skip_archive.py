import boto3
import pytest

from moto import mock_aws


@mock_aws
def test_skip_archive_preserves_older_versions_and_optimistic_locking():
    client = boto3.client("glue", region_name="us-east-1")
    client.create_database(DatabaseInput={"Name": "database"})
    client.create_table(DatabaseName="database", TableInput={"Name": "events"})
    client.update_table(DatabaseName="database", TableInput={"Name": "events"})
    for skip_archive, expected_versions in [
        (True, ["1", "3"]),
        (True, ["1", "4"]),
        (False, ["1", "4", "5"]),
    ]:
        current = client.get_table(DatabaseName="database", Name="events")["Table"]
        client.update_table(
            DatabaseName="database",
            TableInput={"Name": "events", "Description": "updated"},
            VersionId=current["VersionId"],
            SkipArchive=skip_archive,
        )
        versions = client.get_table_versions(
            DatabaseName="database", TableName="events"
        )
        assert [v["VersionId"] for v in versions["TableVersions"]] == expected_versions
        with pytest.raises(client.exceptions.ConcurrentModificationException):
            client.update_table(
                DatabaseName="database",
                TableInput={"Name": "events", "Description": "stale"},
                VersionId=current["VersionId"],
                SkipArchive=True,
            )
        assert (
            client.get_table_versions(DatabaseName="database", TableName="events")[
                "TableVersions"
            ]
            == versions["TableVersions"]
        )
        assert (
            client.get_table(DatabaseName="database", Name="events")["Table"][
                "Description"
            ]
            == "updated"
        )

    with pytest.raises(client.exceptions.EntityNotFoundException):
        client.get_table_version(
            DatabaseName="database", TableName="events", VersionId="2"
        )
