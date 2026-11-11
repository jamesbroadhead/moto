"""MOTO_S3_VERIFY_PRESIGNED_URLS: S3 recomputes the SigV4 signature of a presigned URL.

A presigned URL signs the HTTP method, the path, the query and the signed headers. AWS answers a request that
differs in any of them, such as a HEAD sent to a URL presigned for GET, with 403 SignatureDoesNotMatch:
https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
"""

import json
import os
from unittest import SkipTest, mock

import boto3
import pytest
import requests

from moto import mock_aws, settings

REGION = "eu-central-1"
BUCKET = "presigned-bucket"


@pytest.fixture(autouse=True)
def decorator_mode_only():
    # The setting is read by the process that serves S3; only decorator mode shares the test's environment
    if not settings.TEST_DECORATOR_MODE:
        raise SkipTest("Sets an environment variable of the S3 server")


def client(access_key="FOOBARKEY", secret_key="FOOBARSECRET", session_token=None):
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )


def put_object():
    s3 = client()
    s3.create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    s3.put_object(Bucket=BUCKET, Key="file.txt", Body=b"ABCD")


def presign(s3, method="get_object"):
    return s3.generate_presigned_url(method, {"Bucket": BUCKET, "Key": "file.txt"})


def verify(secrets):
    return mock.patch.dict(
        os.environ, {"MOTO_S3_VERIFY_PRESIGNED_URLS": json.dumps(secrets)}
    )


@mock_aws
def test_head_on_url_presigned_for_get_is_refused():
    put_object()
    url = presign(client())
    with verify({"FOOBARKEY": "FOOBARSECRET"}):
        assert requests.get(url).content == b"ABCD"
        head = requests.head(url, stream=True)
    assert head.status_code == 403


@mock_aws
def test_delete_on_url_presigned_for_get_is_refused():
    put_object()
    url = presign(client())
    with verify({"FOOBARKEY": "FOOBARSECRET"}):
        resp = requests.delete(url)
    assert resp.status_code == 403
    assert b"<Code>SignatureDoesNotMatch</Code>" in resp.content
    assert client().get_object(Bucket=BUCKET, Key="file.txt")["Body"].read() == b"ABCD"


@mock_aws
def test_presigned_head_object_is_accepted():
    put_object()
    url = presign(client(), "head_object")
    with verify({"FOOBARKEY": "FOOBARSECRET"}):
        assert requests.head(url, stream=True).status_code == 200


@mock_aws
def test_url_signed_with_another_secret_is_refused():
    put_object()
    url = presign(client(secret_key="not-the-secret"))
    with verify({"FOOBARKEY": "FOOBARSECRET"}):
        assert requests.get(url).status_code == 403


@mock_aws
def test_changed_query_is_refused():
    put_object()
    url = presign(client())
    with verify({"FOOBARKEY": "FOOBARSECRET"}):
        assert requests.get(url + "&versionId=null").status_code == 403


@mock_aws
def test_iam_access_keys_are_verified_without_listing_them():
    put_object()
    iam = boto3.client("iam", region_name=REGION)
    iam.create_user(UserName="presigner")
    key = iam.create_access_key(UserName="presigner")["AccessKey"]
    url = presign(client(key["AccessKeyId"], key["SecretAccessKey"]))
    with verify({}):
        assert requests.get(url).status_code == 200
        assert requests.head(url, stream=True).status_code == 403


@mock_aws
def test_sts_credentials_are_verified_without_listing_them():
    put_object()
    role = boto3.client("iam", region_name=REGION).create_role(
        RoleName="presigner", AssumeRolePolicyDocument="{}"
    )["Role"]
    creds = boto3.client("sts", region_name=REGION).assume_role(
        RoleArn=role["Arn"], RoleSessionName="presigner"
    )["Credentials"]
    s3 = client(creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"])
    url = presign(s3)
    with verify({}):
        assert requests.get(url).status_code == 200
        assert requests.head(url, stream=True).status_code == 403


@mock_aws
def test_unknown_access_key_is_not_verified():
    put_object()
    url = presign(client())
    with verify({}):
        assert requests.head(url, stream=True).status_code == 200


@mock_aws
def test_signatures_are_not_verified_by_default():
    put_object()
    url = presign(client())
    assert requests.head(url, stream=True).status_code == 200


@mock_aws
def test_key_that_needs_encoding_is_verified():
    put_object()
    s3 = client()
    key = "results/run 1/füll+data=1.json.gz"
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"ABCD")
    url = s3.generate_presigned_url("get_object", {"Bucket": BUCKET, "Key": key})
    with verify({"FOOBARKEY": "FOOBARSECRET"}):
        assert requests.get(url).content == b"ABCD"
        assert requests.head(url, stream=True).status_code == 403
