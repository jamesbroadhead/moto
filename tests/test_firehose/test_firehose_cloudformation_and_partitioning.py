import gzip
import json

import boto3
import pytest

from moto import mock_aws

REGION = "us-east-1"
QUERY = '{ flow_id: .detail.flow_id, year: (.detail.start | strptime("%Y-%m-%dT%H:%M:%SZ") | strftime("%Y")) }'
TEMPLATE = {
    "Resources": {
        "Bucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "dump"}},
        "Stream": {
            "Type": "AWS::KinesisFirehose::DeliveryStream",
            "Properties": {
                "DeliveryStreamName": "history",
                "DeliveryStreamType": "DirectPut",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::dump",
                    "RoleARN": "arn:aws:iam::123456789012:role/firehose",
                    "Prefix": "dump/year=!{partitionKeyFromQuery:year}/flow=!{partitionKeyFromQuery:flow_id}/",
                    "ErrorOutputPrefix": "dump/error/",
                    "CompressionFormat": "GZIP",
                    "DynamicPartitioningConfiguration": {"Enabled": True},
                    "ProcessingConfiguration": {
                        "Enabled": True,
                        "Processors": [
                            {
                                "Type": "MetadataExtraction",
                                "Parameters": [
                                    {
                                        "ParameterName": "JsonParsingEngine",
                                        "ParameterValue": "JQ-1.6",
                                    },
                                    {
                                        "ParameterName": "MetadataExtractionQuery",
                                        "ParameterValue": QUERY,
                                    },
                                ],
                            },
                            {
                                "Type": "AppendDelimiterToRecord",
                                "Parameters": [
                                    {
                                        "ParameterName": "Delimiter",
                                        "ParameterValue": "\\n",
                                    }
                                ],
                            },
                        ],
                    },
                },
            },
        },
    },
    "Outputs": {
        "Arn": {"Value": {"Fn::GetAtt": ["Stream", "Arn"]}},
        "Name": {"Value": {"Ref": "Stream"}},
    },
}


@mock_aws
def test_delivery_stream_from_cloudformation_partitions_and_compresses():
    pytest.importorskip("jq")
    cfn = boto3.client("cloudformation", region_name=REGION)
    cfn.create_stack(StackName="s", TemplateBody=json.dumps(TEMPLATE))
    outputs = {
        o["OutputKey"]: o["OutputValue"]
        for o in cfn.describe_stacks(StackName="s")["Stacks"][0]["Outputs"]
    }
    assert outputs["Name"] == "history"
    assert outputs["Arn"].endswith(":deliverystream/history")

    firehose = boto3.client("firehose", region_name=REGION)
    events = [
        {"detail": {"flow_id": "f1", "start": "2024-03-01T10:00:00Z"}},
        {"detail": {"flow_id": "f2", "start": "2024-03-01T11:00:00Z"}},
        {"detail": {"flow_id": "f1", "start": "2024-03-01T12:00:00Z"}},
    ]
    firehose.put_record_batch(
        DeliveryStreamName="history",
        Records=[{"Data": json.dumps(e).encode()} for e in events],
    )

    s3 = boto3.client("s3", region_name=REGION)
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket="dump")["Contents"])
    assert [k.rsplit("/", 1)[0] for k in keys] == [
        "dump/year=2024/flow=f1",
        "dump/year=2024/flow=f2",
    ]
    assert all(k.endswith(".gz") for k in keys)
    body = gzip.decompress(s3.get_object(Bucket="dump", Key=keys[0])["Body"].read())
    assert body.decode().splitlines() == [json.dumps(events[0]), json.dumps(events[2])]
