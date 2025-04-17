import pytest
import boto3
from moto import mock_s3
from src.ingest.ingesta import download_file, upload_to_s3
import os

@pytest.fixture
def s3_bucket():
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield s3, bucket_name


def test_download_file(tmp_path):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    local_file = tmp_path / "yellow_tripdata_2023-01.parquet"
    download_file(url, str(local_file))
    assert local_file.exists()
    assert local_file.stat().st_size > 0


def test_upload_to_s3(s3_bucket, tmp_path):
    s3, bucket_name = s3_bucket
    local_file = tmp_path / "yellow_tripdata_2023-01.parquet"
    local_file.write_text("conteúdo de teste")
    s3_key = "ingested/yellow_tripdata_2023-01.parquet"
    upload_to_s3(str(local_file), bucket_name, s3_key, "fake-access-key", "fake-secret-key")
    response = s3.list_objects_v2(Bucket=bucket_name)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    assert s3_key in keys