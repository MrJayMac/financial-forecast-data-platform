from __future__ import annotations

from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import settings


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
        config=Config(s3={"addressing_style": "path"} if settings.AWS_S3_FORCE_PATH_STYLE else None, signature_version="s3v4"),
        use_ssl=bool(settings.S3_SECURE),
        verify=bool(settings.S3_SECURE),
    )


def ensure_bucket(bucket: str | None = None) -> None:
    client = get_s3_client()
    bucket_name = bucket or settings.S3_BUCKET
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError:
        if settings.S3_ENDPOINT:
            # Likely local MinIO without region constraints
            client.create_bucket(Bucket=bucket_name)
        else:
            client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": settings.S3_REGION})


def object_exists(key: str, bucket: str | None = None) -> bool:
    client = get_s3_client()
    bucket_name = bucket or settings.S3_BUCKET
    try:
        client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey"):
            return False
        raise


def put_json(key: str, data: Any, bucket: str | None = None) -> bool:
    """Put JSON object if not exists. Returns True if uploaded, False if skipped (already exists)."""
    client = get_s3_client()
    bucket_name = bucket or settings.S3_BUCKET
    if object_exists(key, bucket_name):
        return False
    import orjson

    client.put_object(Bucket=bucket_name, Key=key, Body=orjson.dumps(data), ContentType="application/json")
    return True
