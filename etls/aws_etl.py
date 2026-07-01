import boto3
import requests
from io import BytesIO
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

CLOUDFRONT_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_s3_client():
    """Create boto3 S3 client using default credentials."""
    return boto3.client("s3")

def _key_exists(bucket: str, key: str) -> bool:
    """Return True if the S3 object already exists."""
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise  # Re-raise unexpected errors (permissions, network, etc.)


def download_from_cloudfront(year: int, month: int) -> BytesIO:
    """Download yellow_tripdata file from NYC TLC CloudFront."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{CLOUDFRONT_BASE_URL}/{filename}"

    logger.info(f"Downloading from CloudFront: {url}")

    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        file_buffer = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_buffer.write(chunk)
        file_buffer.seek(0)

        logger.info(f"Successfully downloaded {filename} ({file_buffer.getbuffer().nbytes} bytes)")
        return file_buffer

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {filename} from CloudFront: {e}")
        raise


def upload_to_s3(file_buffer: BytesIO, bucket: str, key: str):
    """Upload BytesIO content to S3."""
    s3 = get_s3_client()
    try:
        logger.info(f"Uploading to s3://{bucket}/{key}")
        s3.upload_fileobj(file_buffer, bucket, key)
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise


def download_and_upload_to_bronze(year: int, month: int, bronze_bucket: str) -> str:
    """Main function: Download from CloudFront and upload to Bronze layer."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    s3_key = f"bronze/nyc_taxi/year={year}/month={month:02d}/{filename}"

    if _key_exists(bronze_bucket, s3_key):
        logger.info(f"Already exists in S3: s3://{bronze_bucket}/{s3_key} — skipping download.")
        return s3_key

    file_buffer = download_from_cloudfront(year, month)
    upload_to_s3(file_buffer, bronze_bucket, s3_key)

    return s3_key
