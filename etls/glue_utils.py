import boto3
import json
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

GLUE_DATABASE_NAME = "nyc_taxi_db"
GLUE_CRAWLER_ROLE = "AWSGlueServiceRole-nyc-taxi"


def get_glue_client():
    return boto3.client('glue')


def create_glue_database(database_name: str = GLUE_DATABASE_NAME):
    """Create Glue database if it doesn't exist."""
    glue = get_glue_client()
    try:
        glue.get_database(Name=database_name)
        logger.info(f"Glue database '{database_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'NYC Taxi Data Lakehouse Database'
            }
        )
        logger.info(f"Created Glue database: {database_name}")


def create_glue_crawler(
    crawler_name: str,
    database_name: str,
    s3_target_path: str,
    table_prefix: str = "",
    role_arn: str = GLUE_CRAWLER_ROLE
):
    """Create (or update) a Glue Crawler for a specific S3 path."""
    glue = get_glue_client()

    crawler_kwargs = dict(
        Role=role_arn,
        DatabaseName=database_name,
        Description=f"Crawler for {crawler_name}",
        Targets={'S3Targets': [{'Path': s3_target_path, 'Exclusions': []}]},
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'},
        Configuration=json.dumps({
            "Version": 1.0,
            "CrawlerOutput": {
                "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
                "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
            }
        }),
        TablePrefix=table_prefix,
    )

    try:
        glue.get_crawler(Name=crawler_name)
        # Crawler already exists — update it so config/prefix changes actually apply.
        glue.update_crawler(Name=crawler_name, **crawler_kwargs)
        logger.info(f"Updated existing Glue Crawler: {crawler_name}")
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_crawler(Name=crawler_name, **crawler_kwargs)
    logger.info(f"Created Glue Crawler: {crawler_name}")


def start_glue_crawler(crawler_name: str):
    """Start a Glue Crawler."""
    glue = get_glue_client()
    try:
        glue.start_crawler(Name=crawler_name)
        logger.info(f"Started Glue Crawler: {crawler_name}")
    except glue.exceptions.CrawlerRunningException:
        logger.warning(f"Glue Crawler '{crawler_name}' is already running.")