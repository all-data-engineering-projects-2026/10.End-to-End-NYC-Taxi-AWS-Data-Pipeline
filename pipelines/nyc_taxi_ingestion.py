from airflow.sdk import dag, task
from datetime import datetime
import logging
from etls.aws_etl import download_and_upload_to_bronze
from silver.nyc_taxi_silver import process_silver_layer
from silver.quality_checks import run_data_quality_checks
from gold.nyc_taxi_gold import process_gold_layer
from etls.glue_utils import (
    create_glue_database,
    create_glue_crawler,
    start_glue_crawler
)
from warehouse.redshift_utils import (
    create_redshift_schema_and_tables,
    load_all_gold_tables_to_redshift,
)
from utils.constants import AWS_BUCKET_NAME

logger = logging.getLogger(__name__)



@dag(
    dag_id="nyc_taxi_data_pipeline",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc", "taxi", "bronze", "silver", "gold", "glue", "athena", "redshift"],
    params={"year": 2024, "month": 2}
)
def nyc_taxi_data_pipeline():

    @task
    def bronze_ingestion(**context):
        params = context["params"]
        year = params.get("year", 2024)
        month = params.get("month", 1)

        logger.info("=== NYC Taxi Bronze Ingestion ===")
        s3_key = download_and_upload_to_bronze(year, month, AWS_BUCKET_NAME)
        logger.info(f"Bronze file landed at: s3://{AWS_BUCKET_NAME}/{s3_key}")
        return {"year": year, "month": month}

    @task
    def silver_transformation(bronze_result: dict):
        year = bronze_result["year"]
        month = bronze_result["month"]

        process_silver_layer(year, month, AWS_BUCKET_NAME, AWS_BUCKET_NAME)
        return {"year": year, "month": month}

    @task
    def data_quality_check(silver_result: dict):

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col

        year = silver_result["year"]
        month = silver_result["month"]

        spark = (
            SparkSession.builder
            .appName("Airflow_Data_Quality")
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.787")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.multipart.enabled", "false")
            .getOrCreate()
        )

        silver_path = f"s3a://{AWS_BUCKET_NAME}/silver/nyc_taxi/"
        df_silver = (
            spark.read.parquet(silver_path)
            .filter((col("pickup_year") == year) & (col("pickup_month") == month))
        )

        quality_report = run_data_quality_checks(df_silver, layer="Silver")

        spark.stop()
        return {"year": year, "month": month, "quality_status": quality_report["status"]}

    @task
    def gold_aggregation(quality_result: dict):
        year = quality_result["year"]
        month = quality_result["month"]
        process_gold_layer(year, month, AWS_BUCKET_NAME, AWS_BUCKET_NAME)
        return {"year": year, "month": month}

    # =====================
    # Glue + Athena Steps
    # =====================
    @task
    def setup_glue_infrastructure():
        create_glue_database()

        create_glue_crawler(
            crawler_name="nyc_taxi_bronze_crawler",
            database_name="nyc_taxi_db",
            s3_target_path=f"s3://{AWS_BUCKET_NAME}/bronze/nyc_taxi/",
            table_prefix="bronze_"
        )
        create_glue_crawler(
            crawler_name="nyc_taxi_silver_crawler",
            database_name="nyc_taxi_db",
            s3_target_path=f"s3://{AWS_BUCKET_NAME}/silver/nyc_taxi/",
            table_prefix="silver_"
        )
        create_glue_crawler(
            crawler_name="nyc_taxi_gold_crawler",
            database_name="nyc_taxi_db",
            s3_target_path=f"s3://{AWS_BUCKET_NAME}/gold/nyc_taxi/",
            table_prefix="gold_"
        )

    @task
    def run_glue_crawlers(gold_result: dict):
        start_glue_crawler("nyc_taxi_bronze_crawler")
        start_glue_crawler("nyc_taxi_silver_crawler")
        start_glue_crawler("nyc_taxi_gold_crawler")

    # =====================
    # Redshift Warehouse Steps
    # =====================
    @task
    def setup_redshift_warehouse():
        """Ensure the nyc_taxi schema + Gold tables exist in Redshift."""
        create_redshift_schema_and_tables()

    @task
    def load_redshift_warehouse(gold_result: dict):
        """
        COPY the current run's Gold partition (daily_summary, hourly_demand)
        from S3 into Redshift. Runs after the Glue crawlers so Athena/Glue
        and Redshift stay in sync, mirroring the tutorial's S3 -> Athena ->
        Redshift flow.
        """
        year = gold_result["year"]
        month = gold_result["month"]
        load_all_gold_tables_to_redshift(year, month)

    # Task Dependencies
    bronze_result  = bronze_ingestion()
    silver_result  = silver_transformation(bronze_result)
    quality_result = data_quality_check(silver_result)
    gold_result    = gold_aggregation(quality_result)

    glue_setup = setup_glue_infrastructure()
    crawlers = run_glue_crawlers(gold_result)

    redshift_setup = setup_redshift_warehouse()
    redshift_load = load_redshift_warehouse(gold_result)

    gold_result >> glue_setup >> crawlers >> redshift_setup >> redshift_load


nyc_taxi_data_pipeline()