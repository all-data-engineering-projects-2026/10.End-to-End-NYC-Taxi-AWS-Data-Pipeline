from pyspark.sql import SparkSession
from silver.transformations import clean_and_transform_nyc_taxi
from silver.quality_checks import run_data_quality_checks
import logging

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "NYC_Taxi_Silver"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        # S3A Configuration
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.787")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.multipart.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


def process_silver_layer(year: int, month: int, bronze_bucket: str, silver_bucket: str):
    spark = create_spark_session()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    bronze_path = f"s3a://{bronze_bucket}/bronze/nyc_taxi/year={year}/month={month:02d}/{filename}"
    silver_path = f"s3a://{silver_bucket}/silver/nyc_taxi/"

    logger.info(f"Reading Bronze data from: {bronze_path}")

    df_bronze = spark.read.parquet(bronze_path)
    run_data_quality_checks(df_bronze, layer="Bronze")

    df_silver = clean_and_transform_nyc_taxi(df_bronze)
    run_data_quality_checks(df_silver, layer="Silver")

    logger.info(f"Writing Silver data to: {silver_path}")
    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(silver_path)
    )

    logger.info("Silver layer processing completed successfully.")
    spark.stop()