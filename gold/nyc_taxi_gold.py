from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count,
)
import logging

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "NYC_Taxi_Gold"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.787")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.multipart.enabled", "false")
        .getOrCreate()
    )


def process_gold_layer(year: int, month: int, silver_bucket: str, gold_bucket: str):
    spark = create_spark_session()

    silver_path = f"s3a://{silver_bucket}/silver/nyc_taxi/"
    gold_base_path = f"s3a://{gold_bucket}/gold/nyc_taxi/"

    logger.info(f"Reading Silver data from: {silver_path} (pickup_year={year}, pickup_month={month})")
    df_silver = (
        spark.read.parquet(silver_path)
        .filter((col("pickup_year") == year) & (col("pickup_month") == month))
    )

    # =====================
    # 1. Daily Summary
    # =====================
    daily_summary = (
        df_silver
        .groupBy("pickup_year", "pickup_month", "pickup_dayofweek")
        .agg(
            count("*").alias("total_trips"),
            _sum("total_amount").alias("total_revenue"),
            avg("trip_duration_minutes").alias("avg_trip_duration_min"),
            avg("tip_amount").alias("avg_tip_amount"),
            avg("passenger_count").alias("avg_passengers"),
        )
        .orderBy("pickup_year", "pickup_month", "pickup_dayofweek")
    )

    daily_path = f"{gold_base_path}daily_summary/"
    logger.info(f"Writing Daily Summary to: {daily_path}")
    (
        daily_summary.write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")  # FIX 4.4
        .parquet(daily_path)
    )

    # =====================
    # 2. Hourly Demand
    # =====================
    hourly_demand = (
        df_silver
        .groupBy("pickup_year", "pickup_month", "pickup_dayofweek", "pickup_hour")
        .agg(
            count("*").alias("total_trips"),
            _sum("total_amount").alias("total_revenue"),
            avg("trip_duration_minutes").alias("avg_trip_duration_min"),
        )
        .orderBy("pickup_year", "pickup_month", "pickup_dayofweek", "pickup_hour")
    )

    # FIX 4.4: Same partitioning fix as daily_summary above.
    hourly_path = f"{gold_base_path}hourly_demand/"
    logger.info(f"Writing Hourly Demand to: {hourly_path}")
    (
        hourly_demand.write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")  # FIX 4.4
        .parquet(hourly_path)
    )

    logger.info("Gold layer processing completed successfully.")
    spark.stop()
