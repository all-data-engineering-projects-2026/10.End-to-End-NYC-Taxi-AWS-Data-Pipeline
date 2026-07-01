from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, hour, dayofweek, year, month,
    unix_timestamp, round as _round
)
from pyspark.sql.types import IntegerType, DoubleType, TimestampType
import logging

logger = logging.getLogger(__name__)


def clean_and_transform_nyc_taxi(df: DataFrame) -> DataFrame:
    """
    Clean and transform NYC Taxi data for Silver layer.
    Produces pickup_date, pickup_year, pickup_month, pickup_dayofweek,
    pickup_hour, trip_duration_minutes as derived columns.
    """
    logger.info("Starting Silver transformations (PySpark)...")

    df_transformed = (
        df
        .select(
            # FIX (minor): Explicit casts added to every column so the Silver schema is
            # stable regardless of how the source Parquet was written.
            col("VendorID").cast(IntegerType()),
            col("tpep_pickup_datetime").cast(TimestampType()),
            col("tpep_dropoff_datetime").cast(TimestampType()),
            col("passenger_count").cast(IntegerType()),
            col("trip_distance").cast(DoubleType()),
            col("PULocationID").cast(IntegerType()),
            col("DOLocationID").cast(IntegerType()),
            col("fare_amount").cast(DoubleType()),
            col("tip_amount").cast(DoubleType()),
            col("total_amount").cast(DoubleType()),
        )
        .filter(
            (col("passenger_count") > 0) &
            (col("trip_distance") > 0) &
            (col("total_amount") > 0)
        )
        # Derived columns — used by both quality checks and gold aggregations
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            _round(
                (unix_timestamp(col("tpep_dropoff_datetime")) -
                 unix_timestamp(col("tpep_pickup_datetime"))) / 60,
                2
            )
        )
    )

    logger.info(f"Silver transformation complete. Final rows: {df_transformed.count()}")
    return df_transformed
