"""
Redshift warehouse utilities.

Mirrors the final stage of the Reddit pipeline tutorial: after data is
catalogued in Glue and query-able via Athena, curated Gold tables are also
COPY-loaded into Redshift for fast, repeatable BI/analytics querying.

Flow:
    Gold (S3, Parquet) --COPY--> Redshift (analytics tables)
"""

import logging
import psycopg2
from utils.constants import (
    REDSHIFT_HOST,
    REDSHIFT_PORT,
    REDSHIFT_DB,
    REDSHIFT_USER,
    REDSHIFT_PASSWORD,
    REDSHIFT_IAM_ROLE,
    AWS_BUCKET_NAME,
)

logger = logging.getLogger(__name__)

REDSHIFT_SCHEMA = "nyc_taxi"

# Table DDLs — one per Gold dataset produced in gold/nyc_taxi_gold.py
TABLE_DDLS = {
    "daily_summary": f"""
        CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.daily_summary (
            pickup_year            INT,
            pickup_month            INT,
            pickup_dayofweek         INT,
            total_trips              BIGINT,
            total_revenue             DOUBLE PRECISION,
            avg_trip_duration_min      DOUBLE PRECISION,
            avg_tip_amount            DOUBLE PRECISION,
            avg_passengers             DOUBLE PRECISION
        )
        DISTSTYLE KEY
        DISTKEY (pickup_year)
        SORTKEY (pickup_year, pickup_month, pickup_dayofweek);
    """,
    "hourly_demand": f"""
        CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.hourly_demand (
            pickup_year            INT,
            pickup_month            INT,
            pickup_dayofweek         INT,
            pickup_hour              INT,
            total_trips              BIGINT,
            total_revenue             DOUBLE PRECISION,
            avg_trip_duration_min      DOUBLE PRECISION
        )
        DISTSTYLE KEY
        DISTKEY (pickup_year)
        SORTKEY (pickup_year, pickup_month, pickup_dayofweek, pickup_hour);
    """,
}

# S3 prefix for each gold dataset, relative to the bucket root
S3_GOLD_PREFIXES = {
    "daily_summary": "gold/nyc_taxi/daily_summary/",
    "hourly_demand": "gold/nyc_taxi/hourly_demand/",
}

# Gold Parquet files are written with .partitionBy("pickup_year", "pickup_month"),
# so Spark strips those two columns OUT of the physical files and encodes them
# only in the S3 folder path (pickup_year=X/pickup_month=Y/). Plain Redshift
# COPY (unlike Spectrum/Glue-Catalog external tables) has no way to re-derive
# partition values from the S3 path, so it must target only the columns that
# actually exist in the file — the partition columns get backfilled afterward.
FILE_COLUMNS = {
    "daily_summary": [
        "pickup_dayofweek",
        "total_trips",
        "total_revenue",
        "avg_trip_duration_min",
        "avg_tip_amount",
        "avg_passengers",
    ],
    "hourly_demand": [
        "pickup_dayofweek",
        "pickup_hour",
        "total_trips",
        "total_revenue",
        "avg_trip_duration_min",
    ],
}


def get_redshift_connection():
    """Open a psycopg2 connection to the Redshift cluster."""
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        sslmode="require",
    )


def create_redshift_schema_and_tables():
    """Create the nyc_taxi schema and Gold tables if they don't already exist."""
    conn = get_redshift_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA};")
            logger.info(f"Ensured Redshift schema '{REDSHIFT_SCHEMA}' exists.")

            for table_name, ddl in TABLE_DDLS.items():
                cur.execute(ddl)
                logger.info(f"Ensured Redshift table '{REDSHIFT_SCHEMA}.{table_name}' exists.")
    finally:
        conn.close()


def load_gold_table_to_redshift(table_name: str, year: int, month: int):
    """
    COPY a single partition (year/month) of a Gold Parquet dataset from S3
    into the matching Redshift table.

    Uses DELETE + COPY (idempotent replace) instead of a bare INSERT so
    re-running a month doesn't create duplicate rows in Redshift.
    """
    if table_name not in S3_GOLD_PREFIXES:
        raise ValueError(f"Unknown gold table: {table_name}")

    s3_path = (
        f"s3://{AWS_BUCKET_NAME}/{S3_GOLD_PREFIXES[table_name]}"
        f"pickup_year={year}/pickup_month={month}/"
    )

    conn = get_redshift_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Idempotent replace for this partition only.
            cur.execute(
                f"""
                DELETE FROM {REDSHIFT_SCHEMA}.{table_name}
                WHERE pickup_year = %s AND pickup_month = %s;
                """,
                (year, month),
            )

            # COPY only the columns physically present in the Parquet files
            # (partition columns pickup_year/pickup_month are stripped out by
            # Spark's partitionBy and encoded only in the S3 path — plain
            # Redshift COPY can't read them back from there).
            file_cols = FILE_COLUMNS[table_name]
            col_list_sql = ", ".join(file_cols)
            cur.execute(
                f"""
                COPY {REDSHIFT_SCHEMA}.{table_name} ({col_list_sql})
                FROM %s
                IAM_ROLE %s
                FORMAT AS PARQUET;
                """,
                (s3_path, REDSHIFT_IAM_ROLE),
            )

            # Backfill the partition columns on the rows just inserted.
            # Safe because the DELETE above guarantees no pre-existing rows
            # for this (year, month) remain, so every NULL row here is one
            # we just copied in.
            cur.execute(
                f"""
                UPDATE {REDSHIFT_SCHEMA}.{table_name}
                SET pickup_year = %s, pickup_month = %s
                WHERE pickup_year IS NULL AND pickup_month IS NULL;
                """,
                (year, month),
            )
        conn.commit()
        logger.info(
            f"Loaded {REDSHIFT_SCHEMA}.{table_name} from {s3_path} "
            f"(year={year}, month={month})"
        )
    except Exception:
        conn.rollback()
        logger.exception(f"Failed to load {table_name} into Redshift — rolled back.")
        raise
    finally:
        conn.close()


def load_all_gold_tables_to_redshift(year: int, month: int):
    """Convenience wrapper: load every Gold dataset for the given year/month."""
    for table_name in S3_GOLD_PREFIXES:
        load_gold_table_to_redshift(table_name, year, month)
