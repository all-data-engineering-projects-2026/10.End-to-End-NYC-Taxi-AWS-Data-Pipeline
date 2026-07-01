from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan
from pyspark.sql.types import NumericType
import logging

logger = logging.getLogger(__name__)

BRONZE_CRITICAL_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

SILVER_CRITICAL_COLUMNS = [
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "passenger_count",
    "trip_distance",
    "total_amount",
    "trip_duration_minutes",
]


def _is_null_or_nan(df: DataFrame, column: str):
    """
    Return a Column expression that is True when the value is null (any type)
    or NaN (numeric types only).

    FIX 1.4: Previously `isnan()` was called unconditionally on all columns.
    isnan() only accepts numeric types — calling it on DateType or StringType
    raises AnalysisException. This helper checks the schema first.
    """
    field = next((f for f in df.schema.fields if f.name == column), None)
    is_numeric = field is not None and isinstance(field.dataType, NumericType)

    null_expr = col(column).isNull()
    if is_numeric:
        return null_expr | isnan(col(column))
    return null_expr


def run_data_quality_checks(
        df: DataFrame,
        layer: str = "Silver",
        critical_columns: list = None  # FIX 1.2: Allow caller to override column list
) -> dict:
    """
    Run comprehensive data quality checks on a DataFrame.
    Returns a dict with quality metrics and a status of PASSED or WARNINGS.
    """
    logger.info(f"=== Running Data Quality Checks on {layer} Layer ===")

    if critical_columns is None:
        if layer == "Bronze":
            critical_columns = BRONZE_CRITICAL_COLUMNS  # FIX 1.2
        else:
            critical_columns = SILVER_CRITICAL_COLUMNS  # FIX 1.2

    total_rows = df.count()
    logger.info(f"[{layer}] Total rows: {total_rows}")

    null_counts = {}
    for column in critical_columns:
        if column not in df.columns:
            logger.warning(f"[{layer}] Expected column '{column}' not found in DataFrame — skipping.")
            continue

        null_count = df.filter(_is_null_or_nan(df, column)).count()  # FIX 1.4
        null_counts[column] = null_count

        if null_count > 0:
            logger.warning(f"[{layer}] Null/NaN values in '{column}': {null_count}")
        else:
            logger.info(f"[{layer}] No nulls in '{column}'")

    # Basic range checks
    if "total_amount" in df.columns:
        negative_amounts = df.filter(col("total_amount") < 0).count()
        if negative_amounts > 0:
            logger.warning(f"[{layer}] Negative total_amount found: {negative_amounts}")

    if "trip_duration_minutes" in df.columns:
        negative_duration = df.filter(col("trip_duration_minutes") < 0).count()
        if negative_duration > 0:
            logger.warning(f"[{layer}] Negative trip_duration found: {negative_duration}")

    if "trip_distance" in df.columns:
        zero_distance = df.filter(col("trip_distance") <= 0).count()
        if zero_distance > 0:
            logger.warning(f"[{layer}] Zero or negative trip_distance found: {zero_distance}")

    quality_report = {
        "layer": layer,
        "total_rows": total_rows,
        "null_counts": null_counts,
        "status": "PASSED" if all(v == 0 for v in null_counts.values()) else "WARNINGS",
    }

    logger.info(f"[{layer}] Data Quality Check completed. Status: {quality_report['status']}")
    return quality_report
