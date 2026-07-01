"""
NYC Taxi Data Pipeline - DAG Entry Point
This file imports and exposes the DAG defined in pipelines/ for Airflow discovery.
"""

# FIX 1.3: Removed sys.path.insert — PYTHONPATH=/opt/airflow is already set in
# docker-compose.yml, making manual sys.path manipulation redundant and fragile.
# Keeping this file minimal: its only job is to make the DAG discoverable.

from pipelines.nyc_taxi_ingestion import nyc_taxi_data_pipeline  # noqa: F401
# FIX 1.3: Removed `dag = nyc_taxi_data_pipeline()` — nyc_taxi_ingestion.py already
# calls nyc_taxi_data_pipeline() at the bottom of the file, so the DAG is registered
# when this import executes. Calling it again here would register a duplicate DAG_ID
# causing Airflow warnings or non-deterministic behaviour.
