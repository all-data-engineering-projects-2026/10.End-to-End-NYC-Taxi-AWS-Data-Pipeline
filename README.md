# NYC Taxi AWS Data Pipeline (Airflow 3.x)

End-to-end data engineering pipeline to ingest NYC Yellow Taxi data into AWS S3 (Bronze layer), adapted from the original Reddit Data Engineering project.

## Key Changes from Original
- Replaced Reddit API with **NYC Taxi public data** (no API key issues)
- Upgraded to **Airflow 3.2.2**
- Uses direct S3-to-S3 copy (more efficient)
- Supports parameterized ingestion by `year` and `month`
- Follows Hive-style partitioning in Bronze layer

## Project Structure
```
nyc-taxi-aws-pipeline/
├── dags/
│   └── nyc_taxi_dag.py
├── pipelines/
│   └── nyc_taxi_ingestion.py
├── etls/
│   └── aws_etl.py
├── utils/
│   └── constants.py
├── config/
│   └── config.conf.example
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Setup Instructions

1. **Clone / Download** this project folder.

2. **Create config file**:
   ```bash
   cp config/config.conf.example config/config.conf
   ```
   Edit `config/config.conf` and fill in your AWS credentials and target S3 bucket name.

3. **Start Airflow**:
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**:
   - Go to: http://localhost:8080
   - Login: `admin` / `admin`

5. **Trigger the DAG**:
   - Go to DAG `nyc_taxi_data_pipeline`
   - Click **Trigger DAG**
   - You can pass parameters:
     ```json
     {
       "year": 2025,
       "month": 6
     }
     ```

## What This Pipeline Does
- Copies `yellow_tripdata_YYYY-MM.parquet` from the public `s3://nyc-tlc/` bucket
- Stores it in your S3 bucket under:
  ```
  bronze/nyc_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
  ```

## Next Steps (After Ingestion)
- Create Glue Crawler on the Bronze prefix
- Run Glue ETL jobs or use Athena for transformations
- Load curated data into Redshift (similar to original tutorial)

## Notes
- This project only handles the **Ingestion + Bronze layer** part (same scope as original repo).
- The Glue + Athena + Redshift steps remain manual (as in the original video).

## Data Source
NYC Taxi & Limousine Commission (TLC) Trip Record Data  
Public S3 Bucket: `s3://nyc-tlc/trip-data/`
