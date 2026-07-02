# 🚕 NYC Taxi AWS Data Engineering Pipeline

> **End-to-End Modern Data Pipeline with Airflow 3.x, PySpark, Glue & Redshift**

---

## 📌 Project Overview

This project builds a complete **Medallion Architecture** (Bronze → Silver → Gold) data pipeline for **NYC Yellow Taxi** trip records on AWS.

**Key Goals:**
- Ingest raw taxi data from public NYC TLC sources into S3
- Clean, transform, and validate data using PySpark
- Create business-ready aggregations (daily & hourly)
- Catalog everything with AWS Glue
- Load curated data into Amazon Redshift for analytics

---

## 🏛️ Architecture

```
Public NYC TLC (CloudFront)
          │
          ▼
┌─────────────────────┐
│   Bronze Layer      │  ← S3 (Hive partitioned: year=YYYY/month=MM)
│   (Raw Parquet)     │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Silver Layer      │  ← PySpark cleaning + derived columns + Data Quality
│   (Cleaned + SCD)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Gold Layer        │  ← Aggregations (daily_summary, hourly_demand)
│   (Business Ready)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐     ┌─────────────────────┐
│   AWS Glue Catalog  │     │   Amazon Redshift   │
│   (Crawlers)        │     │   (Analytics DW)    │
└─────────────────────┘     └─────────────────────┘
```

**Orchestration:** Apache Airflow 3.2.2 (Dockerized with CeleryExecutor)

---

## 🛠️ Technology Stack

| Layer              | Technology                          | Purpose |
|--------------------|-------------------------------------|---------|
| **Orchestration**  | Apache Airflow 3.2.2 (Docker)       | DAGs, task dependencies, parameterization |
| **Bronze Ingestion** | boto3 + CloudFront                | Download from public NYC TLC → S3 |
| **Silver/Gold**    | PySpark 3.5 + Hadoop AWS            | Cleaning, transformations, aggregations |
| **Data Quality**   | Custom PySpark framework            | Null checks, range validation, reporting |
| **Catalog**        | AWS Glue Crawlers                   | Auto-catalog Bronze/Silver/Gold tables |
| **Warehouse**      | Amazon Redshift                     | Final Gold data via COPY (idempotent) |
| **Infrastructure** | Docker + docker-compose             | Reproducible Airflow 3.x environment |

---

## ✅ What Was Implemented

### 1. Bronze Layer (Ingestion)
- Downloads `yellow_tripdata_YYYY-MM.parquet` from NYC TLC CloudFront
- Uploads to S3 with Hive partitioning: `bronze/nyc_taxi/year=YYYY/month=MM/`
- Idempotent (skips if file already exists)
- Parameterized by `year` and `month`

### 2. Silver Layer (Transformation)
- PySpark cleaning with explicit type casting
- Derived columns: `pickup_date`, `pickup_year`, `pickup_month`, `pickup_dayofweek`, `pickup_hour`, `trip_duration_minutes`
- Comprehensive data quality checks (nulls, negative values, zero distances)
- Writes partitioned Parquet to Silver S3 location

### 3. Gold Layer (Aggregations)
- `daily_summary`: Trips, revenue, duration, tips, passengers by day of week
- `hourly_demand`: Same metrics + hourly granularity
- Both partitioned by `year` and `month`

### 4. Orchestration (Airflow 3.x)
- Modern TaskFlow API (`@dag` + `@task`)
- Clear dependency chain: Bronze → Silver → Quality Check → Gold
- Parallel branches for Glue setup and Redshift setup
- Full parameterization support

### 5. Governance & Warehouse
- AWS Glue database + 3 crawlers (Bronze/Silver/Gold)
- Amazon Redshift schema + tables with proper distribution/sort keys
- Idempotent Redshift loading (`DELETE` + `COPY` + backfill)

---

## 🔄 End-to-End Flow

1. **Trigger DAG** with `year` and `month`
2. **Bronze** — Download from CloudFront → S3 (idempotent)
3. **Silver** — PySpark clean + transform + quality checks
4. **Gold** — Create aggregated tables
5. **Glue** — Setup database + run crawlers
6. **Redshift** — Create schema/tables + COPY Gold data (idempotent)

---

## 🧠 Key Implementation Highlights

- **Airflow 3.x Ready** — Uses new `airflow.sdk` imports
- **Robust Data Quality** — Layer-aware null/NaN handling + range checks
- **Idempotent Design** — Safe to re-run any month without duplicates
- **Production Docker Setup** — Full Airflow 3.2.2 stack with Celery + Redis + Postgres
- **Redshift Best Practices** — Proper `DELETE` + `COPY` pattern for partitioned data

---

## 📸 Evidence

The accompanying `NYC_Taxi_AWS_Data_Engineering_Notes.docx` contains detailed explanations, code snippets, and execution evidence from actual pipeline runs.

---

## 🚀 Skills Demonstrated

- Modern Data Engineering with **Medallion Architecture** on AWS
- **Apache Airflow 3.x** (TaskFlow, parameterization, Docker deployment)
- **PySpark** for large-scale cleaning, transformation, and aggregation
- **AWS Glue** for automated cataloging
- **Amazon Redshift** data loading best practices
- **Data Quality** frameworks at scale
- Production-grade pipeline design with idempotency and error handling

---

## 📁 Deliverables

| File | Description |
|------|-------------|
| `NYC_Taxi_AWS_Data_Engineering_Notes.docx` | Complete technical documentation |
| `README_NYC_Taxi_AWS_Data_Engineering.md` | This high-level summary |
| Full project code | Airflow DAGs, PySpark jobs, Glue & Redshift utilities |

---

**Author:** Himanshu  
**Focus:** Azure + AWS Data Engineering, Airflow, PySpark, Modern Data Platforms

---
