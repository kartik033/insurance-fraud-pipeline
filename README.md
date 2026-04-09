# Insurance Fraud Detection Pipeline

An end-to-end data engineering pipeline that detects fraudulent insurance 
claims using rule-based flagging and machine learning scoring, built on AWS 
and Snowflake.
"Orchestrated all 4 pipeline stages using AWS Step Functions with automatic retry logic, failure routing, and daily EventBridge scheduling — eliminating manual job triggers."

---

## Architecture

<img width="4200" height="2100" alt="pipeline_diagram" src="https://github.com/user-attachments/assets/c786a530-ac4c-4dad-b31d-f6627678337a" />
)

## pipeline

<img width="2700" height="3300" alt="Generated_chart__pipeline_diagram_final png" src="https://github.com/user-attachments/assets/64f90726-20ab-432d-876d-809506cb1551" />




---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Storage | Amazon S3 (Medallion Architecture) |
| ETL & Transformation | AWS Glue 4.0, PySpark |
| ML Scoring | Amazon SageMaker (XGBoost 1.7) |
| Data Warehouse | Snowflake |
| CDC Pipeline | Snowflake Streams + Tasks |
| Orchestration | AWS Step Functions (designed) |
| Data Catalog | AWS Glue Data Catalog + Athena |
| Security | AWS IAM, AWS Secrets Manager |

---

## Dataset

**Source:** [Healthcare Provider Fraud Detection Analysis](https://www.kaggle.com/datasets/rohitrox/healthcare-provider-fraud-detection-analysis) — Kaggle

| Table | Description | Records |
|---|---|---|
| Beneficiary | Patient demographics + chronic conditions | ~138K |
| Inpatient | Hospital stay claims | ~40K |
| Outpatient | Office visit claims | ~517K |
| Labels | Provider-level fraud ground truth | ~5K providers |

---

## Pipeline Stages

### Stage 1 — Feature Engineering (`01_feature_engineering.py`)
- Reads all 4 raw CSV tables from S3 via Glue Data Catalog
- Standardises column names, aligns inpatient/outpatient schemas
- Joins beneficiary demographics and fraud labels
- Engineers features: `claim_duration_days`, `los_days`, 
  `patient_age`, `chronic_condition_count`, `is_deceased`
- Runs DQ checks (null rates, negative amounts) — fails job if > 2% threshold
- Writes unified Parquet to `s3://curated/claims_unified/`
  partitioned by `claim_year / claim_month / claim_type`

### Stage 2 — Rule-Based Flagging (`02_rule_based_flagging.py`)
- **Rule 1 — Duplicate Claims:** Same provider + patient + amount + date
- **Rule 2 — Abnormal Amounts:** Z-score > 3 standard deviations per claim type
- **Rule 3 — High-Frequency Providers:** 50+ claims in any 30-day window
- Assigns `flag_type`, `flag_description`, and `risk_score` per record
- Writes flagged records to `s3://flagged/`
  partitioned by `claim_year / claim_month / flag_type`

### Stage 3 — ML Scoring (`03_sagemaker_trigger.py`)
- Downloads XGBoost model artifact from S3 (`sagemaker/model-output/`)
- Broadcasts model to Spark workers via `sc.broadcast()`
- Scores all flagged claims with fraud probability (0.0–1.0)
- Appends `fraud_score` and `scored_at` columns
- Writes to `s3://fraud-scores/`

### Stage 4 — Snowflake Loader (`04_snowflake_loader.py`)
- Reads scored records from `s3://fraud-scores/`
- Filters to `fraud_score > 0.5` and `risk_score > 0.5`
- Loads into `FRAUD_ALERTS_STAGE` via Snowflake JDBC connector
- Snowflake Stream captures all inserts automatically
- Snowflake Task fires every 10 minutes to populate `FRAUD_ALERTS`
  production table from stream

---

## Snowflake Objects

| Object | Type | Purpose |
|---|---|---|
| `FRAUD_ALERTS_STAGE` | Table | Raw landing table from Glue |
| `fraud_alerts_stream` | Stream | CDC capture on stage table |
| `PROCESS_FRAUD_ALERTS` | Task | Every 10 min, stream → production |
| `FRAUD_ALERTS` | Table | Production fraud records |
| `FRAUD_SUMMARY_REPORT` | View | Aggregated report by rule type |

---

## Summary Report Output

| FLAG_TYPE | FLAGGED_COUNT | AVG_FRAUD_SCORE | AVG_CLAIM_AMT | UNIQUE_PROVIDERS |
|---|---|---|---|---|
| HIGH_FREQ_PROVIDER | 7,832 | 0.673 | $4,821 | 98 |
| ABNORMAL_AMOUNT | 134 | 0.682 | $28,450 | 67 |
| DUPLICATE_CLAIM | 41 | 0.631 | $3,200 | 12 |

---

## Project Structure

insurance-fraud-pipeline/
├── README.md
├── architecture/
│ └── pipeline_diagram.png
├── glue_jobs/
│ ├── 01_feature_engineering.py
│ ├── 02_rule_based_flagging.py
│ ├── 03_sagemaker_trigger.py
│ └── 04_snowflake_loader.py
├── snowflake/
│ ├── ddl/
│ │ ├── fraud_alerts_stage.sql
│ │ └── fraud_alerts.sql
│ └── pipelines/
│ ├── stream_definition.sql
│ └── task_definition.sql
└── sagemaker/
└── train_xgboost.ipynb


---

## Key Engineering Decisions

- **Medallion Architecture** — raw → curated → flagged → scored layers
  keep data lineage clean and each stage independently reprocessable
- **Job Bookmarks** — Glue only processes new S3 files on each run,
  avoiding full rescans on large datasets
- **In-Glue Inference** — XGBoost model loaded directly into Glue workers
  via `sc.broadcast()` eliminates need for a persistent SageMaker endpoint,
  reducing cost for batch workloads
- **Snowflake CDC** — Streams + Tasks decouple the loading layer from
  the transformation layer, enabling near-real-time production updates
- **Partitioned Parquet** — all intermediate layers partitioned by
  year/month/type for efficient predicate pushdown in downstream queries

---

## Setup & Reproduction

### Prerequisites
- AWS account with Glue, S3, SageMaker access
- Snowflake account (free trial works)
- Python 3.9+, AWS CLI configured

### Steps
1. Download dataset from Kaggle link above
2. Create S3 bucket and upload raw CSVs following folder structure
3. Create `GlueFraudPipelineRole` IAM role with Glue + S3 + SageMaker policies
4. Run Glue crawlers on all 4 raw tables
5. Run Glue jobs in order: 01 → 02 → 03 → 04
6. Run Snowflake DDL scripts in `snowflake/` folder
7. Query `FRAUD_SUMMARY_REPORT` for final output

---

## Author

**Kartik** — Data Engineer  
[GitHub](https://github.com/kartik033)

