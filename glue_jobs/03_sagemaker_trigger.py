import sys, boto3, tarfile, os
import xgboost as xgb
import numpy as np
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, udf, current_timestamp
from pyspark.sql.types import FloatType

# ── Init ──────────────────────────────────────────────────────────────────────
args        = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "fraud-detection-pipeline007"

# ── Download Model Artifact from S3 ──────────────────────────────────────────
s3 = boto3.client("s3")

# List to find the exact model.tar.gz path
response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix="sagemaker/model-output/"
)
model_key = [
    obj["Key"] for obj in response["Contents"]
    if obj["Key"].endswith("model.tar.gz")
][0]

s3.download_file(BUCKET, model_key, "/tmp/model.tar.gz")

os.makedirs("/tmp/model/", exist_ok=True)
with tarfile.open("/tmp/model.tar.gz") as tar:
    tar.extractall("/tmp/model/")

# Walk all extracted content to find the actual model file
model_path = None
for root, dirs, files in os.walk("/tmp/model/"):
    for f in files:
        full_path = os.path.join(root, f)
        print(f"Found: {full_path}")
        if model_path is None:  # take the first file found
            model_path = full_path

print(f"Using model file: {model_path}")

booster = xgb.Booster()
booster.load_model(model_path)
print("Model loaded ✅")

import os

# List all extracted files to find the right one
extracted_files = os.listdir("/tmp/model/")
print(f"Extracted files: {extracted_files}")

# SageMaker XGBoost saves as either 'xgboost-model' or a .json/.bin file
model_path = f"/tmp/model/{extracted_files[0]}"
print(f"Loading model from: {model_path}")

# Try JSON format first (XGBoost 1.6+), fall back to binary
booster = xgb.Booster()
try:
    booster.load_model(model_path)
    print("Model loaded successfully ✅")
except Exception as e:
    print(f"Direct load failed: {e}")
    # Try saving as JSON first then reload
    import json
    booster2 = xgb.Booster()
    booster2.load_model(model_path)
    booster2.save_model("/tmp/model/converted.json")
    booster.load_model("/tmp/model/converted.json")
    print("Model loaded via JSON conversion ✅")

# Broadcast model to all workers
booster_broadcast = sc.broadcast(booster)

# ── Define Scoring UDF ────────────────────────────────────────────────────────
feature_cols = [
    "inscclaimamtreimbursed", "deductibleamtpaid",
    "claim_duration_days", "los_days",
    "patient_age", "chronic_condition_count"
]

def score_claim(*features):
    try:
        import xgboost as xgb
        import numpy as np
        values = [float(f) if f is not None else 0.0 for f in features]
        dmatrix = xgb.DMatrix(np.array([values]))
        score = booster_broadcast.value.predict(dmatrix)[0]
        return float(score)
    except Exception:
        return 0.0

score_udf = udf(score_claim, FloatType())

# ── Load Flagged Records ───────────────────────────────────────────────────────
flagged = spark.read.parquet(f"s3://{BUCKET}/flagged/")
flagged = flagged.fillna(0, subset=feature_cols)

# ── Apply Scoring ─────────────────────────────────────────────────────────────
scored = flagged.withColumn(
    "fraud_score",
    score_udf(*[col(c) for c in feature_cols])
).withColumn("scored_at", current_timestamp())

print(f"Scored {scored.count()} records")
print(f"High risk (>0.7): {scored.filter(col('fraud_score') > 0.7).count()}")

# ── Write Scored Records Back to S3 ───────────────────────────────────────────
scored.write \
    .mode("overwrite") \
    .partitionBy("claim_year", "claim_month", "flag_type") \
    .parquet(f"s3://{BUCKET}/fraud-scores/")

print("Scored records written to fraud-scores/ ✅")
job.commit()