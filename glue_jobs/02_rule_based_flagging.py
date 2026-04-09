import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, count, avg, stddev, abs as spark_abs,
    lit, current_timestamp, round as spark_round,
    window, countDistinct
)

# ── Init ──────────────────────────────────────────────────────────────────────
args        = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "fraud-detection-pipeline007"

# ── Read Curated Layer ────────────────────────────────────────────────────────
claims = spark.read.parquet(
    f"s3://{BUCKET}/curated/claims_unified/"
)

print(f"Total claims loaded: {claims.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# RULE 1 — Duplicate Claims
# Same provider + beneficiary + claim amount + start date = suspicious duplicate
# ─────────────────────────────────────────────────────────────────────────────
dup_keys = ["provider", "beneid", "inscclaimamtreimbursed", "claimstartdt"]

duplicates = claims \
    .groupBy(dup_keys) \
    .agg(count("claimid").alias("occurrence_count")) \
    .filter(col("occurrence_count") > 1) \
    .join(claims, on=dup_keys, how="inner") \
    .withColumn("flag_type",        lit("DUPLICATE_CLAIM")) \
    .withColumn("flag_description", lit("Same provider/patient/amount/date submitted multiple times")) \
    .withColumn("risk_score",       lit(0.7)) \
    .withColumn("flagged_at",       current_timestamp())

print(f"Rule 1 — Duplicates flagged: {duplicates.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# RULE 2 — Abnormal Claim Amounts (Z-Score > 3)
# Per claim_type to avoid inpatient skewing outpatient stats
# ─────────────────────────────────────────────────────────────────────────────
stats = claims \
    .groupBy("claim_type") \
    .agg(
        avg("inscclaimamtreimbursed").alias("mean_amt"),
        stddev("inscclaimamtreimbursed").alias("std_amt")
    )

claims_with_stats = claims.join(stats, on="claim_type", how="left")

abnormal = claims_with_stats \
    .withColumn("z_score",
        spark_abs(
            (col("inscclaimamtreimbursed") - col("mean_amt")) / col("std_amt")
        )) \
    .filter(col("z_score") > 3) \
    .withColumn("flag_type",        lit("ABNORMAL_AMOUNT")) \
    .withColumn("flag_description", lit("Claim amount is 3+ standard deviations from mean for claim type")) \
    .withColumn("risk_score",
        spark_round(
            (col("z_score") / 10).cast("double"), 2   # higher z = higher risk
        )) \
    .withColumn("flagged_at", current_timestamp()) \
    .drop("mean_amt", "std_amt")

print(f"Rule 2 — Abnormal amounts flagged: {abnormal.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# RULE 3 — High Frequency Providers
# Providers submitting > 50 unique claims in any 30-day window
# ─────────────────────────────────────────────────────────────────────────────
provider_window = claims \
    .groupBy(
        "provider",
        window(col("claimstartdt").cast("timestamp"), "30 days")
    ) \
    .agg(
        count("claimid").alias("claims_in_window"),
        countDistinct("beneid").alias("unique_patients"),
        avg("inscclaimamtreimbursed").alias("avg_claim_amt")
    ) \
    .filter(col("claims_in_window") > 50)

high_freq = provider_window \
    .join(claims, on="provider", how="inner") \
    .withColumn("flag_type",        lit("HIGH_FREQ_PROVIDER")) \
    .withColumn("flag_description", lit("Provider submitted 50+ claims in a 30-day window")) \
    .withColumn("risk_score",       lit(0.8)) \
    .withColumn("flagged_at",       current_timestamp()) \
    .drop("window")

print(f"Rule 3 — High frequency providers flagged: {high_freq.count()}")

# ─────────────────────────────────────────────────────────────────────────────
# Combine All Flagged Records
# ─────────────────────────────────────────────────────────────────────────────
output_cols = [
    "claimid", "beneid", "provider",
    "claim_type", "claimstartdt", "claimenddt",
    "inscclaimamtreimbursed", "deductibleamtpaid",
    "attendingphysician", "operatingphysician",
    "patient_age", "chronic_condition_count",
    "claim_duration_days", "los_days",
    "is_fraud",
    "flag_type", "flag_description", "risk_score", "flagged_at",
    "claim_year", "claim_month"
]

flagged = duplicates.select(output_cols) \
    .union(abnormal.select(output_cols)) \
    .union(high_freq.select(output_cols)) \
    .dropDuplicates(["claimid", "flag_type"])   # one flag per rule per claim

total_flagged = flagged.count()
print(f"Total flagged records: {total_flagged}")

# ─────────────────────────────────────────────────────────────────────────────
# DQ Gate — must flag at least some records
# ─────────────────────────────────────────────────────────────────────────────
if total_flagged == 0:
    raise ValueError("DQ failed: zero records flagged — check input data or rule thresholds")

# ─────────────────────────────────────────────────────────────────────────────
# Write Flagged Records to S3
# ─────────────────────────────────────────────────────────────────────────────
flagged.write \
    .mode("overwrite") \
    .partitionBy("claim_year", "claim_month", "flag_type") \
    .parquet(f"s3://{BUCKET}/flagged/")

# ─────────────────────────────────────────────────────────────────────────────
# Summary Report by Rule
# ─────────────────────────────────────────────────────────────────────────────
summary = flagged \
a    .agg(
        count("claimid").alias("flagged_count"),
        spark_round(avg("risk_score"), 3).alias("avg_risk_score"),
        spark_round(avg("inscclaimamtreimbursed"), 2).alias("avg_claim_amt"),
        countDistinct("provider").alias("unique_providers")
    )

summary.show(truncate=False)

job.commit()