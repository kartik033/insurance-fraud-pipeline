import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, coalesce, lit, to_date, datediff,
    current_date, when, upper, trim, year, month
)

# ── Init ──────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET   = "fraud-detection-pipeline007"
DATABASE = "fraud_detection_db"

# ── Step 1: Read Raw Tables ───────────────────────────────────────────────────
beneficiary = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="raw_beneficiary",
    transformation_ctx="src_beneficiary"
).toDF()

inpatient = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="raw_inpatient",
    transformation_ctx="src_inpatient"
).toDF()

outpatient = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="raw_outpatient",
    transformation_ctx="src_outpatient"
).toDF()

labels = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="raw_labels",
    transformation_ctx="src_labels"
).toDF()

# ── Step 2: Standardise Column Names (lowercase + trim) ───────────────────────
def clean_cols(df):
    return df.toDF(*[c.lower().strip() for c in df.columns])

beneficiary = clean_cols(beneficiary)
inpatient   = clean_cols(inpatient)
outpatient  = clean_cols(outpatient)
labels      = clean_cols(labels)

# ── Step 3: Tag Claim Type + Unify Inpatient & Outpatient ────────────────────
inpatient  = inpatient.withColumn("claim_type", lit("INPATIENT"))
outpatient = outpatient.withColumn("claim_type", lit("OUTPATIENT"))

# Align columns — outpatient won't have admit/discharge dates
outpatient = outpatient \
    .withColumn("clmadmitdt",     lit(None).cast("string")) \
    .withColumn("clmdischargedt", lit(None).cast("string"))

# Keep only shared columns for the union
shared_cols = [
    "claimid", "beneid", "provider",
    "clmadmitdt", "clmdischargedt",
    "clmfromdt", "clmthrudt",
    "attendingphysician", "operatingphysician",
    "clm_pass_bhnd_code_1",
    "inpatientclaimamrtreimbursed",
    "claim_type"
]

# Use coalesce to pick whichever reimbursement column exists
inpatient  = inpatient.withColumn(
    "inpatientclaimamrtreimbursed",
    coalesce(col("inpatientclaimamrtreimbursed"), lit(0.0))
)
outpatient = outpatient.withColumn(
    "inpatientclaimamrtreimbursed",
    coalesce(col("insuranceclaimamtreimbursed"), lit(0.0))
)

claims = inpatient.select(shared_cols).union(outpatient.select(shared_cols))

# ── Step 4: Join Beneficiary Data ─────────────────────────────────────────────
claims = claims.join(beneficiary, on="beneid", how="left")

# ── Step 5: Join Fraud Labels ─────────────────────────────────────────────────
labels = labels.withColumnRenamed("potentialfraud", "is_fraud") \
               .withColumn("is_fraud", when(upper(trim(col("is_fraud"))) == "YES", 1).otherwise(0))

claims = claims.join(labels, on="provider", how="left")

# ── Step 6: Feature Engineering ──────────────────────────────────────────────
claims = claims \
    .withColumn("clmfromdt",      to_date(col("clmfromdt"),      "yyyy-MM-dd")) \
    .withColumn("clmthrudt",      to_date(col("clmthrudt"),      "yyyy-MM-dd")) \
    .withColumn("clmadmitdt",     to_date(col("clmadmitdt"),     "yyyy-MM-dd")) \
    .withColumn("clmdischargedt", to_date(col("clmdischargedt"), "yyyy-MM-dd")) \
    .withColumn("claim_duration_days",
        datediff(col("clmthrudt"), col("clmfromdt"))) \
    .withColumn("los_days",                                      # length of stay
        when(col("claim_type") == "INPATIENT",
             datediff(col("clmdischargedt"), col("clmadmitdt"))
        ).otherwise(lit(0))) \
    .withColumn("claim_year",  year(col("clmfromdt"))) \
    .withColumn("claim_month", month(col("clmfromdt")))

# ── Step 7: DQ Gate — stop the job if data looks wrong ───────────────────────
total = claims.count()
null_claims    = claims.filter(col("claimid").isNull()).count()
null_providers = claims.filter(col("provider").isNull()).count()
negative_amt   = claims.filter(col("inpatientclaimamrtreimbursed") < 0).count()

issues = {
    "null_claim_id_pct":   null_claims    / total,
    "null_provider_pct":   null_providers / total,
    "negative_amount_pct": negative_amt   / total,
}

failed = {k: v for k, v in issues.items() if v > 0.02}
if failed:
    raise ValueError(f"DQ checks failed: {failed}")

print(f"DQ passed — {total} records, issues: {issues}")

# ── Step 8: Write Curated Parquet (partitioned) ───────────────────────────────
claims.write \
    .mode("overwrite") \
    .partitionBy("claim_year", "claim_month", "claim_type") \
    .parquet(f"s3://{BUCKET}/curated/claims_unified/")

print(f"Written {total} records to curated/claims_unified/")

job.commit()