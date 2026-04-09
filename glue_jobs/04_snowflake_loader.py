import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# ── Init ──────────────────────────────────────────────────────────────────────
args        = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD'
])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "fraud-detection-pipeline007"

SF_ACCOUNT  = args['SNOWFLAKE_ACCOUNT']
SF_USER     = args['SNOWFLAKE_USER']
SF_PASSWORD = args['SNOWFLAKE_PASSWORD']

JDBC_URL = (
    f"jdbc:snowflake://{SF_ACCOUNT}.snowflakecomputing.com/"
    f"?db=FRAUD_DETECTION&schema=ANALYTICS&warehouse=FRAUD_WH"
    
)

jdbc_properties = {
    "user":     SF_USER,
    "password": SF_PASSWORD,
    "driver":   "net.snowflake.client.jdbc.SnowflakeDriver"
}

# ── Read Scored Records from S3 ───────────────────────────────────────────────
scored = spark.read.parquet(f"s3://{BUCKET}/fraud-scores/")

filtered = scored.filter(
    (col("fraud_score")  > 0.5) &
    (col("risk_score")   > 0.5) &
    (col("claimid").isNotNull()) &
    (col("provider").isNotNull())
)

total = filtered.count()
print(f"Records to load into Snowflake: {total}")

if total == 0:
    raise ValueError("No records passed score threshold — check fraud-scores/ layer")

# ── Select Final Columns ──────────────────────────────────────────────────────
final = filtered.select(
    "claimid", "beneid", "provider", "claim_type",
    "claimstartdt", "claimenddt",
    "inscclaimamtreimbursed", "deductibleamtpaid",
    "attendingphysician", "operatingphysician",
    "patient_age", "chronic_condition_count",
    "claim_duration_days", "los_days",
    "is_fraud", "flag_type", "flag_description",
    "risk_score", "fraud_score",
    "flagged_at", "scored_at"
)

# ── Write to Snowflake via JDBC ───────────────────────────────────────────────
final.write \
    .mode("append") \
    .jdbc(
        url=JDBC_URL,
        table="FRAUD_ALERTS_STAGE",
        properties=jdbc_properties
    )

print(f"Loaded {total} records into FRAUD_ALERTS_STAGE ✅")

# ── Verify via Read Back ──────────────────────────────────────────────────────
verification = spark.read.jdbc(
    url=JDBC_URL,
    table="""(
        SELECT flag_type,
               COUNT(*)                   AS stage_count,
               ROUND(AVG(fraud_score), 3) AS avg_score
        FROM   FRAUD_ALERTS_STAGE
        GROUP  BY flag_type
        ORDER  BY stage_count DESC
    ) t""",
    properties=jdbc_properties
)

print("=== Stage Table Summary ===")
verification.show(truncate=False)

job.commit()