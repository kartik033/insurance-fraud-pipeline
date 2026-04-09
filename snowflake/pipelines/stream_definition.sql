-- ── CDC Stream on Stage Table ─────────────────────────────────────────────
CREATE OR REPLACE STREAM fraud_alerts_stream
    ON TABLE fraud_alerts_stage
    APPEND_ONLY = TRUE;     -- we only insert, never update/delete stage

-- ── Summary Report View ───────────────────────────────────────────────────
CREATE OR REPLACE VIEW fraud_summary_report AS
SELECT
    flag_type,
    COUNT(*)                              AS flagged_count,
    ROUND(AVG(fraud_score), 3)            AS avg_fraud_score,
    ROUND(AVG(inscclaimamtreimbursed), 2) AS avg_claim_amt,
    COUNT(DISTINCT provider)              AS unique_providers,
    SUM(CASE WHEN is_fraud = 1
        THEN 1 ELSE 0 END)                AS confirmed_fraud_count
FROM fraud_alerts
GROUP BY flag_type
ORDER BY flagged_count DESC;

-- ── Task: fires every 10 min, only when stream has data ──────────────────
CREATE OR REPLACE TASK FRAUD_DETECTION.ANALYTICS.PROCESS_FRAUD_ALERTS
    WAREHOUSE = FRAUD_WH
    SCHEDULE  = '10 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('fraud_alerts_stream')
AS
INSERT INTO fraud_alerts
SELECT
    claimid, beneid, provider, claim_type,
    claimstartdt, claimenddt,
    inscclaimamtreimbursed, deductibleamtpaid,
    attendingphysician, operatingphysician,
    patient_age, chronic_condition_count,
    claim_duration_days, los_days,
    is_fraud, flag_type, flag_description,
    risk_score, fraud_score,
    flagged_at, scored_at,
    CURRENT_TIMESTAMP AS processed_at
FROM fraud_alerts_stream
WHERE METADATA$ACTION = 'INSERT'
  AND fraud_score > 0.5;

ALTER TASK FRAUD_DETECTION.ANALYTICS.PROCESS_FRAUD_ALERTS RESUME;

-- ── Manually trigger and verify ────────────────────────────────────────────
EXECUTE TASK FRAUD_DETECTION.ANALYTICS.PROCESS_FRAUD_ALERTS;

CALL SYSTEM$WAIT(30, 'SECONDS');

SELECT COUNT(*) FROM FRAUD_DETECTION.ANALYTICS.FRAUD_ALERTS;

SELECT * FROM FRAUD_DETECTION.ANALYTICS.FRAUD_SUMMARY_REPORT;