-- ── Environment Setup ─────────────────────────────────────────────────────
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS FRAUD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60          -- suspends after 60s idle (saves credits)
    AUTO_RESUME    = TRUE;

CREATE DATABASE IF NOT EXISTS FRAUD_DETECTION;
CREATE SCHEMA   IF NOT EXISTS FRAUD_DETECTION.ANALYTICS;

USE DATABASE FRAUD_DETECTION;
USE SCHEMA   ANALYTICS;

-- ── Stage Table (Glue writes here first) ──────────────────────────────────
CREATE OR REPLACE TABLE fraud_alerts_stage (
    claimid                   VARCHAR,
    beneid                    VARCHAR,
    provider                  VARCHAR,
    claim_type                VARCHAR,
    claimstartdt              DATE,
    claimenddt                DATE,
    inscclaimamtreimbursed    FLOAT,
    deductibleamtpaid         FLOAT,
    attendingphysician        VARCHAR,
    operatingphysician        VARCHAR,
    patient_age               INT,
    chronic_condition_count   INT,
    claim_duration_days       INT,
    los_days                  INT,
    is_fraud                  INT,
    flag_type                 VARCHAR,
    flag_description          VARCHAR,
    risk_score                FLOAT,
    fraud_score               FLOAT,
    flagged_at                TIMESTAMP,
    scored_at                 TIMESTAMP,
    loaded_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);