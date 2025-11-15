"""
DAG: cms_bq_transform_dag

Purpose:
  1) Transform RAW CMS outpatient table in BigQuery into:
     - SILVER: cleaned, typed table
     - GOLD: aggregated tables for analytics

Trigger:
  - Can be scheduled (e.g. daily) or manually triggered from Composer UI.
"""

import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# ------------------------------------------------------------------------------
# CONFIG (edit these to match your environment)
# ------------------------------------------------------------------------------

# You can also hardcode this if you like.
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "gcp-475816"

RAW_DATASET = os.environ.get("RAW_DATASET", "deliverable2")              # where Cloud Function loaded data.jsonl
RAW_TABLE   = os.environ.get("RAW_TABLE", "cms_raw") # name of the RAW table

SILVER_DATASET = os.environ.get("SILVER_DATASET", "silver")
GOLD_DATASET   = os.environ.get("GOLD_DATASET", "gold")


# ------------------------------------------------------------------------------
# SQL: SILVER LAYER (clean & typed)
# ------------------------------------------------------------------------------

silver_sql = f"""
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{SILVER_DATASET}`;

CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.cms_silver`
PARTITION BY YEAR_DATE AS
SELECT
  -- Keys / identifiers
  SAFE_CAST(PRVDR_ID AS STRING)            AS provider_id,
  PRVDR_NAME                                AS provider_name,
  PRVDR_CITY                                AS provider_city,
  SAFE_CAST(PRVDR_ZIP AS STRING)           AS provider_zip,
  STATE                                     AS provider_state,
  SRVC_CTGRY                                AS service_category,
  SMRY_CTGRY                                AS summary_category,
  YEAR_TYPE,
  SAFE_CAST(YEAR AS INT64)                 AS YEAR,
  DATE(SAFE_CAST(YEAR AS INT64), 1, 1) AS YEAR_DATE,

  -- Volumes / counts
  SAFE_CAST(TOT_SRVC_DAYS AS INT64)        AS total_service_days,
  SAFE_CAST(TOT_EPSD_STAY_CNT AS INT64)    AS total_episode_stays,
  SAFE_CAST(BENE_DSTNCT_CNT AS INT64)      AS distinct_beneficiaries,

  -- Payment / charge metrics
  SAFE_CAST(TOT_MDCR_PYMT_AMT AS NUMERIC)       AS total_medicare_payment_amt,
  SAFE_CAST(TOT_MDCR_STDZD_PYMT_AMT AS NUMERIC) AS total_medicare_standardized_payment_amt,
  SAFE_CAST(TOT_CHRG_AMT AS NUMERIC)            AS total_charge_amt,
  SAFE_CAST(TOT_ALOWD_AMT AS NUMERIC)           AS total_allowed_amt,

  -- Beneficiary averages
  SAFE_CAST(NULLIF(CAST(BENE_AVG_AGE AS STRING), '*') AS FLOAT64)        AS bene_avg_age,
  SAFE_CAST(NULLIF(CAST(BENE_AVG_RISK_SCRE AS STRING), '*') AS FLOAT64)  AS bene_avg_risk_score,

  -- Key percentages (you can add more with same pattern)
  SAFE_CAST(NULLIF(CAST(BENE_FEML_PCT AS STRING), '*') AS FLOAT64)        AS bene_female_pct,
  SAFE_CAST(NULLIF(CAST(BENE_MALE_PCT AS STRING), '*') AS FLOAT64)        AS bene_male_pct,
  SAFE_CAST(NULLIF(CAST(BENE_DUAL_PCT AS STRING), '*') AS FLOAT64)        AS bene_dual_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RRL_PCT AS STRING), '*') AS FLOAT64)         AS bene_rural_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_WHT_PCT AS STRING), '*') AS FLOAT64)    AS bene_race_white_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_BLACK_PCT AS STRING), '*') AS FLOAT64)  AS bene_race_black_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_HSPNC_PCT AS STRING), '*') AS FLOAT64)  AS bene_race_hispanic_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_API_PCT AS STRING), '*') AS FLOAT64)    AS bene_race_api_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_NATIND_PCT AS STRING), '*') AS FLOAT64) AS bene_race_natind_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_OTHR_PCT AS STRING), '*') AS FLOAT64)   AS bene_race_other_pct,
  SAFE_CAST(NULLIF(CAST(BENE_RACE_UNK_PCT AS STRING), '*') AS FLOAT64)    AS bene_race_unknown_pct,

  -- Example diagnosis mix percentages (add more if you need)
  SAFE_CAST(NULLIF(CAST(PRMRY_DX_RSPSYSTM_PCT AS STRING), '*') AS FLOAT64)   AS dx_resp_system_pct,
  SAFE_CAST(NULLIF(CAST(PRMRY_DX_NERVSYSTM_PCT AS STRING), '*') AS FLOAT64)  AS dx_nervous_system_pct,
  SAFE_CAST(NULLIF(CAST(PRMRY_DX_DIGSYSTM_PCT AS STRING), '*') AS FLOAT64)   AS dx_dig_system_pct,
  SAFE_CAST(NULLIF(CAST(PRMRY_DX_MNTBEHNEUDIS_PCT AS STRING), '*') AS FLOAT64) AS dx_mental_behavior_pct,

  -- Example condition prevalence (again, add as needed)
  SAFE_CAST(NULLIF(CAST(BENE_CC_PH_DIABETES_V2_PCT AS STRING), '*') AS FLOAT64)      AS bene_diabetes_pct,
  SAFE_CAST(NULLIF(CAST(BENE_CC_PH_HYPERTENSION_V2_PCT AS STRING), '*') AS FLOAT64)  AS bene_hypertension_pct,
  SAFE_CAST(NULLIF(CAST(BENE_CC_PH_HF_NONIHD_V2_PCT AS STRING), '*') AS FLOAT64)     AS bene_heart_failure_pct,
  SAFE_CAST(NULLIF(CAST(BENE_CC_PH_COPD_V2_PCT AS STRING), '*') AS FLOAT64)          AS bene_copd_pct

FROM `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`;
"""


# ------------------------------------------------------------------------------
# SQL: GOLD LAYER (business aggregates)
#   Two tables:
#    1) provider_service_summary
#    2) yearly_state_snapshot
# ------------------------------------------------------------------------------

gold_sql = f"""
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}`;

-- ============================================================================
-- 1) Provider-year metrics (PROVIDER rows only)
--    Grain: (year, provider_id, service_category)
--    Goal: compare providers on cost, utilization, and patient mix.
-- ============================================================================

CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.provider_year_metrics`
PARTITION BY YEAR_DATE AS
WITH base AS (
  SELECT
    YEAR,
    DATE(YEAR, 1, 1) AS YEAR_DATE,
    provider_id,
    provider_name,
    provider_state,
    service_category,
    summary_category,

    -- volumes
    SUM(total_service_days)      AS total_service_days,
    SUM(total_episode_stays)     AS total_episode_stays,
    SUM(distinct_beneficiaries)  AS total_distinct_benes,

    -- finance
    SUM(total_medicare_payment_amt)        AS total_medicare_payment_amt,
    SUM(total_medicare_standardized_payment_amt) AS total_medicare_std_payment_amt,
    SUM(total_charge_amt)                  AS total_charge_amt,
    SUM(total_allowed_amt)                 AS total_allowed_amt,

    -- patient profile (simple averages)
    AVG(bene_avg_age)          AS avg_bene_age,
    AVG(bene_avg_risk_score)   AS avg_bene_risk_score,
    AVG(bene_dual_pct)         AS avg_dual_pct,
    AVG(bene_rural_pct)        AS avg_rural_pct,
    AVG(bene_female_pct)       AS avg_female_pct,

    -- chronic conditions (simple averages)
    AVG(bene_diabetes_pct)     AS avg_diabetes_pct,
    AVG(bene_hypertension_pct) AS avg_hypertension_pct,
    AVG(bene_heart_failure_pct) AS avg_heart_failure_pct,
    AVG(bene_copd_pct)          AS avg_copd_pct

  FROM `{PROJECT_ID}.{SILVER_DATASET}.cms_silver`
  WHERE summary_category = 'PROVIDER'
  GROUP BY
    YEAR,
    YEAR_DATE,
    provider_id,
    provider_name,
    provider_state,
    service_category,
    summary_category
)

SELECT
  *,
  -- cost/utilization ratios
  total_medicare_payment_amt / NULLIF(total_distinct_benes, 0) AS pay_per_bene,
  total_medicare_payment_amt / NULLIF(total_service_days, 0)   AS pay_per_service_day,
  total_charge_amt / NULLIF(total_distinct_benes, 0)           AS charge_per_bene,
  total_allowed_amt / NULLIF(total_distinct_benes, 0)          AS allowed_per_bene,
  total_medicare_payment_amt / NULLIF(total_charge_amt, 0)     AS payment_to_charge_ratio,
  total_allowed_amt / NULLIF(total_charge_amt, 0)              AS allowed_to_charge_ratio,
  total_episode_stays / NULLIF(total_distinct_benes, 0)        AS episodes_per_bene,

  -- risk band
  CASE
    WHEN avg_bene_risk_score IS NULL THEN 'UNKNOWN'
    WHEN avg_bene_risk_score < 1.5   THEN 'LOW_RISK'
    WHEN avg_bene_risk_score < 3.0   THEN 'MEDIUM_RISK'
    ELSE 'HIGH_RISK'
  END AS risk_band,

  -- dual eligibility band
  CASE
    WHEN avg_dual_pct IS NULL THEN 'UNKNOWN'
    WHEN avg_dual_pct < 25            THEN 'LOW_DUAL'
    WHEN avg_dual_pct >= 25 AND avg_dual_pct < 50 THEN 'MEDIUM_DUAL'
    WHEN avg_dual_pct >= 50 AND avg_dual_pct < 75 THEN 'HIGH_DUAL'
    ELSE 'VERY_HIGH_DUAL'
  END AS dual_share_band,

  -- rural mix band
  CASE
    WHEN avg_rural_pct IS NULL THEN 'UNKNOWN'
    WHEN avg_rural_pct >= 50   THEN 'RURAL_DOMINANT'
    WHEN avg_rural_pct >= 20   THEN 'MIXED'
    ELSE 'URBAN_DOMINANT'
  END AS rural_mix_band

FROM base;


-- ============================================================================
-- 2) State-year summary
--    Grain: (year, provider_state, service_category)
--    Goal: compare states on cost, utilization, and risk.
-- ============================================================================

CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.state_year_summary`
PARTITION BY YEAR_DATE AS
WITH base AS (
  SELECT
    YEAR,
    DATE(YEAR, 1, 1) AS YEAR_DATE,
    provider_state,
    service_category,

    SUM(total_medicare_payment_amt) AS total_medicare_payment_amt,
    SUM(total_charge_amt)           AS total_charge_amt,
    SUM(distinct_beneficiaries)     AS total_distinct_benes,
    SUM(total_service_days)         AS total_service_days,

    AVG(bene_avg_risk_score)        AS avg_bene_risk_score,
    AVG(bene_dual_pct)              AS avg_dual_pct,
    AVG(bene_rural_pct)             AS avg_rural_pct
  FROM `{PROJECT_ID}.{SILVER_DATASET}.cms_silver`
  WHERE summary_category = 'PROVIDER'
  GROUP BY
    YEAR,
    YEAR_DATE,
    provider_state,
    service_category
)

SELECT
  *,
  total_medicare_payment_amt / NULLIF(total_distinct_benes, 0) AS pay_per_bene,
  total_medicare_payment_amt / NULLIF(total_service_days, 0)   AS pay_per_service_day,
  total_charge_amt / NULLIF(total_distinct_benes, 0)           AS charge_per_bene,
  total_medicare_payment_amt / NULLIF(total_charge_amt, 0)     AS payment_to_charge_ratio
FROM base;
"""


# ------------------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------------------

default_args = {
    "owner": "data-eng",
    "start_date": days_ago(1),
    "retries": 0,
}

with DAG(
    dag_id="cms_transform",
    default_args=default_args,
    schedule_interval=None,  # set to "@daily" or similar if you want a schedule
    catchup=False,
    tags=["cms", "bigquery", "medallion"],
) as dag:

    # Task 1: build SILVER from RAW
    silver_task = BigQueryInsertJobOperator(
        task_id="build_silver_layer",
        configuration={
            "query": {
                "query": silver_sql,
                "useLegacySql": False,
            }
        },
        location="us-south1",  # change if your BQ datasets are in another location
    )

    # Task 2: build GOLD from SILVER
    gold_task = BigQueryInsertJobOperator(
        task_id="build_gold_layer",
        configuration={
            "query": {
                "query": gold_sql,
                "useLegacySql": False,
            }
        },
        location="us-south1",
    )

    silver_task >> gold_task
