# cms_bq_transform_spark.py
#
# PySpark job to:
#   1) Read RAW (bronze) CMS table from BigQuery
#   2) Build SILVER table:  silver.cms_silver
#   3) Build GOLD tables:   gold.provider_year_metrics, gold.state_year_summary
#
# You will upload this file to GCS and submit it as a Dataproc job.

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ------------------------------------------------------------------------------
# CONFIG – mirrors your DAG env vars
# ------------------------------------------------------------------------------

PROJECT_ID = (
    os.environ.get("GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or "gcp-475816"
)

RAW_DATASET = os.environ.get("RAW_DATASET", "deliverable2")
RAW_TABLE   = os.environ.get("RAW_TABLE", "cms_raw")

SILVER_DATASET = os.environ.get("SILVER_DATASET", "silver")
SILVER_TABLE   = "cms_silver"

GOLD_DATASET        = os.environ.get("GOLD_DATASET", "gold")
GOLD_PROVIDER_TABLE = "provider_year_metrics"
GOLD_STATE_TABLE    = "state_year_summary"


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def pct(col_name: str, alias_name: str):
    """
    Equivalent of:
      SAFE_CAST(NULLIF(CAST(col AS STRING), '*') AS FLOAT64) AS alias_name
    in BigQuery.
    """
    return (
        F.when(F.col(col_name) == "*", None)
         .otherwise(F.col(col_name).cast("double"))
         .alias(alias_name)
    )


def safe_div(num_col, den_col):
    """
    Safe division: returns NULL if denominator is 0 or NULL.
    """
    return F.when((den_col.isNull()) | (den_col == 0), None).otherwise(num_col / den_col)


# ------------------------------------------------------------------------------
# SILVER: transform RAW → silver.cms_silver
# ------------------------------------------------------------------------------

def build_silver(df_raw):
    """
    Apply the same logic as your silver_sql:
      - type casting
      - YEAR_DATE = DATE(YEAR, 1, 1)
      - '*' → NULL for % columns
    """
    df_silver = df_raw.select(
        # Identifiers
        F.col("PRVDR_ID").cast("string").alias("provider_id"),
        F.col("PRVDR_NAME").alias("provider_name"),
        F.col("PRVDR_CITY").alias("provider_city"),
        F.col("PRVDR_ZIP").cast("string").alias("provider_zip"),
        F.col("STATE").alias("provider_state"),
        F.col("SRVC_CTGRY").alias("service_category"),
        F.col("SMRY_CTGRY").alias("summary_category"),
        F.col("YEAR_TYPE").alias("YEAR_TYPE"),
        F.col("YEAR").cast("long").alias("YEAR"),
        F.to_date(
            F.concat_ws("-", F.col("YEAR").cast("string"), F.lit("01"), F.lit("01"))
        ).alias("YEAR_DATE"),

        # Volumes / counts
        F.col("TOT_SRVC_DAYS").cast("long").alias("total_service_days"),
        F.col("TOT_EPSD_STAY_CNT").cast("long").alias("total_episode_stays"),
        F.col("BENE_DSTNCT_CNT").cast("long").alias("distinct_beneficiaries"),

        # Payment / charge metrics
        F.col("TOT_MDCR_PYMT_AMT").cast("double").alias("total_medicare_payment_amt"),
        F.col("TOT_MDCR_STDZD_PYMT_AMT").cast("double").alias("total_medicare_standardized_payment_amt"),
        F.col("TOT_CHRG_AMT").cast("double").alias("total_charge_amt"),
        F.col("TOT_ALOWD_AMT").cast("double").alias("total_allowed_amt"),

        # Beneficiary averages
        pct("BENE_AVG_AGE", "bene_avg_age"),
        pct("BENE_AVG_RISK_SCRE", "bene_avg_risk_score"),

        # Key percentages
        pct("BENE_FEML_PCT", "bene_female_pct"),
        pct("BENE_MALE_PCT", "bene_male_pct"),
        pct("BENE_DUAL_PCT", "bene_dual_pct"),
        pct("BENE_RRL_PCT", "bene_rural_pct"),
        pct("BENE_RACE_WHT_PCT", "bene_race_white_pct"),
        pct("BENE_RACE_BLACK_PCT", "bene_race_black_pct"),
        pct("BENE_RACE_HSPNC_PCT", "bene_race_hispanic_pct"),
        pct("BENE_RACE_API_PCT", "bene_race_api_pct"),
        pct("BENE_RACE_NATIND_PCT", "bene_race_natind_pct"),
        pct("BENE_RACE_OTHR_PCT", "bene_race_other_pct"),
        pct("BENE_RACE_UNK_PCT", "bene_race_unknown_pct"),

        # Diagnosis mix percentages
        pct("PRMRY_DX_RSPSYSTM_PCT", "dx_resp_system_pct"),
        pct("PRMRY_DX_NERVSYSTM_PCT", "dx_nervous_system_pct"),
        pct("PRMRY_DX_DIGSYSTM_PCT", "dx_dig_system_pct"),
        pct("PRMRY_DX_MNTBEHNEUDIS_PCT", "dx_mental_behavior_pct"),

        # Condition prevalence
        pct("BENE_CC_PH_DIABETES_V2_PCT", "bene_diabetes_pct"),
        pct("BENE_CC_PH_HYPERTENSION_V2_PCT", "bene_hypertension_pct"),
        pct("BENE_CC_PH_HF_NONIHD_V2_PCT", "bene_heart_failure_pct"),
        pct("BENE_CC_PH_COPD_V2_PCT", "bene_copd_pct"),
    )

    return df_silver


# ------------------------------------------------------------------------------
# GOLD: provider_year_metrics (equivalent to provider CTE + SELECT)
# ------------------------------------------------------------------------------

def build_gold_provider(df_silver):
    # base CTE aggregation
    df_base = (
        df_silver
        .filter(F.col("summary_category") == "PROVIDER")
        .groupBy(
            "YEAR",
            "YEAR_DATE",
            "provider_id",
            "provider_name",
            "provider_state",
            "service_category",
            "summary_category",
        )
        .agg(
            # volumes
            F.sum("total_service_days").alias("total_service_days"),
            F.sum("total_episode_stays").alias("total_episode_stays"),
            F.sum("distinct_beneficiaries").alias("total_distinct_benes"),
            # finance
            F.sum("total_medicare_payment_amt").alias("total_medicare_payment_amt"),
            F.sum("total_medicare_standardized_payment_amt").alias("total_medicare_std_payment_amt"),
            F.sum("total_charge_amt").alias("total_charge_amt"),
            F.sum("total_allowed_amt").alias("total_allowed_amt"),
            # patient profile averages
            F.avg("bene_avg_age").alias("avg_bene_age"),
            F.avg("bene_avg_risk_score").alias("avg_bene_risk_score"),
            F.avg("bene_dual_pct").alias("avg_dual_pct"),
            F.avg("bene_rural_pct").alias("avg_rural_pct"),
            F.avg("bene_female_pct").alias("avg_female_pct"),
            # chronic conditions
            F.avg("bene_diabetes_pct").alias("avg_diabetes_pct"),
            F.avg("bene_hypertension_pct").alias("avg_hypertension_pct"),
            F.avg("bene_heart_failure_pct").alias("avg_heart_failure_pct"),
            F.avg("bene_copd_pct").alias("avg_copd_pct"),
        )
    )

    # add ratios and bands
    df_provider = (
        df_base
        .withColumn(
            "pay_per_bene",
            safe_div(F.col("total_medicare_payment_amt"), F.col("total_distinct_benes"))
        )
        .withColumn(
            "pay_per_service_day",
            safe_div(F.col("total_medicare_payment_amt"), F.col("total_service_days"))
        )
        .withColumn(
            "charge_per_bene",
            safe_div(F.col("total_charge_amt"), F.col("total_distinct_benes"))
        )
        .withColumn(
            "allowed_per_bene",
            safe_div(F.col("total_allowed_amt"), F.col("total_distinct_benes"))
        )
        .withColumn(
            "payment_to_charge_ratio",
            safe_div(F.col("total_medicare_payment_amt"), F.col("total_charge_amt"))
        )
        .withColumn(
            "allowed_to_charge_ratio",
            safe_div(F.col("total_allowed_amt"), F.col("total_charge_amt"))
        )
        .withColumn(
            "episodes_per_bene",
            safe_div(F.col("total_episode_stays"), F.col("total_distinct_benes"))
        )
        .withColumn(
            "risk_band",
            F.when(F.col("avg_bene_risk_score").isNull(), "UNKNOWN")
             .when(F.col("avg_bene_risk_score") < 1.5, "LOW_RISK")
             .when(F.col("avg_bene_risk_score") < 3.0, "MEDIUM_RISK")
             .otherwise("HIGH_RISK")
        )
        .withColumn(
            "dual_share_band",
            F.when(F.col("avg_dual_pct").isNull(), "UNKNOWN")
             .when(F.col("avg_dual_pct") < 25, "LOW_DUAL")
             .when((F.col("avg_dual_pct") >= 25) & (F.col("avg_dual_pct") < 50), "MEDIUM_DUAL")
             .when((F.col("avg_dual_pct") >= 50) & (F.col("avg_dual_pct") < 75), "HIGH_DUAL")
             .otherwise("VERY_HIGH_DUAL")
        )
        .withColumn(
            "rural_mix_band",
            F.when(F.col("avg_rural_pct").isNull(), "UNKNOWN")
             .when(F.col("avg_rural_pct") >= 50, "RURAL_DOMINANT")
             .when(F.col("avg_rural_pct") >= 20, "MIXED")
             .otherwise("URBAN_DOMINANT")
        )
    )

    return df_provider


# ------------------------------------------------------------------------------
# GOLD: state_year_summary (equivalent to state CTE + SELECT)
# ------------------------------------------------------------------------------

def build_gold_state(df_silver):
    df_base = (
        df_silver
        .filter(F.col("summary_category") == "PROVIDER")
        .groupBy(
            "YEAR",
            "YEAR_DATE",
            "provider_state",
            "service_category",
        )
        .agg(
            F.sum("total_medicare_payment_amt").alias("total_medicare_payment_amt"),
            F.sum("total_charge_amt").alias("total_charge_amt"),
            F.sum("distinct_beneficiaries").alias("total_distinct_benes"),
            F.sum("total_service_days").alias("total_service_days"),
            F.avg("bene_avg_risk_score").alias("avg_bene_risk_score"),
            F.avg("bene_dual_pct").alias("avg_dual_pct"),
            F.avg("bene_rural_pct").alias("avg_rural_pct"),
        )
    )

    df_state = (
        df_base
        .withColumn(
            "pay_per_bene",
            safe_div(F.col("total_medicare_payment_amt"), F.col("total_distinct_benes"))
        )
        .withColumn(
            "pay_per_service_day",
            safe_div(F.col("total_medicare_payment_amt"), F.col("total_service_days"))
        )
        .withColumn(
            "charge_per_bene",
            safe_div(F.col("total_charge_amt"), F.col("total_distinct_benes"))
        )
        .withColumn(
            "payment_to_charge_ratio",
            safe_div(F.col("total_medicare_payment_amt"), F.col("total_charge_amt"))
        )
    )

    return df_state


# ------------------------------------------------------------------------------
# MAIN – read bronze → build silver + gold → write to BigQuery
# ------------------------------------------------------------------------------

def main():
    spark = (
        SparkSession.builder
        .appName("cms_bq_transform_spark")
        .getOrCreate()
    )

    # 1) Read RAW (bronze) from BigQuery
    bronze_fq = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}"
    print(f"Reading RAW table: {bronze_fq}")

    df_raw = (
        spark.read.format("bigquery")
        .option("table", bronze_fq)
        .load()
    )

    # 2) Build SILVER
    df_silver = build_silver(df_raw)
    silver_fq = f"{PROJECT_ID}.{SILVER_DATASET}.{SILVER_TABLE}"
    print(f"Writing SILVER table: {silver_fq}")

    (
        df_silver.write
        .format("bigquery")
        .option("table", silver_fq)
        .option("writeMethod", "direct")
        .option("partitionField", "YEAR_DATE")   # PARTITION BY YEAR_DATE
        .mode("overwrite")                       # CREATE OR REPLACE
        .save()
    )

    # 3) Build GOLD: provider metrics
    df_provider = build_gold_provider(df_silver)
    provider_fq = f"{PROJECT_ID}.{GOLD_DATASET}.{GOLD_PROVIDER_TABLE}"
    print(f"Writing GOLD provider table: {provider_fq}")

    (
        df_provider.write
        .format("bigquery")
        .option("table", provider_fq)
        .option("writeMethod", "direct")
        .option("partitionField", "YEAR_DATE")
        .mode("overwrite")
        .save()
    )

    # 4) Build GOLD: state summary
    df_state = build_gold_state(df_silver)
    state_fq = f"{PROJECT_ID}.{GOLD_DATASET}.{GOLD_STATE_TABLE}"
    print(f"Writing GOLD state table: {state_fq}")

    (
        df_state.write
        .format("bigquery")
        .option("table", state_fq)
        .option("writeMethod", "direct")
        .option("partitionField", "YEAR_DATE")
        .mode("overwrite")
        .save()
    )

    print("Spark transform completed.")
    spark.stop()


if __name__ == "__main__":
    main()
