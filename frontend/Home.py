import os
import streamlit as st
import pandas as pd
import plotly.express as px

from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from dotenv import load_dotenv

st.set_page_config(
    page_title="CMS Analysis",
    page_icon=":hospital:", 
    layout="wide"  
)
st.title("🏥 CMS Post-Acute Care Analytics Dashboard")

st.markdown(
    """
This dashboard helps you **analyze Medicare post-acute care utilization and payments**  
for providers across the United States, using data modeled in **BigQuery (gold layer)**.

The focus is on **Skilled Nursing Facilities (SNF)** and other post-acute service
categories, showing patterns in:

- **Medicare payments & charges**
- **Utilization** (beneficiaries, service days, episodes)
- **Risk & demographics** (age, risk scores, dual-eligible share, rural mix)
- **Condition prevalence** (diabetes, hypertension, heart failure, COPD)
"""
)

st.info(
    "Use the sidebar filters in the other pages "
    "to drill into specific segments of the data."
)


# -------------------------------------------------------------------
# Data Dictionary for Main BigQuery Table
# -------------------------------------------------------------------
st.header("📚 Data Dictionary ")

st.markdown(
    """
This is the table that the gold layer queries and aggregates.
"""
)

data_dict = [
    # Keys / identifiers
    {"Column": "provider_id", "Type": "STRING", "Description": "Provider ID (from `PRVDR_ID`), cast to string for consistent joins."},
    {"Column": "provider_name", "Type": "STRING", "Description": "Provider / facility name (`PRVDR_NAME`)."},
    {"Column": "provider_city", "Type": "STRING", "Description": "City where the provider is located (`PRVDR_CITY`)."},
    {"Column": "provider_zip", "Type": "STRING", "Description": "ZIP code of the provider (`PRVDR_ZIP`) stored as string (preserves leading zeros)."},
    {"Column": "provider_state", "Type": "STRING", "Description": "Two-letter state code (`STATE`)."},
    {"Column": "service_category", "Type": "STRING", "Description": "Service category (`SRVC_CTGRY`), e.g., SNF / IRF / LTCH / HH / HOSPICE."},
    {"Column": "summary_category", "Type": "STRING", "Description": "Summary category (`SMRY_CTGRY`), indicates aggregation level (e.g., PROVIDER)."},
    {"Column": "YEAR_TYPE", "Type": "STRING", "Description": "Raw year type from source (e.g., calendar vs fiscal), kept for traceability."},
    {"Column": "YEAR", "Type": "INT64", "Description": "Year extracted and cast from raw `YEAR` field."},
    {"Column": "YEAR_DATE", "Type": "DATE", "Description": "First day of the year: `DATE(YEAR, 1, 1)` – used for partitioning and time-series."},

    # Volumes / counts
    {"Column": "total_service_days", "Type": "INT64", "Description": "Total service days (`TOT_SRVC_DAYS`) for this provider, year, and service category."},
    {"Column": "total_episode_stays", "Type": "INT64", "Description": "Total episode / stay count (`TOT_EPSD_STAY_CNT`)."},
    {"Column": "distinct_beneficiaries", "Type": "INT64", "Description": "Number of distinct beneficiaries (`BENE_DSTNCT_CNT`)."},

    # Payment / charge metrics
    {"Column": "total_medicare_payment_amt", "Type": "NUMERIC", "Description": "Total Medicare payment amount (`TOT_MDCR_PYMT_AMT`)."},
    {"Column": "total_medicare_standardized_payment_amt", "Type": "NUMERIC", "Description": "Standardized Medicare payment amount (`TOT_MDCR_STDZD_PYMT_AMT`)."},
    {"Column": "total_charge_amt", "Type": "NUMERIC", "Description": "Total charges submitted by the provider (`TOT_CHRG_AMT`)."},
    {"Column": "total_allowed_amt", "Type": "NUMERIC", "Description": "Total allowed amount (`TOT_ALOWD_AMT`) – what Medicare considers allowable."},

    # Beneficiary averages
    {"Column": "bene_avg_age", "Type": "FLOAT64", "Description": "Average age of beneficiaries (`BENE_AVG_AGE`), with `*` cleaned to NULL."},
    {"Column": "bene_avg_risk_score", "Type": "FLOAT64", "Description": "Average risk score (`BENE_AVG_RISK_SCRE`) for beneficiaries, cleaned from `*`."},

    # Key percentages – gender / dual / rural
    {"Column": "bene_female_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are female (`BENE_FEML_PCT`)."},
    {"Column": "bene_male_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are male (`BENE_MALE_PCT`)."},
    {"Column": "bene_dual_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are dual-eligible (Medicare + Medicaid) (`BENE_DUAL_PCT`)."},
    {"Column": "bene_rural_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries classified as rural (`BENE_RRL_PCT`)."},

    # Race / ethnicity mix
    {"Column": "bene_race_white_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are White (`BENE_RACE_WHT_PCT`)."},
    {"Column": "bene_race_black_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are Black (`BENE_RACE_BLACK_PCT`)."},
    {"Column": "bene_race_hispanic_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are Hispanic (`BENE_RACE_HSPNC_PCT`)."},
    {"Column": "bene_race_api_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are Asian / Pacific Islander (`BENE_RACE_API_PCT`)."},
    {"Column": "bene_race_natind_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries who are Native American / Alaska Native (`BENE_RACE_NATIND_PCT`)."},
    {"Column": "bene_race_other_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries in 'Other' race category (`BENE_RACE_OTHR_PCT`)."},
    {"Column": "bene_race_unknown_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries with unknown race (`BENE_RACE_UNK_PCT`)."},

    # Primary diagnosis mix (high-level clinical groupings)
    {"Column": "dx_resp_system_pct", "Type": "FLOAT64", "Description": "Share of primary diagnoses related to the respiratory system (`PRMRY_DX_RSPSYSTM_PCT`)."},
    {"Column": "dx_nervous_system_pct", "Type": "FLOAT64", "Description": "Share of primary diagnoses related to the nervous system (`PRMRY_DX_NERVSYSTM_PCT`)."},
    {"Column": "dx_dig_system_pct", "Type": "FLOAT64", "Description": "Share of primary diagnoses related to the digestive system (`PRMRY_DX_DIGSYSTM_PCT`)."},
    {"Column": "dx_mental_behavior_pct", "Type": "FLOAT64", "Description": "Share of primary diagnoses related to mental/behavioral/neuro disorders (`PRMRY_DX_MNTBEHNEUDIS_PCT`)."},

    # Condition prevalence (chronic disease flags)
    {"Column": "bene_diabetes_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries with diabetes (`BENE_CC_PH_DIABETES_V2_PCT`)."},
    {"Column": "bene_hypertension_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries with hypertension (`BENE_CC_PH_HYPERTENSION_V2_PCT`)."},
    {"Column": "bene_heart_failure_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries with non-ischemic heart failure (`BENE_CC_PH_HF_NONIHD_V2_PCT`)."},
    {"Column": "bene_copd_pct", "Type": "FLOAT64", "Description": "Percent of beneficiaries with COPD (`BENE_CC_PH_COPD_V2_PCT`)."},
]

dict_df = pd.DataFrame(data_dict)

with st.expander("🔍 Click to view full `CMS` data dictionary", expanded=False):
    st.dataframe(dict_df, use_container_width=True, hide_index=True)
