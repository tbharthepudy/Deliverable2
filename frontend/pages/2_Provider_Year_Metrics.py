import os
import streamlit as st
import pandas as pd
import plotly.express as px

from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError


# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------

PROJECT_ID = "gcp-475816"
GOLD_DATASET = "gold"


# ------------------------------------------------------------------------------
# BigQuery auth client
# ------------------------------------------------------------------------------
@st.cache_resource
def get_bq_client():
    try:
        return bigquery.Client(project=PROJECT_ID)
    except DefaultCredentialsError:
        key_path = "gcp-475816-d19d36130b17.json"
        if not key_path or not os.path.exists(key_path):
            raise RuntimeError("Missing GCP credentials.")

        creds = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(project=PROJECT_ID, credentials=creds)


client = get_bq_client()


# ------------------------------------------------------------------------------
# Data Loader — provider_year_metrics from BigQuery
# ------------------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_provider_data(year: int, service_category: str):
    query = f"""
        SELECT
            provider_id,
            provider_name,
            provider_state,
            service_category,
            YEAR,
            YEAR_DATE,
            total_service_days,
            total_episode_stays,
            total_distinct_benes,
            total_medicare_payment_amt,
            total_charge_amt,
            total_allowed_amt,
            pay_per_bene,
            pay_per_service_day,
            charge_per_bene,
            allowed_per_bene,
            payment_to_charge_ratio,
            allowed_to_charge_ratio,
            episodes_per_bene,
            avg_bene_age,
            avg_bene_risk_score,
            avg_dual_pct,
            avg_rural_pct,
            avg_female_pct,
            avg_diabetes_pct,
            avg_hypertension_pct,
            avg_heart_failure_pct,
            avg_copd_pct,
            risk_band,
            dual_share_band,
            rural_mix_band
        FROM `{PROJECT_ID}.{GOLD_DATASET}.provider_year_metrics`
        WHERE YEAR=@year
          AND service_category=@svc
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("year", "INT64", year),
                bigquery.ScalarQueryParameter("svc", "STRING", service_category),
            ]
        ),
    )

    df = job.to_dataframe()

    # Fix numeric columns to ensure Plotly bubbles don't break
    numeric_cols = [
        "total_service_days", "total_episode_stays", "total_distinct_benes",
        "total_medicare_payment_amt", "total_charge_amt", "total_allowed_amt",
        "pay_per_bene", "pay_per_service_day", "charge_per_bene",
        "allowed_per_bene", "payment_to_charge_ratio", "allowed_to_charge_ratio",
        "episodes_per_bene", "avg_bene_age", "avg_bene_risk_score",
        "avg_dual_pct", "avg_rural_pct", "avg_female_pct",
        "avg_diabetes_pct", "avg_hypertension_pct",
        "avg_heart_failure_pct", "avg_copd_pct"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ------------------------------------------------------------------------------
# Provider Dashboard
# ------------------------------------------------------------------------------
def provider_dashboard():

    st.header("🏥 Provider-Level Analytics")

    # Sidebar Filters
    st.sidebar.subheader("Filters")
    year = st.sidebar.selectbox("Year", [2020, 2021, 2022, 2023], index=3)
    service_category = "SNF"

    df = load_provider_data(year, service_category)

    if df.empty:
        st.warning("No provider data found.")
        return

    # State Filter (on loaded DF)
    states = sorted(df["provider_state"].unique())
    selected_state = st.sidebar.selectbox("Select State", states)

    df_state = df[df["provider_state"] == selected_state]

    # ------------------------------------------------------------------
    # KPIs
    # ------------------------------------------------------------------
    st.header(f"📌 Summary for {selected_state} — {service_category}, {year}")

    total_pay = df_state["total_medicare_payment_amt"].sum()
    total_benes = df_state["total_distinct_benes"].sum()
    total_days = df_state["total_service_days"].sum()

    avg_pay_bene = total_pay / total_benes if total_benes else 0
    avg_pay_day = total_pay / total_days if total_days else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Medicare Payment", f"${total_pay:,.0f}")
    c2.metric("Total Beneficiaries", f"{total_benes:,.0f}")
    c3.metric("Avg Pay per Beneficiary", f"${avg_pay_bene:,.0f}")
    c4.metric("Avg Pay per Service Day", f"${avg_pay_day:,.0f}")

    st.markdown("---")

    # ------------------------------------------------------------------
    # Chart 1 — Top Providers by Payment per Beneficiary
    # ------------------------------------------------------------------
    st.subheader("💰 Top Providers by Payment per Beneficiary")


    df_top = df_state.sort_values("pay_per_bene", ascending=False).head(10)

    fig_top = px.bar(
        df_top.sort_values("pay_per_bene", ascending=True),
        x="pay_per_bene",
        y="provider_name",
        orientation="h",
        color="pay_per_bene",
        color_continuous_scale="teal",
        text="pay_per_bene",
        title=f"Top {10} Providers in {selected_state}",
    )
    fig_top.update_layout(height=550)
    st.plotly_chart(fig_top, use_container_width=True)

    

    # ------------------------------------------------------------------
    # Chart 2 — Episodes per Bene by Risk Band
    # ------------------------------------------------------------------
    st.subheader("📦 Episodes per Beneficiary by Risk Band")

    fig_ep = px.box(
        df_state,
        x="risk_band",
        y="episodes_per_bene",
        color="risk_band",
        title="Episodes per Beneficiary by Risk Band",
    )
    fig_ep.update_layout(height=450)
    st.plotly_chart(fig_ep, use_container_width=True)

    # ------------------------------------------------------------------
    # Raw Table
    # ------------------------------------------------------------------
    st.subheader("📄 Raw Provider Data")
    st.dataframe(df_state, use_container_width=True)


# ------------------------------------------------------------------------------
# Run as standalone Streamlit page
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    st.set_page_config(page_title="Provider Metrics", layout="wide")
    provider_dashboard()
