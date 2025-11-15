import os
import streamlit as st
import pandas as pd
import plotly.express as px

from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account


# ------------------------------------------------------------------------------
# Load environment variables from .env
# ------------------------------------------------------------------------------

PROJECT_ID = "gcp-475816"
GOLD_DATASET = "gold"


# ------------------------------------------------------------------------------
# BigQuery client with authentication
# ------------------------------------------------------------------------------
@st.cache_resource
def get_bq_client():
    try:
        return bigquery.Client(project=PROJECT_ID)
    except DefaultCredentialsError:
        key_path = "gcp-475816-d19d36130b17.json"
        # if not key_path or not os.path.exists(key_path):
        #     raise RuntimeError(
        #         "No credentials found. Ensure .env has GOOGLE_APPLICATION_CREDENTIALS=/path.json"
        #     )
        creds = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(project=PROJECT_ID, credentials=creds)


client = get_bq_client()


# ------------------------------------------------------------------------------
# Data loaders (Gold layer)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_state_summary(year: int, service_category: str) -> pd.DataFrame:
    query = f"""
    SELECT
      provider_state,
      service_category,
      YEAR,
      YEAR_DATE,
      total_medicare_payment_amt,
      total_charge_amt,
      total_distinct_benes,
      total_service_days,
      avg_bene_risk_score,
      avg_dual_pct,
      avg_rural_pct,
      pay_per_bene,
      pay_per_service_day,
      charge_per_bene,
      payment_to_charge_ratio
    FROM `{PROJECT_ID}.{GOLD_DATASET}.state_year_summary`
    WHERE YEAR = @year
      AND service_category = @service_category
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("year", "INT64", year),
                bigquery.ScalarQueryParameter("service_category", "STRING", service_category),
            ]
        ),
    )
    return job.to_dataframe()


@st.cache_data(ttl=600)
def load_provider_metrics(year: int, state: str, service_category: str) -> pd.DataFrame:
    query = f"""
    SELECT
      provider_id,
      provider_name,
      provider_state,
      service_category,
      YEAR,
      YEAR_DATE,
      total_distinct_benes,
      total_service_days,
      total_episode_stays,
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
    WHERE YEAR = @year
      AND provider_state = @state
      AND service_category = @service_category
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("year", "INT64", year),
                bigquery.ScalarQueryParameter("state", "STRING", state),
                bigquery.ScalarQueryParameter("service_category", "STRING", service_category),
            ]
        ),
    )
    return job.to_dataframe()


# ------------------------------------------------------------------------------
# Streamlit UI
# ------------------------------------------------------------------------------
def main():

    st.set_page_config(page_title="CMS Post-Acute Care Analytics", layout="wide")
    st.title("📊 CMS Post-Acute Care Analytics Dashboard")

    # Sidebar
    st.sidebar.header("Filters")
    year = st.sidebar.selectbox("Year", [2020, 2021, 2022, 2023], index=3)
    service_category = "SNF"

    # --------------------------------------------------------------------------
    # State-level metrics
    # --------------------------------------------------------------------------
    st.header(f"📍 State-Level Summary — {service_category}, {year}")
    df_state = load_state_summary(year, service_category)

    if df_state.empty:
        st.warning("No data for this selection.")
        return

    # KPIs
    total_pay = df_state["total_medicare_payment_amt"].sum()
    total_benes = df_state["total_distinct_benes"].sum()
    total_days = df_state["total_service_days"].sum()

    avg_pay_per_bene = total_pay / total_benes if total_benes else 0
    avg_pay_per_day = total_pay / total_days if total_days else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Medicare Payments", f"${total_pay:,.0f}")
    c2.metric("Total Beneficiaries", f"{total_benes:,.0f}")
    c3.metric("Avg Pay per Beneficiary", f"${avg_pay_per_bene:,.0f}")
    c4.metric("Avg Pay per Service Day", f"${avg_pay_per_day:,.0f}")

    # --------------------------- GOOD CHARTS START -----------------------------

    # Bar chart — Pay per Bene

    st.subheader("💵 Payment per Beneficiary by State")

    df_state["pay_per_bene"] = pd.to_numeric(
        df_state["pay_per_bene"], errors="coerce"
    )

    fig_state_bar = px.bar(
        df_state.sort_values("pay_per_bene", ascending=False),
        x="provider_state",
        y="pay_per_bene",
        color="pay_per_bene",
        color_continuous_scale="Plasma",
        labels={"provider_state": "State", "pay_per_bene": "Payment per Beneficiary"},
        title="Payment per Beneficiary by State",
    )
    fig_state_bar.update_layout(height=450)
    st.plotly_chart(fig_state_bar, use_container_width=True)

    # Choropleth map

    st.subheader("🗺️ Medicare Payments Across States")
    df_state["total_medicare_payment_amt"] = pd.to_numeric(
        df_state["total_medicare_payment_amt"], errors="coerce"
    )
    fig_map = px.choropleth(
        df_state,
        locations="provider_state",
        locationmode="USA-states",
        color="total_medicare_payment_amt",
        color_continuous_scale="Blues",
        scope="usa",
        title="Total Medicare Payments by State",
    )
    fig_map.update_layout(height=500)
    st.plotly_chart(fig_map, use_container_width=True)

    st.subheader("🗺️ Medicare Payments Across States")

    # --------------------------------------------------------------------------
    # Provider-Level Explorer
    # --------------------------------------------------------------------------
    st.header("🏥 Provider-Level Explorer")

    state = st.selectbox(
        "Select State",
        sorted(df_state["provider_state"].unique()),
    )

    df_prov = load_provider_metrics(year, state, service_category)

    if df_prov.empty:
        st.warning("No provider data for this selection.")
        return

    df_top = df_prov.sort_values("pay_per_bene", ascending=False).head(10)

    # Top providers table
    st.subheader(f"Top {10} Providers in {state}")
    st.dataframe(
        df_top[
            [
                "provider_name",
                "provider_id",
                "pay_per_bene",
                "pay_per_service_day",
                "payment_to_charge_ratio",
                "episodes_per_bene",
                "avg_bene_risk_score",
                "risk_band",
                "dual_share_band",
                "rural_mix_band",
            ]
        ],
        use_container_width=True,
    )

    # Horizontal bar chart — Top Providers
    st.subheader("💰 Top Providers by Payment per Beneficiary")
    fig_topprov = px.bar(
        df_top.sort_values("pay_per_bene", ascending=True),
        x="pay_per_bene",
        y="provider_name",
        orientation="h",
        color="pay_per_bene",
        color_continuous_scale="teal",
        title=f"Top {10} Providers in {state}",
    )
    fig_topprov.update_layout(height=600)
    st.plotly_chart(fig_topprov, use_container_width=True)


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
