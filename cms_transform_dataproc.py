"""
DAG: cms_dataproc_transform_dag

Purpose:
  - Submit a Dataproc PySpark job that:
      * reads RAW (bronze) from BigQuery
      * writes SILVER: silver.cms_silver[_spark]
      * writes GOLD:   gold.provider_year_metrics[_spark],
                      gold.state_year_summary[_spark]

The actual transformation logic is in:
  gs://deliverable-cms-bucket/code/cms_bq_transform_spark.py
"""

import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


# ----------------------------------------------------------------------
# CONFIG – update to match your environment
# ----------------------------------------------------------------------
PROJECT_ID = (
    os.environ.get("GCP_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or "gcp-475816"
)

REGION = "us-south1"                      # Dataproc region
CLUSTER_NAME = "cms-dataproc-cluster-sa"  # your Dataproc cluster name

# GCS location of the PySpark script you uploaded
PYSPARK_URI = "gs://deliverable-cms-bucket/transform.py"

# Optional: if you want to override defaults inside the script, you can
# pass them as args and read them via sys.argv in the script
PYSPARK_ARGS = [
    # "--raw_dataset", "deliverable2",
    # "--silver_dataset", "silver",
    # "--gold_dataset", "gold",
]


default_args = {
    "owner": "data-eng",
    "start_date": days_ago(1),
    "retries": 0,
}


with DAG(
    dag_id="cms_dataproc_transform_dag",
    default_args=default_args,
    schedule_interval=None,   # set to "@daily" or "0 3 * * *" if you want a schedule
    catchup=False,
    tags=["cms", "dataproc", "spark", "medallion"],
) as dag:

    # Dataproc job definition
    dataproc_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            # If you add arguments in the script, pass them here:
            # "args": PYSPARK_ARGS,
        },
    }

    run_cms_transform_spark = DataprocSubmitJobOperator(
        task_id="run_cms_bq_transform_spark",
        job=dataproc_job,
        region=REGION,
        project_id=PROJECT_ID,
    )
