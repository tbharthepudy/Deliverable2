# CMS Post-Acute Care Analytics Dashboard

## Application Link: https://streamlit-app-318671825419.us-south1.run.app/

# Overview

This dashboard helps you analyze Medicare post-acute care utilization and payments for providers across the United States, using data modeled in BigQuery (gold layer).

# Technologies Used
![Python](https://img.shields.io/badge/python-grey?style=for-the-badge&logo=python&logoColor=orange)
![](https://img.shields.io/badge/Streamlit-green?style=for-the-badge&logo=Streamlit&logoColor=white)
![](https://img.shields.io/badge/Docker-blue?style=for-the-badge&logo=Docker&logoColor=white)
![](https://img.shields.io/badge/CloudRun-white?style=for-the-badge&logo=Google%20Cloud&logoColor=black)
![](https://img.shields.io/badge/DataProc-red?style=for-the-badge&logo=Google%20Cloud&logoColor=white)
![](https://img.shields.io/badge/BigQuery-yellow?style=for-the-badge&logo=Google%20Cloud&logoColor=white)
![](https://img.shields.io/badge/Composer-orange?style=for-the-badge&logo=Google%20Cloud&logoColor=white)
![](https://img.shields.io/badge/CloudStorage-black?style=for-the-badge&logo=Google%20Cloud&logoColor=white)
![](https://img.shields.io/badge/CloudFunction-purple?style=for-the-badge&logo=Google%20Cloud&logoColor=white)

# Architecture
![Architecture_diagram](https://github.com/tbharthepudy/Deliverable2/blob/main/cms_post-acute_care_analytics_dashboard.png)

## 🔄 Application Workflow

1. **Data Ingestion (API → GCS)**
   - A Python script `ingest.py` calls the source API and retrieves CMS data as **JSON Lines (JSONL)** files.
   - The script writes these JSONL files directly to **Google Cloud Storage (GCS)** in a designated raw bucket.
   - File naming conventions (e.g., year/service category) are used to keep ingestion organized and traceable.

2. **Event-Driven Load into BigQuery (Raw / Bronze Layer)**
   - A **Cloud Function** is triggered on GCS `storage.objects.create` events whenever a new JSONL file lands in the bucket.
   - The Cloud Function:
     - Reads the new object metadata (bucket + path).
     - Loads the JSONL file into **BigQuery** as a **raw/bronze table**.
   - This creates an automated, event-driven pipeline from **API → GCS → BigQuery (bronze)** with no manual intervention.

3. **Data Transformation with Composer (Silver & Gold Layers)**
   - **Cloud Composer (Airflow)** orchestrates downstream transformations using **DataProc Pyspark Job**.
   - DAG steps:
     1. **Silver Layer (`cms_silver`)**
         - Executed via a Dataproc PySpark job.
         - Cleans and standardizes the raw data (type casting, * → NULL, fixing numeric and percentage fields).
         - Normalizes provider attributes (ID, name, city, state, ZIP).
         - Adds derived fields such as YEAR_DATE, demographic percentages, diagnosis mix, and chronic condition prevalence.
         - Stores a clean, analytics-ready provider × year × service category table.
     2. **Gold Layer Tables**
        Executed via a Dataproc PySpark job.
        - **State-level summary table**: aggregates silver data by state and year to compute total payments, charges, beneficiaries, and risk metrics.
        - **Provider-level metrics table**: calculates provider-level KPIs such as:
          - Payment per beneficiary
          - Payment per service day
          - Charge / allowed per beneficiary
          - Payment-to-charge and allowed-to-charge ratios
          - Episodes per beneficiary and risk bands
        - These two gold tables are the primary data sources for the dashboard.

4. **Analytics Application (Streamlit)**
   - A **Streamlit app** connects to the **gold BigQuery tables** for interactive analysis.
   - The app provides:
     - State-level comparisons (payments, utilization, risk).
     - Provider-level drill-downs (top providers, efficiency ratios, demographics).
     - Visualizations such as bar charts, scatter plots, and geographic views driven by gold metrics.
   - Configuration (project ID, dataset names, etc.) is handled via environment variables and BigQuery client authentication.

5. **Containerization & Image Management**
   - The Streamlit application is packaged into a **Docker image**.
   - The image is pushed to **Artifact Registry** under:
     - `us-south1-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/<IMAGE_NAME>:<TAG>`
   - This makes the app image centrally available for deployment across GCP services.

6. **Application Deployment with Cloud Run**
   - The Docker image from Artifact Registry is deployed to **Cloud Run**.
   - Cloud Run:
     - Runs the containerized Streamlit app as a fully managed, serverless service.
     - Handles autoscaling, health checks, and revisions.
     - Exposes the app via an HTTPS endpoint that users can access directly.
   - Environment variables (e.g., BigQuery project/dataset, credentials config) are set at the Cloud Run service level.

7. **End-to-End Flow (Summary)**
   - **API → `ingest.py` → GCS → Cloud Function → BigQuery (bronze)**  
   - **Composer DAGs → BigQuery (silver + gold)**  
   - **Streamlit → Docker → Artifact Registry → Cloud Run (user-facing dashboard)**


# Directory Structure
```
Deliverable-2/
┣
┣ ingest.py             # Ingestion step
┣ Dockerfile            # Dockerfile for containerization
┣ cms_transform.py      # DAG code
┣ frontend/
┃ ┣ Home.py
┃ ┗ pages
┃   ┣ 1_State_Level_Summary.py
┃   ┗ 2_Provider_Year_Metrics.py
┣ requirements.txt
┣ .env                  # Secrets file
┣ years.json            # config file
┣ gcp-475816-d19d36130b17.json          # GCP Authentication Credentials
┣ .venv/                # Virtual Environment
┣ assets/
┣ cms_post-acute_care_analytics_dashboard.png                
┗ README.md
 
```


# Local Installation 
## Streamlit

Step 1 -  Clone the repository on your local system using the below command and Change the directory to streamlit:
```bash
git clone https://github.com/tbharthepudy/Deliverable2.git
cd Deliverable2
```

Step 2 - Create Virtual Environment and activate
```bash
python -m venv .venv
source .venv/bin/activate
```

Step 3 - Install all the requirements by navigating to the streamlit folder and enter the command:
```bash
pip install -r requirements.txt
```

Step 4 - Run the streamlit application using the below command
```bash
streamlit run frontend/Home.py
```
