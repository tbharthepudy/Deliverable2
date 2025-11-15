from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.programming.language import Python
from diagrams.onprem.container import Docker
from diagrams.gcp.analytics import Bigquery, Composer
from diagrams.gcp.compute import Run, Functions
from diagrams.gcp.devtools import ContainerRegistry
from diagrams.gcp.storage import Storage

with Diagram(
    "CMS Post-Acute Care Analytics Dashboard",
    show=False
):

    cms_api = Custom("CMS API", "/Users/tammie/Documents/Deliverable-2/assets/API.png")

    with Cluster("GCP", direction="LR"): 
        cloud_functions = Functions("Cloud Functions")
        gcs = Storage("Cloud Storage")

        with Cluster("Bronze Layer"):
            bigquery_raw = Bigquery("Raw")

        with Cluster("Cloud Composer DAG", direction="LR"):
            with Cluster("Silver Layer"):
                silver_bigquery = Bigquery("Transformed")

            with Cluster("Gold Layer"):
                gold_bigquery_state = Bigquery("Aggregated - State Level Summary")
                gold_bigquery_provider = Bigquery("Aggregated - Provider Year Metrics")

            silver_bigquery >> [gold_bigquery_state, gold_bigquery_provider]

        cms_api >> Edge(label="Data Ingestion") >> cloud_functions \
                >> Edge(label="Event Trigger") >> gcs
        gcs >> Edge(label="Event Trigger") >> bigquery_raw
        bigquery_raw >> Edge(label="Transformation") >> silver_bigquery

    with Cluster("Frontend & Containerization", direction="TB"):
        streamlit = Custom("Streamlit",
                           "/Users/tammie/Documents/Deliverable-2/assets/Streamlit.png")
        docker = Docker("Docker Image")
        with Cluster("GCP"):
            artifact_registry = ContainerRegistry("Docker Container\n(Artifact Registry)")
            cloud_run = Run("Deploy Application\n(Cloud Run)")

        gold_bigquery_provider >> Edge(label="Frontend") >> streamlit
        streamlit >> Edge(label="Containerized") >> docker
        docker >> Edge(label="Upload Artifact") >> artifact_registry \
               >> Edge(label="Deploy Application") >> cloud_run
