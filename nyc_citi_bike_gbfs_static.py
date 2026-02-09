from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.google.cloud.transfers.http_to_gcs import HttpToGCSOperator
import requests
from urllib.parse import urlparse

GBFS_DYNAMIC_DATA=["station_status", "system_alerts"]

@dag(
    dag_id="nyc_citi_bike_gbfs_static",
    tags=["citi_bike"],
    default_args={"owner": "JW"},
    schedule="@daily",
    catchup=False,
    start_date=datetime(2026, 1, 1),
    end_date=datetime(2030, 1, 1),
    max_active_runs=1
)

def dag_creator():

    @task
    def json_unwrapper(gbfs_feed):

         data = requests.get(gbfs_feed)
         json_data = data.json()
         all_endpoints = json_data["data"]["en"]["feeds"]
         endpoints = [f for f in all_endpoints if f["name"] not in GBFS_DYNAMIC_DATA]

         gbfs_endpoints = [
             {
                 "endpoint": urlparse(f["url"]).path,
                 "object_name":
                     f"raw-data/gbfs-data/static/"
                     f"{f['name']}/"
                     f"year={{{{ logical_date.year }}}}/"
                     f"month={{{{ logical_date.strftime('%m') }}}}/"
                     f"day={{{{ logical_date.strftime('%d') }}}}/"
                     f"{f['name']}_{{{{ logical_date.strftime('%Y%m%d_%H%M%S') }}}}_{{{{ macros.uuid.uuid4() }}}}.json"
             }
             for f in endpoints
         ]

         return gbfs_endpoints

    task1 = json_unwrapper(gbfs_feed="{{ var.value.nyc_citi_bike_gbfs_feed }}")

    http_to_gcs_task = HttpToGCSOperator.partial(
         task_id="http_to_gcs_task",
         http_conn_id="nyc_citi_bike_gbfs_conn_id",
         method="GET",
         mime_type="application/json",
         gcp_conn_id="gcp_conn_id",
         bucket_name="{{ var.value.nyc_citi_bike_gbfs_bucket }}"
    ).expand_kwargs(task1)

dag = dag_creator()