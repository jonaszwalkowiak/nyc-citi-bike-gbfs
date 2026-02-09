from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowSkipException
import json
import pandas as pd
from io import BytesIO

GBFS_DYNAMIC_DATA=["station_status", "system_alerts"]

@task
def printer(blobs_list):
    if not blobs_list:
        raise AirflowSkipException("Brak plików na liście. Pomijam zadanie.")
        
    print(f"Znaleziono {len(blobs_list)} plików")
    for blob in blobs_list:
        print(f"FILE: {blob}")

@task
def convert_json_to_parquet(blobs_list, data_type, bucket_name, **context):
    if not blobs_list:
        raise AirflowSkipException(f"Brak plików {data_type} do przerobienia w tej godzinie. Pomijam zadanie.")
    
    gcs_hook = GCSHook(gcp_conn_id="gcp_conn_id")

    dfs = []

    for blob_path in blobs_list:
        file_content = gcs_hook.download(bucket_name=bucket_name, object_name=blob_path)
        data = json.loads(file_content)        
        
        if "data" in data:
            key = "stations" if data_type == "station_status" else "alerts"
            records = data["data"].get(key, [])
            
            if records:
                df_temp = pd.json_normalize(records)
                dfs.append(df_temp)

    if not dfs:
        raise AirflowSkipException("Pliki nie zawierały oczekiwanej struktury danych. Pomijam zadanie.")
    
    final_df = pd.concat(dfs, ignore_index=True)

    parquet_buffer = BytesIO()
    final_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

    logical_date = context["logical_date"]
    dest_blob_name = (
        f"processed-data/gbfs-data/dynamic/{data_type}/"
        f"year={logical_date.year}/"
        f"month={logical_date.strftime('%m')}/"
        f"day={logical_date.strftime('%d')}/"
        f"hour={logical_date.strftime('%H')}/"
        f"combined_data.parquet"
    )

    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=dest_blob_name,
        data=parquet_buffer.getvalue(),
        mime_type="application/octet-stream"
    )

    print(f"Zapisano {len(final_df)} rekordów do pliku: {dest_blob_name}")
    return dest_blob_name

@dag(
    dag_id="nyc_citi_bike_gbfs_dynamic_converter_json_to_parquet",
    tags=["citi_bike"],
    default_args={"owner": "JW"},
    schedule="10 * * * *",
    catchup=True,
    start_date=datetime(2026, 2, 8),
    end_date=datetime(2026, 2, 28),
    max_active_runs=8
)

def dag_creator():

    for data_type in GBFS_DYNAMIC_DATA:

        bucket_lister = GCSListObjectsOperator(
            task_id=f"bucket_lister_{data_type}",
            gcp_conn_id="gcp_conn_id",
            bucket="{{ var.value.nyc_citi_bike_gbfs_bucket }}",
            prefix=(
                f"raw-data/gbfs-data/dynamic/{data_type}/"
                f"year={{{{ logical_date.year }}}}/"
                f"month={{{{ logical_date.strftime('%m') }}}}/"
                f"day={{{{ logical_date.strftime('%d') }}}}/"
                f"hour={{{{ logical_date.strftime('%H') }}}}/"
            )
        )

        printer.override(task_id=f"printer_{data_type}")(
            blobs_list=bucket_lister.output
        )

        convert_json_to_parquet.override(task_id=f"convert_json_to_parquet_{data_type}")(
            blobs_list=bucket_lister.output,
            data_type=data_type,
            bucket_name="{{ var.value.nyc_citi_bike_gbfs_bucket }}"
        )

dag = dag_creator()