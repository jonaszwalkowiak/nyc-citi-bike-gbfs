from datetime import datetime
from airflow.sdk import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

@dag(
    dag_id="nyc_weather_direct_bq_load",
    tags=["citi_bike"],
    default_args={"owner": "JW"},
    schedule="5 * * * *",
    catchup=True,
    start_date=datetime(2026, 2, 8),
    end_date=datetime(2026, 2, 28),
    max_active_runs=4,
)

def dag_creator():

    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_weather_to_bq",
        gcp_conn_id="gcp_conn_id",
        configuration={
            "load": {
                "sourceUris": [
                    "gs://{{ var.value.nyc_citi_bike_gbfs_bucket }}/raw-data/weather-data/"
                    "year={{ (logical_date - macros.timedelta(minutes=5)).strftime('%Y') }}/"
                    "month={{ (logical_date - macros.timedelta(minutes=5)).strftime('%m') }}/"
                    "day={{ (logical_date - macros.timedelta(minutes=5)).strftime('%d') }}/"
                    "hour={{ (logical_date - macros.timedelta(minutes=5)).strftime('%H') }}/"
                    "nyc_weather_{{ (logical_date - macros.timedelta(minutes=5)).strftime('%Y%m%d_%H') }}*{{ '.json' }}"
                ],
                "destinationTable": {
                    "projectId": "{{ var.value.data_sedum_project_id }}",
                    "datasetId": "{{ var.value.nyc_citi_bike_gbfs_dataset_id }}",
                    "tableId": "{{ var.value.nyc_citi_bike_gbfs_table_id }}",
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": False,
                "ignoreUnknownValues": True,
                
                "schema": {
                    "fields": [
                        {"name": "cod", "type": "INTEGER"},
                        {"name": "name", "type": "STRING"},
                        {"name": "id", "type": "INTEGER"},
                        {"name": "dt", "type": "INTEGER"},
                        {"name": "base", "type": "STRING"},
                        {"name": "timezone", "type": "INTEGER"},
                        {"name": "visibility", "type": "INTEGER"},
                        {
                            "name": "clouds", "type": "RECORD", "fields": [
                                {"name": "all", "type": "INTEGER"}
                            ]
                        },
                        {
                            "name": "wind", "type": "RECORD", "fields": [
                                {"name": "gust", "type": "FLOAT"},
                                {"name": "deg", "type": "INTEGER"},
                                {"name": "speed", "type": "FLOAT"}
                            ]
                        },
                        {
                            "name": "sys", "type": "RECORD", "fields": [
                                {"name": "sunset", "type": "INTEGER"},
                                {"name": "id", "type": "INTEGER"},
                                {"name": "sunrise", "type": "INTEGER"},
                                {"name": "country", "type": "STRING"},
                                {"name": "type", "type": "INTEGER"}
                            ]
                        },
                        {
                            "name": "main", "type": "RECORD", "fields": [
                                {"name": "grnd_level", "type": "INTEGER"},
                                {"name": "pressure", "type": "INTEGER"},
                                {"name": "sea_level", "type": "INTEGER"},
                                {"name": "temp", "type": "FLOAT"},
                                {"name": "temp_max", "type": "FLOAT"},
                                {"name": "temp_min", "type": "FLOAT"},
                                {"name": "humidity", "type": "INTEGER"},
                                {"name": "feels_like", "type": "FLOAT"}
                            ]
                        },
                        {
                            "name": "coord", "type": "RECORD", "fields": [
                                {"name": "lat", "type": "FLOAT"},
                                {"name": "lon", "type": "FLOAT"}
                            ]
                        },
                        {
                            "name": "rain", "type": "RECORD", "fields": [
                                {"name": "1h", "type": "FLOAT"},
                                {"name": "3h", "type": "FLOAT"}
                            ]
                        },
                        {
                            "name": "snow", "type": "RECORD", "fields": [
                                {"name": "1h", "type": "FLOAT"},
                                {"name": "3h", "type": "FLOAT"}
                            ]
                        },
                        {
                            "name": "weather", "type": "RECORD", "mode": "REPEATED", "fields": [
                                {"name": "description", "type": "STRING"},
                                {"name": "icon", "type": "STRING"},
                                {"name": "main", "type": "STRING"},
                                {"name": "id", "type": "INTEGER"}
                            ]
                        },
                        {"name": "year", "type": "STRING"},
                        {"name": "month", "type": "STRING"},
                        {"name": "day", "type": "STRING"},
                        {"name": "hour", "type": "STRING"}
                    ]
                },
                "hivePartitioningOptions": {
                    "mode": "STRINGS",
                    "sourceUriPrefix": "gs://{{ var.value.nyc_citi_bike_gbfs_bucket }}/raw-data/weather-data"
                }
            }
        }
    )

dag = dag_creator()