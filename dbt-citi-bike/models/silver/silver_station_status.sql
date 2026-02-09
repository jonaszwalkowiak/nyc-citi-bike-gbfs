{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "data_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['station_id']
) }}

WITH source_data AS (
    SELECT 
        *
    FROM {{ source('nyc_citi_bike', 'bronze_station_status') }}

    {% if is_incremental() %}
    WHERE 
        year = CAST(EXTRACT(YEAR FROM DATE('{{ var("target_day", "2030-01-01") }}')) AS STRING)
        AND month = FORMAT_DATE('%m', DATE('{{ var("target_day", "2030-01-01") }}'))
        AND day = FORMAT_DATE('%d', DATE('{{ var("target_day", "2030-01-01") }}'))
    {% endif %}
)

SELECT
    PARSE_TIMESTAMP('%Y-%m-%d', CONCAT(year, '-', month, '-', day)) AS data_date,
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    CAST(day AS INT64) AS day,
    CAST(hour AS INT64) AS hour,
    TIMESTAMP_SECONDS(last_reported) AS reported_at,
    CAST(station_id AS STRING) AS station_id,
    CAST(num_docks_available AS INT64) AS docks_available,
    CAST(num_docks_disabled AS INT64) AS docks_disabled,
    CAST(num_bikes_available AS INT64) AS bikes_available,
    CAST(num_bikes_disabled AS INT64) AS bikes_disabled,
    COALESCE(CAST(num_scooters_available AS INT64), 0) AS scooters_available,
    COALESCE(CAST(num_scooters_unavailable AS INT64), 0) AS scooters_unavailable,
    CAST(num_ebikes_available AS INT64) AS ebikes_available,
    -- ARRAY(SELECT element.count FROM UNNEST(vehicle_types_available.list)) AS counts_of_available_vehicle_types,
    -- ARRAY(SELECT element.vehicle_type_id FROM UNNEST(vehicle_types_available.list)) AS type_ids_of_available_vehicle,
    -- CAST(vehicle_types_available.list.element.count AS INT64) AS count_of_available_vehicle_types,
    -- CAST(vehicle_types_available.list.element.vehicle_type_id AS STRING) AS type_id_of_available_vehicle,
    CAST(is_installed AS BOOL) AS is_installed,
    CAST(is_renting AS BOOL) AS is_renting,
    CAST(is_returning AS BOOL) AS is_returning
FROM source_data