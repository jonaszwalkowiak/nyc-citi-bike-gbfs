{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    unique_key='dt',
    partition_by={
      "field": "data_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['weather_main']
) }}

WITH source_data AS (
    SELECT 
        *
    FROM {{ source('nyc_citi_bike', 'bronze_weather') }}

    {% if is_incremental() %}
    WHERE _PARTITIONTIME = TIMESTAMP_TRUNC(TIMESTAMP('{{ var("target_day", "2030-01-01") }}'), DAY)
    {% endif %}
)

SELECT
    TIMESTAMP_SECONDS(dt) AS data_date,
    CAST(dt AS INT64) AS dt,
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    CAST(day AS INT64) AS day,
    CAST(hour AS INT64) AS hour,
    CAST(id AS INT64) AS city_id,
    CAST(name AS STRING) AS city_name,
    CAST(timezone AS INT64) AS timezone,
    CAST(visibility AS INT64) AS visibility,
    CAST(main.temp AS FLOAT64) AS temperature,
    CAST(main.feels_like AS FLOAT64) AS feels_like_temperature,
    CAST(main.pressure AS INT64) AS pressure,
    CAST(main.humidity AS INT64) AS humidity_pct,
    CAST(wind.speed AS FLOAT64) AS wind_speed,
    CAST(wind.deg AS INT64) AS wind_direction_deg,
    CAST(wind.gust AS FLOAT64) AS wind_gust,
    CAST(clouds.all AS INT64) AS cloudiness_pct,
    TIMESTAMP_SECONDS(sys.sunrise) AS sunrise_ts,
    TIMESTAMP_SECONDS(sys.sunset) AS sunset_ts,
    CAST(weather[0].main AS STRING) AS weather_main,
    CAST(weather[0].description AS STRING) AS weather_description,
    COALESCE(CAST(rain.`1h` AS FLOAT64), 0) AS rain_1h,
    COALESCE(CAST(rain.`3h` AS FLOAT64), 0) AS rain_3h,
    COALESCE(CAST(snow.`1h` AS FLOAT64), 0) AS snow_1h,
    COALESCE(CAST(snow.`3h` AS FLOAT64), 0) AS snow_3h
FROM source_data