{{ config(
    materialized='table',
    partition_by={
      "field": "data_hour",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['station_id']
) }}

WITH hourly_station_status AS (
    SELECT
        TIMESTAMP_TRUNC(reported_at, HOUR) AS data_hour,
        station_id,
        AVG(bikes_available) AS avg_bikes_available,
        AVG(ebikes_available) AS avg_ebikes_available,
        AVG(docks_available) AS avg_docks_available,
        MAX(is_installed) AS is_installed,
        MAX(is_renting) AS is_renting,
        MAX(is_returning) AS is_returning
    FROM {{ ref('silver_station_status') }}
    GROUP BY 1, 2
),

weather_hourly AS (
    SELECT
        TIMESTAMP_TRUNC(data_date, HOUR) AS data_hour,
        temperature,
        feels_like_temperature,
        humidity_pct,
        wind_speed,
        weather_main,
        weather_description,
        rain_1h,
        snow_1h
    FROM {{ ref('silver_weather') }}
)

SELECT
    s.data_hour,
    s.station_id,
    s.avg_bikes_available,
    s.avg_ebikes_available,
    s.avg_docks_available,
    s.is_installed,
    s.is_renting,
    s.is_returning,
    w.temperature,
    w.feels_like_temperature,
    w.humidity_pct,
    w.wind_speed,
    w.weather_main,
    w.weather_description,
    w.rain_1h,
    w.snow_1h
FROM hourly_station_status s
LEFT JOIN weather_hourly w ON s.data_hour = w.data_hour
