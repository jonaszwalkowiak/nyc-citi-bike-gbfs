{{ config(
    materialized='table',
    partition_by={
      "field": "data_date",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['station_id']
) }}

WITH daily_stats AS (
    SELECT
        TIMESTAMP_TRUNC(data_hour, DAY) AS data_date,
        station_id,
        AVG(avg_bikes_available) AS daily_avg_bikes_available,
        MIN(avg_bikes_available) AS daily_min_bikes_available,
        MAX(avg_bikes_available) AS daily_max_bikes_available,
        AVG(avg_ebikes_available) AS daily_avg_ebikes_available,
        AVG(temperature) AS daily_avg_temperature,
        MAX(rain_1h) AS daily_max_rain_1h,
        CASE WHEN MAX(rain_1h) > 0 THEN TRUE ELSE FALSE END AS was_raining
    FROM {{ ref('gold_station_weather_hourly') }}
    GROUP BY 1, 2
)

SELECT * FROM daily_stats
