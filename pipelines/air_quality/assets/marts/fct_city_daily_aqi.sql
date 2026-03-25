/* @bruin
name: air_quality_marts.fct_city_daily_aqi
type: bq.sql
connection: gcp

depends:
  - air_quality_staging.measurements
  - air_quality_raw.locations

materialization:
  type: table

columns:
  - name: city
    type: varchar
    checks:
      - name: not_null
  - name: measurement_date
    type: date
    checks:
      - name: not_null
  - name: station_count
    type: integer
    checks:
      - name: positive
@bruin */

WITH daily_station AS (
    SELECT
        m.location_id,
        l.locality AS city,
        l.name AS station_name,
        m.parameter,
        m.measurement_date,
        m.latitude,
        m.longitude,

        AVG(m.value_normalized) AS avg_value,
        MIN(m.value_normalized) AS min_value,
        MAX(m.value_normalized) AS max_value,
        COUNT(*) AS reading_count,
        COUNT(DISTINCT m.measurement_hour) AS hours_with_readings

    FROM `dtc-de-course-454704.air_quality_staging.measurements` m
    LEFT JOIN `dtc-de-course-454704.air_quality_raw.locations` l
        ON m.location_id = l.location_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

city_daily AS (
    SELECT
        city,
        measurement_date,
        parameter,

        ROUND(AVG(avg_value), 2) AS city_avg_value,
        ROUND(MAX(max_value), 2) AS city_max_value,
        ROUND(MIN(min_value), 2) AS city_min_value,
        COUNT(DISTINCT location_id) AS station_count,
        SUM(reading_count) AS total_readings,
        ROUND(AVG(hours_with_readings / 24.0 * 100), 1) AS avg_coverage_pct,

        AVG(latitude) AS city_latitude,
        AVG(longitude) AS city_longitude

    FROM daily_station
    GROUP BY 1, 2, 3
)

SELECT
    *,

    -- AQI category based on WHO guidelines (PM2.5 and PM10 only)
    CASE
        WHEN parameter = 'pm25' THEN
            CASE
                WHEN city_avg_value <= 15 THEN 'Good'
                WHEN city_avg_value <= 25 THEN 'Moderate'
                WHEN city_avg_value <= 50 THEN 'Unhealthy for Sensitive Groups'
                WHEN city_avg_value <= 100 THEN 'Unhealthy'
                WHEN city_avg_value <= 250 THEN 'Very Unhealthy'
                ELSE 'Hazardous'
            END
        WHEN parameter = 'pm10' THEN
            CASE
                WHEN city_avg_value <= 45 THEN 'Good'
                WHEN city_avg_value <= 100 THEN 'Moderate'
                WHEN city_avg_value <= 150 THEN 'Unhealthy for Sensitive Groups'
                ELSE 'Unhealthy'
            END
        ELSE NULL
    END AS aqi_category,

    -- WHO guideline exceedance flag
    CASE
        WHEN parameter = 'pm25' AND city_avg_value > 15 THEN TRUE
        WHEN parameter = 'pm10' AND city_avg_value > 45 THEN TRUE
        WHEN parameter = 'no2' AND city_avg_value > 25 THEN TRUE
        WHEN parameter = 'o3' AND city_avg_value > 100 THEN TRUE
        ELSE FALSE
    END AS exceeds_who_guideline

FROM city_daily
WHERE city IS NOT NULL