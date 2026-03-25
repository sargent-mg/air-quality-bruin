/* @bruin
name: air_quality_marts.dim_stations
type: bq.sql
connection: gcp

depends:
  - air_quality_staging.measurements
  - air_quality_raw.locations

materialization:
  type: table

columns:
  - name: location_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: reliability_tier
    type: varchar
    checks:
      - name: accepted_values
        value:
          - High
          - Medium
          - Low
@bruin */

WITH station_activity AS (
    SELECT
        m.location_id,
        l.name AS station_name,
        l.locality AS city,
        l.latitude,
        l.longitude,
        l.parameters,
        l.sensor_count,

        MIN(m.measurement_date) AS first_reading_date,
        MAX(m.measurement_date) AS last_reading_date,
        COUNT(DISTINCT m.measurement_date) AS active_days,
        COUNT(DISTINCT m.parameter) AS parameters_observed,
        COUNT(*) AS total_readings,

        DATE_DIFF(MAX(m.measurement_date), MIN(m.measurement_date), DAY) + 1 AS date_range_days

    FROM `dtc-de-course-454704.air_quality_staging.measurements` m
    LEFT JOIN `dtc-de-course-454704.air_quality_raw.locations` l
        ON m.location_id = l.location_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    *,
    ROUND(SAFE_DIVIDE(active_days, date_range_days) * 100, 1) AS uptime_pct,

    CASE
        WHEN SAFE_DIVIDE(active_days, date_range_days) >= 0.9 THEN 'High'
        WHEN SAFE_DIVIDE(active_days, date_range_days) >= 0.7 THEN 'Medium'
        ELSE 'Low'
    END AS reliability_tier

FROM station_activity