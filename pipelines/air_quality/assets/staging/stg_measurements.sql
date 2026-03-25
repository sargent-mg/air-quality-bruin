/* @bruin
name: air_quality_staging.measurements
type: bq.sql
connection: gcp

depends:
  - air_quality_raw.measurements

materialization:
  type: table

columns:
  - name: location_id
    type: integer
    checks:
      - name: not_null
  - name: parameter
    type: varchar
    checks:
      - name: not_null
  - name: value_normalized
    type: float
    checks:
      - name: not_null
      - name: non_negative
  - name: measurement_datetime
    type: timestamp
    checks:
      - name: not_null
@bruin */

WITH cleaned AS (
    SELECT
        location_id,
        sensors_id,
        location,
        CAST(datetime AS TIMESTAMP) AS measurement_datetime,
        lat AS latitude,
        lon AS longitude,
        parameter,
        units,
        value,

        -- Normalize units to µg/m³
        CASE
            WHEN units = 'µg/m³' THEN value
            WHEN units = 'ppm' AND parameter = 'co' THEN value * 1145.0
            WHEN units = 'ppm' AND parameter = 'no2' THEN value * 1880.0
            WHEN units = 'ppm' AND parameter = 'so2' THEN value * 2620.0
            WHEN units = 'ppm' AND parameter = 'o3' THEN value * 1960.0
            WHEN units = 'ppm' AND parameter = 'no' THEN value * 1230.0
            WHEN units = 'ppm' AND parameter = 'nox' THEN value * 1880.0
            ELSE value
        END AS value_normalized,

        -- Time dimensions
        EXTRACT(DATE FROM CAST(datetime AS TIMESTAMP)) AS measurement_date,
        EXTRACT(HOUR FROM CAST(datetime AS TIMESTAMP)) AS measurement_hour,
        EXTRACT(DAYOFWEEK FROM CAST(datetime AS TIMESTAMP)) AS day_of_week,

        -- Deduplicate: keep latest ingestion per location + parameter + datetime
        ROW_NUMBER() OVER (
            PARTITION BY location_id, parameter, CAST(datetime AS TIMESTAMP)
            ORDER BY CAST(datetime AS TIMESTAMP) DESC
        ) AS _row_num

    FROM `dtc-de-course-454704.air_quality_raw.measurements`
    WHERE
        value IS NOT NULL
        AND value >= 0
        AND value < 10000
        AND datetime IS NOT NULL
)

SELECT * EXCEPT(_row_num)
FROM cleaned
WHERE _row_num = 1