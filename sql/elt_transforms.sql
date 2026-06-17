-- sql/elt_transforms.sql
-- In-database SQL transforms from staging to star schema tables.
 
-- Step 1: Populate dim_location from staging
INSERT INTO dim_location (location_name, latitude, longitude)
SELECT DISTINCT
    INITCAP(TRIM(location_name)),
    CAST(latitude  AS DOUBLE PRECISION),
    CAST(longitude AS DOUBLE PRECISION)
FROM staging_weather
ON CONFLICT (location_name) DO NOTHING;
 
-- Step 2: Populate dim_date from staging
INSERT INTO dim_date (date, year, month, hour)
SELECT DISTINCT
    DATE(time::TIMESTAMP),
    EXTRACT(YEAR  FROM time::TIMESTAMP)::INT,
    EXTRACT(MONTH FROM time::TIMESTAMP)::INT,
    EXTRACT(HOUR  FROM time::TIMESTAMP)::INT
FROM staging_weather
ON CONFLICT (date, hour) DO NOTHING;
 
-- Step 3: Populate fact_weather from staging
INSERT INTO fact_weather (
    time, location_name, temperature_2m, relative_humidity_2m,
    precipitation, wind_speed_10m, wind_direction_10m,
    surface_pressure, cloud_cover, weather_code
)
SELECT
    time::TIMESTAMP,
    INITCAP(TRIM(location_name)),
    CAST(temperature_2m        AS DOUBLE PRECISION),
    CAST(relative_humidity_2m  AS DOUBLE PRECISION),
    CAST(precipitation         AS DOUBLE PRECISION),
    CAST(wind_speed_10m        AS DOUBLE PRECISION),
    CAST(wind_direction_10m    AS DOUBLE PRECISION),
    CAST(surface_pressure      AS DOUBLE PRECISION),
    CAST(cloud_cover           AS DOUBLE PRECISION),
    CAST(weather_code          AS INT)
FROM staging_weather
ON CONFLICT DO NOTHING;
