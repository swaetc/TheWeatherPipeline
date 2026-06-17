-- sql/schema.sql
-- Star Schema DDL for the Weather Analytics Pipeline
 
-- ── Dimension: Location ─────────────────────────────────────────────
-- Stores each city ONCE. Prevents repeating city names in fact_weather.
CREATE TABLE IF NOT EXISTS dim_location (
    location_id   SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL UNIQUE,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    created_at    TIMESTAMP DEFAULT NOW()
);
 
-- ── Dimension: Date / Time ───────────────────────────────────────────
-- Pre-broken-down date components for fast GROUP BY year/month/hour queries.
CREATE TABLE IF NOT EXISTS dim_date (
    date_id  SERIAL PRIMARY KEY,
    date     DATE NOT NULL,
    year     INT  NOT NULL,
    month    INT  NOT NULL,
    hour     INT  NOT NULL,
    UNIQUE (date, hour)
);
 
-- ── Dimension: Weather Condition ─────────────────────────────────────
-- Maps WMO numeric codes to human-readable descriptions.
-- Avoids storing text strings in every fact row.
CREATE TABLE IF NOT EXISTS dim_weather_condition (
    condition_id  SERIAL PRIMARY KEY,
    weather_code  INT  NOT NULL UNIQUE,
    description   TEXT NOT NULL
);
 
-- Seed WMO standard weather codes (idempotent — safe to run multiple times)
INSERT INTO dim_weather_condition (weather_code, description) VALUES
    (0,  'Clear sky'),
    (1,  'Mainly clear'), (2, 'Partly cloudy'), (3, 'Overcast'),
    (45, 'Fog'), (48, 'Depositing rime fog'),
    (51, 'Light drizzle'), (53, 'Moderate drizzle'), (55, 'Dense drizzle'),
    (61, 'Slight rain'), (63, 'Moderate rain'), (65, 'Heavy rain'),
    (71, 'Slight snow'), (73, 'Moderate snow'), (75, 'Heavy snow'),
    (80, 'Slight showers'), (81, 'Moderate showers'), (82, 'Violent showers'),
    (95, 'Thunderstorm'), (96, 'Thunderstorm with slight hail'),
    (99, 'Thunderstorm with heavy hail')
ON CONFLICT (weather_code) DO NOTHING;
 
-- ── Fact Table: Weather Measurements ────────────────────────────────
-- Central table. One row = one city + one hour of weather data.
CREATE TABLE IF NOT EXISTS fact_weather (
    fact_id               SERIAL PRIMARY KEY,
    time                  TIMESTAMP NOT NULL,
    location_name         VARCHAR(100),
    temperature_2m        DOUBLE PRECISION,
    relative_humidity_2m  DOUBLE PRECISION,
    precipitation         DOUBLE PRECISION,
    wind_speed_10m        DOUBLE PRECISION,
    wind_direction_10m    DOUBLE PRECISION,
    surface_pressure      DOUBLE PRECISION,
    cloud_cover           DOUBLE PRECISION,
    weather_code          INT,
    heat_index            DOUBLE PRECISION,
    loaded_at             TIMESTAMP DEFAULT NOW()
);
 
-- Indexes for common analytical queries
CREATE INDEX IF NOT EXISTS idx_fact_time         ON fact_weather(time);
CREATE INDEX IF NOT EXISTS idx_fact_location     ON fact_weather(location_name);
CREATE INDEX IF NOT EXISTS idx_fact_weather_code ON fact_weather(weather_code);
