

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    neighborhood_name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    zip_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE fact_weather (
    weather_id SERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    location_key INTEGER NOT NULL REFERENCES dim_location(location_key),
    temperature_f DECIMAL(5,2),
    feels_like_f DECIMAL(5,2),
    temp_min_f DECIMAL(5,2),
    temp_max_f DECIMAL(5,2),
    weather_main VARCHAR(50), 
    weather_description VARCHAR(100),
    humidity_percent INTEGER,
    pressure_hpa INTEGER,
    wind_speed_mph DECIMAL(5,2),
    cloud_coverage_percent INTEGER,
    observation_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_key, location_key, observation_time)
);

CREATE INDEX idx_fact_weather_date ON fact_weather(date_key);
CREATE INDEX idx_fact_weather_location ON fact_weather(location_key);
CREATE INDEX idx_fact_weather_obs_time ON fact_weather(observation_time);