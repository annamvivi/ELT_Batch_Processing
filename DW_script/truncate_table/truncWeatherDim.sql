-- Use the database
USE flight_dw;

-- Create the weather dimension table
TRUNCATE TABLE weather_dimension (
    Weather_ID INT PRIMARY KEY,
    weather_date DATE,
    weather_time STRING,
    temperature_2m DOUBLE,
    relativehumidity_2m DOUBLE,
    dewpoint_2m DOUBLE,
    apparent_temperature DOUBLE,
    rain DOUBLE,
    pressure_msl DOUBLE,
    surface_pressure DOUBLE,
    cloudcover DOUBLE,
    vapor_pressure_deficit DOUBLE
);