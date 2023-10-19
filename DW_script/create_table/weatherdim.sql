-- Use the database
USE flight_dw;

-- Create the weather dimension table
CREATE TABLE weather_dimension (
    Weather_ID INT,
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
    vapor_pressure_deficit DOUBLE,
    PRIMARY KEY(Weather_ID) DISABLE NOVALIDATE
);

-- Populate the weather dimension table with data
 
-- INSERT OVERWRITE TABLE weather_dimension
-- SELECT
--     row_number() over() AS Weather_ID,
--     '2023-10-18' AS weather_date,    
--     '12:00 PM' AS weather_time,      
--     25.5 AS temperature_2m,          
--     60.0 AS relativehumidity_2m,     
--     15.2 AS dewpoint_2m,            
--     24.8 AS apparent_temperature,    
--     0.0 AS rain,                    
--     1013.2 AS pressure_msl,         
--     1014.5 AS surface_pressure,      
--     35.0 AS cloudcover,              
--     12.3 AS vapor_pressure_deficit   
 
-- ;

-- This script creates a weather dimension table and populates it with sample data.
