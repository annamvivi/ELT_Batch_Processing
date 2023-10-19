-- Use the database
USE flight_dw;

-- Create the delay type dimension table
CREATE TABLE delay_type_dimension (
    DelayType_ID INT,
    CarrierDelay INT,
    NASDelay INT,
    WeatherDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT,
    PRIMARY KEY(DelayType_ID) DISABLE NOVALIDATE
);

-- -- Populate the delay type dimension table with data
-- INSERT OVERWRITE TABLE delay_type_dimension
-- SELECT
--     1 AS DelayType_ID, 
--     10 AS CarrierDelay,  
--     5 AS NASDelay,      
--     2 AS WeatherDelay,  
--     0 AS SecurityDelay, 
--     15 AS LateAircraftDelay 
-- ;

-- This script creates a delay type dimension table and populates it with sample data.
