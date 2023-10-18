-- Use the database
USE flight_dw;

-- Create the destination dimension table
CREATE TABLE destination_dimension (
    Destination_ID INT PRIMARY KEY,
    OriginCityName STRING,
    DestCityName STRING,
    Distance INT,
    DistanceGroup INT
);

-- Populate the destination dimension table with data
INSERT OVERWRITE TABLE destination_dimension
SELECT
    row_number() over() AS Destination_ID,
    'SampleOriginCity' AS OriginCityName, 
    'SampleDestCity' AS DestCityName,     
    1000 AS Distance,                     
    2 AS DistanceGroup                    
;

-- This script creates a destination dimension table and populates it with sample data.
