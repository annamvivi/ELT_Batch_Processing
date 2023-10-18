-- Use the database
USE flight_dw;

-- Create the destination dimension table
TRUNCATE TABLE destination_dimension (
    Destination_ID INT PRIMARY KEY,
    OriginCityName STRING,
    DestCityName STRING,
    Distance INT,
    DistanceGroup INT
);
