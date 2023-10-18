-- Use the database
USE flight_dw;

-- Create the delay type dimension table
TRUNCATE TABLE delay_type_dimension (
    DelayType_ID INT PRIMARY KEY,
    CarrierDelay INT,
    NASDelay INT,
    WeatherDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
);