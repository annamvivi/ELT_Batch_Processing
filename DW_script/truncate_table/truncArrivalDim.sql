-- Use the database
USE flight_dw;

-- Create the arrival dimension table
TRUNCATE TABLE arrival_dimension (
    Arrival_ID INT PRIMARY KEY,
    ArrTime STRING,
    ArrDelay INT,
    ArrDelayMinutes INT,
    CRSArrTimeMinute INT,
    CRSArrTimeHour INT
);