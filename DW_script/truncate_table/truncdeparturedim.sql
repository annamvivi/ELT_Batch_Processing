-- Use the database
USE flight_dw;

-- Create the departure dimension table
TRUNCATE TABLE departure_dimension (
    Departure_ID INT PRIMARY KEY,
    DepTime STRING,
    DepDelay INT,
    DepDelayMinutes INT,
    CRSDepTimeMinute INT,
    CRSDepTimeHour INT
);