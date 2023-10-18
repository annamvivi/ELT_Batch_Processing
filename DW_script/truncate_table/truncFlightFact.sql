-- Use the database
USE flight_dw;

-- Create the flight fact table
TRUNCATE TABLE flight_fact (
    Flight_ID INT PRIMARY KEY,
    Destination_ID INT,
    DelayType_ID INT,
    Arrival_ID INT,
    Departure_ID INT,
    Weather_ID INT,
    FlightDate DATE,
    FlightTime STRING,
    Marketing_Airline_Network STRING,
    TaxiIn DOUBLE,
    TaxiOut DOUBLE,
    WheelsOn STRING,
    WheelsOnMinute INT,
    WheelsOnHour INT,
    WheelsOff STRING,
    WheelsOffMinute INT,
    WheelsOffHour INT,
    CRSElapsedTime INT,
    ActualElapsedTime INT,
    CRSElapsedTimeGroup INT,
    AirTime INT
);