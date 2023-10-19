-- Use the database
USE flight_dw;

-- Create the flight fact table
-- CREATE TABLE flight_fact (
--     Flight_ID INT,
--     Destination_ID INT,
--     DelayType_ID INT,
--     Arrival_ID INT,
--     Departure_ID INT,
--     Weather_ID INT,
--     FlightDate DATE,
--     FlightTime STRING,
--     Marketing_Airline_Network STRING,
--     TaxiIn DOUBLE,
--     TaxiOut DOUBLE,
--     WheelsOn STRING,
--     WheelsOnMinute INT,
--     WheelsOnHour INT,
--     WheelsOff STRING,
--     WheelsOffMinute INT,
--     WheelsOffHour INT,
--     CRSElapsedTime INT,
--     ActualElapsedTime INT,
--     CRSElapsedTimeGroup INT,
--     AirTime INT
-- );

CREATE TABLE flight_fact (
    Flight_ID INT,
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
    AirTime INT,
    PRIMARY KEY(Flight_ID) DISABLE NOVALIDATE,
    CONSTRAINT fk_destination FOREIGN KEY (Destination_ID) REFERENCES flight_dw.destination_dimension (Destination_ID) DISABLE,
    CONSTRAINT fk_delay_type FOREIGN KEY (DelayType_ID) REFERENCES flight_dw.delay_type_dimension (DelayType_ID) DISABLE,
    CONSTRAINT fk_arrival FOREIGN KEY (Arrival_ID) REFERENCES flight_dw.arrival_dimension (Arrival_ID) DISABLE,
    CONSTRAINT fk_departure FOREIGN KEY (Departure_ID) REFERENCES flight_dw.departure_dimension (Departure_ID) DISABLE,
    CONSTRAINT fk_weather FOREIGN KEY (Weather_ID) REFERENCES flight_dw.weather_dimension (Weather_ID) DISABLE
);



-- -- Populate the flight fact table with data
-- INSERT OVERWRITE TABLE flight_fact
-- SELECT
--     1 AS Flight_ID,               
--     1 AS Destination_ID,          
--     1 AS DelayType_ID,            
--     1 AS Arrival_ID,             
--     1 AS Departure_ID,           
--     1 AS Weather_ID,             
--     '2023-10-18' AS FlightDate,  
--     '12:00 PM' AS FlightTime,    
--     'Airline XYZ' AS Marketing_Airline_Network, 
--     10.5 AS TaxiIn,              
--     25.5 AS TaxiOut,             
--     '12:45 PM' AS WheelsOn,     
--     745 AS WheelsOnMinute,      
--     12 AS WheelsOnHour,         
--     '11:30 AM' AS WheelsOff,      
--     690 AS WheelsOffMinute,       
--     11 AS WheelsOffHour,          
--     180 AS CRSElapsedTime,        
--     175 AS ActualElapsedTime,     
--     3 AS CRSElapsedTimeGroup,     
--     160 AS AirTime                
-- -- You can add more rows with different flight details as needed.
-- ;

-- This script creates a flight fact table and populates it with sample data.
