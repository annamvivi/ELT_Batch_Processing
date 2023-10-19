-- Use the database
USE flight_dw;

-- Truncate the arrival dimension table
TRUNCATE TABLE arrival_dimension;
TRUNCATE TABLE date_dimension;
TRUNCATE TABLE delay_type_dimension;
TRUNCATE TABLE departure_dimension;
TRUNCATE TABLE destination_dimension;
TRUNCATE TABLE time_dimension;
TRUNCATE TABLE weather_dimension;
TRUNCATE TABLE flight_fact;