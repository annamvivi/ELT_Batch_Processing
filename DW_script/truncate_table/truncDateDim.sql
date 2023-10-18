-- Use the database
USE flight_dw;

-- Create the date dimension table
TRUNCATE TABLE date_dimension (
    Date DATE PRIMARY KEY,
    Year INT,
    Month INT,
    DayOfMonth INT,
    DayOfWeek INT,
    FullDate TIMESTAMP
);