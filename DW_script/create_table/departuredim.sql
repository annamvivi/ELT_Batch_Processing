-- Use the database
USE flight_dw;

-- Create the departure dimension table
CREATE TABLE departure_dimension (
    Departure_ID INT,
    DepTime STRING,
    DepDelay INT,
    DepDelayMinutes INT,
    CRSDepTimeMinute INT,
    CRSDepTimeHour INT,
    PRIMARY KEY(Departure_ID) DISABLE NOVALIDATE
);

-- -- Populate the departure dimension table with data
-- INSERT OVERWRITE TABLE departure_dimension
-- SELECT
--     row_number() over() AS Departure_ID,
--     CONCAT(
--         LPAD(HOUR, 2, '0'),
--         ":",
--         LPAD(MINUTE, 2, '0')
--     ) AS DepTime,
--     IF(DepDelay IS NOT NULL, DepDelay, 0) AS DepDelay,
--     IF(DepDelayMinutes IS NOT NULL, DepDelayMinutes, 0) AS DepDelayMinutes,
--     (HOUR * 60 + MINUTE) AS CRSDepTimeMinute,
--     HOUR AS CRSDepTimeHour
-- FROM (
--     SELECT posexplode(split('00 15 30 45', ' ')) AS (MINUTE, _) 
--     LATERAL VIEW inline(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)) h AS HOUR
-- ) t;

-- This script creates a departure dimension table and populates it with sample data.
