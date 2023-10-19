-- Use the database
USE flight_dw;

-- Create the time dimension table
CREATE TABLE time_dimension (
    Time_ID INT,
    `Time` STRING,
    Hour INT,
    Quarter INT,
    Season STRING,
    HolidayFlag INT,
    CRSDepTimeHourDis INT,
    CRSArrTimeHourDis INT,
    WheelsOffHourDis INT,
    WheelsOnHourDis INT,
    PRIMARY KEY(Time_ID) DISABLE NOVALIDATE
);

-- Populate the time dimension table with data
 
-- INSERT OVERWRITE TABLE time_dimension
-- SELECT
--     row_number() over() AS Time_ID,
--     CONCAT(
--         LPAD(HOUR, 2, '0'),
--         ":",
--         LPAD(MINUTE, 2, '0')
--     ) AS Time,
--     HOUR AS Hour,
--     CASE 
--         WHEN MINUTE < 15 THEN 1
--         WHEN MINUTE < 30 THEN 2
--         WHEN MINUTE < 45 THEN 3
--         ELSE 4
--     END AS Quarter,
--     CASE 
--         WHEN MONTH IN (12, 1, 2) THEN 'Winter'
--         WHEN MONTH IN (3, 4, 5) THEN 'Spring'
--         WHEN MONTH IN (6, 7, 8) THEN 'Summer'
--         ELSE 'Fall'
--     END AS Season,
--     0 AS HolidayFlag, -- You can set this flag based on actual holiday data
--     CAST(HOUR AS INT) AS CRSDepTimeHourDis,
--     CAST(HOUR AS INT) AS CRSArrTimeHourDis,
--     CAST(HOUR AS INT) AS WheelsOffHourDis,
--     CAST(HOUR AS INT) AS WheelsOnHourDis
-- FROM (
--     SELECT posexplode(split('00 15 30 45', ' ')) AS (MINUTE, _) 
--     LATERAL VIEW inline(array(0,1,2,3,4,5,6,7,8,9,10,11,12)) t AS MONTH
--     LATERAL VIEW inline(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)) h AS HOUR
-- ) t;

-- This script creates a time dimension table and populates it with sample data.
