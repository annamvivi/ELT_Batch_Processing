-- Use the database
USE flight_dw;

-- Create the arrival dimension table
CREATE TABLE arrival_dimension (
    Arrival_ID INT PRIMARY KEY,
    ArrTime STRING,
    ArrDelay INT,
    ArrDelayMinutes INT,
    CRSArrTimeMinute INT,
    CRSArrTimeHour INT
);

-- Populate the arrival dimension table with data
 
INSERT OVERWRITE TABLE arrival_dimension
SELECT
    row_number() over() AS Arrival_ID,
    CONCAT(
        LPAD(HOUR, 2, '0'),
        ":",
        LPAD(MINUTE, 2, '0')
    ) AS ArrTime,
    IF(ArrDelay IS NOT NULL, ArrDelay, 0) AS ArrDelay,
    IF(ArrDelayMinutes IS NOT NULL, ArrDelayMinutes, 0) AS ArrDelayMinutes,
    (HOUR * 60 + MINUTE) AS CRSArrTimeMinute,
    HOUR AS CRSArrTimeHour
FROM (
    SELECT posexplode(split('00 15 30 45', ' ')) AS (MINUTE, _) 
    LATERAL VIEW inline(array(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)) h AS HOUR
) t;

-- This script creates an arrival dimension table and populates it with sample data.
