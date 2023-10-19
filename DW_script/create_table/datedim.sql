-- Use the database
USE flight_dw;

-- Create the date dimension table
CREATE TABLE date_dimension (
    `Date` DATE,
    Year INT,
    Month INT,
    DayOfMonth INT,
    DayOfWeek INT,
    FullDate TIMESTAMP,
    PRIMARY KEY (`Date`) DISABLE NOVALIDATE
);

-- Populate the date dimension table with data
-- INSERT OVERWRITE TABLE date_dimension
-- SELECT
--     d.Date,
--     YEAR(d.Date) AS Year,
--     MONTH(d.Date) AS Month,
--     DAY(d.Date) AS DayOfMonth,
--     CASE 
--         WHEN DATE_FORMAT(d.Date, 'u') = 7 THEN 1  -- Sunday
--         ELSE CAST(DATE_FORMAT(d.Date, 'u') AS INT) + 1
--     END AS DayOfWeek,
--     d.Date AS FullDate
-- FROM (
--     SELECT 
--         date_add('2000-01-01', t.i) AS Date
--     FROM (
--         SELECT posexplode(split(space(datediff('2030-12-31', '2000-01-01'), ' '))) AS (i, _) 
--     ) t
-- ) d;




