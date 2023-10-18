-- Use the database
USE flight_dw;

-- Create the time dimension table
TRUNCATE TABLE time_dimension (
    Time_ID INT PRIMARY KEY,
    Time STRING,
    Hour INT,
    Quarter INT,
    Season STRING,
    HolidayFlag INT,
    CRSDepTimeHourDis INT,
    CRSArrTimeHourDis INT,
    WheelsOffHourDis INT,
    WheelsOnHourDis INT
);
