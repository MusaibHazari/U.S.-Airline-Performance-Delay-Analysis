---Phase 1---

CREATE TABLE "Flights Data"(
    "YEAR"                        SMALLINT,
    "MONTH"                       SMALLINT,
    "DAY"                         SMALLINT,
    "DAY_OF_WEEK"                 SMALLINT,
    "AIRLINE"                     CHAR(5),
    "FLIGHT_NUMBER"               INTEGER,
    "TAIL_NUMBER"                 VARCHAR(10),
    "ORIGIN_AIRPORT"              CHAR(5),
    "DESTINATION_AIRPORT"         CHAR(5),
    "SCHEDULED_DEPARTURE"         TIME,
    "DEPARTURE_TIME"              TIME,
    "DEPARTURE_DELAY"             SMALLINT,
    "TAXI_OUT"                    SMALLINT,
    "WHEELS_OFF"                  TIME,
    "SCHEDULED_TIME"              SMALLINT,
    "ELAPSED_TIME"                SMALLINT,
    "AIR_TIME"                    SMALLINT,
    "DISTANCE"                    SMALLINT,
    "WHEELS_ON"                   TIME,
    "TAXI_IN"                     SMALLINT,
    "SCHEDULED_ARRIVAL"           TIME,
    "ARRIVAL_TIME"                TIME,
    "ARRIVAL_DELAY"               SMALLINT,
    "DIVERTED"                    SMALLINT,
    "CANCELLED"                   SMALLINT,
    "CANCELLATION_REASON"         VARCHAR(50),
    "AIR_SYSTEM_DELAY"            SMALLINT,
    "SECURITY_DELAY"              SMALLINT,
    "AIRLINE_DELAY"               SMALLINT,
    "LATE_AIRCRAFT_DELAY"         SMALLINT,
    "WEATHER_DELAY"               SMALLINT
); 
SELECT * FROM "Flights Data";
SELECT * FROM "Flights Data";

COPY "Flights Data"
FROM 'D:\Labmentix\Fly emirites Final_project_data_archive Project 3\flights.csv'
DELIMITER ','
CSV HEADER;

.
.
.

CREATE TABLE "Airports Data"(
    "IATA_CODE"                   CHAR(5),         
    "AIRPORT"                     VARCHAR(200),                     
    "CITY"                        VARCHAR(50),                         
    "STATE"                       CHAR(5),                           
    "COUNTRY"                     VARCHAR(50),                    
    "LATITUDE"                    DECIMAL(9,6),                 
    "LONGITUDE"                   DECIMAL(9,6)                 
);
SELECT * FROM "Airports Data";

COPY "Airports Data"
FROM 'D:\Labmentix\Fly emirites Final_project_data_archive Project 3\airports.csv'
DELIMITER ','
CSV HEADER;

.
.
.

CREATE TABLE "Airlines Data"(
    "IATA_CODE"                   CHAR(5),         
    "AIRLINE"                     VARCHAR(100)
);
SELECT * FROM "Airlines Data";

COPY "Airlines Data"
FROM 'D:\Labmentix\Fly emirites Final_project_data_archive Project 3\airlines.csv'
DELIMITER ','
CSV HEADER;


---Phase 2---

-- Date Column Inclusion --

ALTER TABLE "Flights Data"
ADD COLUMN "FLIGHT_DATE" DATE;

-- Step 2: Update it using safe string concatenation
UPDATE "Flights Data"
SET "FLIGHT_DATE" = TO_DATE(
    LPAD("DAY"::TEXT, 2, '0') || '/' ||
    LPAD("MONTH"::TEXT, 2, '0') || '/' ||
    "YEAR"::TEXT,
    'DD/MM/YYYY'
)
WHERE "DAY" IS NOT NULL AND "MONTH" IS NOT NULL AND "YEAR" IS NOT NULL;


------Dealing with null value------

-- Set all related fields to NULL where flight was cancelled
UPDATE "Flights Data"
SET 
    "DEPARTURE_DELAY" = NULL,
    "TAXI_OUT" = NULL,
    "WHEELS_OFF" = NULL,
    "ELAPSED_TIME" = NULL,
    "AIR_TIME" = NULL,
    "WHEELS_ON" = NULL,
    "TAXI_IN" = NULL,
    "ARRIVAL_TIME" = NULL,
    "ARRIVAL_DELAY" = NULL,
    "AIR_SYSTEM_DELAY" = NULL,
    "SECURITY_DELAY" = NULL,
    "AIRLINE_DELAY" = NULL,
    "LATE_AIRCRAFT_DELAY" = NULL,
    "WEATHER_DELAY" = NULL
WHERE "CANCELLED" = 1;

-- Remove cancellation reason where flight was not cancelled
UPDATE "Flights Data"
SET "CANCELLATION_REASON" = NULL
WHERE "CANCELLED" = 0;

-- Clean empty tail numbers
UPDATE "Flights Data"
SET "TAIL_NUMBER" = NULL
WHERE "TAIL_NUMBER" = '';



-- Adding Cancellation Reason --

UPDATE "Flights Data"
SET "CANCELLATION_REASON" = CASE "CANCELLATION_REASON"
    WHEN 'A' THEN 'Carrier Delay'
    WHEN 'B' THEN 'Weather Delay'
    WHEN 'C' THEN 'National Air System Delay'
    WHEN 'D' THEN 'Security Delay'
    ELSE NULL
END;


-------------------------------------------------------------------

UPDATE "Airports Data"
SET "AIRPORT" = CONCAT_WS(', ',
    TRIM("AIRPORT"),
    TRIM("CITY"),
    TRIM("STATE"),
    TRIM("COUNTRY"),
    TRIM(CAST("LATITUDE" AS TEXT)),
    TRIM(CAST("LONGITUDE" AS TEXT))
);

ALTER TABLE "Airports Data" 
DROP COLUMN "CITY",
DROP COLUMN "STATE",
DROP COLUMN "COUNTRY",
DROP COLUMN "LATITUDE",
DROP COLUMN "LONGITUDE";

Select * from "Airports Data" ;


------------------------------------------------------------

CREATE TABLE "Integrated Table" AS
SELECT 
    FD."YEAR",
    FD."MONTH",
    FD."DAY",
    FD."DAY_OF_WEEK",
    AD."AIRLINE" AS "AIRLINE_NAME",  -- From Airlines Data
    FD."FLIGHT_NUMBER",
    FD."TAIL_NUMBER",
    AO."AIRPORT" AS "ORIGIN_AIRPORT_NAME",  -- From Airports Data
    ADST."AIRPORT" AS "DESTINATION_AIRPORT_NAME",  -- From Airports Data
    FD."SCHEDULED_DEPARTURE",
    FD."DEPARTURE_TIME",
    FD."DEPARTURE_DELAY",
    FD."TAXI_OUT",
    FD."WHEELS_OFF",
    FD."SCHEDULED_TIME",
    FD."ELAPSED_TIME",
    FD."AIR_TIME",
    FD."DISTANCE",
    FD."WHEELS_ON",
    FD."TAXI_IN",
    FD."SCHEDULED_ARRIVAL",
    FD."ARRIVAL_TIME",
    FD."ARRIVAL_DELAY",
    FD."DIVERTED",
    FD."CANCELLED",
    FD."CANCELLATION_REASON",
    FD."AIR_SYSTEM_DELAY",
    FD."SECURITY_DELAY",
    FD."AIRLINE_DELAY",
    FD."LATE_AIRCRAFT_DELAY",
    FD."WEATHER_DELAY",
    FD."FLIGHT_DATE"
FROM "Flights Data" FD

-- Join with Airlines Data on AIRLINE
LEFT JOIN "Airlines Data" AD
    ON FD."AIRLINE" = AD."IATA_CODE"

-- Join with Airports Data for Origin Airport
LEFT JOIN "Airports Data" AO
    ON FD."ORIGIN_AIRPORT" = AO."IATA_CODE"

-- Join with Airports Data again for Destination Airport
LEFT JOIN "Airports Data" ADST
    ON FD."DESTINATION_AIRPORT" = ADST."IATA_CODE";

select * from "Flights Data" where "AIRLINE_DELAY" is not null;


--------------------------------------------------------

-- EDA Analysis --

-- A. Overall flight volumes, cancellations (total, by reason), and diversions --

-- 1. Total Number of Flights --

SELECT COUNT(*) AS total_flights
FROM "Integrated Table";

-- 2. Total Number of Cancelled Flights --

SELECT COUNT(*) AS total_cancelled
FROM "Integrated Table"
WHERE "CANCELLED" = 1;

-- 3. Cancelled Flights by Reason --

SELECT "CANCELLATION_REASON", COUNT(*) AS count
FROM "Integrated Table"
WHERE "CANCELLED" = 1
GROUP BY "CANCELLATION_REASON"
ORDER BY count DESC;

-- 4. Total Number of Diverted Flights --

SELECT COUNT(*) AS total_diverted
FROM "Integrated Table"
WHERE "DIVERTED" = 1;

-- 5. Summary: Flights, Cancellations, Diversions

SELECT
    COUNT(*) AS total_flights,
    SUM(CASE WHEN "CANCELLED" = 1 THEN 1 ELSE 0 END) AS total_cancelled,
    SUM(CASE WHEN "DIVERTED" = 1 THEN 1 ELSE 0 END) AS total_diverted
FROM "Integrated Table";

-- B. Basic statistics for departure and arrival delays (average, median, min, max) --

-- 1. Departure Delay Statistics --

SELECT
    ROUND(AVG("DEPARTURE_DELAY")::NUMERIC, 2) AS avg_departure_delay,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "DEPARTURE_DELAY") AS median_departure_delay,
    MIN("DEPARTURE_DELAY") AS min_departure_delay,
    MAX("DEPARTURE_DELAY") AS max_departure_delay
FROM "Integrated Table"
WHERE "DEPARTURE_DELAY" IS NOT NULL;

-- 2. Arrival Delay Statistics

SELECT
    ROUND(AVG("ARRIVAL_DELAY")::NUMERIC, 2) AS avg_arrival_delay,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "ARRIVAL_DELAY") AS median_arrival_delay,
    MIN("ARRIVAL_DELAY") AS min_arrival_delay,
    MAX("ARRIVAL_DELAY") AS max_arrival_delay
FROM "Integrated Table"
WHERE "ARRIVAL_DELAY" IS NOT NULL;

-- C. The distribution of different types of delays (airline, weather, NAS, etc.) --

-- 1. Total Delay by Type

SELECT
    SUM("AIRLINE_DELAY") AS total_airline_delay,
    SUM("WEATHER_DELAY") AS total_weather_delay,
    SUM("AIR_SYSTEM_DELAY") AS total_nas_delay,
    SUM("SECURITY_DELAY") AS total_security_delay,
    SUM("LATE_AIRCRAFT_DELAY") AS total_late_aircraft_delay
FROM "Integrated Table";

-- Key Performance Indicators (KPIs) Analysis --

-- 1. On-Time Performance (OTP) Rate: The percentage of flights that arrived on or within 15 minutes of their scheduled arrival time (i.e., not significantly delayed). --

SELECT 
    ROUND(100.0 * COUNT(*) FILTER (WHERE "ARRIVAL_DELAY" <= 15) / COUNT(*), 2) AS "OTP_Rate_Percentage"
FROM "Flights Data"
WHERE "CANCELLED" = 0;

-- 2. Average Arrival and Departure Delay: The average delay in minutes for arrivals and departures. --

SELECT 
    ROUND(AVG("DEPARTURE_DELAY"), 2) AS "Avg_Departure_Delay",
    ROUND(AVG("ARRIVAL_DELAY"), 2) AS "Avg_Arrival_Delay"
FROM "Flights Data"
WHERE "CANCELLED" = 0;

-- 3. Cancellation Rate: The percentage of flights that were cancelled.

SELECT 
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CANCELLED" = 1) / COUNT(*), 2) AS "Cancellation_Rate_Percentage"
FROM "Flights Data";

-- 4. Percentage Contribution of Each Delay Type: What percentage of the total delay time is caused by each delay category.

SELECT
    ROUND(100.0 * SUM("AIRLINE_DELAY") / NULLIF(SUM("AIRLINE_DELAY" + "WEATHER_DELAY" + "AIR_SYSTEM_DELAY" + "SECURITY_DELAY" + "LATE_AIRCRAFT_DELAY"), 0), 2) AS "Airline_Delay_Percent",
    ROUND(100.0 * SUM("WEATHER_DELAY") / NULLIF(SUM("AIRLINE_DELAY" + "WEATHER_DELAY" + "AIR_SYSTEM_DELAY" + "SECURITY_DELAY" + "LATE_AIRCRAFT_DELAY"), 0), 2) AS "Weather_Delay_Percent",
    ROUND(100.0 * SUM("AIR_SYSTEM_DELAY") / NULLIF(SUM("AIRLINE_DELAY" + "WEATHER_DELAY" + "AIR_SYSTEM_DELAY" + "SECURITY_DELAY" + "LATE_AIRCRAFT_DELAY"), 0), 2) AS "NAS_Delay_Percent",
    ROUND(100.0 * SUM("SECURITY_DELAY") / NULLIF(SUM("AIRLINE_DELAY" + "WEATHER_DELAY" + "AIR_SYSTEM_DELAY" + "SECURITY_DELAY" + "LATE_AIRCRAFT_DELAY"), 0), 2) AS "Security_Delay_Percent",
    ROUND(100.0 * SUM("LATE_AIRCRAFT_DELAY") / NULLIF(SUM("AIRLINE_DELAY" + "WEATHER_DELAY" + "AIR_SYSTEM_DELAY" + "SECURITY_DELAY" + "LATE_AIRCRAFT_DELAY"), 0), 2) AS "Late_Aircraft_Delay_Percent"
FROM "Flights Data"
WHERE "CANCELLED" = 0;

-- 5. KPI Aggregation by Airline

SELECT 
    "AIRLINE",
    COUNT(*) AS "Total_Flights",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "ARRIVAL_DELAY" <= 15) / COUNT(*), 2) AS "OTP_Percentage",
    ROUND(AVG("DEPARTURE_DELAY"), 2) AS "Avg_Departure_Delay",
    ROUND(AVG("ARRIVAL_DELAY"), 2) AS "Avg_Arrival_Delay",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CANCELLED" = 1) / COUNT(*), 2) AS "Cancellation_Rate"
FROM "Flights Data"
GROUP BY "AIRLINE"
ORDER BY "OTP_Percentage" DESC;

-- 6. KPI Aggregation by Origin and Destination Airport

SELECT 
    "ORIGIN_AIRPORT",
    "DESTINATION_AIRPORT",
    COUNT(*) AS "Total_Flights",
    ROUND(AVG("ARRIVAL_DELAY"), 2) AS "Avg_Arrival_Delay",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CANCELLED" = 1) / COUNT(*), 2) AS "Cancellation_Rate"
FROM "Flights Data"
GROUP BY "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"
ORDER BY "Avg_Arrival_Delay" DESC
LIMIT 20;

-- 7. KPI Aggregation by Month

SELECT 
    "MONTH",
    COUNT(*) AS "Total_Flights",
    ROUND(AVG("ARRIVAL_DELAY"), 2) AS "Avg_Arrival_Delay",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CANCELLED" = 1) / COUNT(*), 2) AS "Cancellation_Rate"
FROM "Flights Data"
GROUP BY "MONTH"
ORDER BY "MONTH";

-- 8. 4. KPI Aggregation by Day of Week

SELECT 
    "DAY_OF_WEEK",
    COUNT(*) AS "Total_Flights",
    ROUND(AVG("ARRIVAL_DELAY"), 2) AS "Avg_Arrival_Delay",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CANCELLED" = 1) / COUNT(*), 2) AS "Cancellation_Rate"
FROM "Flights Data"
GROUP BY "DAY_OF_WEEK"
ORDER BY "DAY_OF_WEEK";

-- 9. KPI Aggregation by Time of Day

SELECT 
    CASE 
        WHEN EXTRACT(HOUR FROM "SCHEDULED_DEPARTURE") BETWEEN 0 AND 5 THEN 'Night (00:00-05:59)'
        WHEN EXTRACT(HOUR FROM "SCHEDULED_DEPARTURE") BETWEEN 6 AND 11 THEN 'Morning (06:00-11:59)'
        WHEN EXTRACT(HOUR FROM "SCHEDULED_DEPARTURE") BETWEEN 12 AND 17 THEN 'Afternoon (12:00-17:59)'
        WHEN EXTRACT(HOUR FROM "SCHEDULED_DEPARTURE") BETWEEN 18 AND 23 THEN 'Evening (18:00-23:59)'
        ELSE 'Unknown'
    END AS "Time_of_Day",
    COUNT(*) AS "Total_Flights",
    ROUND(AVG("ARRIVAL_DELAY"), 2) AS "Avg_Arrival_Delay",
    ROUND(100.0 * COUNT(*) FILTER (WHERE "CANCELLED" = 1) / COUNT(*), 2) AS "Cancellation_Rate"
FROM "Flights Data"
GROUP BY "Time_of_Day"
ORDER BY "Time_of_Day";

select * from "Integrated Table";

UPDATE "Integrated Table"
SET "SCHEDULED_ARRIVAL" = '00:00:00'
WHERE "SCHEDULED_ARRIVAL" = '24:00:00';

