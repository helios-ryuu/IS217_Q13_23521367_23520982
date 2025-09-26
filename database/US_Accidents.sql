-- ============================================
-- Create Database US_Accidents
-- ============================================
CREATE DATABASE US_Accidents
ON PRIMARY 
(
    NAME = US_Accidents_Data,
    FILENAME = 'D:\Projects\IS217_Q13_23521367_23520982\database\US_Accidents_Data.mdf',
    SIZE = 200MB,        -- Initial size
    MAXSIZE = UNLIMITED, -- Allow growth as needed
    FILEGROWTH = 100MB   -- Increase gradually by block
)
LOG ON
(
    NAME = US_Accidents_Log,
    FILENAME = 'D:\Projects\IS217_Q13_23521367_23520982\database\US_Accidents_Log.ldf',
    SIZE = 50MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 50MB
);
GO

-- ============================================
-- Database context
-- ============================================
SELECT DB_NAME() AS CurrentDatabase;
SELECT SUSER_NAME() AS CurrentLogin;
SELECT USER_NAME() AS CurrentUser;

USE US_Accidents;

-- ============================================
-- DROP TABLE IF EXISTS
-- ============================================
DROP TABLE IF EXISTS [dbo].[ACCIDENTS];

-- ============================================
-- ACCIDENTS (Staging table)
-- ============================================
CREATE TABLE [dbo].[ACCIDENTS] (
    [ID] INT IDENTITY(1,1) PRIMARY KEY,
    -- Fact measures
    [SEVERITY] INT NOT NULL,
    [DISTANCE] DECIMAL(8,4) NOT NULL,

    -- Source
    [SOURCE] NVARCHAR(100) NOT NULL,

    -- Time
    [YEAR] INT NOT NULL,
    [QUARTER] INT NOT NULL,
    [MONTH] INT NOT NULL,
    [DAY] INT NOT NULL,
    [HOUR] INT NOT NULL,
    [MINUTE] INT NOT NULL,
    [SECOND] INT NOT NULL,
    [IS_WEEKEND] BIT NOT NULL,

    -- Location
    [STATE]        NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [COUNTY]       NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [CITY]         NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [STREET]       NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [ZIPCODE]      NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [AIRPORT_CODE] NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [TIMEZONE]     NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [LATITUDE]     DECIMAL(9,6) NOT NULL,
    [LONGITUDE]    DECIMAL(9,6) NOT NULL,

    -- Weather
    [TEMPERATURE]       DECIMAL(8,4) NULL,
    [WIND_CHILL]        DECIMAL(8,4) NULL,
    [HUMIDITY]          DECIMAL(8,4) NULL,
    [PRESSURE]          DECIMAL(8,4) NULL,
    [VISIBILITY]        DECIMAL(8,4) NULL,
    [WIND_DIRECTION]    NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [WIND_SPEED]        DECIMAL(8,4) NULL,
    [PRECIPITATION]     DECIMAL(8,4) NULL,
    [WEATHER_CONDITION] NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [SUNRISE_SUNSET]       NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [CIVIL_TWILIGHT]       NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [NAUTICAL_TWILIGHT]    NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),
    [ASTRONOMICAL_TWILIGHT] NVARCHAR(100) NOT NULL DEFAULT ('Unknown'),

    -- Environment
    [AMENITY]         BIT NOT NULL,
    [BUMP]            BIT NOT NULL,
    [CROSSING]        BIT NOT NULL,
    [GIVE_WAY]        BIT NOT NULL,
    [JUNCTION]        BIT NOT NULL,
    [NO_EXIT]         BIT NOT NULL,
    [RAILWAY]         BIT NOT NULL,
    [ROUNDABOUT]      BIT NOT NULL,
    [STATION]         BIT NOT NULL,
    [STOP]            BIT NOT NULL,
    [TRAFFIC_CALMING] BIT NOT NULL,
    [TRAFFIC_SIGNAL]  BIT NOT NULL,
    [TURNING_LOOP]    BIT NOT NULL
);