-- ============================================
-- Create Database US_Accidents_DW
-- ============================================
CREATE DATABASE US_Accidents_DW
ON PRIMARY 
(
    NAME = US_Accidents_DW_Data,
    FILENAME = 'D:\Projects\IS217_Q13_23521367_23520982\database\US_Accidents_DW_Data.mdf',
    SIZE = 200MB,        -- Initial size
    MAXSIZE = UNLIMITED, -- Allow growth as needed
    FILEGROWTH = 100MB   -- Increase gradually by block
)
LOG ON
(
    NAME = US_Accidents_DW_Log,
    FILENAME = 'D:\Projects\IS217_Q13_23521367_23520982\database\US_Accidents_DW_Log.ldf',
    SIZE = 50MB,
    MAXSIZE = UNLIMITED,
    FILEGROWTH = 50MB
);
GO

-- ============================================
-- Star Schema DDL
-- Database context
-- ============================================
SELECT DB_NAME() AS CurrentDatabase;
SELECT SUSER_NAME() AS CurrentLogin;
SELECT USER_NAME() AS CurrentUser;

USE US_Accidents_DW;
GO

-- ============================================
-- DROP TABLES
-- ============================================
DROP TABLE IF EXISTS [dbo].[FACT_ACCIDENT];
DROP TABLE IF EXISTS [dbo].[DIM_SOURCE];
DROP TABLE IF EXISTS [dbo].[DIM_TIME];
DROP TABLE IF EXISTS [dbo].[DIM_LOCATION];
DROP TABLE IF EXISTS [dbo].[DIM_WEATHER];
DROP TABLE IF EXISTS [dbo].[DIM_ENVIRONMENT];

-- ============================================
-- DIM_SOURCE
-- ============================================
CREATE TABLE [dbo].[DIM_SOURCE] (
    [SOURCE_ID] INT IDENTITY(1,1) PRIMARY KEY,
    [SOURCE] NVARCHAR(100) NOT NULL
);

-- ============================================
-- DIM_TIME
-- ============================================
CREATE TABLE [dbo].[DIM_TIME] (
    [TIME_ID] INT IDENTITY(1,1) PRIMARY KEY,
    [YEAR] INT NOT NULL,
    [QUARTER] INT NOT NULL,
    [MONTH] INT NOT NULL,
    [DAY] INT NOT NULL,
    [HOUR] INT NOT NULL,
    [MINUTE] INT NOT NULL,
    [SECOND] INT NOT NULL,
    [IS_WEEKEND] BIT NOT NULL
);

-- ============================================
-- DIM_LOCATION
-- ============================================
CREATE TABLE [dbo].[DIM_LOCATION] (
    [LOCATION_ID]   INT IDENTITY(1,1) PRIMARY KEY,
    [STATE]         NVARCHAR(100) NOT NULL,
    [COUNTY]        NVARCHAR(100) NOT NULL,
    [CITY]          NVARCHAR(100) NOT NULL,
    [STREET]        NVARCHAR(100) NOT NULL,
    [ZIPCODE]       NVARCHAR(100) NOT NULL,
    [AIRPORT_CODE]  NVARCHAR(100) NOT NULL,
    [TIMEZONE]      NVARCHAR(100) NOT NULL,
    [LATITUDE]      DECIMAL(9,6) NOT NULL,
    [LONGITUDE]     DECIMAL(9,6) NOT NULL
);

-- ============================================
-- DIM_WEATHER
-- ============================================
CREATE TABLE [dbo].[DIM_WEATHER] (
    [WEATHER_ID] INT IDENTITY(1,1) PRIMARY KEY,
    [TEMPERATURE] DECIMAL(8,4) NULL,
    [WIND_CHILL] DECIMAL(8,4) NULL,
    [HUMIDITY] DECIMAL(8,4) NULL,
    [PRESSURE] DECIMAL(8,4) NULL,
    [VISIBILITY] DECIMAL(8,4) NULL,
    [WIND_DIRECTION] NVARCHAR(100) NOT NULL,
    [WIND_SPEED] DECIMAL(8,4) NULL,
    [PRECIPITATION] DECIMAL(8,4) NULL,
    [WEATHER_CONDITION] NVARCHAR(100) NOT NULL,
    [SUNRISE_SUNSET] NVARCHAR(100) NOT NULL,
    [CIVIL_TWILIGHT] NVARCHAR(100) NOT NULL,
    [NAUTICAL_TWILIGHT] NVARCHAR(100) NOT NULL,
    [ASTRONOMICAL_TWILIGHT] NVARCHAR(100) NOT NULL
);

-- ============================================
-- DIM_ENVIRONMENT
-- ============================================
CREATE TABLE [dbo].[DIM_ENVIRONMENT] (
    [ENVIRONMENT_ID] INT IDENTITY(1,1) PRIMARY KEY,
    [AMENITY] BIT NOT NULL,
    [BUMP] BIT NOT NULL,
    [CROSSING] BIT NOT NULL,
    [GIVE_WAY] BIT NOT NULL,
    [JUNCTION] BIT NOT NULL,
    [NO_EXIT] BIT NOT NULL,
    [RAILWAY] BIT NOT NULL,
    [ROUNDABOUT] BIT NOT NULL,
    [STATION] BIT NOT NULL,
    [STOP] BIT NOT NULL,
    [TRAFFIC_CALMING] BIT NOT NULL,
    [TRAFFIC_SIGNAL] BIT NOT NULL,
    [TURNING_LOOP] BIT NOT NULL
);

-- ============================================
-- FACT_ACCIDENT 
-- ============================================
CREATE TABLE [dbo].[FACT_ACCIDENT] (
    [ACCIDENT_ID]     BIGINT IDENTITY(1,1) PRIMARY KEY,
    [SOURCE_ID]       INT NOT NULL,
    [TIME_ID]         INT NOT NULL,
    [LOCATION_ID]     INT NOT NULL,
    [WEATHER_ID]      INT NOT NULL,
    [ENVIRONMENT_ID]  INT NOT NULL,
    [SEVERITY]        INT NOT NULL,        
    [DISTANCE]        DECIMAL(8,4) NOT NULL
);

-- ============================================
-- CHECK
-- ============================================
ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [CK_FACT_SEVERITY] CHECK ([SEVERITY] BETWEEN 1 AND 4);

ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [CK_FACT_DISTANCE] CHECK ([DISTANCE] >= 0);

-- ============================================
-- FOREIGN KEY CONSTRAINTS
-- ============================================
ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [FK_FACT_SOURCE] FOREIGN KEY ([SOURCE_ID]) REFERENCES [dbo].[DIM_SOURCE]([SOURCE_ID]);

ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [FK_FACT_TIME] FOREIGN KEY ([TIME_ID]) REFERENCES [dbo].[DIM_TIME]([TIME_ID]);

ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [FK_FACT_LOCATION] FOREIGN KEY ([LOCATION_ID]) REFERENCES [dbo].[DIM_LOCATION]([LOCATION_ID]);

ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [FK_FACT_WEATHER] FOREIGN KEY ([WEATHER_ID]) REFERENCES [dbo].[DIM_WEATHER]([WEATHER_ID]);

ALTER TABLE [dbo].[FACT_ACCIDENT]
ADD CONSTRAINT [FK_FACT_ENVIRONMENT] FOREIGN KEY ([ENVIRONMENT_ID]) REFERENCES [dbo].[DIM_ENVIRONMENT]([ENVIRONMENT_ID]);