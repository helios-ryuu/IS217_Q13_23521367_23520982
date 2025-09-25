USE master;
ALTER DATABASE US_Accidents SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE US_Accidents;

ALTER DATABASE [US_Accidents_DW] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE [US_Accidents_DW];

SELECT
    spid, dbid, loginame, status
FROM sys.sysprocesses
WHERE dbid = DB_ID('US_Accidents');

SELECT
    spid, dbid, loginame, status
FROM sys.sysprocesses
WHERE dbid = DB_ID('US_Accidents_DW');


IF EXISTS (SELECT name FROM sys.databases WHERE name = N'US_Accidents')
BEGIN
    DROP DATABASE US_Accidents;
END
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = N'US_Accidents_DW')
BEGIN
    DROP DATABASE US_Accidents_DW;
END
GO