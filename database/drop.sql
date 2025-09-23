USE US_Accidents_DW;
GO

DECLARE @sql NVARCHAR(MAX) = N'';

-- 1. Drop FOREIGN KEYS
SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];' + CHAR(13)
FROM sys.foreign_keys fk
JOIN sys.tables t ON fk.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id;

-- 2. Drop VIEWS
SELECT @sql += 'DROP VIEW [' + s.name + '].[' + v.name + '];' + CHAR(13)
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id;

-- 3. Drop PROCEDURES
SELECT @sql += 'DROP PROCEDURE [' + s.name + '].[' + p.name + '];' + CHAR(13)
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id;

-- 4. Drop FUNCTIONS
SELECT @sql += 'DROP FUNCTION [' + s.name + '].[' + f.name + '];' + CHAR(13)
FROM sys.objects f
JOIN sys.schemas s ON f.schema_id = s.schema_id
WHERE f.type IN ('FN', 'IF', 'TF'); -- scalar, inline table, table valued

-- 5. Drop TRIGGERS (Database level)
SELECT @sql += 'DROP TRIGGER [' + name + '];' + CHAR(13)
FROM sys.triggers
WHERE parent_class = 0;

-- 6. Drop TABLES
SELECT @sql += 'DROP TABLE [' + s.name + '].[' + t.name + '];' + CHAR(13)
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id;

-- Thực thi toàn bộ
EXEC sp_executesql @sql;
