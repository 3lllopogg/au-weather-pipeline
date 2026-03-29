-- =============================================================
-- 00_setup_database.sql
-- Run this first. Creates the database and all three schemas.
-- Execute against master, then switch context to AUWeatherPipeline.
-- =============================================================

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'AUWeatherPipeline')
BEGIN
    CREATE DATABASE AUWeatherPipeline;
    PRINT 'Database AUWeatherPipeline created.';
END
ELSE
    PRINT 'Database AUWeatherPipeline already exists — skipping.';
GO

USE AUWeatherPipeline;
GO

-- Medallion schemas
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

PRINT 'Schemas: bronze, silver, gold — ready.';
GO
