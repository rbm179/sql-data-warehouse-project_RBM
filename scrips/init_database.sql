

/*
Script Purpose:
	This script creates a new database name 'DataWarehouse' after checking if it already exists. 
	if the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within
	the the database; 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution and
	ensure have property backups before running this script.

*/


USE master;
GO

-- Drop and recreate the 'DataWare house' database
IF EXISTS (SELECT 1 FROM sys.databases where name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the database
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA siver; 
GO
CREATE SCHEMA gold; 
GO

