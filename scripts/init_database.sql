/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    1. This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    2. If the database exists, it is dropped and recreated. 
	3. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

use master;


-- Drop if already exists and recreate a new 'DataWarehouse' database:
-- 1) Drop if exists
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin 
	alter database DataWarehouse 

	-- In SQL Server, a database cannot be dropped if other users or processes are connected to it,
	-- Allows only one connection to the database
	set single_user 

	-- cancels any running transactions immediately
	with rollback immediate;

	-- Drop (delete) the DataWarehouse database from the SQL Server instance
	drop database DataWarehouse;
end;
go


-- 2) Create a new 'DataWarehouse' database
create database DataWarehouse;

use DataWarehouse;
go

-- 3) Create Schemas
create schema bronze;
go  -- it is a batch separator, not a command: it tells SQL Server tools (like SSMS) to: “Stop here. Execute everything written above before moving to the next part.”
create schema silver;
go
create schema gold;
