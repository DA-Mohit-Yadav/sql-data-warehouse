
/* 
Purpose -

It creates a database named DataWarehouse if it doesn't exists, and also creates bronze, silver and gold schemas.

Warning -
Running this will drop the entire database and will create a new one 

*/










use master;

-- create datapage
drop database if exists DataWarehouse;


--creating Database
create database DataWarehouse;
use DataWarehouse;
go

-- Creating Schemas
create schema BronzeLayer;
go   -- works like a seprator
create schema SilverLayer;
go
create schema GoldLayer;
go



