-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS gold;

-- COMMAND ----------

USE CATALOG bronze;
CREATE SCHEMA IF NOT EXISTS igsql03;
CREATE SCHEMA IF NOT EXISTS tag02;

-- COMMAND ----------

USE CATALOG silver;
CREATE SCHEMA IF NOT EXISTS igsql03;
CREATE SCHEMA IF NOT EXISTS tag02;

-- COMMAND ----------

GRANT ALL PRIVILEGES on catalog bronze to az_edw_data_engineers;
GRANT ALL PRIVILEGES on catalog silver to az_edw_data_engineers
