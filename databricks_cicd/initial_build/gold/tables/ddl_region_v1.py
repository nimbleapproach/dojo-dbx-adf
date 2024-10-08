# Databricks notebook source

# Importing Libraries
import os
# COMMAND ----------
ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT
# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ---------- 

catalog = spark.catalog.currentCatalog()
schema = 'orion'
# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.reseller_group_staging (
  Reseller_Group_Code,
  Reseller_Group_Name,
  Reseller_Group_Start_Date,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
WITH SCHEMA BINDING
AS select distinct
    coalesce(rg.ResellerGroupCode,'NaN') AS Reseller_Group_Code,
    coalesce(rg.ResellerGroupName,'NaN') AS Reseller_Group_Name,
    to_date('1900-01-01', 'yyyy-MM-dd') AS Reseller_Group_Start_Date,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_dev.masterdata.resellergroups AS rg
WHERE rg.Sys_Silver_IsCurrent = true