# Databricks notebook source
# Importing Libraries
import os
from pyspark.sql import SparkSession

# COMMAND ----------

# Create a Spark session
spark = SparkSession.builder \
    .appName("Databricks Notebook") \
    .getOrCreate()

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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_vendor_staging (
  Vendor_Code COMMENT 'Business Key',
  Vendor_Name_Internal COMMENT 'TODO',
  local_vendor_ID COMMENT 'Surrogate Key',
  Country_Code,
  source_system_id,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
code AS Vendor_Code,
Name AS Vendor_Name_Internal,
SID as local_vendor_ID,
replace(Sys_DatabaseName,'Reports','') as Country_Code,
(select source_system_pk from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP') as source_system_id,
-- SHA2(CONCAT_WS(' ', COALESCE(TRIM(code), ''), COALESCE(TRIM(
--     concat(regexp_replace(Name, 'NaN', ''))
--     ), '')), 256) AS vendor_hash_key,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_dev.igsql03.dimension_value
WHERE DimensionCode = 'VENDOR'
AND Sys_Silver_IsCurrent = true
""")
