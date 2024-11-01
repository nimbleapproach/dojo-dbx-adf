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

# REMOVE ONCE SOLUTION IS LIVE
if ENVIRONMENT == 'dev':
    spark.sql(f"""
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_vendor_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_vendor_staging 
AS with cte_sources as 
(
  select distinct source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Infinigate ERP' 
  and s.is_current = 1
)
-- vendors per country
select distinct
code AS vendor_code,
Name AS Vendor_Name_Internal,
SID as local_vendor_ID,
replace(Sys_DatabaseName,"Reports","") as Country_Code,
coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.dimension_value d
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(d.Sys_DatabaseName,2))
WHERE UPPER(DimensionCode) = 'VENDOR'
AND Sys_Silver_IsCurrent = true
""")
