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
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.vw_dim_region_late (
  region_code,
  reseller_country_region_code,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
    drg.DimensionSetID AS region_code,
    coalesce(drg.DimensionValueCode,'NaN') AS region_name,
    ''  country_code,
    ''  country,
    ''  country_detail,
    ''  country_visuals,
    ''  region_group,    
    ''  start_datetime,
    ''  end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_dev.masterdata.dimension_set_entry AS drg
WHERE rg.Sys_Silver_IsCurrent = true
 and dim.DimensionCode = 'RPTREGION'
    AND sih.Sys_DatabaseName = dim.Sys_DatabaseName
    AND dim.Sys_Silver_IsCurrent = true
""")