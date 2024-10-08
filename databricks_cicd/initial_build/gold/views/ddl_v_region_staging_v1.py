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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.region_staging (
  region_code COMMENT 'Region Code',
  region_name COMMENT 'The description name of the region',
  country_code COMMENT 'The country code to which the region relates',
  country COMMENT 'The name of the country',
  country_detail COMMENT 'Context of the country potentially including the reference to entity',
  country_visuals COMMENT 'Country name to be used in Power BI visualisations',
  region_group COMMENT 'The high level region grouping',
  start_datetime COMMENT 'The dimensional start date of the record',
  end_datetime COMMENT 'The dimensional end date of the record, those with a NULL value is curent',
  is_current COMMENT 'Flag to indicate if this is the active dimension record per code',
  Sys_Gold_InsertedDateTime_UTC COMMENT 'The timestamp when this record was inserted into gold',
  Sys_Gold_ModifiedDateTime_UTC COMMENT 'The timestamp when this record was last updated in gold')
AS SELECT region_code,
       region_name,
       country_code,
       country,
       country_detail,
       country_visuals,
       region_group,    
       start_datetime,
       end_datetime,
       is_current,
       Sys_Gold_InsertedDateTime_UTC,
       Sys_Gold_ModifiedDateTime_UTC
FROM gold_{ENVIRONMENT}.tag02.dim_region
""")