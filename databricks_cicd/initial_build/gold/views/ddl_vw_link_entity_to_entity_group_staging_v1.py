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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_link_entity_to_entity_group_staging (
  entity_group_pk,
  entity_pk,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select DISTINCT  
  eg.entity_group_pk  ,
  e.entity_pk , 
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
  from gold_{ENVIRONMENT}.tag02.entity_group_mapping egm
  inner join {catalog}.{schema}.dim_entity_group eg on eg.entity_group_code = egm.entity_group
  inner join {catalog}.{schema}.dim_entity e on e.entity_code = egm.entity_code
  where eg.is_current = 1
  and e.is_current = 1
""")
