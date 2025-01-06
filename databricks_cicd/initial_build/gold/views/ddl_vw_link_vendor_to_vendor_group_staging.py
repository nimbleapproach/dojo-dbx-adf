# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_link_vendor_to_vendor_group_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_link_vendor_to_vendor_group_staging (
  vendor_fk,
  vendor_group_fk,
  vendor_code,
  vendor_group_code,
  vendor_group_name,
  vendor_group_start_date,
  start_datetime,
  end_datetime,
  source_system_fk,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
    coalesce(dr.vendor_pk,-1) as vendor_fk,
    coalesce(drg.vendor_group_pk,-1) as vendor_group_fk,
    coalesce(lnk.vendor_code,'NaN') AS vendor_code,
    coalesce(lnk.vendor_group_code,'NaN') AS vendor_group_code,
    coalesce(drg.vendor_group_name_internal,'NaN') AS vendor_group_name,
    to_date('1900-01-01', 'yyyy-MM-dd') AS vendor_group_start_date,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,  
    ss.source_system_pk as source_system_fk,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM {catalog}.{schema}.link_vendor_to_vendor_group_stg lnk
left join {catalog}.{schema}.dim_vendor AS dr 
    on dr.vendor_code = lnk.vendor_code
    and dr.is_current = 1
left join {catalog}.{schema}.dim_vendor_group AS drg 
    on drg.vendor_group_code = lnk.vendor_group_code
    and drg.is_current = 1
  cross join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Managed Datasets' and is_current = 1) ss 

where lnk.is_current = true;

""")
