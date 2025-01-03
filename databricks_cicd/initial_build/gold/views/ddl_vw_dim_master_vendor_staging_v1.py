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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_master_vendor_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_master_vendor_staging 
AS with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.is_current = 1
)
  select 
    vendor_code AS master_vendor_code,
    vendor_name_internal AS master_vendor_name,
    "" as country_code,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    d.sys_gold_inserteddatetime_utc AS sys_gold_inserteddatetime_utc,
    d.sys_gold_modifieddatetime_utc AS sys_gold_modifieddatetime_utc
  FROM {catalog}.{schema}.link_vendor_to_master_vendor_pre1 d
  cross join cte_sources s on s.source_system = 'Managed Datasets' 
  WHERE is_current = true and is_parent = true
""")
