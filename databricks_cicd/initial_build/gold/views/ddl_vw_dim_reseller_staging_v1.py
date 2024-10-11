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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_reseller_staging (
  Reseller_Code,
  Reseller_Name_Internal,
  Country_Code,
  Reseller_Geography_Internal COMMENT 'TODO',
  source_system_id,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
  coalesce(cu.No_, 'NaN') AS Reseller_Code,
  case
    when cu.Name2 = 'NaN' THEN cu.Name
    ELSE concat_ws(' ', cu.Name, cu.Name2)
  END AS Reseller_Name_Internal,
  replace(Sys_DatabaseName,'Reports','') as Country_Code,
  cu.Country_RegionCode AS Reseller_Geography_Internal,
  (select source_system_pk from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP') as source_system_id,
  -- SHA2(CONCAT_WS(' ', COALESCE(TRIM(cu.No_), ''), COALESCE(TRIM(
  --   concat(regexp_replace(case
  --       when cu.Name2 = 'NaN' THEN cu.Name
  --       ELSE concat_ws(' ', cu.Name, cu.Name2)
  --       END, 'NaN', ''))
  --   ), '')), 256) AS resellers_hash_key,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    NOW() AS Sys_Gold_InsertedDateTime_UTC,
    NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.customer cu 
where cu.Sys_Silver_IsCurrent = true
""")
