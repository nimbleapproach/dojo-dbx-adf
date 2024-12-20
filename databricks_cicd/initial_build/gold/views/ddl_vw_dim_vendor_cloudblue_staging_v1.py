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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_vendor_cloudblue_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_vendor_cloudblue_staging 
AS 
with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Cloudblue PBA ERP' 
  and s.is_current = 1
),
cte_duplicate_cloudblue_vendor as 
(
  select Manufacturer, MAX(ManufacturerName) as Vendor_Name_Internal, count(*) as vendor_count
  from silver_{ENVIRONMENT}.cloudblue_pba.bmresource
  where sys_silver_iscurrent = true
  group by Manufacturer
  having count(*) > 1
),
cte_source_data as
(
  SELECT
    cast(coalesce(bm.Manufacturer,'N/A') AS string) AS Vendor_Code,
    CASE WHEN d.vendor_count > 1 THEN d.Vendor_Name_Internal ELSE coalesce(bm.ManufacturerName,'N/A') end AS Vendor_Name_Internal,
    cast(coalesce(bm.Manufacturer,'N/A') AS string) AS local_vendor_ID, 
    'N/A' as Country_Code,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(bm.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(bm.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
  FROM silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
  LEFT JOIN cte_duplicate_cloudblue_vendor d ON d.Manufacturer = bm.Manufacturer
  LEFT JOIN cte_sources s ON s.source_system = 'Cloudblue PBA ERP'
  WHERE bm.Sys_Silver_IsCurrent = 1
  GROUP BY ALL
)
SELECT DISTINCT 
  csd.vendor_code,
  csd.Vendor_Name_Internal,
  csd.local_vendor_ID,
  csd.Country_Code,
  csd.source_system_fk,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END as start_datetime,
  csd.end_datetime,
  csd.is_current,
  csd.Sys_Gold_InsertedDateTime_UTC,
  csd.Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN  {catalog}.{schema}.dim_vendor d on d.local_vendor_ID = csd.local_vendor_ID
AND d.source_system_fk = csd.source_system_fk
WHERE csd.vendor_code IS NOT NULL
""")


