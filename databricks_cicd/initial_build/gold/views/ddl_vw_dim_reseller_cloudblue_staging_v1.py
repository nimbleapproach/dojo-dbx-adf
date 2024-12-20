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
# REMOVE ONCE SOLUTION IS LIVE
if ENVIRONMENT == 'dev':
    spark.sql(f"""
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_reseller_cloudblue_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_reseller_cloudblue_staging 
AS 
with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Cloudblue PBA ERP' 
  and s.is_current = 1
),
cte_source_data as
(
select distinct
    cast(coalesce(ar.Customer_AccountID,'N/A') AS string) AS Reseller_Code,
    coalesce(r.CompanyName,'N/A') AS Reseller_Name_Internal,
    'N/A' AS Reseller_Geography_Internal,
    to_date('1900-01-01') AS Reseller_Start_Date,   
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(ar.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(ar.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.cloudblue_pba.ARDoc ar
LEFT OUTER JOIN silver_{ENVIRONMENT}.cloudblue_pba.Account r
ON   ar.Customer_AccountID = r.AccountID
AND  r.Sys_Silver_IsCurrent = true
LEFT JOIN cte_sources s on s.source_system = 'Cloudblue PBA ERP'
WHERE ar.Sys_Silver_IsCurrent = true
GROUP BY ALL
)
SELECT 
  csd.Reseller_Code,
  csd.Reseller_Name_Internal,
  csd.Reseller_Geography_Internal,
  csd.Reseller_Start_Date,
  csd.source_system_fk,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
  csd.end_datetime,
  csd.is_current,
  MAX(csd.Sys_Gold_InsertedDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(csd.Sys_Gold_ModifiedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_reseller d ON csd.Reseller_Code = d.Reseller_Code 
AND csd.source_system_fk = d.source_system_fk
WHERE csd.Reseller_Code IS NOT NULL
GROUP BY ALL
""")


