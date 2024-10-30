
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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_reseller_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_reseller_staging 
AS 
with cte_sources as 
(
  select distinct source_system_pk, reporting_source_database 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Infinigate ERP' 
  and s.is_current = 1
),
cte_reseller as
(
select distinct
    coalesce(cu.No_, 'N/A') AS Reseller_Code,
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS Reseller_Name_Internal,
    cu.Country_RegionCode AS Reseller_Geography_Internal,
    to_date(cu.Createdon) AS Reseller_Start_Date,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sih.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(sih.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND UPPER(rg.Entity) = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
LEFT JOIN cte_sources s on lower(s.reporting_source_database) = lower(sih.Sys_DatabaseName)
WHERE sih.Sys_Silver_IsCurrent = true
GROUP BY 
    coalesce(cu.No_, 'N/A'),
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END,
    cu.Country_RegionCode,
    to_date(cu.Createdon),
    coalesce(s.source_system_pk,-1)
union
select distinct
  coalesce(cu.No_, 'N/A') AS Reseller_Code,
  case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END AS Reseller_Name_Internal,
    cu.Country_RegionCode AS Reseller_Geography_Internal,
    to_date(cu.Createdon) AS Reseller_Start_Date,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sih.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(sih.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
   LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sih.`Sell-toCustomerNo_` = cu.No_
    AND sih.Sys_DatabaseName = cu.Sys_DatabaseName
    AND cu.Sys_Silver_IsCurrent = true
    LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
    LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
    ON rg.ResellerID = cu.No_
    AND UPPER(rg.Entity) = UPPER(entity.TagetikEntityCode)
    AND rg.Sys_Silver_IsCurrent = true
LEFT JOIN cte_sources s on lower(s.reporting_source_database) = lower(sih.Sys_DatabaseName)
WHERE sih.Sys_Silver_IsCurrent = true
GROUP BY 
    coalesce(cu.No_, 'N/A'),
    case
      when cu.Name2 = 'NaN' THEN cu.Name
      ELSE concat_ws(' ', cu.Name, cu.Name2)
    END,
    cu.Country_RegionCode,
    to_date(cu.Createdon),
    coalesce(s.source_system_pk,-1)
)
SELECT 
  Reseller_Code,
  Reseller_Name_Internal,
  Reseller_Geography_Internal,
  Reseller_Start_Date,
  source_system_fk,
  start_datetime,
  end_datetime,
  is_current,
  MAX(Sys_Gold_InsertedDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(Sys_Gold_ModifiedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_reseller
GROUP BY
  Reseller_Code,
  Reseller_Name_Internal,
  Reseller_Geography_Internal,
  Reseller_Start_Date,
  source_system_fk,
  start_datetime,
  end_datetime,
  is_current
""")
