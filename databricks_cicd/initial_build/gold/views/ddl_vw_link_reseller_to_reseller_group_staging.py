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
              DROP VIEW IF {catalog}.{schema}.vw_link_reseller_to_reseller_group_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_link_reseller_to_reseller_group_staging (
  reseller_code,
  reseller_group_code,
  reseller_group_name,
  reseller_group_start_date,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
    dr.reseller_pk as reseller_fk,
    coalesce(drg.reseller_group_pk,0) as reseller_group_fk,
    coalesce(rg.ResellerID,'NaN') AS reseller_code,
    coalesce(rg.ResellerGroupCode,'NaN') AS reseller_group_code,
    coalesce(rg.ResellerGroupName,'NaN') AS reseller_group_name,
    to_date('1900-01-01', 'yyyy-MM-dd') AS reseller_group_start_date,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.masterdata.resellergroups AS rg
left join {catalog}.{schema}.dim_reseller AS dr on dr.reseller_code=dg.ResellerID
and dr.country=  replace(rg.Sys_DatabaseName,'Reports','')
and dr.is_current=1
left join {catalog}.{schema}.dim_reseller_group AS drg on drg.ResellerGroupCode=dg.ResellerID
    AND rg.Entity = UPPER(entity.TagetikEntityCode)
and drg.is_current=1
WHERE rg.Sys_Silver_IsCurrent = true
""")
