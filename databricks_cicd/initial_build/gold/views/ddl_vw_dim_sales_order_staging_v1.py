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
              DROP VIEW IF {catalog}.{schema}.vw_dim_sales_order_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_sales_order_staging (
  country_code,
  source_system_fk,
  document_id COMMENT 'Business Key',
  document_date,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
  replace(sla.Sys_DatabaseName,'Reports','') as country_code,
  ss.source_system_pk as source_system_fk,
  sla.DocumentNo_ As document_id,
  case when sha.DocumentDate = 'NaN' then to_date('1990-12-31') else to_date(coalesce(sha.DocumentDate,'1990-12-31')) end AS document_date,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM 
  silver_{ENVIRONMENT}.igsql03.sales_line_archive as sla
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sla.Sys_DatabaseName, 2)
  inner join (
    select
      No_,
      Sys_DatabaseName,
      max(DocumentDate) DocumentDate,
      max(VersionNo_) VersionNo_
    from
      silver_{ENVIRONMENT}.igsql03.sales_header_archive
    where
      Sys_Silver_IsCurrent = 1
    group by
      No_,
      Sys_DatabaseName
  ) as sha on sla.DocumentNo_ = sha.No_
  and sla.DocumentType = 1
  and sla.Doc_No_Occurrence = 1
  and sla.VersionNo_ = sha.VersionNo_
  and sla.Sys_DatabaseName = sha.Sys_DatabaseName
  and sla.Sys_Silver_IsCurrent = 1
""")
