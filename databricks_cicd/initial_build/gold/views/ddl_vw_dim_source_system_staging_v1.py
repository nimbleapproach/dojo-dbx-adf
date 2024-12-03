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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_source_system_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_source_system_staging AS
with cte_report_dbs as 
(
  select distinct sys_databasename from silver_{ENVIRONMENT}.igsql03.sales_invoice_line
),
cte_nuvias_entity as 
(
  select distinct UPPER(entity.TagetikEntityCode) AS source_entity, dataareaid as data_area_id 
  FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicetrans trans
  LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON upper(trans.DataAreaId) = entity.SourceEntityCode 
  WHERE Sys_Silver_IsCurrent = 1
),
cte_source_data as 
(
select
  'Infinigate ERP' as source_system,
  'igsql03' as source_database,
  right(sys_databasename,2) as source_entity,
  sys_databasename as reporting_source_database,
  'N/A' as data_area_id,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC 
  FROM cte_report_dbs
UNION
select
  'Wavelink ERP' as source_system,
  'TBC',
  'TBC',
  'TBC',
  'TBC',
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC 
UNION
SELECT
  'Managed Datasets' AS source_system,
  'TBC',
  'TBC',
  'TBC',
  'TBC',
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
UNION
SELECT
  'Nuvias ERP' AS source_system,
  'nuvias_operations',
  cne.source_entity,
  'N/A' AS reporting_source_database,
  cne.data_area_id,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_nuvias_entity cne
UNION 
SELECT
  'Nuvias ERP' AS source_system,
  'nuvias_operations',
  'N/A',
  'N/A',
  'N/A',
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('1990-01-01' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
)
SELECT DISTINCT
  csd.source_system,
  csd.source_database,
  csd.source_entity,
  csd.reporting_source_database,
  csd.data_area_id,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END as start_datetime,
  csd.end_datetime,
  csd.is_current,
  csd.Sys_Gold_InsertedDateTime_UTC,
  csd.Sys_Gold_ModifiedDateTime_UTC 
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_source_system d on d.source_system = csd.source_system
WHERE csd.source_system IS NOT NULL
"""
)
