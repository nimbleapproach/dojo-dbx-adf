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
  select distinct sys_databasename from silver_dev.igsql03.sales_invoice_line
)
select
  'Infinigate ERP' as source_system,
  'igsql03' as source_database,
  right(sys_databasename,2) as source_entity,
  sys_databasename as reporting_source_database,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC 
  FROM cte_report_dbs
UNION
select
  'Wavelink ERP' as source_system,
  'TBC',
  'TBC',
  'TBC',
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC 
UNION
SELECT
  'Managed Datasets' AS source_system,
  'TBC',
  'TBC',
  'TBC',
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST(NOW() AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST(NOW() AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
"""
)
