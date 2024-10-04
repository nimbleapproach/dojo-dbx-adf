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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.source_system_staging as
select
  'Infinigate ERP' as source_system,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  '2024-10-02' AS Sys_Gold_InsertedDateTime_UTC,
  '2024-10-02' AS Sys_Gold_ModifiedDateTime_UTC
  ;
""")