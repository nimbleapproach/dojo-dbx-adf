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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_document_type_staging (
  document_type,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select 'Credit Memo' as document_type,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
  """)
