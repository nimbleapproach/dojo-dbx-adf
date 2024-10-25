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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_source_system_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_source_system_staging AS
WITH cte_report_igsql03 AS 
(
  SELECT DISTINCT RIGHT(a.Sys_DatabaseName, 2) AS source_entity
  FROM silver_{ENVIRONMENT}.igsql03.item a
  -- LEFT OUTER JOIN gold_{ENVIRONMENT}.tag02.dim_entity e ON RIGHT(a.Sys_DatabaseName, 2) = e.ventity_code
)
SELECT
  'Infinigate ERP' AS source_system,
  source_entity AS source_entity,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC 
FROM cte_report_igsql03
UNION
SELECT
  'Wavelink ERP' AS source_system,
  'TBC' AS source_entity,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2024-10-02' AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
""")
