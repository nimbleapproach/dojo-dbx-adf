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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_entity_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_entity_staging (
  entity_code COMMENT 'Entity Code',
  entity_code_legacy COMMENT 'Legacy reference to the Infinigate entity Id',
  entity_description COMMENT 'The descriptive name of the associated entity',
  entity_type,
  legal_headquarters,
  administrative_city,
  date_established,
  consolidation_type,
  entity_local_currency,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS SELECT DISTINCT
  b.entity_code,
  egm.entity_code_legacy,
  b.entity_description,
  b.entity_type,
  coalesce(b.legal_headquarters,'NaN') as legal_headquarters,
  coalesce(b.administrative_city,'NaN') as administrative_city,
  coalesce(b.date_established,'NaN') as date_established,
  coalesce(b.consolidation_type,'NaN') as consolidation_type,
  coalesce(b.entity_local_currency,'NaN') as entity_local_currency,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM gold_{ENVIRONMENT}.tag02.dim_entity b
INNER JOIN gold_{ENVIRONMENT}.tag02.entity_group_mapping egm on b.entity_code = egm.entity_code
  """)
