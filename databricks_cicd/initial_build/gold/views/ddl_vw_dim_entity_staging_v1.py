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
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_entity_staging 
AS 
with cte_source_data AS
(
  SELECT DISTINCT
  b.entity_code,
  egm.entity_code_legacy,
  b.entity_description,
  b.entity_type,
  coalesce(b.legal_headquarters,'N/A') AS legal_headquarters,
  coalesce(b.administrative_city,'N/A') AS administrative_city,
  'N/A' AS date_established,
  coalesce(b.consolidation_type,'N/A') AS consolidation_type,
  coalesce(b.entity_local_currency,'N/A') AS entity_local_currency,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(Sys_Gold_InsertedDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(Sys_Gold_ModifiedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM gold_{ENVIRONMENT}.tag02.dim_entity b
LEFT JOIN gold_{ENVIRONMENT}.tag02.entity_group_mapping egm on b.entity_code = egm.entity_code
WHERE b.is_current = TRUE
AND b.entity_code IS NOT NULL
GROUP BY ALL
UNION
--had to do this as tag02.dim_entity does not have BE1
SELECT DISTINCT
  'BE1',
  'BE1 + NL1',
  'BENELUX',
  1 AS entity_type,
  'N/A' AS legal_headquarters,
  'N/A' AS administrative_city,
  'N/A' AS date_established,
  'N/A' AS consolidation_type,
  'N/A' AS entity_local_currency,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
)
SELECT DISTINCT
  csd.entity_code,
  csd.entity_code_legacy,
  csd.entity_description,
  csd.entity_type,
  csd.legal_headquarters,
  csd.administrative_city,
  csd.date_established,
  csd.consolidation_type,
  csd.entity_local_currency,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
  csd.end_datetime,
  csd.is_current,
  csd.Sys_Gold_InsertedDateTime_UTC,
  csd.Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_entity d ON d.entity_code = csd.entity_code
  """
)
