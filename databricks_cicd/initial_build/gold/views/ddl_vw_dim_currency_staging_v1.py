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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_currency_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_currency_staging 
AS 
with cte_source_data as 
(
  select distinct COD_VALUTA AS Currency_Code,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
    FROM
    silver_{ENVIRONMENT}.tag02.dati_cambio as c
WHERE c.Sys_Silver_IsCurrent = 1
AND COD_VALUTA IS NOT NULL
)
SELECT DISTINCT
  csd.Currency_Code,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
  csd.end_datetime,
  csd.is_current,
  csd.Sys_Gold_InsertedDateTime_UTC,
  csd.Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data as csd
LEFT JOIN {catalog}.{schema}.dim_currency d on d.Currency_Code = csd.Currency_Code
""")

