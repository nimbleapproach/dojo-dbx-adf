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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_currency_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_currency_staging (
  Currency_Code,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct Case
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN msp_l.PurchaseCurrencyCode = 'NaN'
      AND right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
      ELSE msp_l.PurchaseCurrencyCode
    END AS Currency_Code,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
    CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC

    FROM
    silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header as msp_h
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.inf_msp_usage_line AS msp_l on msp_h.BizTalkGuid = msp_l.BizTalkGuid
    and msp_h.Sys_DatabaseName = msp_l.Sys_DatabaseName
    and msp_h.Sys_Silver_IsCurrent = 1
    and msp_l.Sys_Silver_IsCurrent = 1
WHERE 
  case when right(msp_h.Sys_DatabaseName, 2) IN ('DE','FR') AND msp_h.VENDORDimensionValue IN 
  (
   'RFT'
  ,'HOS'
  ,'SOW'
  ,'LIG'
  ,'KAS'
  ,'KAS_MSP'
  ,'DAT'
  ,'BUS'
  ,'ITG'
  ,'AR'
  ,'HP'
  ) THEN 1 ELSE 0 END = 0
  and msp_l.PurchaseCurrencyCode is not null

""")
