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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_transaction_ig_enrich01_v1
              """)

# COMMAND ----------


spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_transaction_ig_enrich01_v1 as 
SELECT
    concat(right(msp_h.Sys_DatabaseName, 2), '1') as EntityCode,
    ss.source_system_pk as source_system_fk,
    msp_h.BizTalkGuid,
    cast(msp_h.DocumentDate as date) as DocumentDate,
    case
      when msp_h.CreditMemo = '1' THEN msp_h.SalesCreditMemoNo_
      else msp_h.SalesInvoiceNo_
    end as DocumentNo,
    case
      when msp_h.CreditMemo = '1' THEN msp_l.SalesCreditMemoLineNo_
      else msp_l.SalesInvoiceLineNo_
    end as DocumentLineNo,
    Case
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
    END AS CurrencyCode,
    /*[yz] 2024-03-22 add local currency filed to convert purchase currency back from EUR to LCY*/
    Case
      WHEN  right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT')  THEN 'EUR'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
        END AS LocalCurrencyCode,
    msp_h.VENDORDimensionValue,
    msp_l.LineNo_ as MSPLineNo,
    msp_l.ItemNo_,
    msp_l.Quantity,
    case
      when msp_h.CreditMemo = '1' THEN msp_l.TotalPrice *(-1)
      else msp_l.TotalPrice
    end as TotalPrice,

    cast (
      case
        when msp_l.PurchaseCurrencyCode = 'NaN' and coalesce( msp_l.TotalCostPCY,0 ) =0 
          then msp_l.UnitCostPCY * msp_l.Quantity
        when msp_l.PurchaseCurrencyCode = 'NaN'
          then msp_l.TotalCostPCY
        when msp_l.PurchaseCurrencyCode != 'NaN' and coalesce( msp_l.TotalCostPCY,0 ) =0  
          then( msp_l.UnitCostPCY * msp_l.Quantity)/ fx.Period_FX_rate * fx2.Period_FX_rate
        when msp_l.PurchaseCurrencyCode != 'NaN'
          then msp_l.TotalCostPCY / fx.Period_FX_rate * fx2.Period_FX_rate
      end *(-1) as decimal(10, 2)
    ) as TotalCostLCY
  FROM
    silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header as msp_h
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(msp_h.Sys_DatabaseName, 2)
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.inf_msp_usage_line AS msp_l on msp_h.BizTalkGuid = msp_l.BizTalkGuid
    and msp_h.Sys_DatabaseName = msp_l.Sys_DatabaseName
    and msp_h.Sys_Silver_IsCurrent = 1
    and msp_l.Sys_Silver_IsCurrent = 1
    left join (
      SELECT
        DISTINCT Calendar_Year,
        Month,
        Currency,
        Period_FX_rate
      FROM
        gold_dev.obt.exchange_rate
      WHERE
        ScenarioGroup = 'Actual'
    ) fx on msp_l.PurchaseCurrencyCode = fx.Currency
    and year(msp_h.DocumentDate) = fx.Calendar_Year
    and month(msp_h.DocumentDate) = fx.Month
    left join (
      SELECT
        DISTINCT Calendar_Year,
        Month,
        Currency,
        Period_FX_rate
      FROM
        gold_dev.obt.exchange_rate
      WHERE
        ScenarioGroup = 'Actual'
    ) fx2 on Case
      WHEN  right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
        END = fx2.Currency
    and year(msp_h.DocumentDate) = fx2.Calendar_Year
    and month(msp_h.DocumentDate) = fx2.Month
      WHERE case when right(msp_h.Sys_DatabaseName, 2) IN('DE','FR') AND msp_h.VENDORDimensionValue IN (
  -- [yz]#21165  03.09.2024 accrued vendor exclusion
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
  ,'HP') THEN 1
  ELSE  0
  END =0
""")
