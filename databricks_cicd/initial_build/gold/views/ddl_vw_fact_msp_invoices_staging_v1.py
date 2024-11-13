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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_msp_invoices_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_msp_invoices_staging AS
WITH cte_sources AS 
(
  SELECT DISTINCT source_system_pk, source_entity FROM {catalog}.{schema}.dim_source_system s 
  WHERE s.source_system = 'Infinigate ERP' AND s.is_current = 1
) ,
min_fx_rate AS 
(
  SELECT Currency, MIN(Period_FX_rate) AS Min_Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate r
  WHERE lower(ScenarioGroup) = 'actual' 
  AND Calendar_Year = (SELECT MIN(Calendar_Year) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Currency = m.Currency)
  AND Month = (SELECT MIN(Month) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Calendar_Year = m.Calendar_Year AND r.Currency = m.Currency)
  GROUP BY Currency 
)
--MSP
SELECT 
  msp_l.sid AS local_fact_id, --
  msp_l.BizTalkGuid AS local_document_id, --JOINs to header 
  msp_l.LineNo_ AS document_line_number,
  CASE WHEN msp_h.CreditMemo = '1' THEN msp_l.SalesCreditMemoLineNo_ ELSE msp_l.SalesInvoiceLineNo_ END AS associated_document_line_number,
  msp_l.Description AS description,
  'N/A' AS gen_prod_posting_group,
  'N/A' AS gen_bus_posting_group,
  0.00 AS currency_factor,
  CAST(msp_h.DocumentDate AS date) AS document_date, --JOIN to dim_date
  CONCAT('msp ' , CASE WHEN msp_h.CreditMemo = '1' THEN 'sales credit memo' ELSE 'sales invoice' END ) AS document_source, 
  --concat(right(msp_h.Sys_DatabaseName, 2), '1') as EntityCode,
  COALESCE(msp_l.ItemNo_, 'N/A') AS product_code, -- JOIN to dim_product
  CASE WHEN it.No_ IS NOT NULL THEN 'item' ELSE 'MSP Line Item' END AS line_item_type,
  COALESCE('N/A') AS product_type,
  COALESCE(ven.code, 'N/A') AS vendor_code,
  COALESCE('N/A') AS reseller_code,
  'N/A' AS manufacturer_item_number,
  CASE
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
    ELSE msp_l.PurchaseCurrencyCode
  END AS currency_code,  
  coalesce(
    CASE WHEN msp_h.Sys_DatabaseName ='ReportsBE' AND TO_DATE(msp_h.DocumentDate) >='2024-07-01' THEN 'BE4' 
       ELSE CONCAT(RIGHT(msp_h.Sys_DatabaseName, 2),'1') 
    END, 
    'N/A') AS entity_code,
  COALESCE(s.source_system_pk,-1) AS source_system_fk,
  CASE WHEN msp_h.CreditMemo = '1' THEN msp_l.TotalPrice *(-1) ELSE msp_l.TotalPrice END AS amount_local_currency ,
  CAST(
    CASE WHEN msp_h.CreditMemo = '1' THEN msp_l.TotalPrice *(-1) ELSE msp_l.TotalPrice END 
    / 
    COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10, 2)
  ) AS amount_EUR ,
  NULL AS amount_including_vat_local_currency ,
  NULL AS amount_including_vat_EUR,
  CAST (
      CASE
        WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND COALESCE( msp_l.TotalCostPCY,0 ) = 0 THEN msp_l.UnitCostPCY * msp_l.Quantity
        WHEN msp_l.PurchaseCurrencyCode = 'NaN' THEN msp_l.TotalCostPCY
        WHEN msp_l.PurchaseCurrencyCode != 'NaN' AND COALESCE( msp_l.TotalCostPCY,0 ) = 0 
          THEN( msp_l.UnitCostPCY * msp_l.Quantity)/ fx.Period_FX_rate * fx2.Period_FX_rate
        WHEN msp_l.PurchaseCurrencyCode != 'NaN' THEN msp_l.TotalCostPCY / fx.Period_FX_rate * fx2.Period_FX_rate
      END * (-1) AS decimal(10, 2)
  ) AS cost_amount_local_currency,
  CAST (
      CASE
        WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND COALESCE( msp_l.TotalCostPCY,0 ) = 0  THEN msp_l.UnitCostPCY * msp_l.Quantity
        WHEN msp_l.PurchaseCurrencyCode = 'NaN' THEN msp_l.TotalCostPCY
        WHEN msp_l.PurchaseCurrencyCode != 'NaN' AND COALESCE( msp_l.TotalCostPCY,0 ) = 0  
          THEN( msp_l.UnitCostPCY * msp_l.Quantity)/ fx.Period_FX_rate * fx2.Period_FX_rate
        WHEN msp_l.PurchaseCurrencyCode != 'NaN' THEN msp_l.TotalCostPCY / fx.Period_FX_rate * fx2.Period_FX_rate
      END *(-1) 
      / 
      COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10, 2)
  ) AS cost_amount_EUR,
  msp_l.Quantity AS quantity ,
  NULL AS total_cost_purchase_currency,
  NULL AS total_cost_EUR ,
  NULL AS unit_cost_local_currency,
  NULL AS unit_cost_purchase_currency ,
  NULL AS unit_cost_EUR,
  NULL AS unit_price,
  msp_l.Sys_Silver_InsertDateTime_UTC as Sys_Gold_InsertedDateTime_UTC,
  msp_l.Sys_Silver_ModifedDateTime_UTC Sys_Gold_ModifiedDateTime_UTC

FROM  silver_{ENVIRONMENT}.igsql03.inf_msp_usage_line AS msp_l

INNER JOIN silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header AS msp_h on msp_h.BizTalkGuid = msp_l.BizTalkGuid
AND msp_h.Sys_DatabaseName = msp_l.Sys_DatabaseName
AND msp_h.Sys_Silver_IsCurrent = true

LEFT JOIN (SELECT DISTINCT Calendar_Year, Month, Currency, Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate WHERE lower(ScenarioGroup) = 'actual'
) fx on msp_l.PurchaseCurrencyCode = fx.Currency
AND YEAR(msp_h.DocumentDate) = fx.Calendar_Year
AND MONTH(msp_h.DocumentDate) = fx.Month

LEFT JOIN (SELECT DISTINCT Calendar_Year,Month,Currency,Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate WHERE lower(ScenarioGroup) = 'actual'
) fx2 ON CASE
  WHEN  RIGHT(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
  WHEN RIGHT(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
  WHEN RIGHT(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
  WHEN RIGHT(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
  WHEN RIGHT(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
  WHEN RIGHT(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
  END = fx2.Currency
AND YEAR(msp_h.DocumentDate) = fx2.Calendar_Year
AND MONTH(msp_h.DocumentDate) = fx2.Month

LEFT JOIN cte_sources s on LOWER(s.source_entity) = LOWER(msp_h.Sys_DatabaseName)

LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON msp_l.ItemNo_ = it.No_
AND msp_l.Sys_DatabaseName = it.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true

LEFT JOIN (SELECT Code,Name,Sys_DatabaseName FROM silver_{ENVIRONMENT}.igsql03.dimension_value WHERE lower(DimensionCode) = 'vendor' AND Sys_Silver_IsCurrent = true
) ven ON it.GlobalDimension1Code = ven.Code
AND it.Sys_DatabaseName = ven.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true

LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = CAST(YEAR(TO_DATE(msp_h.PostingDate)) AS string)
AND e.Month = RIGHT(CONCAT('0',CAST(MONTH(TO_DATE(msp_h.PostingDate)) AS string)),2)
AND CASE WHEN   
        CASE WHEN msp_h.Sys_DatabaseName ='ReportsBE' AND TO_DATE(msp_h.PostingDate) >='2024-07-01' 
        THEN 'BE4' ELSE CONCAT(RIGHT(msp_h.Sys_DatabaseName, 2),'1') END = 'BE1' THEN 'NL1' 
      ELSE 
        CASE WHEN msp_h.Sys_DatabaseName ='ReportsBE' AND TO_DATE(msp_h.PostingDate) >='2024-07-01' THEN 'BE4' ELSE CONCAT(RIGHT(msp_h.Sys_DatabaseName,2 ),'1') END  
END = e.COD_AZIENDA 
AND lower(e.ScenarioGroup) = 'actual'

LEFT JOIN min_fx_rate mfx ON mfx.currency =  CASE
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
    WHEN msp_l.PurchaseCurrencyCode = 'NaN' AND RIGHT(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
    ELSE msp_l.PurchaseCurrencyCode
END
WHERE msp_l.Sys_Silver_IsCurrent = true
AND msp_l.sid IS NOT NULL
LIMIT(100)
"""
)