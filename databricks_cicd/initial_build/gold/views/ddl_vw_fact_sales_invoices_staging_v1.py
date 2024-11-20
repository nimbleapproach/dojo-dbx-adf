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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_invoices_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_invoices_staging AS
WITH cte_sources AS 
(
  SELECT DISTINCT source_system_pk, source_entity FROM {catalog}.{schema}.dim_source_system s 
  WHERE s.source_system = 'Infinigate ERP' AND s.is_current = 1
),
min_fx_rate AS 
(
  SELECT Currency, MIN(Period_FX_rate) AS Min_Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate r
  WHERE lower(ScenarioGroup) = 'actual' 
  AND Calendar_Year = (SELECT MIN(Calendar_Year) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Currency = m.Currency)
  AND Month = (SELECT MIN(Month) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Calendar_Year = m.Calendar_Year AND r.Currency = m.Currency)
  GROUP BY Currency 
)
--sales invoices
SELECT
  sil.sid AS local_fact_id, --
  sil.DocumentNo_ As local_document_id, -- JOIN to dim_sales_document
  sil.LineNo_ AS document_line_number,
  'N/A' AS associated_document_line_number,
  sil.Description AS description,
  sil.Gen_Prod_PostingGroup AS gen_prod_posting_group,
  SIL.Gen_Bus_PostingGroup AS gen_bus_posting_group,
  sih.CurrencyFactor AS currency_factor,
  TO_DATE(sih.PostingDate) AS document_date, --JOIN to dim_date
  'sales invoice' AS document_source, 
  --concat(right(sih.Sys_DatabaseName, 2), '1') as EntityCode,
  COALESCE(it.No_, sil.No_, 'N/A') AS product_code, -- JOIN to dim_product
  CASE WHEN it.No_ IS NOT NULL THEN 'item' ELSE 'Sales Invoice Line Item' END AS line_item_type,
  COALESCE(it.ProductType, 'N/A') AS product_type,
  COALESCE(ven1.Code, ven.Code, 'N/A') AS vendor_code,
  COALESCE(cu.No_, 'N/A') AS reseller_code,
  COALESCE(sil.manufactureritemno_, 'N/A') AS manufacturer_item_number,
  CASE
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT','BE')  THEN 'EUR'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
      ELSE sih.CurrencyCode
  END AS currency_code,  
  coalesce(
    CASE WHEN sih.Sys_DatabaseName ='ReportsBE' AND TO_DATE(sih.PostingDate) >='2024-07-01' THEN 'BE4' 
       ELSE CONCAT(RIGHT(sih.Sys_DatabaseName, 2),'1') 
    END, 
    'N/A') AS entity_code,
  COALESCE(s.source_system_pk,-1) AS source_system_fk,
  CAST( CASE WHEN sih.CurrencyFactor > 0 THEN Amount / sih.CurrencyFactor ELSE Amount END AS DECIMAL(10, 2)) AS amount_local_currency ,
  CAST(
      CASE WHEN sih.CurrencyFactor > 0 THEN Amount / sih.CurrencyFactor ELSE Amount END 
      / 
      COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS DECIMAL(10,2)
  ) AS amount_EUR ,
  NULL AS amount_including_vat_local_currency ,
  NULL AS amount_including_vat_EUR,
  CAST(CASE WHEN sil.Quantity > 0 THEN sil.UnitCostLCY * sil.Quantity *(-1) ELSE 0 END AS DECIMAL(10, 2)) AS cost_amount_local_currency ,
  CAST(
    CASE WHEN sil.Quantity > 0 THEN sil.UnitCostLCY * sil.Quantity *(-1) ELSE 0 END 
    / 
    COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS DECIMAL(10,2)
  ) AS cost_amount_EUR ,
  sil.Quantity AS quantity ,
  NULL AS total_cost_purchase_currency,
  NULL AS total_cost_EUR ,
  NULL AS unit_cost_local_currency,
  NULL AS unit_cost_purchase_currency ,
  NULL AS unit_cost_EUR,
  NULL AS unit_price,
  sil.Sys_Silver_InsertDateTime_UTC as Sys_Gold_InsertedDateTime_UTC,
  sil.Sys_Silver_ModifedDateTime_UTC Sys_Gold_ModifiedDateTime_UTC

FROM silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil
INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih ON sih.No_ = sil.DocumentNo_
AND sih.Sys_DatabaseName = sil.Sys_DatabaseName

LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
AND sil.Sys_DatabaseName = it.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true

LEFT JOIN cte_sources s on LOWER(s.source_entity) = LOWER(right(sih.Sys_DatabaseName,2))

LEFT JOIN 
(
  SELECT Code,Name,Sys_DatabaseName FROM silver_{ENVIRONMENT}.igsql03.dimension_value WHERE lower(DimensionCode) = 'vendor' AND Sys_Silver_IsCurrent = true
) ven ON it.GlobalDimension1Code = ven.Code
AND it.Sys_DatabaseName = ven.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true

LEFT JOIN (
          SELECT Code,Name,Sys_DatabaseName FROM silver_{ENVIRONMENT}.igsql03.dimension_value WHERE lower(DimensionCode) = 'vendor' AND Sys_Silver_IsCurrent = true
) ven1 ON sil.ShortcutDimension1Code = ven1.Code
AND sil.Sys_DatabaseName = ven1.Sys_DatabaseName

LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON cu.Sys_DatabaseName = sih.Sys_DatabaseName
AND cu.Sys_Silver_IsCurrent = TRUE
AND cu.No_ = sih.`Sell-toCustomerNo_`

LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = CAST(YEAR(TO_DATE(sih.PostingDate)) AS string)
AND e.Month = RIGHT(CONCAT('0',CAST(MONTH(TO_DATE(sih.PostingDate)) AS string)),2)
AND CASE WHEN   
        CASE WHEN sih.Sys_DatabaseName ='ReportsBE' AND TO_DATE(sih.PostingDate) >='2024-07-01' 
        THEN 'BE4' ELSE CONCAT(RIGHT(sih.Sys_DatabaseName,2 ),'1') END = 'BE1' THEN 'NL1' 
      ELSE 
        CASE WHEN sih.Sys_DatabaseName ='ReportsBE' AND TO_DATE(sih.PostingDate) >='2024-07-01' THEN 'BE4' ELSE CONCAT(RIGHT(sih.Sys_DatabaseName,2 ),'1') END  
END = e.COD_AZIENDA 
AND lower(e.ScenarioGroup) = 'actual'

LEFT JOIN min_fx_rate mfx on mfx.currency =  CASE
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT','BE')  THEN 'EUR'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
      WHEN sih.CurrencyCode = 'NaN' AND RIGHT(sih.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
      ELSE sih.CurrencyCode
    END
WHERE sih.Sys_Silver_IsCurrent = true
AND sil.Sys_Silver_IsCurrent = true
AND sil.sid IS NOT NULL
--limit(100)
"""
)