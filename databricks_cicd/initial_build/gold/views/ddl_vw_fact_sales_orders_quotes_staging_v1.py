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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_orders_quotes_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_orders_quotes_staging AS
WITH cte_sources AS 
(
  SELECT DISTINCT source_system_pk, reporting_source_database FROM {catalog}.{schema}.dim_source_system s 
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
SELECT 
  sla.sid AS local_fact_id, --
  sha.No_ As local_document_id, -- JOIN to dim_sales_document
  sla.LineNo_ AS document_line_number,
  'N/A' AS associated_document_line_number,
  'N/A' AS description,
  'N/A' AS gen_prod_posting_group,
  'N/A' AS gen_bus_posting_group,
  sha2.CurrencyFactor AS currency_factor,
  CAST(sha.DocumentDate AS date) AS document_date, --JOIN to dim_date
  CASE WHEN sla.DocumentType = 0 THEN 'sales quote' WHEN sla.DocumentType = 1 THEN 'sales order' ELSE 'N/A' END AS document_source, 
  COALESCE(sla.No_,'N/A') AS product_code,
  CASE WHEN it.No_ IS NOT NULL THEN 'item' ELSE 'Sales Archive Line Item' END AS line_item_type,
  COALESCE('N/A') AS product_type,
  COALESCE(ven.code, 'N/A') AS vendor_code,
  COALESCE('N/A') AS reseller_code,
  'N/A' AS manufacturer_item_number,
  CASE
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
    ELSE sha2.CurrencyCode
  END AS currency_code,  
 coalesce(
    CASE WHEN sha.Sys_DatabaseName ='ReportsBE' AND TO_DATE(sha.DocumentDate) >='2024-07-01' THEN 'BE4' 
       ELSE CONCAT(RIGHT(sha.Sys_DatabaseName, 2),'1') 
    END, 
    'N/A') AS entity_code,
  COALESCE(s.source_system_pk,-1) AS source_system_fk,
  sla.Amount AS amount_local_currency ,
  CAST(sla.Amount / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) ) AS amount_EUR ,
  sla.AmountIncludingVAT AS amount_including_vat_local_currency ,
  CAST(sla.AmountIncludingVAT / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) ) AS amount_including_vat_EUR,
  sla.CostAmountLCY AS cost_amount_local_currency,
  CAST(sla.CostAmountLCY / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) ) AS cost_amount_EUR,
  sla.Quantity AS quantity ,
  NULL AS total_cost_purchase_currency,
  NULL AS total_cost_EUR ,
  NULL AS unit_cost_local_currency,
  NULL AS unit_cost_purchase_currency ,
  NULL AS unit_cost_EUR,
  NULL AS unit_price,
  sla.Sys_Silver_InsertDateTime_UTC as Sys_Gold_InsertedDateTime_UTC,
  sla.Sys_Silver_ModifedDateTime_UTC Sys_Gold_ModifiedDateTime_UTC

FROM silver_{ENVIRONMENT}.igsql03.sales_line_archive AS sla

INNER JOIN 
  (
    SELECT No_,Sys_DatabaseName,max(DocumentDate) DocumentDate,max(VersionNo_) VersionNo_
    FROM silver_{ENVIRONMENT}.igsql03.sales_header_archive WHERE Sys_Silver_IsCurrent = true
    group by No_, Sys_DatabaseName
  ) AS sha ON sla.DocumentNo_ = sha.No_
AND sla.Doc_No_Occurrence = 1
AND sla.VersionNo_ = sha.VersionNo_
AND sla.Sys_DatabaseName = sha.Sys_DatabaseName

INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_header_archive sha2 ON sla.DocumentNo_ = sha2.No_
AND sla.Doc_No_Occurrence = 1
AND sla.VersionNo_ = sha2.VersionNo_
AND sla.Sys_DatabaseName = sha2.Sys_DatabaseName
AND sha2.Sys_Silver_IsCurrent = true

LEFT JOIN cte_sources s ON LOWER(s.reporting_source_database) = LOWER(sla.Sys_DatabaseName)

LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON it.No_ = sla.No_ 
AND it.Sys_Silver_IsCurrent = true 
AND it.Sys_DatabaseName = sla.Sys_DatabaseName

LEFT JOIN 
  (
    SELECT Code,Name,Sys_DatabaseName FROM silver_{ENVIRONMENT}.igsql03.dimension_value WHERE lower(DimensionCode) = 'vendor' AND Sys_Silver_IsCurrent = true
  ) ven ON it.GlobalDimension1Code = ven.Code
AND it.Sys_DatabaseName = ven.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true

LEFT JOIN silver_{ENVIRONMENT}.igsql03.customer cu ON sha2.`Sell-toCustomerNo_` = cu.No_
AND sha.Sys_DatabaseName = cu.Sys_DatabaseName
AND cu.Sys_Silver_IsCurrent = true

LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = CAST(YEAR(TO_DATE(sha.DocumentDate)) AS string)
AND e.Month = RIGHT(CONCAT('0',CAST(MONTH(TO_DATE(sha.DocumentDate)) AS string)),2)
AND CASE WHEN   
        CASE WHEN sha.Sys_DatabaseName ='ReportsBE' AND TO_DATE(sha.DocumentDate) >='2024-07-01' 
        THEN 'BE4' ELSE CONCAT(RIGHT(sha.Sys_DatabaseName, 2),'1') END = 'BE1' THEN 'NL1' 
      ELSE 
        CASE WHEN sha.Sys_DatabaseName ='ReportsBE' AND TO_DATE(sha.DocumentDate) >='2024-07-01' THEN 'BE4' ELSE CONCAT(RIGHT(sha.Sys_DatabaseName,2 ),'1') END  
END = e.COD_AZIENDA 
AND lower(e.ScenarioGroup) = 'actual'

LEFT JOIN min_fx_rate mfx on mfx.currency =  CASE
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT') THEN 'EUR'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
    WHEN sha2.CurrencyCode = 'NaN' AND RIGHT(sha.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
    ELSE sha2.CurrencyCode
  END
WHERE sla.Sys_Silver_IsCurrent = true
AND sla.sid IS NOT NULL
"""
)