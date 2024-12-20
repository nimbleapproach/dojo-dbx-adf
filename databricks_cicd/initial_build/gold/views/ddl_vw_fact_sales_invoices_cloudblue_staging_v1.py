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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_invoices_cloudblue_staging
              """)

# COMMAND ----------

spark.sql(f"""CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_invoices_cloudblue_staging AS
WITH cte_sources AS 
(
  SELECT DISTINCT source_system,source_system_pk, s.source_entity FROM {catalog}.{schema}.dim_source_system s 
  WHERE s.source_system = 'Cloudblue PBA ERP' AND s.is_current = 1
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
  dd.SID AS local_fact_id,
  cast(ar.DocID AS STRING) AS local_document_id,
  'N/A' AS document_line_number,
  'N/A' AS associated_document_line_number,
  coalesce(dd.Descr,'N/A') AS description,
  'N/A' AS gen_prod_posting_group,
  'N/A' AS gen_bus_posting_group,
  1 AS currency_factor,
  to_date(cast(ar.DocDate AS TIMESTAMP)) AS document_date,
  to_date(cast(dd.detsdate AS TIMESTAMP)) AS deferred_revenue_startdate,
  to_date(cast(dd.detedate AS TIMESTAMP)) AS deferred_revenue_enddate,
  CASE WHEN 
    (months_between(to_date(cast(dd.detedate AS TIMESTAMP)), to_date(cast(dd.detsdate AS TIMESTAMP))) > 2) -- Not deffered revenune if < = 2 months
    AND
    (months_between(to_date(cast(dd.detedate AS TIMESTAMP)), to_date(cast(dd.detsdate AS TIMESTAMP))) < 120) -- ignore ten year revenue
  THEN 1 ELSE 0 END AS is_deferred,
  'cloudblue sales invoice' AS document_source,
  COALESCE(CAST(bm.ResourceID AS STRING), CASE WHEN trim(dd.SKU) = 'NaN' THEN 'N/A' ELSE trim(dd.SKU) END, 'N/A') AS product_code,
  CASE WHEN bm.ResourceID IS NULL THEN 'Cloudblue Line Item' ELSE 'Cloudblue Resource Item' END AS line_item_type,
  COALESCE(bm.BMResourceType,'N/A') AS product_type,
  cast(coalesce(CAST(bm.Manufacturer AS STRING),'N/A') AS string) AS vendor_code,
  cast(coalesce(CAST(ar.Customer_AccountID AS STRING),'N/A') AS string) AS reseller_code,
  'N/A' AS manufacturer_item_number,
  coalesce(ar.CurrencyID,'N/A') AS currency_code, 
  'N/A' AS entity_code,
  coalesce(s.source_system_pk, -1) AS source_system_fk,
  CASE WHEN ar.DocType = 80 
    THEN cast((coalesce(dd.ExtendedPrice_Value,0.00) * -1) AS DECIMAL(10,2)) 
    ELSE cast(coalesce(dd.ExtendedPrice_Value,0.00) AS DECIMAL(10,2)) 
  END AS amount_local_currency, 
  CASE WHEN ar.DocType = 80 
    THEN cast((coalesce(dd.ExtendedPrice_Value,0.00) * -1) AS DECIMAL(10,2)) 
    ELSE cast(coalesce(dd.ExtendedPrice_Value,0.00) AS DECIMAL(10,2)) 
  END 
  /
  CAST(COALESCE(e1.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10, 2)) AS amount_EUR ,

  CASE WHEN ar.DocType = 80 
    THEN cast((coalesce(dd.ExtendedPrice_Value,0.00) * -1) AS DECIMAL(10,2)) 
    ELSE cast(coalesce(dd.ExtendedPrice_Value,0.00) AS DECIMAL(10,2)) 
  END + dd.TaxAmt_Value AS amount_including_vat_local_currency ,

  (CASE WHEN ar.DocType = 80 
    THEN cast((coalesce(dd.ExtendedPrice_Value,0.00) * -1) AS DECIMAL(10,2)) 
    ELSE cast(coalesce(dd.ExtendedPrice_Value,0.00) AS DECIMAL(10,2)) 
  END + dd.TaxAmt_Value)
  /
  CAST(COALESCE(e1.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10, 2)) AS amount_including_vat_EUR,

  NULL AS cost_amount_local_currency,
  NULL / CAST(COALESCE(e1.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10, 2)) AS cost_amount_EUR,
  try_cast(dd.servqty as DECIMAL(10,2)) AS quantity,--cannot cast from float to decimal(10,2)
  NULL AS total_cost_purchase_currency,
  NULL AS total_cost_EUR ,
  NULL AS unit_cost_local_currency,
  NULL AS unit_cost_purchase_currency ,
  NULL AS unit_cost_EUR,
  NULL AS unit_price,
  dd.Sys_Silver_InsertDateTime_UTC as Sys_Gold_InsertedDateTime_UTC,
  dd.Sys_Silver_ModifedDateTime_UTC Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.cloudblue_pba.ARDoc ar
INNER JOIN silver_{ENVIRONMENT}.cloudblue_pba.DocDet dd
ON   ar.DocID = dd.DocID
AND  ar.Sys_Silver_IsCurrent = true
AND  dd.Sys_Silver_IsCurrent = true

LEFT JOIN cte_sources s ON s.source_system = 'Cloudblue PBA ERP'

LEFT OUTER JOIN  silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
ON  dd.resourceID = bm.resourceID
AND  bm.Sys_Silver_IsCurrent = true

--Only for VU and entitycode 'NOTINTAGETIK'
LEFT JOIN (SELECT DISTINCT Calendar_Year, Month, Currency, Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate WHERE ScenarioGroup = 'Actual') e1
ON e1.Calendar_Year = CAST(YEAR(to_date(cast(ar.DocDate AS TIMESTAMP))) AS string)
AND e1.Month = RIGHT(CONCAT('0',CAST(MONTH(to_date(cast(ar.DocDate AS TIMESTAMP))) AS string)),2)
AND e1.Currency = ar.CurrencyID

LEFT JOIN min_fx_rate mfx on mfx.currency =  ar.CurrencyID
""")
