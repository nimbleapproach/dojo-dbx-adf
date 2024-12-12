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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_invoices_netsafe_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_invoices_netsafe_staging AS
WITH cte_sources AS 
(
  SELECT DISTINCT source_system_pk, source_entity FROM {catalog}.{schema}.dim_source_system s 
  WHERE s.source_system = 'Netsafe ERP' AND s.is_current = 1
) ,
min_fx_rate AS 
(
  SELECT Currency, MIN(Period_FX_rate) AS Min_Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate r
  WHERE lower(ScenarioGroup) = 'actual' 
  AND Calendar_Year = (SELECT MIN(Calendar_Year) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Currency = m.Currency)
  AND Month = (SELECT MIN(Month) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Calendar_Year = m.Calendar_Year AND r.Currency = m.Currency)
  GROUP BY Currency 
)
SELECT DISTINCT 
    invoice.SID AS local_fact_id,
    coalesce(invoice.Invoice_Number,'N/A') AS local_document_id,
    Invoice_Line_Nb AS document_line_number,
    'N/A' AS associated_document_line_number,
    COALESCE(invoice.SKU_Description,'N/A') AS description,
    'N/A' AS gen_prod_posting_group,
    'N/A' AS gen_bus_posting_group,
    0 AS currency_factor,
    to_date(invoice.Invoice_Date) AS document_date,
    'netsafe sales invoice' AS document_source,
    CASE WHEN trim(invoice.SKU) = 'NaN' THEN 'N/A' ELSE COALESCE(trim(invoice.SKU), "N/A") END as product_code,
    'Netsafe Line Item' AS line_item_type,
    COALESCE(invoice.Item_Type,'N/A') AS product_type,
    CASE WHEN invoice.Vendor_ID = 'NaN' then UPPER(invoice.Vendor_Name) ELSE coalesce(invoice.Vendor_ID, 'N/A') END AS vendor_code,
    CASE WHEN invoice.Customer_Account = 'NaN' THEN 'N/A' ELSE coalesce(invoice.Customer_Account,'N/A') END AS reseller_code,
    'N/A' AS manufacturer_item_number,
    Transaction_Currency AS currency_code, 
    CASE 
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2' 
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2' 
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1' 
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1' 
    END AS entity_code,
    COALESCE(s.source_system_pk,-1) AS source_system_fk,
    cast(Revenue_Transaction_Currency as DECIMAL(10, 2)) AS amount_local_currency,
    CAST(Revenue_Transaction_Currency / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) )  AS amount_EUR,
    NULL AS amount_including_vat_local_currency,
    NULL AS amount_including_vat_EUR,
    cast(Cost_Transaction_Currency as DECIMAL(10, 2)) AS cost_amount_local_currency,
    CAST(cast(Cost_Transaction_Currency as DECIMAL(10, 2)) / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) ) AS cost_amount_EUR,
    1 AS quantity,
    NULL AS total_cost_purchase_currency,
    NULL AS total_cost_EUR,
    NULL AS unit_cost_local_currency,
    NULL AS unit_cost_purchase_currency,
    NULL AS unit_cost_EUR,
    NULL AS unit_price,
    Sys_Silver_InsertDateTime_UTC AS Sys_Gold_InsertedDateTime_UTC,
    Sys_Silver_ModifedDateTime_UTC  AS Sys_Gold_ModifiedDateTime_UTC
FROM 
  silver_{ENVIRONMENT}.netsafe.invoicedata AS invoice
LEFT JOIN
  gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr
ON
  trim(invoice.SKU) = trim(datanowarr.SKU)
--LEFT JOIN 
--(
--  SELECT DISTINCT ResellerID, ResellerGroupCode, ResellerGroupName, ResellerName, Entity
--  FROM silver_{ENVIRONMENT}.masterdata.resellergroups
--  WHERE InfinigateCompany = 'Nuvias'
--  AND Sys_Silver_IsCurrent = true
  /*
  Change Date [22/02/2024]
  Change BY [MS]
  Filter only relevant entities
  */
--  AND Entity IN ('RO2', 'HR2', 'SI1', 'BG1')
--) rg
--ON 
--  cast(invoice.Customer_Account as string) = rg.ResellerID

--AND
--  CASE
--WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
--WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
--WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
--WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
--END = rg.Entity

LEFT JOIN min_fx_rate mfx on mfx.currency = Transaction_Currency

LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = CAST(YEAR(TO_DATE(invoice.Invoice_Date)) AS string)
AND e.Month = RIGHT(CONCAT('0',CAST(MONTH(TO_DATE(invoice.Invoice_Date)) AS string)),2)
AND CASE
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
END = e.COD_AZIENDA 
AND lower(e.ScenarioGroup) = 'actual'
and e.currency = Transaction_Currency

LEFT JOIN cte_sources s on CASE
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
    END = s.source_entity

WHERE invoice.Sys_Silver_IsCurrent = true
AND invoice.SID is not null
--limit(100)
"""
)