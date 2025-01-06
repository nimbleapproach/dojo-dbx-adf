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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_invoices_starlink_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_invoices_starlink_staging AS
WITH cte_sources AS 
(
  SELECT DISTINCT source_system_pk, source_entity FROM {catalog}.{schema}.dim_source_system s 
  WHERE s.source_system = 'Starlink (Netsuite) ERP' AND s.is_current = 1
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
    si.SID AS local_fact_id,
    si.Invoice_Number as local_document_id,
    Line_ID AS document_line_number,
    'N/A' AS associated_document_line_number,
    COALESCE(it.Description,'N/A') AS description,
    coalesce(rg.ResellerGroupCode,'N/A') AS gen_prod_posting_group,
    coalesce(rg.ResellerGroupName,'N/A') AS gen_bus_posting_group,
    0 AS currency_factor,
    TO_DATE(si.Sales_Order_Date) AS document_date,
    NULL as deferred_revenue_startdate, 
    NULL as deferred_revenue_enddate, 
    0 as is_deferred,
    'starlink (netsuite) sales invoice' AS document_source,
    COALESCE(si.SKU_ID, 'N/A') AS product_code,
    'Starlink (Netsuite) Line Item' AS line_item_type,
    COALESCE(it.Item_Category,'N/A') AS product_type,
    coalesce(ven.Vendor_ID, si.SID,   'N/A') AS vendor_code,
    coalesce(rs.Reseller_Name,si.Reseller_Name,'N/A') AS reseller_code,
    'N/A' AS manufacturer_item_number,
    si.Deal_Currency AS currency_code, 
    'AE1'  AS entity_code,
    COALESCE(s.source_system_pk,-1)  AS source_system_fk,
    CAST(si.Revenue_USD AS DECIMAL(10, 2)) AS amount_local_currency,
    CAST(si.Revenue_USD / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) ) AS amount_EUR,
    NULL AS amount_including_vat_local_currency,
    NULL AS amount_including_vat_EUR,
    cASt((si.Revenue_USD - si.GP_USD)*(-1) AS DECIMAL(10, 2))  AS cost_amount_local_currency,
    CAST((si.Revenue_USD - si.GP_USD)*(-1) / COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10,2) )  AS cost_amount_EUR,
    si.quantity AS quantity,
    NULL AS total_cost_purchase_currency,
    NULL AS total_cost_EUR,
    NULL AS unit_cost_local_currency,
    NULL AS unit_cost_purchase_currency,
    NULL AS unit_cost_EUR,
    NULL AS unit_price,
    si.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_InsertedDateTime_UTC,
    si.Sys_Silver_ModifedDateTime_UTC  AS Sys_Gold_ModifiedDateTime_UTC

FROM silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdataSku AS it ON si.SKU_ID = it.SKU_ID
AND it.Sys_Silver_IsCurrent = 1
LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdataVendor AS ven ON si.Vendor_Name = ven.Vendor_Name
AND ven.Sys_Silver_IsCurrent is not false
LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdatareseller AS rs ON si.Reseller_Name = rs.Reseller_Name
AND rs.Sys_Silver_IsCurrent = 1
LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = it.SKU_ID
LEFT JOIN silver_{ENVIRONMENT}.masterdata.resellergroups AS rg 
ON si.Reseller_Name = rg.ResellerName
AND rg.InfinigateCompany = 'Starlink'
AND rg.Sys_Silver_IsCurrent = true
   
LEFT JOIN min_fx_rate mfx on mfx.currency = si.Deal_Currency

LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e ON e.Calendar_Year = CAST(YEAR(TO_DATE(si.Sales_Order_Date)) AS string)
AND e.Month = RIGHT(CONCAT('0',CAST(MONTH(TO_DATE(si.Sales_Order_Date)) AS string)),2)
AND 'AE1' = e.COD_AZIENDA 
AND lower(e.ScenarioGroup) = 'actual'
and e.currency = si.Deal_Currency

LEFT JOIN cte_sources s on 'AE1' = s.source_entity 

where si.Sys_Silver_IsCurrent = 1
AND si.SID is not null

"""
)