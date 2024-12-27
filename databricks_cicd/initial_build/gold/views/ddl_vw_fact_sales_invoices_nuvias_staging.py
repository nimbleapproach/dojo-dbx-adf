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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_invoices_nuvias_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_invoices_nuvias_staging AS

WITH cte_sources AS 
(
  SELECT DISTINCT source_system_pk, s.source_entity FROM {catalog}.{schema}.dim_source_system s 
  WHERE s.source_system = 'Nuvias ERP' AND s.is_current = 1
) ,
min_fx_rate AS 
(
  SELECT Currency, MIN(Period_FX_rate) AS Min_Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate r
  WHERE lower(ScenarioGroup) = 'actual' 
  AND Calendar_Year = (SELECT MIN(Calendar_Year) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Currency = m.Currency)
  AND Month = (SELECT MIN(Month) FROM gold_{ENVIRONMENT}.obt.exchange_rate m WHERE lower(ScenarioGroup) = 'actual' AND r.Calendar_Year = m.Calendar_Year AND r.Currency = m.Currency)
  GROUP BY Currency 
),
cte_nuvias_erp as 
(
--Nuvias ERP
SELECT 
  trans.sid AS local_fact_id, --
  CASE WHEN trans.SalesId = 'NaN' OR trans.SalesId is NULL THEN 'N/A' ELSE trans.SalesId END AS local_document_id, --JOINs to header 
  trans.LineNum AS document_line_number,
  'N/A' AS associated_document_line_number,
  COALESCE(itdesc.Description, 'N/A') AS description,
  'N/A' AS gen_prod_posting_group,
  'N/A' AS gen_bus_posting_group,
  0.00 AS currency_factor,
  to_date(trans.InvoiceDate)  AS document_date, --JOIN to dim_date
  NULL as deferred_revenue_startdate, 
  NULL as deferred_revenue_enddate, 
  0 as is_deferred,
   'nuvias sales invoice' AS document_source, 
   COALESCE(it.Description, 'N/A') AS product_code, -- JOIN to dim_product
  CASE WHEN it.DataAreaId IS NOT NULL THEN CONCAT(it.DataAreaId,' Line Item') ELSE 'N/A' END AS line_item_type,
  COALESCE('N/A') AS product_type,
  coalesce(disit.PrimaryVendorID, 'N/A') AS vendor_code,
  COALESCE(inv.InvoiceAccount,'N/A') AS reseller_code,
  'N/A' AS manufacturer_item_number,
  trans.CurrencyCode AS currency_code,  
  UPPER(entity.TagetikEntityCode) AS entity_code,
  COALESCE(s.source_system_pk,-1) AS source_system_fk,
  CAST(trans.LineAmountMST AS DECIMAL(10, 2)) AS amount_local_currency,
  NULL AS amount_including_vat_local_currency ,
  NULL AS amount_including_vat_EUR,
  trans.qty AS quantity ,
  NULL AS total_cost_purchase_currency,
  NULL AS total_cost_EUR ,
  NULL AS unit_cost_local_currency,
  NULL AS unit_cost_purchase_currency ,
  NULL AS unit_cost_EUR,
  NULL AS unit_price,
  trans.Sys_Silver_InsertDateTime_UTC as Sys_Gold_InsertedDateTime_UTC,
  trans.Sys_Silver_ModifedDateTime_UTC Sys_Gold_ModifiedDateTime_UTC,
  --needed for later join 
  trans.InvoiceId,
  trans.ItemId,
  trans.DataAreaId
FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicetrans AS trans

LEFT JOIN cte_sources s on trans.dataareaid = s.source_entity

LEFT JOIN (
  SELECT InvoiceId,DataAreaId,InvoiceAccount,MAX(InvoicingName) AS InvoicingName
  FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicejour
  WHERE Sys_Silver_IsCurrent = 1
  GROUP BY InvoiceId,DataAreaId,InvoiceAccount
  ) AS inv ON trans.InvoiceId = inv.InvoiceId
AND trans.DataAreaId = inv.DataAreaId

LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem AS it ON trans.ItemId = it.ItemId
AND trans.DataAreaId = it.DataAreaId
AND it.Sys_Silver_IsCurrent = 1

LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.ecoresproducttranslation AS itdesc ON it.Description = itdesc.Name
AND itdesc.Sys_Silver_IsCurrent = 1

LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.ecoresproduct AS prod ON trans.ItemId = prod.DisplayProductNumber
AND prod.Sys_Silver_IsCurrent = 1

LEFT JOIN silver_{ENVIRONMENT}.nuav_prod_sqlbyod.dbo_v_distinctitems AS disit ON trans.ItemId = disit.ItemId
AND CASE
  WHEN UPPER(trans.DataAreaId) = 'NUK1' then 'NGS1'
  WHEN UPPER(trans.DataAreaId) IN ('NPO1','NDK1','NNO1','NAU1','NCH1','NSW1','NFR1','NNL1','NES1','NDE1','NFI1') then 'NNL2'
  ELSE UPPER(trans.DataAreaId)
  END = CASE
  WHEN UPPER(disit.CompanyID) = 'NUK1' then 'NGS1'
  WHEN UPPER(disit.CompanyID) IN ('NPO1','NDK1','NNO1','NAU1','NCH1','NSW1','NFR1','NNL1','NES1','NDE1','NFI1') then 'NNL2'
  ELSE UPPER(disit.CompanyID)
END
AND disit.Sys_Silver_IsCurrent = true

LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = it.Description

LEFT JOIN (
  SELECT DISTINCT AccountNum, CREATEDDATETIME, DataAreaId
  FROM silver_{ENVIRONMENT}.nuvias_operations.custtable
  WHERE Sys_Silver_IsCurrent = 1
) AS cust ON inv.InvoiceAccount = cust.AccountNum
AND inv.DataAreaId = cust.DataAreaId

LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.salesline AS salestrans ON trans.SalesId = salestrans.Salesid
AND salestrans.Sys_Silver_IsCurrent = 1
AND salestrans.SalesStatus <> '4' --This is removed as it identifies cancelled lines on the sales order
AND trans.DataAreaId = salestrans.DataAreaId
AND trans.ItemId = salestrans.ItemId
AND trans.InventTransId = salestrans.InventTransId

LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem as so_it ON salestrans.ItemId = so_it.ItemId
AND so_it.DataAreaId = salestrans.DataAreaId
AND so_it.Sys_Silver_IsCurrent = 1

LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON upper(trans.DataAreaId) = entity.SourceEntityCode 

WHERE trans.Sys_Silver_IsCurrent = 1
),
invoice as (
  select
  local_fact_id, --
  local_document_id, --JOINs to header 
  document_line_number,
  associated_document_line_number,
  description,
  gen_prod_posting_group,
  gen_bus_posting_group,
  currency_factor,
  document_date, --JOIN to dim_date
  deferred_revenue_startdate, 
  deferred_revenue_enddate, 
  is_deferred,
  document_source, 
  product_code, -- JOIN to dim_product
  line_item_type,
  product_type,
  vendor_code,
  reseller_code,
  manufacturer_item_number,
  currency_code,  
  entity_code,
  source_system_fk,
  sum(amount_local_currency) AS amount_local_currency,
  amount_including_vat_local_currency ,
  amount_including_vat_EUR,
  sum(quantity) as quantity ,
  total_cost_purchase_currency,
  total_cost_EUR ,
  unit_cost_local_currency,
  unit_cost_purchase_currency ,
  unit_cost_EUR,
  unit_price,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC,
  InvoiceId,
  ItemId,
  DataAreaId

  from
    cte_nuvias_erp
  group by
    all
)

select
  local_fact_id, --
  local_document_id, --JOINs to header 
  document_line_number,
  associated_document_line_number,
  description,
  gen_prod_posting_group,
  gen_bus_posting_group,
  currency_factor,
  document_date, --JOIN to dim_date
  deferred_revenue_startdate, 
  deferred_revenue_enddate, 
  is_deferred,
  document_source, 
  product_code, -- JOIN to dim_product
  line_item_type,
  product_type,
  vendor_code,
  reseller_code,
  manufacturer_item_number,
  currency_code,  
  entity_code,
  source_system_fk,
  amount_local_currency,
  CAST(
  CAST(amount_local_currency AS DECIMAL(10, 2)) 
  / 
  COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS decimal(10, 2)
    ) AS amount_EUR ,

  amount_including_vat_local_currency ,
  amount_including_vat_EUR,
  CAST(inventtrans.CostAmountPosted as decimal(10, 2)) AS cost_amount_local_currency ,
  CAST(
    cast(inventtrans.CostAmountPosted as decimal(10, 2)) 
    / 
    COALESCE(e.Period_FX_rate, mfx.Min_Period_FX_rate) AS DECIMAL(10,2)
  ) AS cost_amount_EUR ,
  quantity ,
  total_cost_purchase_currency,
  total_cost_EUR ,
  unit_cost_local_currency,
  unit_cost_purchase_currency ,
  unit_cost_EUR,
  unit_price,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC

FROM invoice i
LEFT JOIN (
  SELECT InvoiceId, ItemId, DataAreaId, SUM(CostAmountPosted) as CostAmountPosted, SUM(CostAmountAdjustment) as CostAmountAdjustment
  FROM silver_{ENVIRONMENT}.nuvias_operations.inventtrans
  WHERE DataAreaId NOT IN ('NNL2', 'NGS1')
  AND Sys_Silver_IsCurrent = 1
  GROUP BY all
  ) inventtrans ON i.InvoiceId = inventtrans.InvoiceId
AND i.ItemId = inventtrans.ItemId
AND i.DataAreaId = inventtrans.DataAreaId

LEFT JOIN gold_{ENVIRONMENT}.obt.exchange_rate e
ON e.Calendar_Year = cast(year(i.document_date) as string)
AND e.Month = right(concat('0',cast(month(i.document_date) as string)),2)
AND CASE WHEN i.Entity_Code = 'BE1' THEN 'NL1' ELSE  i.Entity_Code  END   = e.COD_AZIENDA
AND e.ScenarioGroup = 'Actual'
AND e.Currency = i.Currency_Code

--Only for VU and entitycode 'NOTINTAGETIK'
LEFT JOIN (SELECT DISTINCT Calendar_Year, Month, Currency, Period_FX_rate FROM gold_{ENVIRONMENT}.obt.exchange_rate WHERE ScenarioGroup = 'Actual') e1
ON e1.Calendar_Year = cast(year(i.document_date) as string)
AND e1.Month = right(concat('0',cast(month(i.document_date) as string)),2)
AND i.Currency_Code = cast(e1.Currency as string)

LEFT JOIN min_fx_rate mfx 
ON i.Currency_Code = cast(mfx.Currency as string)

"""
)