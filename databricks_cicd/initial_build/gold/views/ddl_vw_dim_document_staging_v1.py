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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_document_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_document_staging  
AS 
with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.is_current = 1
),
cte_source_data as 
(
--orders & quotes
select distinct
  sla.DocumentNo_ As local_document_id,
  'N/A' as associated_document_id,
  case when sha.DocumentDate = 'NaN' then to_date('1990-12-31') else to_date(coalesce(sha.DocumentDate,'1990-12-31')) end AS document_date,
  case 
    when sla.DocumentType = 0 then 'sales quote' 
    when sla.DocumentType = 1 then 'sales order' 
    else 'N/A' 
  end as document_source,
  CAST(sla.DocumentType AS STRING) as document_type,
  'N/A' as document_status,
  replace(sla.Sys_DatabaseName,"Reports","") as country_code,
  'N/A' as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) as source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(sla.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(sla.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
FROM 
  silver_{ENVIRONMENT}.igsql03.sales_line_archive as sla
  inner join (
    select No_, Sys_DatabaseName, max(DocumentDate) DocumentDate, max(VersionNo_) VersionNo_
    from silver_{ENVIRONMENT}.igsql03.sales_header_archive where Sys_Silver_IsCurrent = 1 
    group by No_, Sys_DatabaseName
  ) as sha on sla.DocumentNo_ = sha.No_
  and sla.Doc_No_Occurrence = 1 
  and sla.VersionNo_ = sha.VersionNo_
  and sla.Sys_DatabaseName = sha.Sys_DatabaseName
  and sla.Sys_Silver_IsCurrent = 1
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sha.Sys_DatabaseName,2))
AND s.source_system = 'Infinigate ERP'
GROUP BY ALL
union all
--msp
select distinct
  msp_h.biztalkguid as local_document_id,
  case
        when msp_h.CreditMemo = '1' THEN msp_h.SalesCreditMemoNo_
        else msp_h.SalesInvoiceNo_
  end as associated_document_id,
  cast(msp_h.DocumentDate as date) as document_date,
  concat('msp ' , case when msp_h.CreditMemo = '1' then 'sales credit memo' else 'sales invoice' end ) as document_source,
  'N/A' as document_type,
  'N/A' as document_status,
  replace(msp_h.Sys_DatabaseName,"Reports","") as country_code,
  msp_h.vendordimensionvalue as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) as source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(msp_h.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(msp_h.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
from silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header as msp_h
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(msp_h.Sys_DatabaseName,2))
AND s.source_system = 'Infinigate ERP'
where  msp_h.Sys_Silver_IsCurrent = true
GROUP BY ALL
union all
--invoices
select distinct
  sil.DocumentNo_ AS local_document_id,
  'N/A' as associated_document_id,
  to_date(sih.PostingDate) AS document_date, 
  'sales invoice' as document_source,
  'N/A' as document_type,
  'N/A' as document_status,
  replace(sih.Sys_DatabaseName,"Reports","") as Country_Code,
  'N/A' as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) as source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(sil.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(sil.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil 
ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sih.Sys_DatabaseName,2))
AND s.source_system = 'Infinigate ERP'
GROUP BY ALL
union all
--credit memos
select distinct
  sil.DocumentNo_ AS local_document_id,
  'N/A' as associated_document_id,
  to_date(sih.PostingDate) AS document_date, 
  'credit memo' as document_source,
  'N/A' as document_type,
  'N/A' as document_status,
  replace(sih.Sys_DatabaseName,"Reports","") as Country_Code,
  'N/A' as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) as source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(sil.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(sil.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sih.Sys_DatabaseName,2))
AND s.source_system = 'Infinigate ERP'
GROUP BY ALL
UNION all
--Nuvias data
SELECT 
  trans.SalesId As local_document_id,
  'N/A' AS associated_document_id,
  MAX(to_date(trans.invoicedate)) AS document_date,
  'nuvias sales invoice' AS document_source,
  'N/A' as document_type,
  'N/A' as document_status,
  'N/A' AS country_code,
  'N/A' as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(trans.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(trans.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.nuvias_operations.custinvoicetrans AS trans
LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.salesline AS salestrans ON trans.SalesId = salestrans.Salesid
  AND salestrans.Sys_Silver_IsCurrent = 1
  AND salestrans.SalesStatus <> '4' --This is removed as it identifies cancelled lines on the sales order
  AND trans.DataAreaId = salestrans.DataAreaId
  AND trans.ItemId = salestrans.ItemId
  and trans.InventTransId = salestrans.InventTransId 
  AND salestrans.Sys_Silver_IsCurrent = 1
LEFT JOIN cte_sources s on trans.dataareaid = s.source_entity
AND s.source_system = 'Nuvias ERP'
WHERE trans.Sys_Silver_IsCurrent = 1
GROUP BY ALL
UNION all
--Netsafe data
SELECT 
  invoice.Invoice_Number As local_document_id,
  'N/A' AS associated_document_id,
  MAX(to_date(invoice.Invoice_Date)) AS document_date,
  'netsafe sales invoice' AS document_source,
  'N/A' as document_type,
  'N/A' as document_status,
  'N/A' AS country_code,
  'N/A' as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(invoice.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(invoice.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
FROM  
  silver_{ENVIRONMENT}.netsafe.invoicedata AS invoice
LEFT JOIN cte_sources s on CASE
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
    END = s.source_entity
AND s.source_system = 'Netsafe ERP'
WHERE invoice.sys_silver_iscurrent = true
GROUP BY ALL
UNION all
--Netsuite data
SELECT 
  si.Invoice_Number as local_document_id,
  RIGHT(si.Sales_Order_Number,9) AS associated_document_id,
  TO_DATE(si.Sales_Order_Date) AS document_date,
  'starlink (netsuite) sales invoice' AS document_source,
  'N/A' AS country_code,
  CAST(si.Type as STRING) as document_type,
  si.status as document_status,
  'N/A' as vendor_dimension_value,
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(CAST(si.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(CAST(si.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
FROM
  silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
INNER JOIN (Select invoice_number,MAX(TO_DATE(si1.Sales_Order_Date)) as sales_order_date 
  FROM silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si1 
  GROUP BY ALL) as m 
ON m.invoice_number = si.invoice_number AND m.sales_order_date = si.Sales_Order_Date
LEFT JOIN cte_sources s on 'AE1' = s.source_entity AND s.source_system = 'Starlink (Netsuite) ERP'
WHERE si.sys_silver_iscurrent = true
GROUP BY ALL
UNION ALL
SELECT 
  local_document_id,
  associated_document_id,
  document_date,
  document_source,
  country_code,
  document_type,
  document_status,
  vendor_dimension_value,
  source_system_fk,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC
FROM {catalog}.{schema}.vw_dim_document_cloudblue_staging
)
SELECT
  csd.local_document_id,
  csd.associated_document_id,
  csd.document_date,
  csd.document_source,
  csd.document_type,
  csd.document_status,
  csd.country_code,
  csd.vendor_dimension_value,
  csd.source_system_fk,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
  csd.end_datetime,
  csd.is_current,
  MAX(csd.Sys_Gold_InsertedDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
  MAX(csd.Sys_Gold_ModifiedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_document d ON csd.local_document_id = d.local_document_id and csd.source_system_fk = d.source_system_fk and csd.document_source = d.document_source
WHERE csd.local_document_id IS NOT NULL
GROUP BY ALL
""")

