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
              DROP VIEW IF {catalog}.{schema}.vw_dim_document_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_document_staging  (
  document_id COMMENT 'Business Key',
  document_date,
  document_source,
  country_code,
  source_system_fk,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
  sla.DocumentNo_ As document_id,
  case when sha.DocumentDate = 'NaN' then to_date('1990-12-31') else to_date(coalesce(sha.DocumentDate,'1990-12-31')) end AS document_date,
  'sales order' as document_source,
  RIGHT(sla.Sys_DatabaseName,2) as country_code,
  ss.source_system_pk as source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM 
  silver_{ENVIRONMENT}.igsql03.sales_line_archive as sla
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sla.Sys_DatabaseName, 2)
  inner join (
    select
      No_,
      Sys_DatabaseName,
      max(DocumentDate) DocumentDate,
      max(VersionNo_) VersionNo_
    from
      silver_{ENVIRONMENT}.igsql03.sales_header_archive
    where
      Sys_Silver_IsCurrent = 1
    group by
      No_,
      Sys_DatabaseName
  ) as sha on sla.DocumentNo_ = sha.No_
  and sla.DocumentType = 1
  and sla.Doc_No_Occurrence = 1
  and sla.VersionNo_ = sha.VersionNo_
  and sla.Sys_DatabaseName = sha.Sys_DatabaseName
  and sla.Sys_Silver_IsCurrent = 1
union
select distinct
  case
        when msp_h.CreditMemo = '1' THEN msp_h.SalesCreditMemoNo_
        else msp_h.SalesInvoiceNo_
      end as document_id,
  cast(msp_h.DocumentDate as date) as document_date,
  'msp' as document_source,
  replace(msp_h.Sys_DatabaseName,'Reports','') as country_code,
  (select source_system_pk from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) as source_system_id,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC  -- Case
  --       WHEN  right(msp_h.Sys_DatabaseName, 2) = 'CH' THEN 'CHF'
  --       WHEN right(msp_h.Sys_DatabaseName, 2) IN('DE', 'FR', 'NL', 'FI', 'AT')  THEN 'EUR'
  --       WHEN right(msp_h.Sys_DatabaseName, 2) = 'UK' THEN 'GBP'
  --       WHEN right(msp_h.Sys_DatabaseName, 2) = 'SE' THEN 'SEK'
  --       WHEN right(msp_h.Sys_DatabaseName, 2) = 'NO' THEN 'NOK'
  --       WHEN right(msp_h.Sys_DatabaseName, 2) = 'DK' THEN 'DKK'
  -- END AS currency
from silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header as msp_h
where  msp_h.Sys_Silver_IsCurrent = true
union
select distinct
  sil.DocumentNo_ AS document_id,
  to_date(sih.PostingDate) AS document_date, --because we don;lt have all the sales order data
  'sales invoice' as document_source,
  replace(sih.Sys_DatabaseName,'Reports','') as Country_Code,
  (select source_system_pk from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) as source_system_id,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil 
ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
union
select distinct
  sil.DocumentNo_ AS document_id,
  to_date(sih.PostingDate) AS document_date, --because we don;lt have all the sales order data
  'credit memo' as document_source,
  replace(sih.Sys_DatabaseName,'Reports','') as Country_Code,
  (select source_system_pk from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) as source_system_id,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true AND sih.Sys_Silver_IsCurrent = true AND sil.Sys_Silver_IsCurrent = true
""")
