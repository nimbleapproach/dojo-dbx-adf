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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_sales_invoice_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_sales_invoice_staging (
  invoice_type,
  country_code,
  source_system_fk,
  document_id,
  document_date,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
  'msp' as invoice_type,
  replace(msp_h.Sys_DatabaseName,'Reports','') as country_code,
  ss.source_system_pk as source_system_fk,
  case
        when msp_h.CreditMemo = '1' THEN msp_h.SalesCreditMemoNo_
        else msp_h.SalesInvoiceNo_
      end as document_id,
  cast(msp_h.DocumentDate as date) as document_date,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
  
from silver_{ENVIRONMENT}.igsql03.inf_msp_usage_header as msp_h
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(msp_h.Sys_DatabaseName, 2)

where  msp_h.Sys_Silver_IsCurrent = true
union
select distinct
  'invoice' as invoice_type,
  replace(sih.Sys_DatabaseName,'Reports','') as Country_Code,
  ss.source_system_pk as source_system_fk,
  sil.DocumentNo_ AS document_number,
  to_date(sih.PostingDate) AS document_date, --because we don;lt have all the sales order data
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sih.Sys_DatabaseName, 2)

INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil 
ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
LEFT JOIN gold_{ENVIRONMENT}.obt.entity_mapping AS entity ON RIGHT(sih.Sys_DatabaseName, 2) = entity.SourceEntityCode
union
select distinct
  'credit memo' as invoice_type,
  replace(sih.Sys_DatabaseName,'Reports','') as Country_Code,
  ss.source_system_pk as source_system_fk,
  sil.DocumentNo_ AS document_number,
  to_date(sih.PostingDate) AS document_date, --because we don;lt have all the sales order data
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
  CAST('2000-01-01' as TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sih.Sys_DatabaseName, 2)

INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
""")
