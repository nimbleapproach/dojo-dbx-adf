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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_product_late_stg1
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_product_late_stg1 AS
WITH cte AS (
-- late arriving sales invoices
select 
    coalesce(sil.No_,'N/A') as product_code,
    'Sales Invoice Line Item' as product_description,
    coalesce(sil.No_,'N/A') as local_product_id ,
    sil.Type product_type,
    'Sales Invoice Line Item' as sys_item_source,
    MAX(sil.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(sil.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC,
    sil.Sys_DatabaseName
FROM silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil 
WHERE 
  sil.Sys_Silver_IsCurrent = true
group by all
union all
select 
    coalesce(m.No_,'N/A') as product_code,
    'Credit Memo Line Item' as product_description,
    coalesce(m.No_,'N/A') as local_product_id ,
    m.Type product_type,
    'Credit Memo Line Item' as sys_item_source,
    MAX(m.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(m.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC,
    m.Sys_DatabaseName
from silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line m
WHERE 
  m.Sys_Silver_IsCurrent = true
group by all
) 
select distinct
sil.product_code,
sil.product_description,
sil.local_product_id as local_product_id ,
sil.product_type,
ss.source_system_pk as source_system_fk,
CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
1 AS is_current,
sil.sys_item_source,
sil.Sys_Gold_InsertedDateTime_UTC,
sil.Sys_Gold_ModifiedDateTime_UTC
FROM cte sil
LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it  
  ON sil.product_code = it.No_
  and sil.product_type = it.Type
  and it.Sys_Silver_IsCurrent = true 
  and sil.Sys_DatabaseName = it.Sys_DatabaseName
inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sil.Sys_DatabaseName, 2)
WHERE it.No_ is null
""")
