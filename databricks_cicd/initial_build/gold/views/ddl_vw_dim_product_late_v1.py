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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_product_late
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_product_late as
with cte 
as
(
  select distinct  sil.No_ from silver_dev.igsql03.sales_invoice_line sil
LEFT JOIN silver_dev.igsql03.item it  ON sil.No_ = it.No_
AND sil.Sys_DatabaseName = it.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true
WHERE 
  sil.Sys_Silver_IsCurrent = true
  and it.No_ is null
union all
select distinct  sil.No_ from silver_dev.igsql03.sales_cr_memo_line sil
LEFT JOIN silver_dev.igsql03.item it  ON sil.No_ = it.No_
AND sil.Sys_DatabaseName = it.Sys_DatabaseName
AND it.Sys_Silver_IsCurrent = true
WHERE 
  sil.Sys_Silver_IsCurrent = true
  and it.No_ is null
) 
select distinct
sil.No_ as product_code,
'NaN' AS product_description,
sil.No_ as local_product_id ,
'NaN' as product_type,
  ss.source_system_pk as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    NOW() AS Sys_Gold_InsertedDateTime_UTC,
    NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM cte sil
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sil.Sys_DatabaseName, 2)
""")
