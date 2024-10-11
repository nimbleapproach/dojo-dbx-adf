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
(select source_system_pk from {catalog}.{schema}.source_system where source_system = 'Infinigate ERP') as source_system_id,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    NOW() AS Sys_Gold_InsertedDateTime_UTC,
    NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM cte sil
""")
