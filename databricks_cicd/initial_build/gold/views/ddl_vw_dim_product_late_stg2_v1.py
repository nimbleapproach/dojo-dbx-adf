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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_product_late_stg2
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_product_late_stg2 as
with cte 
as
(
  select distinct  
  coalesce(sil.ItemNo_, 'N/A') as product_code,
  trim(
    (
      concat(
        regexp_replace(sil.Description, 'NaN', ''),
        regexp_replace(sil.Description2, 'NaN', ''),
        regexp_replace(sil.Description3, 'NaN', ''),
        regexp_replace(sil.Description4, 'NaN', '')
      )
    )
  ) AS product_description,
  concat(RIGHT(sil.Sys_DatabaseName,2) AS source_system_code,
  "|",sil.ItemNo_,"|", product_description) as local_product_id
  FROM
    silver_dev.igsql03.inf_msp_usage_line sil 
  WHERE
    sil.Sys_Silver_IsCurrent = true
) 
select distinct
sil.No_ as product_code,
'NaN' AS product_description,
ss.source_system_pk as source_system_fk,
sil.No_ as local_product_id ,
'NaN' as product_type,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    NOW() AS Sys_Gold_InsertedDateTime_UTC,
    NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM cte sil 
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sil.Sys_DatabaseName, 2)
""")
