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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_product_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_product_staging as
select distinct
it.No_ as product_code,
trim(
  (
    concat(
      regexp_replace(it.Description, 'NaN', ''),
      regexp_replace(it.Description2, 'NaN', ''),
      regexp_replace(it.Description3, 'NaN', ''),
      regexp_replace(it.Description4, 'NaN', '')
    )
  )
) AS product_description,
coalesce(it.sid,'NaN') as local_product_id ,
coalesce(it.ProductType,'NaN') as product_type,
  ss.source_system_pk as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
'item' as sys_item_source,
    it.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_InsertedDateTime_UTC,
    it.Sys_Silver_InsertDateTime_UTC AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.item it 
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(it.Sys_DatabaseName, 2)
WHERE 
  it.Sys_Silver_IsCurrent = true
""")
