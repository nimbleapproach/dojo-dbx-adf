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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_product_cloudblue_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_product_cloudblue_staging  
AS 
with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Cloudblue PBA ERP' 
  and s.is_current = 1
),
cte_duplicate_cloudblue_sku as 
(
  select trim(sku) as sku,MAX(Descr) as product_description, count(*) as Sku_Count
  from silver_{ENVIRONMENT}.cloudblue_pba.DocDet AS 
  where sys_silver_iscurrent = true 
  group by all
  having count(*) > 1
),
cte_source_data as 
(
SELECT 
    bm.ResourceID AS product_code,
    bm.name AS product_description,
    bm.ResourceID AS local_product_id,
    bm.BMResourceType as product_type,
    'Cloudblue Resource Item' as line_item_type,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(bm.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(bm.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
LEFT JOIN cte_sources s on s.source_system = 'Cloudblue PBA ERP'
WHERE bm.Sys_Silver_IsCurrent = true
GROUP BY ALL
UNION ALL
SELECT 
    CASE WHEN trim(dd.SKU) = 'NaN' THEN NULL ELSE trim(dd.SKU) END AS product_code,
    CASE WHEN d.Sku_Count > 1 THEN d.product_description ELSE COALESCE(dd.Descr, 'N/A') END AS product_description,
    COALESCE(CASE WHEN trim(dd.SKU) = 'NaN' THEN 'N/A' ELSE trim(dd.SKU) END, 'N/A') AS local_product_id,
    'N/A' as product_type,
    'Cloudblue Line Item' as line_item_type,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(dd.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(dd.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.cloudblue_pba.DocDet dd
LEFT JOIN cte_duplicate_cloudblue_sku d ON d.sku = trim(dd.SKU) 
LEFT JOIN cte_sources s on s.source_system = 'Cloudblue PBA ERP'
WHERE dd.Sys_Silver_IsCurrent = true
AND NOT EXISTS (
                SELECT 1 FROM  silver_{ENVIRONMENT}.cloudblue_pba.bmresource bm
                WHERE bm.ResourceID = dd.ResourceID
                AND bm.sys_silver_iscurrent = true
                )
GROUP BY ALL
)
SELECT
    csd.product_code,
    csd.product_description,
    csd.local_product_id ,
    csd.product_type,
    csd.line_item_type,
    csd.source_system_fk,
    case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
    csd.end_datetime,
    csd.is_current,
    MAX(csd.Sys_Gold_InsertedDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(csd.Sys_Gold_ModifiedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_product d ON d.local_product_id = csd.local_product_id 
  AND d.line_item_type = csd.line_item_type 
  AND d.source_system_fk = d.source_system_fk
WHERE csd.product_code IS NOT NULL
GROUP BY ALL
""")
