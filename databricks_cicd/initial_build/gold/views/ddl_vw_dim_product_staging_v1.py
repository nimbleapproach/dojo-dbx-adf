
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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_product_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_product_staging as
with cte_sources as 
(
  select distinct source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Infinigate ERP' 
  and s.is_current = 1
),
cte_product_source as 
(
-- Items
select distinct
    coalesce(it.No_, 'N/A') as product_code,
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
    coalesce(it.sid,'N/A') as local_product_id ,
    coalesce(it.ProductType,'N/A') as product_type,
    'item' as line_item_type,
    --coalesce(it.manufacturerItemNo_, 'N/A') as manufacturer_item_number,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(it.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(it.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.item it 
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(it.Sys_DatabaseName,2))
Where it.Sys_Silver_IsCurrent = true
group by
    coalesce(it.No_, 'N/A'),
    --coalesce(it.manufacturerItemNo_, 'N/A'),
    trim(
      (
        concat(
          regexp_replace(it.Description, 'NaN', ''),
          regexp_replace(it.Description2, 'NaN', ''),
          regexp_replace(it.Description3, 'NaN', ''),
          regexp_replace(it.Description4, 'NaN', '')
        )
      )
    ),
    coalesce(it.sid,'N/A'),
    coalesce(it.ProductType,'N/A'),
    coalesce(s.source_system_pk,-1)
union -- credit notes
select distinct
    coalesce(sil.No_,'N/A') as product_code,
    'Credit Memo Line Item' as product_description,
    coalesce(sil.No_,'N/A') as local_product_id ,
    'N/A' as product_type,
    'Credit Memo Line Item' as line_item_type,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sil.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(sil.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil 
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sil.Sys_DatabaseName,2))
WHERE sil.Sys_Silver_IsCurrent = true
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.No_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
group by
    coalesce(sil.No_,'N/A'),
    --coalesce(sil.manufacturerItemNo_, 'N/A'),
    coalesce(s.source_system_pk,-1)
union -- sales orders & quotes
select distinct
    coalesce(sil.No_,'N/A') as product_code,
    'Sales Archive Line Item' AS product_description,
    coalesce(sil.No_,'N/A') as local_product_id,
    'N/A' as product_type,
    'Sales Archive Line Item'  as line_item_type,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sil.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(sil.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
from silver_{ENVIRONMENT}.igsql03.sales_line_archive as sil
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
) as sha on sil.DocumentNo_ = sha.No_
and sil.Doc_No_Occurrence = 1
and sil.VersionNo_ = sha.VersionNo_
and sil.Sys_DatabaseName = sha.Sys_DatabaseName
and sil.Sys_Silver_IsCurrent = 1
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sil.Sys_DatabaseName,2))
WHERE sil.Sys_Silver_IsCurrent = true
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.No_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
group by
    coalesce(sil.No_,'N/A'),
    coalesce(s.source_system_pk,-1)
union -- sales invoices
select distinct
    coalesce(sil.No_,'N/A') as product_code,
    'Sales Invoice Line Item' as product_description,
    coalesce(sil.No_,'N/A') as local_product_id ,
    'N/A' as product_type,
    'Sales Invoice Line Item' as line_item_type,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sil.Sys_Silver_InsertDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(sil.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil 
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sil.Sys_DatabaseName,2))
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.No_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
WHERE sil.Sys_Silver_IsCurrent = true
group by 
    coalesce(sil.No_,'N/A'),
    --coalesce(sil.manufacturerItemNo_, 'N/A'),
    coalesce(s.source_system_pk,-1)
union -- msp
select distinct
    coalesce(sil.ItemNo_,'N/A') as product_code,
    'MSP Line Item' as product_description,
    coalesce(sil.ItemNo_,'N/A') as local_product_id ,
    'N/A' as product_type,
    'MSP Line Item' as line_item_type,
    coalesce(s.source_system_pk,-1) as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(sil.Sys_Silver_InsertDateTime_UTC)as Sys_Gold_InsertedDateTime_UTC,
    MAX(sil.Sys_Silver_ModifedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.igsql03.inf_msp_usage_line sil 
LEFT JOIN cte_sources s on lower(s.source_entity) = lower(right(sil.Sys_DatabaseName,2))
WHERE sil.Sys_Silver_IsCurrent = true
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.ItemNo_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
group by 
  coalesce(sil.ItemNo_,'N/A'),
  coalesce(s.source_system_pk,-1)
)
SELECT
    product_code,
    product_description,
    local_product_id ,
    product_type,
    line_item_type,
    source_system_fk,
    start_datetime,
    end_datetime,
    is_current,
    MAX(Sys_Gold_InsertedDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
    MAX(Sys_Gold_ModifiedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM cte_product_source
GROUP BY 
    product_code,
    product_description,
    local_product_id ,
    product_type,
    line_item_type,
    source_system_fk,
    start_datetime,
    end_datetime,
    is_current
""")
