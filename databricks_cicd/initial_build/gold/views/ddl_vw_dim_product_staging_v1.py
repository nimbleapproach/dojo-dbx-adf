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
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  --where s.source_system = 'Infinigate ERP' 
  where s.is_current = 1
),
cte_duplicate_netsafe_sku as 
(
  select trim(sku) as sku,Sys_Country,MAX(SKU_Description) as product_description, count(*) as Sku_Count
  from silver_dev.netsafe.invoicedata AS 
  where sys_silver_iscurrent = true 
  group by all
  having count(*) > 1
),
--cte_nuvias_sources as 
--(
-- select distinct source_system_pk, data_area_id
--  from {catalog}.{schema}.dim_source_system s 
--  where s.source_system = 'Nuvias ERP' 
--  and s.is_current = 1
--),,
cte_source_data as 
(
-- Items
select 
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
AND s.source_system = 'Infinigate ERP'
Where it.Sys_Silver_IsCurrent = true
group by
    it.No_,
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
select 
    sil.No_ as product_code,
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
AND s.source_system = 'Infinigate ERP'
WHERE sil.Sys_Silver_IsCurrent = true
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.No_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
group by
    sil.No_,
    --coalesce(sil.manufacturerItemNo_, 'N/A'),
    coalesce(s.source_system_pk,-1)
union -- sales orders & quotes
select 
    sil.No_ as product_code,
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
AND s.source_system = 'Infinigate ERP'
WHERE sil.Sys_Silver_IsCurrent = true
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.No_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
group by
    sil.No_,
    coalesce(s.source_system_pk,-1)
union -- sales invoices
select 
    sil.No_ as product_code,
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
AND s.source_system = 'Infinigate ERP'
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.No_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
WHERE sil.Sys_Silver_IsCurrent = true
group by 
    sil.No_,
    --coalesce(sil.manufacturerItemNo_, 'N/A'),
    coalesce(s.source_system_pk,-1)
union -- msp
select 
    sil.ItemNo_ as product_code,
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
AND s.source_system = 'Infinigate ERP'
WHERE sil.Sys_Silver_IsCurrent = true
AND not exists (select 1 from silver_{ENVIRONMENT}.igsql03.item i 
                where i.No_ = sil.ItemNo_ 
                and i.Sys_Silver_IsCurrent = true 
                and i.Sys_DatabaseName = sil.Sys_DatabaseName)
group by 
  sil.ItemNo_,
  coalesce(s.source_system_pk,-1)
UNION
--Nuvias
SELECT
    it.Description AS product_code,
    COALESCE(itdesc.Description, 'N/A') AS product_description,
    MAX(COALESCE(it.itemid,'N/A')) AS local_product_id ,
    COALESCE(datanowarr.Product_Type, 'N/A') AS product_type,
    CONCAT(it.DataAreaId,' Line Item') as line_item_type,
    --coalesce(it.manufacturerItemNo_, 'N/A') as manufacturer_item_number,
    coalesce(s.source_system_pk,-1) AS source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(it.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(it.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem AS it 

LEFT JOIN silver_{ENVIRONMENT}.nuvias_operations.ecoresproducttranslation AS itdesc ON it.Description = itdesc.Name
AND itdesc.Sys_Silver_IsCurrent = 1

LEFT JOIN gold_{ENVIRONMENT}.obt.datanowarr AS datanowarr ON datanowarr.SKU = it.Description

LEFT JOIN cte_sources s on it.dataareaid = s.source_entity AND s.source_system = 'Nuvias ERP'

WHERE it.Sys_Silver_IsCurrent = 1
GROUP BY all
UNION
--Netsuite
SELECT
  COALESCE(it.SKU_ID,si.SKU_ID) AS product_code,
  COALESCE(it.Description,'N/A') AS product_description,
  MAX(COALESCE(it.SID,si.SID)) AS local_product_id ,
  COALESCE(it.Item_Category,'N/A') AS product_type,
  'Starlink (Netsuite) Line Item' as line_item_type,
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(si.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(si.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM silver_{ENVIRONMENT}.netsuite.InvoiceReportsInfinigate AS si
LEFT JOIN silver_{ENVIRONMENT}.netsuite.masterdataSku AS it ON si.SKU_ID = it.SKU_ID
AND it.Sys_Silver_IsCurrent = 1
LEFT JOIN cte_sources s on 'AE1' = s.source_entity AND s.source_system = 'Starlink (Netsuite) ERP'
WHERE si.sys_silver_iscurrent = true
GROUP BY ALL
UNION
--Netsafe
SELECT
  trim(invoice.SKU) AS product_code,
  CASE WHEN d.Sku_Count > 1 THEN d.product_description ELSE COALESCE(invoice.SKU_Description, 'N/A') END AS product_description,
  COALESCE(trim(invoice.SKU), 'N/A') AS local_product_id,
  'N/A' AS product_type,
  'Netsafe Line Item' as line_item_type,
  coalesce(s.source_system_pk,-1) AS source_system_fk,
  CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
  CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
  1 AS is_current,
  MAX(invoice.Sys_Silver_InsertDateTime_UTC) AS Sys_Gold_InsertedDateTime_UTC,
  MAX(invoice.Sys_Silver_ModifedDateTime_UTC) AS Sys_Gold_ModifiedDateTime_UTC
FROM  
  silver_{ENVIRONMENT}.netsafe.invoicedata AS invoice
LEFT JOIN cte_sources s on CASE
    WHEN lower(invoice.Sys_Country) like '%romania%' THEN 'RO2'
    WHEN lower(invoice.Sys_Country) like '%croatia%' THEN 'HR2'
    WHEN lower(invoice.Sys_Country) like '%slovenia%' THEN 'SI1'
    WHEN lower(invoice.Sys_Country) like '%bulgaria%' THEN 'BG1'
    END = s.source_entity
AND s.source_system = 'Netsafe ERP'
LEFT JOIN cte_duplicate_netsafe_sku d ON d.sku = trim(invoice.SKU) and d.Sys_Country = invoice.Sys_Country
WHERE invoice.sys_silver_iscurrent = true
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
