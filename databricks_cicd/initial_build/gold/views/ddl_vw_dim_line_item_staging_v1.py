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
              DROP VIEW IF {catalog}.{schema}.vw_dim_line_item_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_line_item_staging (
  line_item_code COMMENT 'TODO',
  line_item_description,
  local_line_item_id COMMENT 'TODO',
  source_system_fk,
  line_item_hash_key,
  start_datetime,
  end_datetime,
  is_current,
  Sys_Gold_InsertedDateTime_UTC,
  Sys_Gold_ModifiedDateTime_UTC)
AS select distinct
sil.no_ as line_item_code,
trim(
  (
    concat(
      regexp_replace(it.Description, 'NaN', ''),
      regexp_replace(it.Description2, 'NaN', ''),
      regexp_replace(it.Description3, 'NaN', ''),
      regexp_replace(it.Description4, 'NaN', '')
    )
  )
) AS line_item_description,
sil.no_ as local_line_item_id ,
ss.source_system_pk as source_system_fk,
 SHA2(CONCAT_WS(' ', COALESCE(TRIM(sil.no_), ''), COALESCE(TRIM(
    concat(
      regexp_replace(it.Description, 'NaN', ''),
      regexp_replace(it.Description2, 'NaN', ''),
      regexp_replace(it.Description3, 'NaN', ''),
      regexp_replace(it.Description4, 'NaN', '')
    )
    ), '')), 256) AS line_item_hash_key,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    NOW() AS Sys_Gold_InsertedDateTime_UTC,
    NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM
  silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sih.Sys_DatabaseName, 2)
  INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_line sil ON sih.No_ = sil.DocumentNo_
  AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
  AND sih.Sys_Silver_IsCurrent = true
  AND sil.Sys_Silver_IsCurrent = true
  LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
  AND sil.Sys_DatabaseName = it.Sys_DatabaseName
  AND it.Sys_Silver_IsCurrent = true
WHERE
    (
      UPPER(sil.No_) NOT LIKE 'PORTO'
      AND UPPER(sil.No_) NOT LIKE 'VERSAND%'
      AND UPPER(sil.No_) NOT LIKE 'SHIP%'
      AND UPPER(sil.No_) NOT LIKE 'TRANS%'
      AND UPPER(sil.No_) NOT LIKE 'POST%'
      AND UPPER(sil.No_) NOT LIKE 'FREI%'
      AND UPPER(sil.No_) NOT LIKE 'FRACHT%'
      AND UPPER(sil.No_) NOT LIKE 'EXP%'
      AND UPPER(sil.No_) NOT LIKE '%MARKETING%'
    )
    -- AND sil.Gen_Bus_PostingGroup not like 'IC%'
    and sil.Type <> 0 -- filter out type 0 because it is only placeholder lines (yzc)
    and sil.no_ is not null

union
select distinct
it.no_ as line_item_code,
--coalesce(it.No_, sil.No_, 'NaN') AS SKUInternal,
trim(
  concat(
    regexp_replace(it.Description, 'NaN', ''),
    regexp_replace(it.Description2, 'NaN', ''),
    regexp_replace(it.Description3, 'NaN', ''),
    regexp_replace(it.Description4, 'NaN', '')
  )
) AS line_item_description,
sil.no_ as local_line_item_id ,
ss.source_system_pk as source_system_fk,
SHA2(CONCAT_WS(' ', COALESCE(TRIM(it.no_), ''), COALESCE(TRIM(
    concat(
      regexp_replace(it.Description, 'NaN', ''),
      regexp_replace(it.Description2, 'NaN', ''),
      regexp_replace(it.Description3, 'NaN', ''),
      regexp_replace(it.Description4, 'NaN', '')
    )
    ), '')), 256) AS products_hash_key,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    NOW() AS Sys_Gold_InsertedDateTime_UTC,
    NOW() AS Sys_Gold_ModifiedDateTime_UTC
FROM
    silver_{ENVIRONMENT}.igsql03.sales_cr_memo_header sih
  inner join (select source_system_pk, source_entity from {catalog}.{schema}.dim_source_system where source_system = 'Infinigate ERP' and is_current = 1) ss on ss.source_entity=RIGHT(sih.Sys_DatabaseName, 2)
    INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_cr_memo_line sil ON sih.No_ = sil.DocumentNo_
    AND sih.Sys_DatabaseName = sil.Sys_DatabaseName
    AND sih.Sys_Silver_IsCurrent = true
    AND sil.Sys_Silver_IsCurrent = true
    LEFT JOIN silver_{ENVIRONMENT}.igsql03.item it ON sil.No_ = it.No_
    AND sil.Sys_DatabaseName = it.Sys_DatabaseName
    AND it.Sys_Silver_IsCurrent = true
WHERE
  (
    UPPER(sil.No_) NOT LIKE 'PORTO'
    AND UPPER(sil.No_) NOT LIKE 'VERSAND%'
    AND UPPER(sil.No_) NOT LIKE 'SHIP%'
    AND UPPER(sil.No_) NOT LIKE 'TRANS%'
    AND UPPER(sil.No_) NOT LIKE 'POST%'
    AND UPPER(sil.No_) NOT LIKE 'FREI%'
    AND UPPER(sil.No_) NOT LIKE 'FRACHT%'
    AND UPPER(sil.No_) NOT LIKE 'EXP%'
    AND UPPER(sil.No_) NOT LIKE '%MARKETING%'
  )
-- AND sil.Gen_Bus_PostingGroup not like 'IC%'
  and sil.Type <> 0 -- filter out type 0 because it is only placeholder lines (yzc)
  and it.no_ is not null
""")
