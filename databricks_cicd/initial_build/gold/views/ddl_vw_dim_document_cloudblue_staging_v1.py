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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_dim_document_cloudblue_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_dim_document_cloudblue_staging  
AS 
with cte_sources as 
(
  select distinct s.source_system, source_system_pk, source_entity 
  from {catalog}.{schema}.dim_source_system s 
  where s.source_system = 'Cloudblue PBA ERP' 
  and s.is_current = 1
),
cte_source_data as 
(
  SELECT DISTINCT 
    cast(ar.DocID AS STRING) as local_document_id,
    'N/A' as associated_document_id,
    to_date(cast(ar.DocDate AS TIMESTAMP)) as document_date,
    'cloudblue sales invoice' as document_source,
    ar.doctype as document_type,
    ar.status as document_status,
    'N/A' as country_code,
    'N/A' as vendor_dimension_value,
    ar.Vendor_AccountID as vendor_account_id,
    s.source_system_pk as source_system_fk,
    CAST('1990-01-01' AS TIMESTAMP) AS start_datetime,
    CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
    1 AS is_current,
    MAX(CAST(ar.Sys_Silver_InsertDateTime_UTC as TIMESTAMP)) AS Sys_Gold_InsertedDateTime_UTC,
    MAX(CAST(ar.Sys_Silver_ModifedDateTime_UTC as TIMESTAMP)) AS Sys_Gold_ModifiedDateTime_UTC
  FROM silver_{ENVIRONMENT}.cloudblue_pba.ARDoc ar
  LEFT JOIN cte_sources s on s.source_system = 'Cloudblue PBA ERP'
  WHERE ar.sys_silver_iscurrent = 1
  GROUP BY ALL
)
SELECT
  csd.local_document_id,
  csd.associated_document_id,
  csd.document_date,
  csd.document_source,
  csd.document_type,
  csd.document_status,
  csd.country_code,
  csd.vendor_dimension_value,
  csd.source_system_fk,
  case when d.is_current is null THEN csd.start_datetime ELSE CAST(NOW() as TIMESTAMP) END AS start_datetime,
  csd.end_datetime,
  csd.is_current,
  MAX(csd.Sys_Gold_InsertedDateTime_UTC) as Sys_Gold_InsertedDateTime_UTC,
  MAX(csd.Sys_Gold_ModifiedDateTime_UTC) as Sys_Gold_ModifiedDateTime_UTC
FROM cte_source_data csd
LEFT JOIN {catalog}.{schema}.dim_document d ON csd.local_document_id = d.local_document_id and csd.source_system_fk = d.source_system_fk and csd.document_source = d.document_source
WHERE csd.local_document_id IS NOT NULL
GROUP BY ALL
""")
