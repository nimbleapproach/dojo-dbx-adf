# Databricks notebook source
# MAGIC %run ./nb-orion-meta

# COMMAND ----------
# Databricks notebook source
# MAGIC %run ./nb-orion-meta

# COMMAND ----------

import os
spark = spark  # noqa
dbutils = dbutils  # noqa
ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'phil_orion_testing'


# COMMAND ----------

# SOURCE SYSTEM 
# Now do the insert for DIM source system as the default members need 1 default member per source system
spark.sql(f"""insert into {catalog}.{schema}.dim_source_system (
  source_system,source_database,source_entity,reporting_source_database, data_area_id, start_datetime,end_datetime,is_current,Sys_Gold_InsertedDateTime_UTC,Sys_Gold_ModifiedDateTime_UTC
)
select source_system,source_database,source_entity,reporting_source_database, data_area_id, start_datetime,end_datetime,is_current,Sys_Gold_InsertedDateTime_UTC,Sys_Gold_ModifiedDateTime_UTC
from {catalog}.{schema}.vw_dim_source_system_staging ss
where not exists (select 1 from {catalog}.{schema}.dim_source_system s where s.source_system = ss.source_system and s.source_entity = ss.source_entity and s.data_area_id = ss.data_area_id)
""")


# COMMAND ----------

# DOCUMENT
# Now do the default inserts per reporting database name
sqldf = spark.sql(f"""
with cte_sources as 
(
   select source_system_pk from {catalog}.{schema}.dim_source_system where source_system_pk > 0
),
cte_document_sources as 
(
   select 'msp sales invoice' as document_source union select 'credit memo' union select 'sales invoice' union select 'msp sales credit memo' 
   union select 'nuvias sales invoice' union select 'sales quote' union select 'sales order' union select 'N/A'
)
SELECT DISTINCT
       CAST('N/A' AS STRING) AS local_document_id,
       CAST('N/A' AS STRING) AS associated_document_id,
       CAST('1900-01-01' AS DATE) AS document_date,
       CAST(-1 AS BIGINT) AS document_type,
       CAST(NULL AS STRING) AS country_code,
       CAST(s.source_system_pk AS BIGINT) AS source_system_fk,
       CAST('1900-01-01' AS TIMESTAMP) AS start_datetime,
       CAST(ds.document_source AS STRING) AS document_source, 
       CAST(NULL AS TIMESTAMP) AS end_datetime,
       CAST(1 AS INTEGER) AS is_current,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_sources s
CROSS JOIN cte_document_sources ds
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.{schema}.dim_document v WHERE v.document_source = ds.document_source AND v.source_system_fk = s.source_system_pk)
""").write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_document")



# COMMAND ----------

# PRODUCT
# Now do the default inserts per reporting database name
sqldf= spark.sql(f"""
with cte_sources as 
(
   select source_system_pk from {catalog}.{schema}.dim_source_system where source_system_pk > 0
),
cte_line_item_types as 
(
   select 'item' as line_item_type union select 'Credit Memo Line Item' union
   select 'Sales Archive Line Item' union select 'Sales Invoice Line Item' union
   select 'MSP Line Item' union select 'N/A'
   UNION
   SELECT DISTINCT concat(it.DataAreaId + ' Line Item')
   FROM silver_{ENVIRONMENT}.nuvias_operations.custvendexternalitem AS it 
)
SELECT DISTINCT
       CAST('N/A' AS STRING) AS product_code,
       CAST(NULL AS STRING) AS product_description,
       CAST(NULL AS STRING) AS local_product_id,
       CAST(NULL AS STRING) AS product_type,
       CAST(li.line_item_type AS STRING) AS line_item_type,
       CAST(s.source_system_pk AS BIGINT) AS source_system_fk,
       CAST('1900-01-01' AS TIMESTAMP) AS start_datetime,
       CAST(NULL AS TIMESTAMP) AS end_datetime,
       CAST(1 AS INTEGER) AS is_current,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_sources s
cross join cte_line_item_types li
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.{schema}.dim_product v 
WHERE 
v.product_code = CAST('N/A' AS STRING) 
AND v.line_item_type = li.line_item_type
AND v.source_system_fk = s.source_system_pk )
""").write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_product")


# COMMAND ----------

# RESELLER
# Now do the default inserts per reporting database name
sqldf= spark.sql(f"""
with cte_sources as 
(
   select source_system_pk from {catalog}.{schema}.dim_source_system where source_system_pk > 0
)
SELECT 
       CAST('N/A' AS STRING) AS reseller_code,
       CAST(NULL AS STRING) AS reseller_name_internal,
       CAST(NULL AS STRING) AS reseller_geography_internal,
       CAST('1900-01-01' AS TIMESTAMP) AS reseller_start_date,
       CAST(source_system_pk as BIGINT) AS source_system_fk,
       CAST('1900-01-01' AS TIMESTAMP) AS start_datetime,
       CAST(NULL AS TIMESTAMP) AS end_datetime,
       CAST(1 AS INTEGER) AS is_current,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_sources s
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.{schema}.dim_reseller v 
         WHERE v.reseller_code = CAST('N/A' AS STRING) 
            AND v.source_system_fk = s.source_system_pk)
""").write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_reseller")


# COMMAND ----------

# VENDOR
# Now do the default inserts per reporting database name
sqldf= spark.sql(f"""
with cte_sources as 
(
   select source_system_pk from {catalog}.{schema}.dim_source_system where source_system_pk > 0
)
SELECT 
       CAST('N/A' AS STRING) AS vendor_code,
       CAST(NULL AS STRING) AS vendor_name_internal,
       CAST(NULL AS STRING) AS country_code,
       CAST(NULL AS STRING) AS local_vendor_id,
       CAST(source_system_pk AS BIGINT) AS source_system_fk,
       CAST('1900-01-01' AS TIMESTAMP) AS start_datetime,
       CAST(NULL AS TIMESTAMP) AS end_datetime,
       CAST(1 AS INTEGER) AS is_current,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
FROM cte_sources s
WHERE NOT EXISTS (SELECT 1 FROM {catalog}.{schema}.dim_vendor v WHERE v.vendor_code = CAST('N/A' AS STRING) AND v.source_system_fk = s.source_system_pk)
""").write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_vendor")

# COMMAND ----------

# dbutils.notebook.exit(0)

