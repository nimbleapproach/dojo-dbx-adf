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
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_fact_sales_transaction_ig_staging
              """)

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS {catalog}.{schema}.vw_fact_sales_transaction_ig_staging as 
with fact_delta as 
(
  SELECT document_source,MAX(max_transaction_line_timestamp) as max_transaction_line_timestamp 
  FROM {catalog}.{schema}.fact_delta_timestamp
  GROUP BY document_source
)
SELECT sil.* 
FROM {catalog}.{schema}.vw_fact_sales_invoices_staging sil
JOIN fact_delta on fact_delta.document_source = sil.document_source
AND (
      sil.Sys_Gold_InsertedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    OR
      sil.Sys_Gold_ModifiedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    )
UNION ALL
SELECT sil.* 
FROM {catalog}.{schema}.vw_fact_sales_credit_memos_staging sil
JOIN fact_delta on fact_delta.document_source = sil.document_source
AND (
      sil.Sys_Gold_InsertedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    OR
      sil.Sys_Gold_ModifiedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    )
UNION ALL
SELECT msp_l.* 
FROM {catalog}.{schema}.vw_fact_msp_invoices_staging msp_l
JOIN fact_delta on fact_delta.document_source = msp_l.document_source
AND (
      msp_l.Sys_Gold_InsertedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    OR
      msp_l.Sys_Gold_ModifiedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    )
UNION ALL
SELECT sla.* 
FROM {catalog}.{schema}.vw_fact_sales_orders_quotes_staging sla
JOIN fact_delta on fact_delta.document_source = sla.document_source
AND (
      sla.Sys_Gold_InsertedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    OR
      sla.Sys_Gold_ModifiedDateTime_UTC > fact_delta.max_transaction_line_timestamp
    )
"""
)
