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
              DROP TABLE IF EXISTS {catalog}.{schema}.fact_sales_transaction_ig
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fact_sales_transaction_ig (
  source_system_fk BIGINT,
  local_fact_id BIGINT COMMENT 'Surrogate Key',
  document_line_number STRING COMMENT 'Business Key',
  associated_document_line_number STRING,
  description STRING COMMENT 'TODO',
  gen_prod_posting_group STRING COMMENT 'TODO',
  gen_bus_posting_group STRING COMMENT 'TODO',
  currency_factor DOUBLE,
  document_date DATE,
  deferred_revenue_startdate DATE,
  deferred_revenue_enddate DATE,
  is_deferred BOOLEAN,
  document_source STRING,
  product_type STRING,
  manufacturer_item_number STRING,
  amount_local_currency DECIMAL(38,20),
  amount_EUR DECIMAL(10,2),
  amount_including_vat_local_currency STRING,
  amount_including_vat_EUR STRING,
  cost_amount_local_currency DECIMAL(38,20),
  cost_amount_EUR DECIMAL(10,2),
  quantity DECIMAL(10,2),
  total_cost_purchase_currency DECIMAL(10,2),
  total_cost_EUR DECIMAL(10,2),
  unit_cost_local_currency DECIMAL(10,2),
  unit_cost_purchase_currency DECIMAL(10,2),
  unit_cost_EUR DECIMAL(10,2),
  unit_price DECIMAL(10,2),
  Sys_Gold_InsertedDateTime_UTC TIMESTAMP,
  Sys_Gold_ModifiedDateTime_UTC TIMESTAMP,
  product_fk BIGINT,
  document_fk BIGINT,
  currency_fk BIGINT,
  reseller_fk BIGINT,
  vendor_fk BIGINT,
  entity_fk BIGINT,
  Sys_Gold_FactProcessedDateTime_UTC TIMESTAMP
  )
USING delta
CLUSTER BY (source_system_fk,document_source)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""")
