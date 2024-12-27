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
              DROP TABLE IF EXISTS {catalog}.{schema}.fact_delta_timestamp
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fact_delta_timestamp 
(
  document_source STRING COMMENT 'credit memo, msp, invoices etc..',
  max_transaction_line_timestamp TIMESTAMP COMMENT 'This is the value used to derive the Fact deltas in the staging layer',
  Sys_Gold_FactProcessedDateTime_UTC TIMESTAMP COMMENT 'The timestamp of when the facts were processed'
)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
"""
)

# COMMAND ----------
# Now populate with a default value - initial values to prevent an empty join
spark.sql(f"""
INSERT INTO {catalog}.{schema}.fact_delta_timestamp (document_source,max_transaction_line_timestamp,Sys_Gold_FactProcessedDateTime_UTC)
VALUES
  ('sales invoice', '2000-01-01', current_timestamp()),
  ('credit memo', '2000-01-01', current_timestamp()),
  ('msp sales credit memo', '2000-01-01', current_timestamp()),
  ('msp sales invoice', '2000-01-01', current_timestamp()),
  ('sales quote', '2000-01-01', current_timestamp()),
  ('sales order', '2000-01-01', current_timestamp()),
  ('nuvias sales invoice', '2000-01-01', current_timestamp()),
  ('starlink (netsuite) sales invoice', '2000-01-01', current_timestamp()),
  ('netsafe sales invoice', '2000-01-01', current_timestamp()),
  ('cloudblue sales order', '2000-01-01', current_timestamp()),
  ('cloudblue sales invoice', '2000-01-01', current_timestamp())
"""
)

