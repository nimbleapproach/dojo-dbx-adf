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
              DROP TABLE IF EXISTS {catalog}.{schema}.dim_product
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_product 
(
  product_pk BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
  product_code STRING NOT NULL COMMENT 'Product Code',
  product_description STRING COMMENT 'The description of the Product',
  local_product_id STRING COMMENT 'The local ID from the Product if exists',
  product_type STRING COMMENT 'Classification of the Product Code',
  line_item_type STRING  COMMENT 'source table of the record, credit memo, item, sales order, msp',
  source_system_fk BIGINT COMMENT 'The ID from the Source System Dimension',
  start_datetime TIMESTAMP NOT NULL COMMENT 'The dimensional start date of the record',
  end_datetime TIMESTAMP COMMENT 'The dimensional end date of the record, those records with a NULL value are current',
  is_current INT COMMENT 'Flag to indicate if this is the active dimension record per code',
  Sys_Gold_InsertedDateTime_UTC TIMESTAMP COMMENT 'The timestamp when this record was inserted into gold',
  Sys_Gold_ModifiedDateTime_UTC TIMESTAMP COMMENT 'The timestamp when this record was last updated in gold',
  CONSTRAINT `dim_product_primary_key` PRIMARY KEY (`product_pk`)
)
USING delta
CLUSTER BY (source_system_fk,product_code)
TBLPROPERTIES (
  'delta.checkpointPolicy' = 'v2',
  'delta.constraints.datewithinrange_start_datetime' = 'start_datetime >= "1900-01-01"',
  'delta.constraints.valid_is_current_value' = 'is_current IN ( 1 , 0 )',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.checkConstraints' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported')
""")

# COMMAND ----------

# Add in the UNKNOWN Member
sqldf = spark.sql(f"""
SELECT CAST(-1 AS BIGINT) AS product_pk,
       CAST('N/A' AS STRING) AS product_code,
       CAST(NULL AS STRING) AS product_description,
       CAST(NULL AS STRING) AS local_product_id,
       CAST(NULL AS STRING) AS product_type,
       CAST('N/A' AS STRING) AS line_item_type,
       CAST(-1 AS BIGINT) AS source_system_fk,
       CAST('1900-01-01' AS TIMESTAMP) AS start_datetime,
       CAST('9999-12-31' AS TIMESTAMP) AS end_datetime,
       CAST(1 AS INTEGER) AS is_current,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_InsertedDateTime_UTC,
       CAST(NULL AS TIMESTAMP) AS Sys_Gold_ModifiedDateTime_UTC
WHERE NOT EXISTS ( SELECT 1 FROM {catalog}.{schema}.dim_product p1 WHERE p1.product_pk = -1 AND source_system_fk = -1)
""").write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.dim_product")
