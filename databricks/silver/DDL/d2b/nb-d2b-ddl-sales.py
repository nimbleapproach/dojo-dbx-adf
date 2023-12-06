# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA d2b;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sales
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,CUSTOMER_ID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CUSTOMER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,INVOICE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,INVOICE_DATE DATE
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CUSTOMER_PO STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKU STRING
# MAGIC       COMMENT 'TODO'  
# MAGIC     ,DESCRIPTION STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,QTY INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SALES_PRICE DECIMAL(38,8)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PURCHASE_PRICE DECIMAL(38,8)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,USD_RATE DECIMAL(38,8)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VENDOR STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_ID STRING
# MAGIC       COMMENT 'Key Column'
# MAGIC     ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC     ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC     ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC     ,Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys columns.'
# MAGIC     ,Sys_Silver_IsCurrent BOOLEAN
# MAGIC       COMMENT 'Flag if this is the current version.'
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC       COMMENT 'Flag if this is the deleted version.'
# MAGIC ,CONSTRAINT d2bsales_pk PRIMARY KEY(Sys_ID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Sys_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE sales ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE sales ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE sales ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
