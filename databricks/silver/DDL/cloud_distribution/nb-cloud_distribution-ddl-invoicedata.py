# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA cloud_distribution;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE invoicedata
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,InvoiceNumber STRING
# MAGIC       COMMENT 'TODO'  
# MAGIC     ,InvoiceDate STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesOrder STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,OrderType STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ItemID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKU STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ItemType STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CustomerName STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Qty INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Currency STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RevenueLocal DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CostLocal DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MarginLocal DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdjustmentLocal DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExchangeRate FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RevenueGBP DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CostGBP DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MarginGBP DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VendorID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VendorName STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AccountManager STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,QuoteID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CustomerAccount STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKUDescription STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Entity STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_FileName STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LineNo int
# MAGIC       COMMENT 'Artificial LineNo added from dataflow'
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
# MAGIC ,CONSTRAINT cloud_distribution_invoicedata_pk PRIMARY KEY(InvoiceNumber, LineNo, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (InvoiceNumber, LineNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
