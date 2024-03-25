# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dcb;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE invoicedata
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,Invoice STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Vlgnr INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LineNo INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Invoice_Date STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Invoice_Month INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Invoice_Year INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SO STRING
# MAGIC       COMMENT 'TODO'  
# MAGIC     ,SO_Date STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Reseller_ID INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Reseller_Name STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VAT_Number STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKU STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Description STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Remark STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,QTY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Unit_Price DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Line_Discount_Percentage DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Line_Discount_Amount DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Net_Price DECIMAL(18,2)
# MAGIC       COMMENT 'TODO' 
# MAGIC     ,Total_Invoice DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Entity STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Vendor STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Vendor_ID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Article_Group STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Product_Type STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Margin DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Margin_Percentage DECIMAL(18,2)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_FileName STRING
# MAGIC       COMMENT 'TODO'
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
# MAGIC ,CONSTRAINT dcb_invoicedata_pk PRIMARY KEY(Invoice, LineNo, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Invoice, LineNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
