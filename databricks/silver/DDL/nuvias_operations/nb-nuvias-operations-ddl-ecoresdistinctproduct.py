# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuvias_operations;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ecoresdistinctproduct
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,_SysRowId STRING NOT NULL
# MAGIC       COMMENT 'Technical Key'
# MAGIC     ,RECID	LONG	NOT NULL
# MAGIC       COMMENT 'Business key'
# MAGIC     ,SAG_NGS1VendorID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PRODUCTTYPE INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RECVERSION INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DISPLAYPRODUCTNUMBER STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LastProcessedChange_DateTime	TIMESTAMP	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DataLakeModified_DateTime	TIMESTAMP	
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
# MAGIC ,CONSTRAINT ecoresdistinctproduct_pk PRIMARY KEY(SAG_NGS1VendorID,DataLakeModified_DateTime)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ecoresdistinctproduct. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (SAG_NGS1VendorID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ecoresdistinctproduct ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ecoresdistinctproduct ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ecoresdistinctproduct ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
