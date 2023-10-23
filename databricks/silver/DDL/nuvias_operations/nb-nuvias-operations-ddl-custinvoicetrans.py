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
# MAGIC CREATE OR REPLACE TABLE custinvoicetrans
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,_SysRowId STRING NOT NULL
# MAGIC       COMMENT 'Technical Key'
# MAGIC     ,InvoiceId	STRING	NOT NULL                    
# MAGIC       COMMENT  'Business key'
# MAGIC     ,InvoiceDate	TIMESTAMP                  
# MAGIC       COMMENT  'TODO'
# MAGIC     ,LineNum DECIMAL(10,2) 
# MAGIC       COMMENT 'line number of the invoice'
# MAGIC     ,DataAreaId	STRING	NOT NULL   	                    
# MAGIC       COMMENT  'Identifier of source data area'
# MAGIC     ,CurrencyCode	STRING	                 
# MAGIC       COMMENT  'TODO'
# MAGIC     ,LineAmountMST DECIMAL(10,2)  
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ItemId	STRING	        
# MAGIC       COMMENT 'TODO'
# MAGIC     ,InventTransId STRING	        
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesId	STRING	      
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LineAmount	DECIMAL(10,2) 	    
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Qty	DECIMAL(10,2) 	          
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesUnit	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC
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
# MAGIC ,CONSTRAINT custinvoicetrans_pk PRIMARY KEY(InvoiceId,LineNum,DataAreaId,DataLakeModified_DateTime)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for custinvoicetrans. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (InvoiceId,LineNum,DataAreaId)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE custinvoicetrans ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE custinvoicetrans ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE custinvoicetrans ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
