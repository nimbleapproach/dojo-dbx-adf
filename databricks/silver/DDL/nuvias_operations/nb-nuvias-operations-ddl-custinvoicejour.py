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
# MAGIC CREATE OR REPLACE TABLE custinvoicejour
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,_SysRowId STRING NOT NULL
# MAGIC       COMMENT 'Technical Key'
# MAGIC     ,InvoiceId	STRING	NOT NULL                    
# MAGIC       COMMENT  'Business key'
# MAGIC     ,DataAreaId	STRING	NOT NULL   	                    
# MAGIC       COMMENT  'Identifier of source data area'
# MAGIC     ,InvoiceDate	TIMESTAMP	                
# MAGIC       COMMENT  'TODO'
# MAGIC     ,InvoiceAccount	STRING	                
# MAGIC       COMMENT  'TODO'
# MAGIC     ,InvoicingName STRING	                
# MAGIC       COMMENT  'TODO'
# MAGIC     ,InterCompanyCompanyId	STRING	        
# MAGIC       COMMENT  'TODO'
# MAGIC     ,ExchRate	DECIMAL	                      
# MAGIC       COMMENT  'TODO'   
# MAGIC     ,CustomerRef	STRING	                  
# MAGIC       COMMENT  'TODO'
# MAGIC     ,CustGroup	STRING	                    
# MAGIC       COMMENT  'TODO'
# MAGIC     ,CurrencyCode	STRING	                  
# MAGIC       COMMENT  'TODO'
# MAGIC     ,InvoiceAmount DECIMAL
# MAGIC       COMMENT  'TODO'
# MAGIC     ,InvoiceAmountMST DECIMAL
# MAGIC       COMMENT  'TODO'
# MAGIC     ,SalesId	STRING	                      
# MAGIC       COMMENT  'TODO'
# MAGIC     ,OrderAccount	STRING
# MAGIC       COMMENT  'TODO'	 
# MAGIC     ,DataLakeModified_DateTime	TIMESTAMP   
# MAGIC       COMMENT  'TODO'                 
# MAGIC     ,LastProcessedChange_DateTime	TIMESTAMP	
# MAGIC       COMMENT  'TODO'
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
# MAGIC ,CONSTRAINT custinvoicejour_pk PRIMARY KEY(InvoiceId,DataAreaId,DataLakeModified_DateTime)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for custinvoicejour. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (InvoiceId,DataAreaId)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE custinvoicejour ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE custinvoicejour ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE custinvoicejour ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
