# Databricks notebook source
# DBTITLE 1,Define invoicereportsinfinigate at Silver
# MAGIC %md
# MAGIC

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA netsuite;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE invoicereportsinfinigate
# MAGIC   (  
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,Invoice_Number	STRING	NOT NULL 
# MAGIC         COMMENT 'BUSINESS KEY/ Invoice Number'
# MAGIC     ,`Date`	TIMESTAMP	              
# MAGIC         COMMENT 'Date of invoice'
# MAGIC     ,Customer_Name	STRING	    
# MAGIC         COMMENT 'Name of the End User'    
# MAGIC     ,Date_Created	TIMESTAMP	  NOT NULL     
# MAGIC         COMMENT 'Date the sales order was created'
# MAGIC     ,Deal_Currency	STRING	    
# MAGIC         COMMENT 'Local Currency of the order'
# MAGIC     ,GP_USD	STRING	            
# MAGIC         COMMENT 'GP in USD'
# MAGIC     ,Last_Modified	TIMESTAMP	 NOT NULL    
# MAGIC         COMMENT 'Date the sales order was last Modified'
# MAGIC     ,Start_Date	TIMESTAMP	  
# MAGIC         COMMENT 'TODO'
# MAGIC     ,End_Date	TIMESTAMP	  
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Line_ID	STRING	          
# MAGIC         COMMENT 'BUSINESS KEY/ Column to be hashed with Invoice number to make unique PK'
# MAGIC     ,Opportunity_Name	STRING	  
# MAGIC         COMMENT 'Name of opportunity'
# MAGIC     ,PO_Number	STRING	        
# MAGIC         COMMENT 'Purchase Order Number'
# MAGIC     ,Quantity	STRING	          
# MAGIC         COMMENT 'Quantiy sold'
# MAGIC     ,Reseller_Name	STRING	    
# MAGIC         COMMENT 'Name of the Reseller'
# MAGIC     ,Revenue_USD	STRING	      
# MAGIC         COMMENT 'Revenue in USD'
# MAGIC     ,Sales_Order_Date	TIMESTAMP	  
# MAGIC         COMMENT 'Date the sales order was placed'
# MAGIC     ,Sales_Order_Number	STRING	
# MAGIC         COMMENT 'Sales Order Number'
# MAGIC     ,SKU_ID	STRING	            
# MAGIC         COMMENT 'SKU Name'
# MAGIC     ,Status	STRING	            
# MAGIC         COMMENT 'Order status'
# MAGIC     ,Type	STRING	              
# MAGIC         COMMENT 'To identiry type of business'
# MAGIC     ,Vendor_Name	STRING	      
# MAGIC         COMMENT 'Vendor Name'
# MAGIC     ,Region STRING
# MAGIC         COMMENT 'Country code'
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
# MAGIC         
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC         
# MAGIC ,CONSTRAINT invoicereportsinfinigate_pk PRIMARY KEY(Invoice_Number,Line_ID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for invoicereportsinfinigate. \n'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Invoice_Number, `Date`)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE invoicereportsinfinigate ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicereportsinfinigate ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicereportsinfinigate ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
