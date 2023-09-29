# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA cloudblue_pba;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR ALTER TABLE orddet
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,DetID	INT	
# MAGIC       COMMENT 'Business key'
# MAGIC     ,DetTypeID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UnitPrice_Code	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UnitPrice_Value	DECIMAL	
# MAGIC       COMMENT 'price per unit'
# MAGIC     ,ServQty	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ServUnitMeasure	STRING	
# MAGIC       COMMENT 'unit can be license, but it can also be azure usage'
# MAGIC     ,DurBillPeriod	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DurBillPeriodType	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC       ,Vendor_AccountID	INT	
# MAGIC       COMMENT 'not customer but the reseller'
# MAGIC     ,OrderID	INT	
# MAGIC       COMMENT 'FK to salesorder'
# MAGIC     ,PromoID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PromotedAmt_Code	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Original_DetID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxPercent	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DiscountAmt_Value	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,InterReselTranID	INT	
# MAGIC       COMMENT 'FK to resellertrans'
# MAGIC     ,ExtendedPrice_Code	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DetDatesAreFinal	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,planCategoryID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxAmt_Value	DECIMAL	
# MAGIC       COMMENT 'amount of tax for that line item'
# MAGIC     ,TaxAmt_Code	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DiscountAmt_Code	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PromotedAmt_Value	DECIMAL
# MAGIC       	COMMENT 'TODO'
# MAGIC     ,resourceID	INT	
# MAGIC       COMMENT 'FK'
# MAGIC     ,Descr	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,OIID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,subscriptionID	INT	
# MAGIC       COMMENT 'FK to subscription'
# MAGIC     ,DiscountPercent	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExtendedPrice_Value	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DetSDate	INT
# MAGIC       	COMMENT 'start'
# MAGIC     ,DetEDate	INT	
# MAGIC       COMMENT 'end date'
# MAGIC     ,PlanPeriodID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DateArc	INT	
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
# MAGIC ,CONSTRAINT orddet_pk PRIMARY KEY(DetID, OrderID,DateArc )
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for orddet. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (DetID, OrderID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE orddet ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE orddet ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE orddet ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
