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
# MAGIC
# MAGIC CREATE OR REPLACE TABLE resellertrans
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,InterReselTranID	INT	
# MAGIC         COMMENT 'PK'
# MAGIC     ,ReselOrdDetID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DetTypeID	INT	
# MAGIC         COMMENT 'billing order, change etc'
# MAGIC     ,PromoID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,TaxAmt_Code	STRING	
# MAGIC         COMMENT 'tax currency'
# MAGIC     ,Settl_DocID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DurBillPeriod	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,UnitPrice_Value	DECIMAL	
# MAGIC         COMMENT 'amount line item is worth'
# MAGIC     ,SKU	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,TaxAmt_Value	DECIMAL	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Status	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Resel_AccountID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ServQty	DECIMAL	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,OIID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ServUnitMeasure	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,subscriptionID	INT	
# MAGIC         COMMENT 'FK'
# MAGIC     ,UnitPrice_Code	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,TaxPercent	DECIMAL	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DetEDate	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DiscountPercent	DECIMAL	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Duration	DECIMAL	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,resourceID	INT	
# MAGIC         COMMENT 'FK'
# MAGIC     ,Descr	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DiscID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ExtendedPrice_Code	STRING	
# MAGIC         COMMENT 'currency charged in'
# MAGIC     ,ExtendedPrice_Value	DECIMAL	
# MAGIC         COMMENT 'amount being charged'
# MAGIC     ,EndCustDocID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DiscountAmt_Code	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Cust_AccountID	INT
# MAGIC         COMMENT 'Customer'
# MAGIC     ,DurBillPeriodType	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DiscountAmt_Value	DECIMAL	
# MAGIC         COMMENT 'extended + discount = unit price value'
# MAGIC     ,DetSDate	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,PlanPeriodID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DateArc	INT	
# MAGIC         COMMENT 'TODO'
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
# MAGIC ,CONSTRAINT resellertrans_pk PRIMARY KEY(InterReselTranID, DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for resellertrans. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (InterReselTranID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE resellertrans ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE resellertrans ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE resellertrans ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
