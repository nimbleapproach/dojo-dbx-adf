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
# MAGIC CREATE OR REPLACE TABLE docdet
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,DetID BIGINT NOT NULL
# MAGIC       COMMENT 'Business Key'
# MAGIC     ,DocID BIGINT
# MAGIC       COMMENT 'FK'
# MAGIC     ,DetType INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UnitPrice_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UnitPrice_Value FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExtendedPrice_Code	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExtendedPrice_Value FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ServQty	FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ServUnitMeasure STRING
# MAGIC       COMMENT 'unit can be license, but it can also be azure usage'
# MAGIC     ,Duration FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DurBillPeriod	INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DurBillPeriodType INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PricePeriodType INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PlanPeriodID INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PromoID INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DiscID INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,resourceID INT
# MAGIC       COMMENT 'FK'
# MAGIC     ,DiscountAmt_Code	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DiscountAmt_Value FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxCatID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Vendor_AccountID INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxAmt_Code STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxAmt_Value FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxInclAmt_Code STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxInclAmt_Value FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ResCatID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,planCategoryID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,OIID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DDOrdDetID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DDOrderID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,subscriptionID STRING
# MAGIC       COMMENT 'FK to subscription'
# MAGIC     ,Descr	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DetSDate	INT
# MAGIC       COMMENT 'start'
# MAGIC     ,DetEDate	INT	
# MAGIC       COMMENT 'end date'
# MAGIC     ,DetBaseDate	INT
# MAGIC       COMMENT 'base start'
# MAGIC     ,DetDatesAreFinal INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Acct STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxAlg INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKU STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,IsTaxCalcError INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxCalcError STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DisplaySDate INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DisplayEDate INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,UserArc INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DateArc	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PromotedAmt_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PromotedAmt_Value FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxEDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxPercent	FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Corrected_DetID INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExchangeTime	INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExchangeRate	FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesUnitPrice_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesUnitPrice_Value	FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesExtendedPrice_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesExtendedPrice_Value	FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesDiscountAmt_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesDiscountAmt_Value	FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesPromotedAmt_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesPromotedAmt_Value	FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesTaxAmt_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesTaxAmt_Value FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesTaxInclAmt_Code STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SalesTaxInclAmt_Value FLOAT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxChargeDate INT
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
# MAGIC ,CONSTRAINT docdet_pk PRIMARY KEY(DetID, DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (DetID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE docdet ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE docdet ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE docdet ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
