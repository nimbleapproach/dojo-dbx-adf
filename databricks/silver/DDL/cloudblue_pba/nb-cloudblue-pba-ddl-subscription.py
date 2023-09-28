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
# MAGIC CREATE OR REPLACE TABLE subscription
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,subscriptionID	INT	
# MAGIC       COMMENT 'Business key'
# MAGIC     ,ExpirationDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AccountID	INT
# MAGIC       	COMMENT 'TODO'
# MAGIC     ,startDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ShutdownDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,IsAutoRenew	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,NextBillDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PromoCode	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,BillingPeriod	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,BillingPeriodType	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LastSyncDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,NumberOfPeriods	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RenewPointType	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RefundPeriod	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CurrencyID	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PlanID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,BaseDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LastBillDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,tariffID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ShutdownAt	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Period	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ServTermID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CustomStatementDay	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Status	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RenewOrderInterval	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SubscriptionName	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,serviceTemplateID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Trial	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PeriodType	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RecurringType	INT	
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
# MAGIC ,CONSTRAINT subscription_pk PRIMARY KEY(subscriptionID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for subscription. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (subscriptionID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE subscription ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE subscription ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE subscription ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
