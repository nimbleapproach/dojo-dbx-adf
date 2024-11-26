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
# MAGIC       COMMENT 'current expiration date of the subscription'
# MAGIC     ,AccountID	INT
# MAGIC       	COMMENT 'FK for accoutns table, customer that has purhcased subscription'
# MAGIC     ,startDate	INT	
# MAGIC       COMMENT 'Bkank: order is not processed'
# MAGIC     ,IsAutoRenew	INT	
# MAGIC       COMMENT 'if it wil automatically renew'
# MAGIC     ,LastBillDate	INT	
# MAGIC       COMMENT 'last time a billing order was generated'
# MAGIC     ,NextBillDate	INT	
# MAGIC       COMMENT 'next time it will be generated'
# MAGIC     ,PromoCode	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,BillingPeriod	INT	
# MAGIC       COMMENT 'how long it can be billed for'
# MAGIC     ,BillingPeriodType	INT	
# MAGIC       COMMENT '2 is month 3 is years 4 is tri'
# MAGIC     ,LastSyncDate	INT	
# MAGIC       COMMENT 'last time subscription was synced'
# MAGIC     ,NumberOfPeriods	DECIMAL	
# MAGIC       COMMENT 'number of times it will recurr'
# MAGIC     ,RenewPointType	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RefundPeriod	INT	
# MAGIC       COMMENT 'how long it can be refunded for'
# MAGIC     ,CurrencyID	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PlanID	INT	
# MAGIC       COMMENT 'FK for plan table, plan purchased'
# MAGIC     ,BaseDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,tariffID	INT	
# MAGIC       COMMENT 'current tarrif'
# MAGIC     ,ShutdownDate	INT	
# MAGIC       COMMENT 'when it was shutdown'
# MAGIC     ,ShutdownAt	INT	
# MAGIC       COMMENT 'when it was shutdown'
# MAGIC     ,ServTermID	INT	
# MAGIC       COMMENT 'FK for subscription '
# MAGIC     ,CustomStatementDay	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Status	INT	
# MAGIC       COMMENT 'subscription can be active but not running'
# MAGIC     ,RenewOrderInterval	INT	
# MAGIC       COMMENT 'days before renewal'
# MAGIC     ,SubscriptionName	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,serviceTemplateID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Trial	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Period	INT	
# MAGIC       COMMENT 'subscription period. 1 of whatever period'
# MAGIC     ,PeriodType	INT	
# MAGIC       COMMENT '2 is month 3 is years 1 is days'
# MAGIC     ,RecurringType	INT	
# MAGIC       COMMENT 'how often it will recurr'
# MAGIC     ,DateArc	INT	
# MAGIC       COMMENT 'Last Modified'
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
