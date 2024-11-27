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
# MAGIC CREATE OR REPLACE TABLE account
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,AccountID	INT	NOT NULL
# MAGIC       COMMENT 'Business key'
# MAGIC     ,AccStatementDay	INT	 
# MAGIC       COMMENT'Day of statement date'
# MAGIC     ,TermID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdminPhAreaCode	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdminMName	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CycleID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CountryID	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AccCurrencyCurrencyID	STRING	
# MAGIC       COMMENT 'Currency, customer market place'
# MAGIC     ,Zip	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxRegID	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AccCreditLimit_Value	DECIMAL
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Address1	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,TaxZoneID	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdminEmail	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ExternalID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,BaseCurrencyCurrencyID	STRING	
# MAGIC       COMMENT 'what they will buy in'
# MAGIC     ,TaxRegIDStatus	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AccCreditLimit_Code	STRING	
# MAGIC       COMMENT 'charged in'
# MAGIC     ,AdminFName	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CompanyName	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,City	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdminLName	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CreditLimitSrc	INT	
# MAGIC       COMMENT 'default credit limit'
# MAGIC     ,State	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdminPhCountryCode	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AdminPhNumber	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,AStatus	INT	
# MAGIC       COMMENT 'On active or not, 0 is active, 2 is inactive'
# MAGIC     ,`Type`	INT
# MAGIC       	COMMENT '2 is customer and 3 is a reseller'
# MAGIC     ,ClassID	INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,VendorAccountID	INT	
# MAGIC       COMMENT 'Reseller'
# MAGIC     ,CreationDate	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Address2	STRING	
# MAGIC       COMMENT 'TODO'
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
# MAGIC ,CONSTRAINT account_pk PRIMARY KEY(AccountID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for account. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (AccountID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE account ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE account ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE account ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver_dev.cloudblue_pba.orddet
# MAGIC limit 10
