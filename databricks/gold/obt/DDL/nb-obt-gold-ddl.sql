-- Databricks notebook source
-- DBTITLE 1,Define managementreport at Gold
-- MAGIC %md
-- MAGIC Widgets are used to give Data Factory a way to hand over parameters. In that we we can control the environment.
-- MAGIC If there is no widget defined, Data Factory will automatically create them.
-- MAGIC For us while developing we can use the try and except trick here.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC
-- MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The target catalog depens on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

-- COMMAND ----------

USE SCHEMA obt;

-- COMMAND ----------

CREATE OR REPLACE TABLE globaltransactions
  ( 
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,TransactionDate Date NOT NULL
        Comment 'Date of the Transaction'
    ,GroupEntityCode STRING NOT NULL 
      COMMENT 'Code to map from with Entity this Transactions came from.'
    ,RevenueAmount DOUBLE NOT NULL 
      COMMENT 'Amount of Revenue.'
    ,CurrencyCode STRING NOT NULL
      COMMENT 'Code of the Currency.'
    ,SKU STRING NOT NULL
      COMMENT 'SKU of the item sold. [MASTERDATA]'
    ,Description STRING NOT NULL
      COMMENT 'Description of the item sold.'
    ,ProductType STRING 
      COMMENT 'Type of the Item.'
    ,ProductSubType STRING 
      COMMENT 'SubType of the Item.'
    ,CommitmentDuration STRING 
      COMMENT 'TODO'
    ,BillingFrequency STRING 
      COMMENT 'TODO'
    ,ConsumptionModel STRING 
      COMMENT 'TODO'
    ,VendorCode STRING 
      COMMENT 'Code of the Vendor.'
    ,VendorName STRING 
      COMMENT 'Name of the Vendor.'
    ,VendorGeography STRING 
      COMMENT 'TODO'
    ,VendorStartDate STRING 
      COMMENT 'First Date a Vendor sold one item.'
    ,ResellerCode STRING 
      COMMENT 'Code of Reseller.'
    ,ResellerName STRING 
      COMMENT 'Name of Reseller.'
    ,ResellerStartDate STRING 
      COMMENT 'TODO'
    ,ResellerGroupCode STRING 
      COMMENT 'TODO'
    ,ResellerGroupName STRING 
      COMMENT 'TODO'
    ,ResellerGeography STRING 
      COMMENT 'TODO'
    ,ResellerGroupStartDate STRING 
      COMMENT 'TODO'
    ,Sys_Gold_InsertDateTime_UTC TIMESTAMP
      COMMENT 'The timestamp when this entry landed in gold.'
    ,Sys_Gold_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modifed in gold.'
    ,Sys_Silver_HashKey BIGINT NOT NULL
      COMMENT 'HashKey over all but Sys columns.'
,CONSTRAINT globaltransactions_pk PRIMARY KEY(TransactionDate,GroupEntityCode,SKU,VendorCode,ResellerCode,ResellerGroupCode)
  )
COMMENT 'This table contains the global needed reports data for the management reports as one big table (obt). \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (SKU,TransactionDate)

-- COMMAND ----------

ALTER TABLE globaltransactions ADD CONSTRAINT Sys_Gold_InsertDateTime_UTC CHECK (Sys_Gold_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE globaltransactions ADD CONSTRAINT Sys_Gold_ModifedDateTime_UTC CHECK (Sys_Gold_ModifedDateTime_UTC > '1900-01-01');
ALTER TABLE globaltransactions ADD CONSTRAINT TransactionDate CHECK (TransactionDate > '1900-01-01');
