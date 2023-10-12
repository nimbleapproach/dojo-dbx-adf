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
    ,RevenueAmount DECIMAL NOT NULL 
      COMMENT 'Amount of Revenue.'
    ,CurrencyCode STRING NOT NULL
      COMMENT 'Code of the Currency.'
    ,SKU STRING NOT NULL
      COMMENT 'SKU of the item sold. [MASTERDATA]'
    ,Description STRING 
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
    ,VendorStartDate Date 
      COMMENT 'First Date a Vendor sold one item.'
    ,ResellerCode STRING 
      COMMENT 'Code of Reseller.'
    ,ResellerName STRING 
      COMMENT 'Name of Reseller.'
    ,ResellerStartDate Date 
      COMMENT 'TODO'
    ,ResellerGroupCode STRING 
      COMMENT 'TODO'
    ,ResellerGroupName STRING 
      COMMENT 'TODO'
    ,ResellerGeography STRING 
      COMMENT 'TODO'
    ,ResellerGroupStartDate Date 
      COMMENT 'TODO'
,CONSTRAINT globaltransactions_pk PRIMARY KEY(TransactionDate,GroupEntityCode,SKU)
  )
COMMENT 'This table contains the global needed reports data for the management reports as one big table (obt).' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (SKU,GroupEntityCode) 
