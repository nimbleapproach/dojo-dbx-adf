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
    ,GroupEntityCode STRING NOT NULL 
      COMMENT 'Code to map from with Entity this Transactions came from.'
    ,EntityCode STRING
      COMMENT 'Reseller Entity code'
    ,TransactionDate DATE NOT NULL
        Comment 'Date of the Transaction'
    ,SalesOrderID STRING
        Comment 'Business Key'
    ,SalesOrderDate DATE
      Comment 'TODO'
    ,SalesOrderItemID STRING
      Comment 'Line item number'
    ,RevenueAmount DECIMAL(10,2)
      COMMENT 'Amount of Revenue.'
    ,CurrencyCode STRING
      COMMENT 'Code of the Currency.'
    ,SKU STRING
      COMMENT 'SKU of the item sold. [MASTERDATA]'
    ,Description STRING 
      COMMENT 'Description of the item sold.'
    ,ProductType_Internal STRING 
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
    ,VendorStartDate DATE 
      COMMENT 'First Date a Vendor sold one item.'
    ,ResellerCode STRING 
      COMMENT 'Code of Reseller.'
    ,ResellerName STRING 
      COMMENT 'Name of Reseller.'
    ,ResellerStartDate DATE 
      COMMENT 'TODO'
    ,ResellerGroupCode STRING 
      COMMENT 'TODO'
    ,ResellerGroupName STRING 
      COMMENT 'TODO'
    ,ResellerGeography STRING 
      COMMENT 'TODO'
    ,ResellerGroupStartDate DATE 
      COMMENT 'TODO'
  )
COMMENT 'This table contains the global needed reports data for the management reports as one big table (obt). \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (GroupEntityCode) 
