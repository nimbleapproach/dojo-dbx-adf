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

CREATE OR REPLACE TABLE globaltransactions_logging
  ( 
    GroupEntityCode STRING NOT NULL 
      COMMENT 'Code to map from with Entity this Transactions came from.'
    ,EntityCode STRING
      COMMENT 'the lower level Entity code for the IG group company - from source ERP but should align to Azienda in Tagetik?'
    ,TransactionDate DATE NOT NULL
        Comment 'Date of the Transaction'
    ,RevenueAmount DECIMAL(10,2) NOT NULL
        Comment 'Amount of Revenue.'
    , Sys_Gold_NumberOfRows BIGINT NOT NULL
    , Sys_Gold_Observation_DateTime_UTC TIMESTAMP
        Comment 'Date when an entry was logged.'
  )
COMMENT 'This table contains the global needed reports data for the management reports as one big table (obt). \n' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY (GroupEntityCode)
