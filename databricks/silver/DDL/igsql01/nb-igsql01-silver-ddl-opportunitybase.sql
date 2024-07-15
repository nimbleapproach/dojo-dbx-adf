-- Databricks notebook source
-- DBTITLE 1,Define Customer at Silver
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
-- MAGIC The target catalog depends on the enivronment. Since we are using Unity Catalog we need to use a unqiue name for the catalog. This is the reason why we name the dev silver catalog "silver_dev" for example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS igsql01

-- COMMAND ----------

USE SCHEMA igsql01;

-- COMMAND ----------

CREATE OR REPLACE TABLE opportunitybase (
    SID bigint
        GENERATED ALWAYS AS IDENTITY
        COMMENT 'Surrogate Key'
    ,OpportunityId STRING NOT NULL
      COMMENT 'Business Key'
    ,CreatedOn TIMESTAMP 
      COMMENT 'TODO'
    ,ModifiedOn TIMESTAMP 
      COMMENT 'TODO'
    ,CustomerId STRING 
      COMMENT 'ID of Customer Account'
    ,CustomerIdName STRING 
      COMMENT 'Name of Customer'
    ,CustomerIdType INT 
      COMMENT 'TODO'
    ,ParentAccountId STRING 
      COMMENT 'ID of Parent Account'
    ,New_ResellerAccountId STRING 
      COMMENT 'Name of Customer'
    ,New_BusinessCategory INT 
      COMMENT 'TODO'
    ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP NOT NULL
      COMMENT 'The timestamp when this entry landed in bronze.'
    ,Sys_DatabaseName STRING NOT NULL
      COMMENT 'Name of the Source Database.'
    ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry landed in silver.'
    ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
      DEFAULT current_timestamp()
      COMMENT 'The timestamp when this entry was last modifed in silver.'
    ,Sys_Silver_HashKey BIGINT NOT NULL
      COMMENT 'HashKey over all but Sys columns.'
    ,Sys_Silver_IsCurrent BOOLEAN
      COMMENT 'Flag if this is the current version.'
  ,CONSTRAINT OpportunityId_pk PRIMARY KEY(OpportunityId, Sys_DatabaseName, Sys_Bronze_InsertDateTime_UTC)
)
COMMENT 'This table contains the data for crm opportunities.' 
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CLUSTER BY   (OpportunityId, Sys_DatabaseName)

-- COMMAND ----------

ALTER TABLE opportunitybase ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE opportunitybase ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
ALTER TABLE opportunitybase ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
